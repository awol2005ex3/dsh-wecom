/**
 * 驱动 SessionBridge 的真实消息处理链路，验证企微回复时序。
 *
 * 关键：实时流式走 `agent/assistant-stream`（reasoning-delta / text-delta），
 * `session/event` 总线【不】携带增量块（只用于干净终稿 assistant/message 与回合错误 turn/end）。
 * 本测试据此构造两套监听，模拟 harness 的真实事件来源。
 *
 *   1. 收到消息**立刻**发出流式占位帧（5 秒窗口要求）
 *   2. stream 模式逐段刷新（推理+答案都实时），end 帧时 finish 全量内容
 *   3. markdown 模式过程不刷新，end 帧时一次性 finish
 *   4. 错误 / 超时分支也必须用 finish 收尾，不能改用一次性 markdown
 *   5. 推理模型：reasoning-delta 实时可见，最终消息是干净答案（不混入思考文本）
 */
import assert from 'node:assert/strict'
import { SessionBridge } from '../lib/bridge.js'
import { pickFinal } from '../lib/ws.js'

function makeCtx({ onTurn }) {
  const streamListeners = []
  const sessionListeners = []
  return {
    logger: () => ({ info() {}, warn() {}, error() {}, debug() {} }),
    agents: {
      get: () => undefined,
      create: async (opts) => {
        // agent 的 carrier ctx 上的 session/event 监听
        const agentCtx = { on: (_event, cb) => { if (_event === 'session/event') sessionListeners.push(cb); return () => {} } }
        const agent = {
          session: { id: opts.sessionId },
          ctx: agentCtx,
          // 把真实 agent 对象一并交给 onTurn，模拟 harness 用同一引用发 agent/assistant-stream
          followup: () => { setTimeout(() => onTurn(opts.sessionId, { streamListeners, sessionListeners, agent }), 0) },
        }
        return { agent, dispose: async () => {} }
      },
    },
    agentPresets: { resolve: async (id) => ({ id }), mount: async () => {} },
    agentDefaultModel: { currentSelection: () => undefined },
    sandboxPolicy: { resolve: () => ({ workspaceRoot: process.cwd() }) },
    get: () => undefined,
    // 关键：agent/assistant-stream 挂在 wecom 插件 ctx 上（全局总线）
    on: (_event, cb) => { if (_event === 'agent/assistant-stream') streamListeners.push(cb); return () => {} },
  }
}

/** 假 WsClient：记录发出的帧，以及每次 append 的增量（用于断言流式确实发生了）。 */
function makeWs() {
  const frames = []
  const deltas = []
  return {
    frames,
    deltas,
    createStreamResponder(reqId, placeholder = '思考中…') {
      let buf = ''
      const push = (content, finish) => { frames.push({ reqId, content, finish }); return true }
      push(placeholder, false)
      return {
        append: (delta) => { buf += delta; deltas.push(delta) },
        reset: (content) => { buf = content; if (content) deltas.push(content) },
        keepAlive: () => { push(buf || placeholder, false) },
        finish: (finalText) => push(pickFinal(buf, finalText), true),
        get pushed() { return true },
      }
    },
    respondMarkdown(reqId, content) { frames.push({ reqId, content, markdown: true, finish: true }) },
    respondWelcome() {},
    createStreamResponderCalls: 0,
  }
}

function packet(msgid, text) {
  return {
    cmd: 'aibot_msg_callback',
    headers: { req_id: `req-${msgid}` },
    body: {
      msgid, chattype: 'single', from: { userid: 'user-1' },
      msgtype: 'text', text: { content: text },
    },
  }
}

/** 向 agent/assistant-stream 监听推一帧。 */
function stream(listeners, agent, frame) {
  for (const cb of listeners) cb({ agent, frame })
}
/** 向 session/event 监听推一个事件。 */
function session(sessionListeners, sessionId, event) {
  for (const cb of sessionListeners) cb({ id: sessionId }, event)
}

/** 模拟一次完整回合：start → chunk(s) → assistant/message → end。 */
function emitTurn(sessionId, { streamListeners, sessionListeners, agent }, { error } = {}) {
  stream(streamListeners, agent, { type: 'start', turn: 1, attemptId: 'a1' })
  stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'text-delta', text: '你好' } })
  stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'text-delta', text: '，世界' } })
  if (!error) {
    session(sessionListeners, sessionId, { type: 'assistant/message', data: { turn: 1, message: { content: [{ type: 'text', text: '你好，世界' }] } } })
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'committed', eventType: 'assistant/message', seq: 1 } })
  } else if (error === 'abandoned') {
    // 罕见路径：持久化 append 失败 → harness 发 abandoned 的 end 帧，无用户消息
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'abandoned' } })
    session(sessionListeners, sessionId, { type: 'turn/end', data: { turn: 1, reason: { kind: 'error', error: { message: 'llm down' } } } })
  } else {
    // 真实失败路径：harness 先 settle('assistant/attempt') → 发 committed 的 end 帧
    // （eventType=assistant/attempt，表示未产出用户消息），再 throw → turn/end 带错误原因。
    // 不能误把增量 liveBuf 当最终答案，必须等 turn/end 错误分支给出文案。
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'committed', eventType: 'assistant/attempt', seq: 1 } })
    session(sessionListeners, sessionId, { type: 'turn/end', data: { turn: 1, reason: { kind: 'error', error: { message: 'llm down' } } } })
  }
}

async function run(replyMode, onTurn) {
  const ctx = makeCtx({ onTurn })
  const ws = makeWs()
  const bridge = new SessionBridge(ctx, { preset: 'standard', replyMode })
  bridge.handle(packet(`m-${replyMode}`, '您好'), ws)
  for (let i = 0; i < 50 && !ws.frames.some((f) => f.finish); i++) await new Promise((r) => setTimeout(r, 10))
  return { ws, bridge }
}

// 1. stream 模式：占位帧先到，最终 finish 带全量
{
  const { ws, bridge } = await run('stream', (sid, ls) => emitTurn(sid, ls))
  await bridge.dispose()
  assert.equal(ws.frames.length, 2, `应只有占位帧 + finish 帧：${JSON.stringify(ws.frames)}`)
  assert.equal(ws.frames[0].content, '思考中…')
  assert.equal(ws.frames[0].finish, false, '首帧必须是未结束的占位')
  assert.equal(ws.frames[1].content, '你好，世界')
  assert.equal(ws.frames[1].finish, true)
  assert.equal(ws.frames[1].markdown, undefined, '收尾必须走 stream，不能退回一次性 markdown')
}

// 2. markdown 模式：同样先占位，结束时一次性带全文
{
  const { ws, bridge } = await run('markdown', (sid, ls) => emitTurn(sid, ls))
  await bridge.dispose()
  assert.equal(ws.frames.length, 2)
  assert.equal(ws.frames[0].content, '思考中…')
  assert.equal(ws.frames[1].content, '你好，世界')
  assert.equal(ws.frames[1].finish, true)
}

// 3. 回合失败（真实路径：committed + assistant/attempt）：也用 finish 收尾，带错误文案，不误用增量内容
{
  const { ws, bridge } = await run('stream', (sid, ls) => emitTurn(sid, ls, { error: 'committed' }))
  await bridge.dispose()
  assert.equal(ws.frames.length, 2)
  assert.match(ws.frames[1].content, /llm down/)
  assert.equal(ws.frames[1].finish, true)
}

// 4. 推理模型：reasoning-delta 实时流式，最终消息是干净答案（不混入思考文本）
{
  const ctx = makeCtx({ onTurn: (sid, { streamListeners, sessionListeners, agent }) => {
    stream(streamListeners, agent, { type: 'start', turn: 1 })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'reasoning-delta', text: '让我想想' } })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'reasoning-delta', text: '，再算算' } })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'text-delta', text: '答案是42' } })
    session(sessionListeners, sid, { type: 'assistant/message', data: { turn: 1, message: { content: [{ type: 'text', text: '答案是42' }] } } })
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'committed', eventType: 'assistant/message', seq: 1 } })
  } })
  const ws = makeWs()
  const bridge = new SessionBridge(ctx, { preset: 'standard', replyMode: 'stream' })
  bridge.handle(packet('m-reason', '深度思考一下'), ws)
  for (let i = 0; i < 50 && !ws.frames.some((f) => f.finish); i++) await new Promise((r) => setTimeout(r, 10))
  await bridge.dispose()
  assert.ok(ws.deltas.length >= 3, `推理+回答都应流式：deltas=${JSON.stringify(ws.deltas)}`)
  assert.equal(ws.frames[ws.frames.length - 1].content, '答案是42', '最终消息应是干净答案（不混入思考文本）')
  assert.equal(ws.frames[ws.frames.length - 1].finish, true)
}

// 5. 罕见路径：持久化 append 失败 → abandoned 的 end 帧，仍交由 turn/end 错误分支带文案收尾
{
  const { ws, bridge } = await run('stream', (sid, ls) => emitTurn(sid, ls, { error: 'abandoned' }))
  await bridge.dispose()
  assert.equal(ws.frames.length, 2)
  assert.match(ws.frames[1].content, /llm down/)
  assert.equal(ws.frames[1].finish, true)
}

// 6. 带工具调用的回合（复现「思考可见、工具调用后答案不出」）：
//    思考实时可见 → 工具调用有「正在调用」标记 → 工具之后的答案仍能完整流式到达
{
  const ctx = makeCtx({ onTurn: (sid, { streamListeners, sessionListeners, agent }) => {
    stream(streamListeners, agent, { type: 'start', turn: 1 })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'reasoning-delta', text: '我需要查一下' } })
    // 第一段 attempt：仅产出工具调用（committed + assistant/attempt，无用户消息）
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'tool-call-delta', id: 'c1', name: 'web_search', argumentsDelta: '{}' } })
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'committed', eventType: 'assistant/attempt', seq: 1 } })
    // 工具返回后第二段：继续思考 + 最终答案
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'reasoning-delta', text: '查到了' } })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'text-delta', text: '前5名是' } })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'text-delta', text: '上海北京江苏浙江福建' } })
    session(sessionListeners, sid, { type: 'assistant/message', data: { turn: 1, message: { content: [{ type: 'text', text: '前5名是上海北京江苏浙江福建' }] } } })
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'committed', eventType: 'assistant/message', seq: 2 } })
  } })
  const ws = makeWs()
  const bridge = new SessionBridge(ctx, { preset: 'standard', replyMode: 'stream' })
  bridge.handle(packet('m-tool', '查人均GDP前五'), ws)
  for (let i = 0; i < 50 && !ws.frames.some((f) => f.finish); i++) await new Promise((r) => setTimeout(r, 10))
  await bridge.dispose()
  assert.ok(ws.deltas.some((d) => d.includes('正在调用工具：web_search')), `工具调用应有可见标记：deltas=${JSON.stringify(ws.deltas)}`)
  assert.equal(ws.frames[ws.frames.length - 1].content, '前5名是上海北京江苏浙江福建', '工具之后的答案应完整流式到达')
  assert.equal(ws.frames[ws.frames.length - 1].finish, true)
}

// 7. 工具调用后模型未产出文本答案（复现「卡在工具标记上」）：
//    思考+工具标记都显示了，但工具失败/模型中断导致没有任何 text-delta 与 cleanText。
//    收尾【绝不能】把「正在调用工具」标记当终稿回显，必须给一句明确提示。
{
  const ctx = makeCtx({ onTurn: (sid, { streamListeners, sessionListeners, agent }) => {
    stream(streamListeners, agent, { type: 'start', turn: 1 })
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'reasoning-delta', text: '我需要查一下' } })
    // 仅产出工具调用，随后 end 帧为 committed/assistant/message 但 message 无文本（工具失败、模型中断）
    stream(streamListeners, agent, { type: 'chunk', turn: 1, chunk: { type: 'tool-call-delta', id: 'c1', name: 'web_search', argumentsDelta: '{}' } })
    stream(streamListeners, agent, { type: 'end', turn: 1, outcome: { kind: 'committed', eventType: 'assistant/message', seq: 1 } })
  } })
  const ws = makeWs()
  const bridge = new SessionBridge(ctx, { preset: 'standard', replyMode: 'stream' })
  bridge.handle(packet('m-tool-fail', '查人均用电量前五'), ws)
  for (let i = 0; i < 50 && !ws.frames.some((f) => f.finish); i++) await new Promise((r) => setTimeout(r, 10))
  await bridge.dispose()
  const last = ws.frames[ws.frames.length - 1]
  assert.equal(last.finish, true)
  assert.ok(!last.content.includes('正在调用工具'), `收尾绝不能回显工具标记：${last.content}`)
  assert.match(last.content, /未返回文本结果|未生成回复/, `应给明确提示而非冻结：${last.content}`)
}

console.log('check-reply: OK')
