/**
 * 驱动 SessionBridge 的真实消息处理链路，验证企微回复时序：
 *   1. 收到消息**立刻**发出流式占位帧（5 秒窗口要求）
 *   2. stream 模式逐段刷新，turn/end 时 finish 全量内容
 *   3. markdown 模式过程不刷新，turn/end 时一次性 finish
 *   4. 错误 / 超时分支也必须用 finish 收尾，不能改用一次性 markdown
 */
import assert from 'node:assert/strict'
import { SessionBridge } from '../lib/bridge.js'
import { pickFinal } from '../lib/ws.js'

function makeCtx({ onTurn }) {
  const listeners = []
  return {
    listeners,
    logger: () => ({ info() {}, warn() {}, error() {}, debug() {} }),
    agents: {
      get: () => undefined,
      create: async (opts) => ({
        agent: {
          session: { id: opts.sessionId },
          followup: () => { setTimeout(() => onTurn(opts.sessionId, listeners), 0) },
        },
        dispose: async () => {},
      }),
    },
    agentPresets: { resolve: async (id) => ({ id }), mount: async () => {} },
    agentDefaultModel: { currentSelection: () => undefined },
    sandboxPolicy: { resolve: () => ({ workspaceRoot: process.cwd() }) },
    get: () => undefined,
    on: (_event, cb) => { listeners.push(cb); return () => {} },
  }
}

/** 假 WsClient：只记录发出的帧。 */
function makeWs() {
  const frames = []
  return {
    frames,
    createStreamResponder(reqId, placeholder = '思考中…') {
      let buf = ''
      const push = (content, finish) => { frames.push({ reqId, content, finish }); return true }
      push(placeholder, false)
      return {
        append: (delta) => { buf += delta },
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

/** 模拟一次完整回合：turn/start → chunk → assistant/message → turn/end */
function emitTurn(sessionId, listeners, { error } = {}) {
  const emit = (event) => { for (const cb of listeners) cb({ id: sessionId }, event) }
  emit({ type: 'turn/start', data: { turn: 1 } })
  emit({ type: 'assistant/chunk', data: { turn: 1, chunk: { type: 'text-delta', text: '你好' } } })
  emit({ type: 'assistant/chunk', data: { turn: 1, chunk: { type: 'text-delta', text: '，世界' } } })
  emit({ type: 'assistant/message', data: { turn: 1, message: { content: [{ type: 'text', text: '你好，世界' }] } } })
  emit({ type: 'turn/end', data: { turn: 1, reason: error ? { kind: 'error', error: { message: 'llm down' } } : { kind: 'stop' } } })
}

async function run(replyMode, onTurn) {
  const ctx = makeCtx({ onTurn })
  const ws = makeWs()
  const bridge = new SessionBridge(ctx, { preset: 'standard', replyMode })
  bridge.handle(packet(`m-${replyMode}`, '您好'), ws)
  // 等队列跑完
  for (let i = 0; i < 50 && !ws.frames.some((f) => f.finish); i++) await new Promise((r) => setTimeout(r, 10))
  return { ws, bridge, ctx }
}

// 1. stream 模式：占位帧先到，最终 finish 带全量
{
  const { ws, bridge } = await run('stream', (sid, listeners) => emitTurn(sid, listeners))
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
  const { ws, bridge } = await run('markdown', (sid, listeners) => emitTurn(sid, listeners))
  await bridge.dispose()
  assert.equal(ws.frames.length, 2)
  assert.equal(ws.frames[0].content, '思考中…')
  assert.equal(ws.frames[1].content, '你好，世界')
  assert.equal(ws.frames[1].finish, true)
}

// 3. 回合失败：也用 finish 收尾，带错误文案
{
  const { ws, bridge } = await run('stream', (sid, listeners) => emitTurn(sid, listeners, { error: true }))
  await bridge.dispose()
  assert.equal(ws.frames.length, 2)
  assert.match(ws.frames[1].content, /llm down/)
  assert.equal(ws.frames[1].finish, true)
}

console.log('check-reply: OK')
