// @dsh-version 0.1.2-rc.1
import type { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { Agent, AgentHandle } from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-agent-presets'

import type {} from '@deepseek-ai/dsh-session'
import type {} from '@deepseek-ai/dsh-sandbox-policy'
import { createUserMessage, errorChain } from '@deepseek-ai/dsh-llm'
import type { ContentBlock } from '@deepseek-ai/dsh-llm'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { WsClient, WecomCallbackPacket } from './ws.js'
import { LRUCache } from './lru.js'
import { SessionQueue } from './queue.js'
import { MediaHandler } from './media.js'
import { debugLog } from './debuglog.js'

/**
 * 单个 agent 回合等待的兜底超时。
 * 企微从流式首帧起 10 分钟后强制结束消息，这里取一半作为安全余量。
 */
const TURN_TIMEOUT_MS = 5 * 60_000

/** 流式长空窗保活间隔（思考初期 / 工具调用执行中等无 chunk 的间隙）。 */
const KEEPALIVE_MS = 4_000

/** `WsClient.createStreamResponder()` 的返回类型。 */
export type StreamResponder = ReturnType<WsClient['createStreamResponder']>

/**
 * 一次企微消息对应的 agent 回合转发器。
 *
 * 关键：session/event 只广播给「attach 该 session 的 carrier 作用域」（agent 的 agentCtx），
 * 注册在 wecom 插件 ctx 上永远收不到。监听挂在 agentCtx 上，事件通过本结构体转发到企微 responder。
 */
interface ActiveTurn {
  responder: StreamResponder
  streamMode: boolean
  /** 该回合归属的 agent（用于按 agent 过滤 `agent/assistant-stream`）。 */
  agent: Agent
  nText: number
  nReason: number
  /** 干净终稿（来自 `assistant/message`），收尾时优先于已流式的 buf。 */
  cleanText: string
  /** 是否已进入 text-delta（答案）阶段——进入后丢弃思考文本。 */
  answering: boolean
  /** 本轮已展示过「正在调用工具」标记的工具名（避免重复刷）。 */
  toolCallsShown: Set<string>
  settled: boolean
  resolve: () => void
  timer: ReturnType<typeof setTimeout>
  heartbeat: ReturnType<typeof setInterval>
}

interface WecomAgentEntry {
  handle: AgentHandle
  /** 是否由本 bridge 创建。借用的（进程内已存在）agent 不能被本 bridge 释放。 */
  owned: boolean
}

/** 判断是否为 session id 冲突（`sessions.prepare/enter` 抛出的唯一口径）。 */
function isAlreadyExists(err: unknown): boolean {
  if (!(err instanceof Error)) return false
  const message = err.message.toLowerCase()
  return message.includes('already exists') || message.includes('already attached')
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

/** 提取 assistant 消息中的全部文本块。 */
function extractText(content: readonly ContentBlock[]): string {
  return content
    .filter((block): block is { type: 'text'; text: string } => block.type === 'text')
    .map((block) => block.text)
    .join('')
}

export class SessionBridge {
  private seen = new LRUCache<string, true>(2000, 5 * 60_000)  // 2000条/5分钟
  private queue = new SessionQueue()
  private media: MediaHandler
  private agents = new Map<string, WecomAgentEntry>()
  /** sessionId -> 当前活跃回合转发器（同一会话串行处理，故同一时刻至多一个）。 */
  private activeTurns = new Map<string, ActiveTurn>()
  /** 已注册 session/event 监听的 agent ctx（按对象去重，避免重复注册）。 */
  private sessionListeners = new WeakSet<object>()
  /** 已注册 agent/assistant-stream 监听的 agent（按对象去重）。 */
  private agentStreams = new WeakSet<object>()

  constructor(private ctx: Context, private cfg: any) {
    this.media = new MediaHandler({
      sandboxRoot: this.workspaceRoot(),
    })
  }

  private logger() {
    return this.ctx.logger('wecom')
  }

  /** 沙箱根目录（sandboxPolicy 的 workspaceRoot），拿不到时退回 cwd。 */
  private workspaceRoot(): string {
    try {
      return (this.ctx as any).sandboxPolicy.resolve({}).workspaceRoot as string
    } catch {
      return process.cwd()
    }
  }

  // ── 消息入口：同步只做幂等+入队，保证 3s ACK ──────────
  handle(pkt: WecomCallbackPacket, ws: WsClient) {
    const msgId = pkt.body.msgid
    if (!msgId || this.seen.has(msgId)) return
    this.seen.set(msgId, true)

    const sessionId = this.sessionKey(pkt.body)
    this.queue.enqueue(sessionId, () => this.process(pkt, ws, sessionId))
  }

  // ── 事件入口：欢迎语 / 反馈，不走消息队列 ─────────
  handleEvent(pkt: WecomCallbackPacket, ws: WsClient) {
    const reqId = pkt.headers.req_id
    const evt = pkt.body?.event?.eventtype   // 官方格式：body.event.eventtype（如 enter_chat）

    switch (evt) {
      case 'enter_chat': {
        const welcome = this.cfg.welcomeText
          ?? '你好！我是智能助手，有什么可以帮你？'
        ws.respondWelcome(reqId, welcome)
        break
      }
      case 'feedback_event': {
        // 接审计日志，不影响用户体验
        this.logger().info('feedback event: %o', pkt.body)
        break
      }
      default:
        this.logger().info('unhandled event: %s', evt)
    }
  }

  private sessionKey(body: any) {
    return body.chattype === 'group'
      ? `wecom:group:${body.chatid}`
      : `wecom:single:${body.from.userid}`
  }

  // ── agent 生命周期 ──────────────────────────────────────
  /**
   * 取（或建）该企微会话对应的 agent。
   *
   * session store 的 id 是进程内唯一的：`sessions.prepare()` 撞上同名会话会直接抛
   * `session "<id>" already exists`。本 bridge 的 `agents` map 被清空而旧 agent 仍存活时
   * （配置热更新 / 插件重载），盲目 create 必然失败，所以先借后建。
   */
  private async ensureAgent(sessionId: string): Promise<AgentHandle> {
    const cached = this.agents.get(sessionId)
    if (cached) return cached.handle

    const ctx = this.ctx as any
    const sid = brandString<SessionId>(sessionId)

    // 1. 进程内已有存活 agent → 借用（handle 归创建者所有，这里只用于 followup）
    const live = this.liveAgent(sid)
    if (live) return this.adopt(sessionId, live, 'live agent')

    const preset = await ctx.agentPresets.resolve(this.cfg.preset)
    const selection = ctx.agentDefaultModel?.currentSelection?.()
    debugLog(`[model] provider=${selection?.provider ?? '?'} model=${selection?.model ?? '?'}`)
    const create = (id: SessionId): Promise<AgentHandle> => ctx.agents.create({
      sessionId: id,
      meta: { cwd: this.workspaceRoot(), agentPreset: preset.id },
      agentOptions: selection
        ? { provider: selection.provider, model: selection.model }
        : undefined,
      setup: async (agentCtx: Context) => {
        await ctx.agentPresets.mount(agentCtx, preset.id)
        // 关键修复：session/event 只广播给 attach 该 session 的 carrier 作用域（即 agentCtx），
        // 注册在 wecom 插件 ctx 上收不到任何 chunk / turn/end。监听必须挂在 agentCtx 上。
        this.registerSessionListener(agentCtx)
      },
    })

    // 2. 正常创建
    let conflict: unknown
    try {
      const handle = await create(sid)
      this.agents.set(sessionId, { handle, owned: true })
      return handle
    } catch (err) {
      if (!isAlreadyExists(err)) throw err
      conflict = err
    }

    // 3. 撞车自愈：先再借一次（并发创建刚落地），再同 id 重试（瞬时竞争），
    //    最后才换新 id —— 宁可丢上下文，也不能让用户收不到回复。
    const raced = this.liveAgent(sid)
    if (raced) return this.adopt(sessionId, raced, 'raced agent')

    this.logger().warn(
      'wecom: create session %s 冲突（%s）；诊断: agent=%s session=%s sessions服务=%s 存活会话=%d',
      sessionId, errorMessage(conflict),
      this.liveAgent(sid) !== undefined, this.liveSession(sid) !== undefined,
      ctx.get('sessions') !== undefined, this.liveSessionCount(),
    )

    try {
      const handle = await create(sid)
      this.agents.set(sessionId, { handle, owned: true })
      return handle
    } catch (err) {
      if (!isAlreadyExists(err)) throw err
      conflict = err
    }

    const freshId = brandString<SessionId>(`${sessionId}#${Date.now()}`)
    this.logger().warn('wecom: %s 仍冲突（%s），改用新会话 %s', sessionId, errorMessage(conflict), freshId)
    const handle = await create(freshId)
    this.agents.set(sessionId, { handle, owned: true })
    return handle
  }

  /** 进程内是否已有该 id 的存活 agent。 */
  private liveAgent(sid: SessionId): Agent | undefined {
    return (this.ctx as any).agents.get(sid) as Agent | undefined
  }

  /** 进程内是否已有该 id 的存活会话（拿不到 sessions 服务时为 undefined）。 */
  private liveSession(sid: SessionId): unknown {
    const store = (this.ctx as any).get('sessions') as
      { get(id: SessionId): unknown; list?(): ReadonlyArray<{ id: unknown }> } | undefined
    if (!store) return undefined
    return store.get(sid) ?? store.list?.().find((session) => session.id === sid)
  }

  private liveSessionCount(): number {
    const store = (this.ctx as any).get('sessions') as { list?(): unknown[] } | undefined
    return store?.list?.().length ?? -1
  }

  /** 借用非本 bridge 创建的 agent：只用于发消息，不在 dispose 时释放。 */
  private adopt(sessionId: string, agent: Agent, why: string): AgentHandle {
    this.logger().info('wecom: adopt %s for %s（不再 create，避免 session 冲突）', why, sessionId)
    const borrowed: AgentHandle = { agent, dispose: async () => {} }
    this.agents.set(sessionId, { handle: borrowed, owned: false })
    return borrowed
  }

  /** 释放本 bridge 创建的全部 agent（插件卸载 / 配置热更新时调用）。 */
  async dispose() {
    for (const [sessionId, entry] of this.agents) {
      if (!entry.owned) continue
      try {
        await entry.handle.dispose()
      } catch (err) {
        this.logger().warn('wecom agent dispose failed: %s', errorChain(err))
      }
      this.agents.delete(sessionId)
    }
    this.agents.clear()
  }

  // ── 异步处理 ───────────────────────────────────────────
  private async process(pkt: WecomCallbackPacket, ws: WsClient, sessionId: string) {
    const reqId = pkt.headers.req_id
    const body = pkt.body

    // 0. 立刻开一条流式消息占位：企微要求回调后 5 秒内回一帧，
    //    否则 req_id 失效、后续所有回复帧都会被丢弃（用户侧一直停在 "…"）。
    //    之后所有出口（成功/失败/超时）都用 finish 收尾，占位内容会被原位替换。
    const responder = ws.createStreamResponder(reqId)

    // 1. 构造 Agent 输入
    let content: string
    switch (body.msgtype) {
      case 'text':
        content = (body.text.content as string).replace(/^\s*@\S+\s*/, '')  // 去 @ 前缀
        break

      case 'image':
      case 'file':
      case 'voice': {
        const extMap = { image: '.png', file: '.bin', voice: '.amr' } as const
        const msgType = body.msgtype as keyof typeof extMap
        const mediaId = body[msgType]?.media_id
        if (!mediaId) { responder.finish('收到媒体但缺少 media_id'); return }
        try {
          const localPath = await this.media.download(mediaId, extMap[msgType])
          content = `[用户上传了${body.msgtype}] 文件路径: ${localPath}\n请根据文件内容回复用户。`
        } catch (e: any) {
          responder.finish(`媒体下载失败：${e.message}`)
          return
        }
        break
      }

      default:
        responder.finish('暂不支持该消息类型')
        return
    }

    // 2. 取/建 agent，注册两类监听：
    //    - `agent/assistant-stream`：真正的实时增量通道（reasoning-delta / text-delta）。
    //      `session/event` 总线【从不】携带增量块，只有回合结束的 assistant/message。
    //    - `session/event`：仅用于干净终稿（assistant/message）与回合错误（turn/end）。
    try {
      const handle = await this.ensureAgent(sessionId)
      this.registerAgentStream(handle.agent)                  // 实时流式通道
      this.registerSessionListener((handle.agent as any).ctx) // 终稿 + 错误兜底
      const sid = String(handle.agent.session.id)
      const streamMode = this.cfg.replyMode === 'stream'
      debugLog(`[proc] sessionId=${sessionId} sid=${sid} streamMode=${streamMode}`)

      const turn = new Promise<void>((resolve) => {
        const at: ActiveTurn = {
          responder, streamMode, agent: handle.agent,
          nText: 0, nReason: 0,
          cleanText: '', answering: false, toolCallsShown: new Set(),
          settled: false, resolve,
          timer: setTimeout(() => {
            if (at.settled) return
            at.settled = true
            clearInterval(at.heartbeat)
            this.activeTurns.delete(sid)
            debugLog(`[turn] TIMEOUT sid=${sid} mode=${streamMode} text=${at.nText} reason=${at.nReason}`)
            at.responder.finish(at.cleanText || '处理超时，请稍后再试')
            resolve()
          }, TURN_TIMEOUT_MS),
          heartbeat: setInterval(() => {
            // 长空窗保活：keepAlive 内部仅在自身 buf 为空（尚未流出任何内容）时重发占位帧，
            // 一旦 append/reset 过真实内容就自动停，不会覆盖已流式内容。
            if (!at.settled) at.responder.keepAlive()
          }, KEEPALIVE_MS),
        }
        this.activeTurns.set(sid, at)
      })

      handle.agent.followup(createUserMessage({
        content: [{ type: 'text', text: content }],
        source: { kind: 'plugin', plugin: 'wecom' },
      }))
      await turn
    } catch (err: any) {
      debugLog(`[process] error: ${errorMessage(err)}`)
      responder.finish(`处理失败：${errorChain(err)}`)
    }
  }

  /** 注册 `session/event` 监听（全局总线，同一 ctx 只注册一次），用于干净终稿与回合错误。 */
  private registerSessionListener(agentCtx: Context | undefined) {
    if (!agentCtx || this.sessionListeners.has(agentCtx)) return
    this.sessionListeners.add(agentCtx)
    agentCtx.on('session/event', (session: any, event: any) => this.onSessionEvent(session, event))
  }

  /** 注册 `agent/assistant-stream` 监听（全局总线，按 agent 过滤），用于实时流式增量。 */
  private registerAgentStream(agent: Agent) {
    if (this.agentStreams.has(agent as unknown as object)) return
    this.agentStreams.add(agent as unknown as object)
    ;(this.ctx as any).on('agent/assistant-stream', (payload: any) => {
      if (payload?.agent !== agent) return
      this.onAgentStream(agent, payload.frame)
    })
  }

  /** `agent/assistant-stream` 回调：把逐块增量实时转发到企微流式消息（含思考过程）。 */
  private onAgentStream(agent: Agent, frame: any) {
    const at = this.activeTurns.get(String(agent.session.id))
    if (!at || at.settled) return
    debugLog(`[astream] type=${frame?.type} turn=${frame?.turn} chunk=${frame?.chunk?.type ?? ''}`)

    // 注：harness 生产帧的 frame.turn 恒为 undefined，且 start 帧不一定到达本监听器，
    // 故【不】依赖 turn 编号做过滤——已按 payload.agent === agent 精确匹配，
    // 且每会话串行（queue），同一时刻仅一个活跃回合，无需 turn 维度去重。
    if (frame.type === 'start') return  // 仅占位标记，不做任何依赖

    if (frame.type === 'chunk') {
      const chunk = frame.chunk
      if (chunk?.type === 'reasoning-delta') {
        at.nReason++
        // 思考过程实时可见：逐 delta 增量推送（responder.append 内部已累积并节流）
        if (!at.answering && at.streamMode) at.responder.append(chunk.text)
        return
      }
      if (chunk?.type === 'text-delta') {
        at.nText++
        if (!at.answering) {
          at.answering = true
          // 进入答案：清空已展示的推理文本（含「正在调用工具」标记），从答案开头重新流式，
          // 避免「先发空帧再补答案」的闪烁，也避免推理+答案重复堆砌
          if (at.streamMode) at.responder.reset(chunk.text)
        } else if (at.streamMode) {
          at.responder.append(chunk.text)
        }
        return
      }
      if (chunk?.type === 'tool-call-delta') {
        // 工具调用可见标记：长工具执行间隙用户至少能看到「正在调用 XX…」，
        // 避免「思考出了一截、调工具后整条流像卡死」的观感；答案到达后由 reset 整体替换。
        const toolName = chunk.name
        if (at.streamMode && toolName && !at.toolCallsShown.has(toolName)) {
          at.toolCallsShown.add(toolName)
          at.responder.append(`\n⏳ 正在调用工具：${toolName}…\n`)
        }
        return
      }
      // block-start / block-end / usage / finish 等不携带用户可见文本，忽略
      return
    }
    if (frame.type === 'end') {
      const outcome = frame.outcome
      // 失败 / 中断 / 持久化异常：本次尝试未产出可交付给用户的最终消息
      // （outcome.kind === 'abandoned'，或 committed 但 eventType 为 'assistant/attempt'），
      // 不应以当前 buf 收尾，交由 session/event 的 turn/end 错误分支给出文案。
      // 注意：真实 LLM 失败走的是「committed + assistant/attempt」而非 abandoned——
      // harness 会先提交这次（可能部分的）尝试，再 throw，最终由 turn/end 携带错误原因。
      const noUserMessage =
        outcome?.kind === 'abandoned' ||
        (outcome?.kind === 'committed' && outcome.eventType !== 'assistant/message')
      if (noUserMessage) {
        debugLog(`[turn] sid=${String(agent.session.id)} outcome=${outcome?.kind}/${outcome?.eventType} 无用户消息，等待 turn/end 错误分支`)
        return
      }
      at.settled = true
      clearTimeout(at.timer)
      clearInterval(at.heartbeat)
      this.activeTurns.delete(String(agent.session.id))
      debugLog(`[turn] sid=${String(agent.session.id)} mode=${at.streamMode} text=${at.nText} reason=${at.nReason} outcome=${outcome?.kind}/${outcome?.eventType} clean=${at.cleanText.length}`)
      // 优先用干净终稿（assistant/message）；未带则 finish 用内部已流式 buf 兜底
      at.responder.finish(at.cleanText)
      at.resolve()
    }
  }

  /**
   * session/event 总线回调：只处理干净终稿（assistant/message）与回合错误（turn/end）。
   * 注意：该总线【不】携带增量块，实时流式由 `agent/assistant-stream` 驱动。
   */
  private onSessionEvent(session: any, event: any) {
    const at = this.activeTurns.get(String(session?.id))
    if (!at || at.settled) return

    if (event.type === 'assistant/message') {
      const text = extractText(event.data.message.content)
      if (text) at.cleanText = text  // 干净终稿，收尾时优先于已流式的 buf
      return
    }
    if (event.type === 'turn/end') {
      const reason = event.data.reason
      if (reason?.kind === 'error') {
        at.settled = true
        clearTimeout(at.timer)
        clearInterval(at.heartbeat)
        this.activeTurns.delete(String(session.id))
        debugLog(`[turn] ERROR sid=${String(session.id)} msg=${reason.error?.message ?? 'agent turn failed'} clean=${at.cleanText.length}`)
        at.responder.finish(`处理失败：${reason.error?.message ?? 'agent turn failed'}`)
        at.resolve()
        return
      }
      // 非错误收尾（如被取消 / 本次尝试被 defer 后没再产生消息）：以当前已有内容兜底，不无限挂起
      if (at.settled) return
      at.settled = true
      clearTimeout(at.timer)
      clearInterval(at.heartbeat)
      this.activeTurns.delete(String(session.id))
      debugLog(`[turn] END sid=${String(session.id)} reason=${reason?.kind} 兜底收尾 clean=${at.cleanText.length}`)
      at.responder.finish(at.cleanText)  // 未带 cleanText 时 finish 用内部已流式 buf 兜底
      at.resolve()
    }
  }
}