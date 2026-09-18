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
import { WsClient, WecomCallbackPacket, type WecomSendTarget } from './ws.js'
import { LRUCache } from './lru.js'
import { SessionQueue } from './queue.js'
import { MediaHandler } from './media.js'
import { debugLog } from './debuglog.js'

/**
 * 企微流式消息硬上限：从首帧起 10 分钟内必须 finish=true，否则企微自动结束消息
 * （见 ws.ts `createStreamResponder` 注释）。之后该流式消息即「死亡」，再发任何帧都被忽略。
 */
const STREAM_MAX_MS = 10 * 60_000

/**
 * 长任务阈值：一个回合耗时超过此值，即视为会超出流式窗口，
 * 最终答案改为「主动推送消息」（aibot_send_msg）送达，而非原地 finish 已死/濒死的流式消息。
 * 取值须严格 < STREAM_MAX_MS：这样走主动推送分支时，原流式消息【尚未】被企微强制结束，
 * 不会出现「流式消息里的旧终稿 + 主动推送的新终稿」重复投递。
 */
const LONG_TASK_MS = 9 * 60_000

/**
 * 兜底硬超时：若回合迟迟不结束（agent 卡死 / 上游 hang），到此强制收尾并主动推送超时提示，
 * 避免 per-session 队列被一条永不 resolve 的 promise 永久阻塞。远大于流式窗口，仅作最后保险。
 */
const HARD_TIMEOUT_MS = 15 * 60_000

/** 长任务提示：跨越阈值且回合尚未结束时主动推送一次，告知用户结论将以新消息送达。 */
const LONG_TASK_HINT = '⏳ 任务还在处理中，预计耗时超过 10 分钟；跑完后我会用一条新消息把完整结论发给你。'

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
  /** 本次消息对应的长连接客户端（用于超时/长任务时主动推送新消息）。 */
  ws: WsClient
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
  /** 流式首帧时刻（用于判断回合是否超出 10 分钟流式窗口，决定 finish 还是主动推送）。 */
  streamStartTs: number
  /** 主动推送目标（单聊 userid / 群聊 chatid），长任务最终结论由此送达。 */
  target: WecomSendTarget
  /** 是否已发过「长任务提示」（避免重复推送）。 */
  hintSent: boolean
  /** 当前展示阶段（驱动「运行中」状态行文案；按优先级不降级）。 */
  phase: 'init' | 'thinking' | 'tool' | 'answering'
  /** 当前状态行文案（去重，避免重复推送刷屏）。 */
  statusText: string
  resolve: () => void
  /** 兜底硬超时定时器（agent 卡死时强制收尾）。 */
  hardTimer: ReturnType<typeof setTimeout>
  /** 长任务提示定时器（跨越阈值时主动告知用户）。 */
  hintTimer: ReturnType<typeof setTimeout>
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

  constructor(
    private ctx: Context,
    private cfg: any,
    /** 超时覆盖（仅测试用，便于快速触发长任务/硬超时分支）。生产路径不传，走默认常量。 */
    private opts: { longTaskMs?: number; hardTimeoutMs?: number } = {},
  ) {
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
    // 先清掉活跃回合的定时器，避免卸载/热重启后孤儿定时器继续触发主动推送
    for (const at of this.activeTurns.values()) {
      clearTimeout(at.hintTimer)
      clearTimeout(at.hardTimer)
      clearInterval(at.heartbeat)
    }
    this.activeTurns.clear()
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
    //    之后所有出口（成功/失败/超时/长任务）都用 finish 或主动推送收尾，占位内容会被原位替换。
    const responder = ws.createStreamResponder(reqId)

    // 0.1 计算主动推送目标（长任务最终结论的送达地址）：
    //     单聊 → from.userid / chat_type=1；群聊 → chatid / chat_type=2。
    const target: WecomSendTarget = body.chattype === 'group'
      ? { chatid: body.chatid, chatType: 2 }
      : { chatid: body.from.userid, chatType: 1 }
    const streamStartTs = Date.now()
    const longTaskMs = this.opts.longTaskMs ?? LONG_TASK_MS
    const hardTimeoutMs = this.opts.hardTimeoutMs ?? HARD_TIMEOUT_MS

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
    let activeAt: ActiveTurn | undefined
    let activeSid: string | undefined
    try {
      const handle = await this.ensureAgent(sessionId)
      this.registerAgentStream(handle.agent)                  // 实时流式通道
      this.registerSessionListener((handle.agent as any).ctx) // 终稿 + 错误兜底
      const sid = String(handle.agent.session.id)
      activeSid = sid
      const streamMode = this.cfg.replyMode === 'stream'
      debugLog(`[proc] sessionId=${sessionId} sid=${sid} streamMode=${streamMode}`)

      const turn = new Promise<void>((resolve) => {
        const at: ActiveTurn = {
          responder, ws, streamMode, agent: handle.agent,
          nText: 0, nReason: 0,
          cleanText: '', answering: false, toolCallsShown: new Set(),
          settled: false, streamStartTs, target, hintSent: false, phase: 'init', statusText: '', resolve,
          // 兜底硬超时：agent 卡死时强制收尾，避免队列永久阻塞
          hardTimer: setTimeout(() => {
            if (at.settled) return
            at.settled = true
            clearTimeout(at.hintTimer)
            clearInterval(at.heartbeat)
            this.activeTurns.delete(sid)
            debugLog(`[turn] HARD_TIMEOUT sid=${sid} mode=${streamMode} text=${at.nText} reason=${at.nReason}`)
            at.ws.sendProactiveMarkdown(
              at.target,
              `⚠️ 处理超时（超过 ${Math.round(hardTimeoutMs / 60000)} 分钟），请稍后重试或简化问题。`,
            )
            resolve()
          }, hardTimeoutMs),
          // 长任务提示：跨越阈值且回合未结束时主动告知用户，结论将以新消息送达
          hintTimer: setTimeout(() => {
            if (at.settled || at.hintSent) return
            at.hintSent = true
            debugLog(`[turn] HINT sid=${sid}`)
            at.ws.sendProactiveMarkdown(at.target, LONG_TASK_HINT)
          }, longTaskMs),
          heartbeat: setInterval(() => {
            if (at.settled) return
            // 流式消息已被企微强制结束（超过 10 分钟窗口）后，再发也无效，停掉省流量
            if (Date.now() - at.streamStartTs >= STREAM_MAX_MS) return
            at.responder.tick()
          }, KEEPALIVE_MS),
        }
        activeAt = at
        this.activeTurns.set(sid, at)
      })

      handle.agent.followup(createUserMessage({
        content: [{ type: 'text', text: content }],
        source: { kind: 'plugin', plugin: 'wecom' },
      }))
      await turn
    } catch (err: any) {
      debugLog(`[process] error: ${errorMessage(err)}`)
      // at 已建好则走统一收尾（会按耗时决定 finish / 主动推送），否则直接 finish 占位流
      if (activeAt && activeSid && !activeAt.settled) {
        this.settleTurn(activeAt, activeSid, `⚠️ 处理失败：${errorChain(err)}`)
      } else {
        responder.finish(`⚠️ 处理失败：${errorChain(err)}`)
      }
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
    //
    // 关键约束：start / end 仅占位标记，【绝不在 end 帧收尾】。多工具循环里每次工具调用都是
    // 一次独立 attempt，其 end 帧同样带 committed/assistant/message（见 harness agent.ts:474 的
    // live.settle('assistant/message')），但此时往往尚无文本答案；若在此收尾，会把「⏳ 正在调用工具」
    // 标记或空内容误当最终回复，并且会丢弃该 attempt 之后其它 attempt 产出的真实答案（当前 bug 根因）。
    // 真正的回合终点是 session/event 的 turn/end：每个逻辑回合仅发一次，位于 agent.ts 外层
    // turn() 的 finally，整段工具循环跑完之后才 append。故收尾只交给 onSessionEvent 的 turn/end
    // 分支（+ 超时兜底），这里只把实时增量转发出去。
    if (frame.type !== 'chunk') return

    const chunk = frame.chunk
    if (chunk?.type === 'reasoning-delta') {
      at.nReason++
      // 思考过程实时可见：逐 delta 增量推送（responder.append 内部已累积并节流）
      if (!at.answering && at.streamMode) at.responder.append(chunk.text)
      // 驱动「运行中」状态行：让用户看到「正在思考」而非「断了」
      this.setPhase(at, 'thinking', '💭 正在思考…')
      return
    }
    if (chunk?.type === 'text-delta') {
      at.nText++
      if (!at.answering) {
        at.answering = true
        // 进入答案：清空已展示的推理文本（含「正在调用工具」标记），从答案开头重新流式，
        // 避免「先发空帧再补答案」的闪烁，也避免推理+答案重复堆砌
        if (at.streamMode) at.responder.reset(chunk.text)
        // 阶段切到「整理回复」，状态行随之更新
        this.setPhase(at, 'answering', '📝 正在整理回复…')
      } else if (at.streamMode) {
        at.responder.append(chunk.text)
      }
      return
    }
    if (chunk?.type === 'tool-call-delta') {
      // 工具调用可见标记：长工具执行间隙用户至少能看到「正在调用 XX…」，
      // 避免「思考出了一截、调工具后整条流像卡死」的观感；答案到达后由 reset 整体替换。
      const toolName = chunk.name
      if (toolName && !at.toolCallsShown.has(toolName)) {
        at.toolCallsShown.add(toolName)
        if (at.streamMode) at.responder.append(`\n⏳ 正在调用工具：${toolName}…\n`)
        // 驱动「运行中」状态行：明确的「正在调用工具」提示，解决「以为断了其实在跑」
        this.setPhase(at, 'tool', `⏳ 正在调用工具：${toolName}…`)
      }
      return
    }
    // block-start / block-end / usage / finish 等不携带用户可见文本，忽略
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
        this.settleTurn(at, String(session.id), `⚠️ 处理失败：${reason.error?.message ?? 'agent turn failed'}`)
        return
      }
      // 非错误收尾（completed / 被取消等）：以当前已有内容兜底收尾
      this.settleTurn(at, String(session.id))
    }
  }

  /**
   * 回合收尾：清定时器、删活跃表，并按「耗时是否超出流式窗口」决定两种送达方式之一：
   *
   * - 耗时 < 长任务阈值（流式消息仍存活）→ 原地 `responder.finish(content)`，
   *   内容原位替换占位帧（正常路径，绝大多数短/中任务走这里）。
   * - 耗时 >= 长任务阈值（已超出/临近企微 10 分钟流式硬上限，原消息随时被强制结束）→
   *   改用 `aibot_send_msg` 把最终结论作为「一条新消息」主动推送给用户（方案 2：
   *   长任务不被 10 分钟窗口掐断，最终答案完整送达）。
   *
   * explicitText 由错误/超时分支传入；成功收尾不传，由 finalString 取干净终稿/已流式答案。
   */
  private settleTurn(at: ActiveTurn, sessionId: string, explicitText?: string) {
    if (at.settled) return
    at.settled = true
    clearTimeout(at.hintTimer)
    clearTimeout(at.hardTimer)
    clearInterval(at.heartbeat)
    this.activeTurns.delete(sessionId)

    const content = explicitText ?? this.finalString(at)
    const elapsed = Date.now() - at.streamStartTs
    const longTaskMs = this.opts.longTaskMs ?? LONG_TASK_MS
    const proactive = elapsed >= longTaskMs
    debugLog(`[turn] END sid=${sessionId} elapsed=${Math.round(elapsed / 1000)}s proactive=${proactive} clean=${at.cleanText.length}`)

    if (proactive) {
      // 旧流式消息仍存活（未超 10 分钟窗口）：先干净收尾并明确告知「完整结论在下方新消息」，
      // 避免残留半截内容像「断了」；已超窗口（企微已强制结束）时 finish 是空操作，无副作用。
      if (elapsed < STREAM_MAX_MS) {
        at.responder.finish('（任务较长，已超出对话流式消息时长上限；完整结论见下方新消息 ↓）')
      }
      at.ws.sendProactiveMarkdown(at.target, content)
    } else {
      at.responder.finish(content)
    }
    at.resolve()
  }

  /**
   * 收尾内容选择器（始终返回具体字符串，供流式 finish 与主动推送共用）：
   * 干净终稿 > 已流式答案(buf) > 明确提示。
   * 关键：当本轮没有任何文本答案（模型只思考 + 调工具后因工具失败而中断，
   * cleanText 为空、nText=0）时，【绝不】把「⏳ 正在调用工具：xxx…」标记当最终内容回显
   * ——否则用户看到的就是「卡在工具标记上、像断了」。改用一句明确提示。
   */
  private finalString(at: ActiveTurn): string {
    if (at.cleanText && at.cleanText.trim()) return at.cleanText
    // 仅当确实流出过文本答案（nText>0）时才用 buf：此时首个 text-delta 已把 buf reset 成纯答案。
    // 否则 buf 里只是「思考文本 + 正在调用工具标记」，绝不能当作终稿回显（见测试 7）。
    if (at.nText > 0) {
      const buf = at.responder.buffer
      if (buf && buf.trim()) return buf
    }
    if (at.toolCallsShown.size > 0) {
      return `⚠️ 已调用工具（${[...at.toolCallsShown].join('、')}）但未返回文本结果，请稍后重试或更换问题。`
    }
    return '（本次未生成回复内容）'
  }

  /**
   * 驱动「运行中」状态行：按优先级切换阶段文案（思考中 / 正在调用工具 / 整理回复中）。
   * 仅在阶段升级或文案变化时调用 responder.setStatus，避免重复推送刷屏；
   * 降级（如已「整理回复」不再退回「思考中」）直接忽略。
   */
  private setPhase(at: ActiveTurn, phase: ActiveTurn['phase'], text: string) {
    const order: Record<ActiveTurn['phase'], number> = { init: 0, thinking: 1, tool: 2, answering: 3 }
    if (order[phase] < order[at.phase]) return
    if (at.statusText === text) return
    at.phase = phase
    at.statusText = text
    at.responder.setStatus(text)
  }
}