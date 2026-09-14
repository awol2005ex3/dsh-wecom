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
  /** 本回合的 turn 编号（收到 turn/start 时记录，用于过滤旧回合事件）。 */
  targetTurn: number | null
  nText: number
  nReason: number
  texts: string[]
  /** 是否已进入 text-delta（答案）阶段——进入后丢弃思考文本。 */
  answering: boolean
  /** 当前已流式发送到企微的内容：思考阶段=推理文本，回答阶段=答案文本。 */
  liveBuf: string
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

    // 2. 取/建 agent，并在其 carrier 作用域注册 session/event 监听（核心修复），
    //    随后把本回合的转发器写入 activeTurns，跟随该 session 的生命周期事件转发到企微。
    try {
      const handle = await this.ensureAgent(sessionId)
      this.registerSessionListener((handle.agent as any).ctx)  // 借用场景兜底（create 已在 setup 注册）
      const sid = String(handle.agent.session.id)
      const streamMode = this.cfg.replyMode === 'stream'

      const turn = new Promise<void>((resolve) => {
        const at: ActiveTurn = {
          responder, streamMode,
          targetTurn: null, nText: 0, nReason: 0,
          texts: [], answering: false, liveBuf: '',
          settled: false, resolve,
          timer: setTimeout(() => {
            if (at.settled) return
            at.settled = true
            clearInterval(at.heartbeat)
            this.activeTurns.delete(sid)
            debugLog(`[turn] TIMEOUT sid=${sid} mode=${streamMode} textDelta=${at.nText} reasonDelta=${at.nReason}`)
            at.responder.finish('处理超时，请稍后再试')
            resolve()
          }, TURN_TIMEOUT_MS),
          heartbeat: setInterval(() => {
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

  /** 在 agent 的 carrier 作用域（agentCtx）注册 session/event 监听，同一 ctx 只注册一次。 */
  private registerSessionListener(agentCtx: Context | undefined) {
    if (!agentCtx || this.sessionListeners.has(agentCtx)) return
    this.sessionListeners.add(agentCtx)
    agentCtx.on('session/event', (session: any, event: any) => this.onSessionEvent(session, event))
  }

  /**
   * session/event 总线回调（监听挂在 agent 的 carrier 作用域上，故能收到该 session 的事件）。
   * 根据 session.id 找到当前活跃回合转发器，把增量 / 消息 / 结束事件转发给企微。
   */
  private onSessionEvent(session: any, event: any) {
    const at = this.activeTurns.get(String(session.id))
    if (!at || at.settled) return
    const sid = String(session.id)

    if (event.type === 'turn/start') {
      if (at.targetTurn === null) at.targetTurn = event.data.turn
      return
    }
    if (at.targetTurn === null) return

    if (at.streamMode
      && event.type === 'assistant/chunk'
      && event.data.turn === at.targetTurn) {
      const chunk = event.data.chunk
      if (chunk?.type === 'text-delta') {
        at.nText++
        if (!at.answering) { at.answering = true; at.liveBuf = '' }  // 进入答案：丢弃思考文本，仅流式答案
        at.liveBuf += chunk.text
        at.responder.append(at.liveBuf)
        return
      }
      if (chunk?.type === 'reasoning-delta' && !at.answering) {
        at.nReason++
        at.liveBuf += chunk.text
        at.responder.append(at.liveBuf)
        return
      }
    }
    if (event.type === 'assistant/message' && event.data.turn === at.targetTurn) {
      const text = extractText(event.data.message.content)
      if (text) at.texts.push(text)
      return
    }
    if (event.type === 'turn/end' && event.data.turn === at.targetTurn) {
      at.settled = true
      clearTimeout(at.timer)
      clearInterval(at.heartbeat)
      this.activeTurns.delete(sid)
      debugLog(`[turn] sid=${sid} mode=${at.streamMode} textDelta=${at.nText} reasonDelta=${at.nReason} msgLen=${at.texts.join('').length}`)
      const reason = event.data.reason
      if (reason.kind === 'error') {
        // 占位帧已经发过，必须用 finish 收尾（错误文案替换占位），不能再走一次性 markdown
        at.responder.finish(`处理失败：${reason.error?.message ?? 'agent turn failed'}`)
      } else {
        // 优先用 assistant/message 的干净答案；拿不到时回退到流式累积内容
        at.responder.finish(at.texts.length ? at.texts[at.texts.length - 1] : at.liveBuf)
      }
      at.resolve()
    }
  }
}