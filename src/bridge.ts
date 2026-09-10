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

/** 单个 agent 回合等待的兜底超时（防止会话队列卡死）。 */
const TURN_TIMEOUT_MS = 10 * 60_000

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
    const live = ctx.agents.get(sid) as Agent | undefined
    if (live) return this.adopt(sessionId, live, 'live agent')

    const preset = await ctx.agentPresets.resolve(this.cfg.preset)
    const selection = ctx.agentDefaultModel?.currentSelection?.()
    const create = (id: SessionId): Promise<AgentHandle> => ctx.agents.create({
      sessionId: id,
      meta: { cwd: this.workspaceRoot(), agentPreset: preset.id },
      agentOptions: selection
        ? { provider: selection.provider, model: selection.model }
        : undefined,
      setup: async (agentCtx: Context) => {
        await ctx.agentPresets.mount(agentCtx, preset.id)
      },
    })

    // 2. 正常创建
    try {
      const handle = await create(sid)
      this.agents.set(sessionId, { handle, owned: true })
      return handle
    } catch (err) {
      if (!isAlreadyExists(err)) throw err
    }

    // 3. 撞车：可能是并发创建刚落地，也可能是没有 agent 的残留会话
    const raced = ctx.agents.get(sid) as Agent | undefined
    if (raced) return this.adopt(sessionId, raced, 'raced agent')

    const stale = (ctx.get('sessions') as { get(id: SessionId): unknown } | undefined)?.get(sid)
    if (stale) {
      // 会话还活着但没有 agent，无法接管，只能用新 id 开一轮，避免用户被卡死
      const freshId = brandString<SessionId>(`${sessionId}#${Date.now()}`)
      this.logger().warn('wecom: session %s 残留且无 agent，改用新会话 %s', sessionId, freshId)
      const handle = await create(freshId)
      this.agents.set(sessionId, { handle, owned: true })
      return handle
    }
    throw new Error(`session "${sessionId}" 冲突且无法恢复`)
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
        if (!mediaId) { ws.respondMarkdown(reqId, '收到媒体但缺少 media_id'); return }
        try {
          const localPath = await this.media.download(mediaId, extMap[msgType])
          content = `[用户上传了${body.msgtype}] 文件路径: ${localPath}\n请根据文件内容回复用户。`
        } catch (e: any) {
          ws.respondMarkdown(reqId, `媒体下载失败：${e.message}`)
          return
        }
        break
      }

      default:
        ws.respondMarkdown(reqId, '暂不支持该消息类型')
        return
    }

    // 3. 调用 Agent：先注册事件监听再入队消息，避免丢失 turn 事件
    try {
      const handle = await this.ensureAgent(sessionId)
      const turn = this.runTurn(handle, ws, reqId)
      handle.agent.followup(createUserMessage({
        content: [{ type: 'text', text: content }],
        source: { kind: 'plugin', plugin: 'wecom' },
      }))
      await turn
    } catch (err: any) {
      ws.respondMarkdown(reqId, `处理失败：${errorChain(err)}`)
    }
  }

  /**
   * 等待一次 followup 对应的 agent 回合结束，并把输出转发给企微：
   * - stream 模式：逐 token（text-delta）累积推流，turn/end 时 finish
   * - markdown 模式：取该回合最后一条 assistant 消息一次性回复
   */
  private runTurn(handle: AgentHandle, ws: WsClient, reqId: string): Promise<void> {
    const sid = handle.agent.session.id
    const streamMode = this.cfg.replyMode === 'stream'
    const responder = streamMode ? ws.createStreamResponder(reqId) : null
    const texts: string[] = []

    return new Promise<void>((resolve, reject) => {
      let targetTurn: number | null = null
      let settled = false

      const cleanup = () => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        off()
      }

      const off = this.ctx.on('session/event', (session, event) => {
        if (session.id !== sid) return
        if (event.type === 'turn/start') {
          if (targetTurn === null) targetTurn = event.data.turn
          return
        }
        if (targetTurn === null) return

        if (event.type === 'assistant/chunk'
          && event.data.turn === targetTurn
          && event.data.chunk.type === 'text-delta') {
          responder?.append(event.data.chunk.text)
          return
        }
        if (event.type === 'assistant/message' && event.data.turn === targetTurn) {
          const text = extractText(event.data.message.content)
          if (text) texts.push(text)
          return
        }
        if (event.type === 'turn/end' && event.data.turn === targetTurn) {
          const reason = event.data.reason
          if (reason.kind === 'error') {
            cleanup()
            if (streamMode && responder) {
              if (responder.pushed) {
                responder.finish()
              } else {
                ws.respondMarkdown(reqId, `处理失败：${reason.error?.message ?? 'agent turn failed'}`)
              }
              resolve()
            } else {
              reject(new Error(reason.error?.message ?? 'agent turn failed'))
            }
            return
          }
          if (streamMode) {
            responder?.finish()
            cleanup()
            resolve()
            return
          }
          const full = texts.length ? texts[texts.length - 1] : ''
          ws.respondMarkdown(reqId, full || '（本次没有生成回复内容）')
          cleanup()
          resolve()
        }
      })

      const timer = setTimeout(() => {
        if (streamMode) {
          responder?.finish()
        } else {
          ws.respondMarkdown(reqId, '处理超时，请稍后再试')
        }
        cleanup()
        resolve()
      }, TURN_TIMEOUT_MS)
    })
  }
}