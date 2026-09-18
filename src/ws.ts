// @dsh-version 0.1.2-rc1
// @protocol 企微智能机器人长连接 developer.work.weixin.qq.com/document/path/101463
import { EventEmitter } from 'node:events'
import { WebSocket } from 'ws'
import { randomUUID } from 'node:crypto'

const WS_URL = 'wss://openws.work.weixin.qq.com'
const HEARTBEAT_INTERVAL_MS = 30_000   // 官方文档「保持心跳」：建议 30s
const HEARTBEAT_TIMEOUT_MS  = 10_000
const MAX_BACKOFF_MS        = 60_000
const STREAM_THROTTLE_MS    = 500      // 流式推送节流
/** 首帧占位文案：满足「收到回调后 5 秒内必须回复」，最终会被 finish 的全量内容原位替换。 */
const STREAM_PLACEHOLDER    = '思考中…'
/** 流式消息既没累积内容也没给终结文案时的兜底。 */
const EMPTY_REPLY           = '（本次没有生成回复内容）'
/**
 * 心跳空闲阈值：距上次增量超过此值（工具执行 / 思考空窗），心跳才叠加旋转状态行。
 * 活跃流式期间（空窗短）只重发内容、不加状态行，避免每 4s 抖动一次。
 */
const STATUS_GAP_MS         = 2_500
/** 状态行动画帧（braille spinner）：让用户明确看到「流仍活着、在运行中」，而非「断了」。 */
const STREAM_STATUS_SPIN    = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

/**
 * req_id 生成：带命令前缀。无 cmd 的服务端回执（订阅响应/心跳响应）
 * 靠 req_id 前缀区分类型（同官方 @wecom/aibot-node-sdk 的做法）。
 */
function buildReqId(prefix: string): string {
  return `${prefix}_${Date.now()}_${randomUUID().replace(/-/g, '').slice(0, 8)}`
}

/** 终结内容取舍：显式文案 > 累积内容 > 兜底文案。 */
export function pickFinal(buf: string, finalText: string | undefined): string {
  if (finalText !== undefined && finalText.trim() !== '') return finalText
  if (buf.trim() !== '') return buf
  return EMPTY_REPLY
}

export interface WecomConfig {
  botId: string
  secret: string
}

export interface WecomCallbackPacket {
  cmd: string
  headers: { req_id: string }
  body: any
  /** 无 cmd 回执帧携带：errcode / errmsg */
  errcode?: number
  errmsg?: string
}

/** 主动推送目标：单聊填 userid(chat_type=1)，群聊填群 chatid(chat_type=2)。 */
export interface WecomSendTarget {
  chatid: string
  chatType: 1 | 2
}

export class WsClient extends EventEmitter {
  private ws?: WebSocket
  private retry = 0
  private hbTimer?: NodeJS.Timeout
  private pongTimer?: NodeJS.Timeout
  private stopping = false

  constructor(private cfg: WecomConfig, private logger: any) {
    super()
  }

  start() {
    this.stopping = false
    this.connect()
  }

  stop() {
    this.stopping = true
    clearInterval(this.hbTimer)
    clearTimeout(this.pongTimer)
    this.ws?.close(1000, 'plugin dispose')
  }

  // ── 连接 ───────────────────────────────────────────────
  private connect() {
    if (this.stopping) return
    this.logger.info('connecting to %s ...', WS_URL)
    this.ws = new WebSocket(WS_URL)

    this.ws.on('open', () => {
      this.logger.info('ws opened, sending aibot_subscribe')
      this.send(this.buildSubscribe())
      this.startHeartbeat()
    })

    this.ws.on('message', (raw) => {
      let pkt: WecomCallbackPacket
      try { pkt = JSON.parse(raw.toString()) }
      catch { this.logger.warn('non-json frame ignored'); return }
      this.dispatch(pkt)
    })

    this.ws.on('close', (code, reason) => {
      this.logger.warn('ws closed: %s %s', code, reason.toString())
      this.scheduleReconnect()
    })

    this.ws.on('error', (err) => this.logger.error('ws error: %s', err.message))
    // error 后必触发 close，重连统一在 close 处理，避免双重连
  }

  // ── 订阅帧（官方文档「订阅请求」：body.bot_id，非 aibotid）──────────
  private buildSubscribe() {
    return {
      cmd: 'aibot_subscribe',
      headers: { req_id: buildReqId('aibot_subscribe') },
      body: {
        bot_id: this.cfg.botId,
        secret: this.cfg.secret,   // 仅此帧携带 secret，日志严禁打印 body
      },
    }
  }

  // ── 分发 ───────────────────────────────────────────────
  private dispatch(pkt: WecomCallbackPacket) {
    // 任何来帧都证明链路存活，取消本轮心跳超时（否则即使服务端正常回 pong，
    // pongTimer 也无人清除，连接会在每个心跳周期后被误杀 → 无限重连循环）
    clearTimeout(this.pongTimer)
    switch (pkt.cmd) {
      case 'aibot_msg_callback':
        this.logger.info('msg received: msgtype=%s msgid=%s', pkt.body?.msgtype, pkt.body?.msgid)
        this.emit('message', pkt)
        break

      case 'aibot_event_callback':       // 进入会话/点赞点踩等事件
        this.logger.info('event received: eventtype=%s', pkt.body?.event?.eventtype)
        this.emit('event', pkt)
        break

      default: {
        // 无 cmd 的回执帧（订阅响应 / 心跳响应 / 回复消息回执）：
        // 形如 { headers: { req_id }, errcode, errmsg }，靠 req_id 前缀区分
        const reqId: string = pkt.headers?.req_id ?? ''
        if (reqId.startsWith('aibot_subscribe')) {
          this.handleSubscribeResponse(pkt)
          break
        }
        if (reqId.startsWith('ping')) {
          if (pkt.errcode !== 0) this.logger.warn('heartbeat ack error: errcode=%s errmsg=%s', pkt.errcode, pkt.errmsg)
          break
        }
        // 回复消息的回执等其余帧 → 交给 raw 监听（含 errcode，排查回复失败）
        this.emit('raw', pkt)
      }
    }
  }

  private handleSubscribeResponse(pkt: WecomCallbackPacket) {
    if (pkt.errcode !== 0) {
      this.logger.error('subscribe failed: errcode=%s errmsg=%s', pkt.errcode, pkt.errmsg)
      this.ws?.terminate()   // 触发 close → 重连
      return
    }
    this.retry = 0
    this.logger.info('subscribed ok')
    this.emit('subscribed')
  }

  // ── 心跳 ───────────────────────────────────────────────
  private startHeartbeat() {
    clearInterval(this.hbTimer)
    this.hbTimer = setInterval(() => {
      if (this.ws?.readyState !== WebSocket.OPEN) return
      // 官方心跳帧：{ cmd: "ping", headers: { req_id } }（非 aibot_heartbeat）
      this.send({ cmd: 'ping', headers: { req_id: buildReqId('ping') } })
      clearTimeout(this.pongTimer)
      this.pongTimer = setTimeout(() => {
        this.logger.warn('heartbeat timeout, force reconnect')
        this.ws?.terminate()
      }, HEARTBEAT_TIMEOUT_MS)
    }, HEARTBEAT_INTERVAL_MS)
  }

  // ── 重连（指数退避 + 抖动）────────────────────────────
  private scheduleReconnect() {
    if (this.stopping) return
    const backoff = Math.min(1000 * 2 ** this.retry, MAX_BACKOFF_MS)
    const delay = backoff + Math.random() * 1000
    this.retry++
    this.logger.info('reconnect in %dms (attempt %d)', Math.round(delay), this.retry)
    setTimeout(() => this.connect(), delay)
  }

  // ── 发送 ───────────────────────────────────────────────
  send(payload: object): boolean {
    if (this.ws?.readyState !== WebSocket.OPEN) {
      this.logger.warn('ws not open, drop payload cmd=%s', (payload as any).cmd)
      return false
    }
    this.ws.send(JSON.stringify(payload))
    return true
  }

  // ── 回复便捷方法 ───────────────────────────────────────

  /** markdown 一次性回复 */
  respondMarkdown(reqId: string, content: string) {
    return this.send({
      cmd: 'aibot_respond_msg',
      headers: { req_id: reqId },
      body: { msgtype: 'markdown', markdown: { content } },
    })
  }

  /** 图片回复（media_id 由 upload 获得） */
  respondImage(reqId: string, mediaId: string) {
    return this.send({
      cmd: 'aibot_respond_msg',
      headers: { req_id: reqId },
      body: { msgtype: 'image', image: { media_id: mediaId } },
    })
  }

  /** 文件回复 */
  respondFile(reqId: string, mediaId: string, filename: string) {
    return this.send({
      cmd: 'aibot_respond_msg',
      headers: { req_id: reqId },
      body: { msgtype: 'file', file: { media_id: mediaId, filename } },
    })
  }

  /**
   * 欢迎语回复。官方 aibot_respond_welcome_msg 仅支持 text（或模板卡片），
   * 不支持 markdown —— content 按纯文本发送。事件回调后须在 5 秒内发出。
   */
  respondWelcome(reqId: string, content: string) {
    return this.send({
      cmd: 'aibot_respond_welcome_msg',
      headers: { req_id: reqId },
      body: { msgtype: 'text', text: { content } },
    })
  }

  /**
   * 主动推送消息（aibot_send_msg）：不需要用户消息触发的 req_id，用于异步任务通知。
   *
   * 关键用途：企微流式消息从首帧起 10 分钟必须 finish，否则被强制结束。
   * 对于超过该窗口的长任务，回合真正结束（turn/end）后无法再写入已死的流式消息，
   * 必须用本方法把最终结论作为「一条新消息」主动推送给用户（前置条件：用户已在本会话发过消息）。
   *
   * 目标定位：单聊 chatid=用户 userid / chat_type=1；群聊 chatid=群 chatid / chat_type=2。
   */
  sendProactiveMarkdown(target: WecomSendTarget, content: string): boolean {
    return this.send({
      cmd: 'aibot_send_msg',
      headers: { req_id: buildReqId('aibot_send_msg') },
      body: {
        chatid: target.chatid,
        chat_type: target.chatType,
        msgtype: 'markdown',
        markdown: { content },
      },
    })
  }

  /**
   * 流式回复句柄：**创建即发出首帧占位**。
   *
   * 企微要求「收到消息回调后 5 秒内回复」，而 Agent 首个 token 通常远晚于此，
   * 不先占位的话 req_id 会超时失效，后续所有帧都被丢弃（用户侧一直停在 "…"）。
   * 占位帧用同 stream.id，后续 finish 的全量内容会把它原位替换掉。
   *
   * 从首帧开始 10 分钟内必须 finish=true，否则企微自动结束消息。
   */
  createStreamResponder(reqId: string, placeholder = STREAM_PLACEHOLDER) {
    const streamId = randomUUID()
    let buf = ''
    let lastPush = 0
    let pushed = false
    let status: string | null = null      // 瞬时状态行（思考中/正在调用工具/整理回复中）；收尾时自动剥离
    let lastChunkTs = Date.now()          // 最近一次增量时刻，用于判断「空闲空窗」是否该显示状态
    let statusLastSent = 0
    let spin = 0
    const statusLine = () => `\n\n${STREAM_STATUS_SPIN[spin++ % STREAM_STATUS_SPIN.length]} ${status}`
    /** 发送当前内容；withStatus 为真且已设置状态行时，追加带旋转动画的状态行（让用户看到「仍在运行」）。 */
    const sendContent = (withStatus: boolean) => {
      const content = buf || placeholder
      const line = withStatus && status ? statusLine() : ''
      this.sendStreamChunk(reqId, streamId, content + line, false)
      lastPush = Date.now()
    }
    if (this.sendStreamChunk(reqId, streamId, placeholder, false)) {
      pushed = true
      lastPush = Date.now()
    } else {
      this.logger.warn('stream placeholder not sent for req_id=%s', reqId)
    }
    return {
      append: (delta: string) => {
        buf += delta
        lastChunkTs = Date.now()
        const now = Date.now()
        if (now - lastPush >= STREAM_THROTTLE_MS) {
          lastPush = now
          pushed = true
          sendContent(false)
        }
      },
      /**
       * 整段替换当前已展示内容（不增量）。用于「思考阶段→答案阶段」切换：
       * 答案首帧先把已展示的推理文本清空，从答案开头重新流式，避免推理+答案重复堆砌。
       * 立即发送（不受 500ms 节流约束），保证切换可见。
       */
      reset: (content: string) => {
        buf = content
        lastChunkTs = Date.now()
        sendContent(false)
      },
      /**
       * 设置瞬时状态行（如「⏳ 正在调用工具：xxx」）。仅在文本变化时调用（bridge 已去重），
       * 立即重发「当前内容 + 状态行」让用户看到阶段切换；空字符串清除状态。
       * 这是「运行中」可见性的核心：工具执行几十秒~几分钟的空窗里，用户能看到状态在旋转而非「断了」。
       */
      setStatus: (text: string | null) => {
        status = text
        const now = Date.now()
        if (now - statusLastSent < STREAM_THROTTLE_MS) return
        statusLastSent = now
        sendContent(true)
      },
      /**
       * 心跳保活（每 KEEPALIVE_MS 调用）：重发当前内容防企微空闲超时掐断。
       * 若距上次增量已超过 STATUS_GAP_MS（工具执行/思考空窗），叠加带旋转动画的状态行，
       * 让用户明确看到「仍在运行中」而非「断了」；活跃增量期间（空窗短）只重发内容、不加状态行避免抖动。
       */
      tick: () => {
        const idle = Date.now() - lastChunkTs
        sendContent(idle >= STATUS_GAP_MS)
      },
      /**
       * 结束流式：content 为全量内容。
       * 显式给了 finalText 就用它（回合结果 / 错误文案 / 超时提示优先），
       * 否则退回累积的 buf，都为空时给一句兜底文案。状态行自动剥离，干净收尾。
       */
      finish: (finalText?: string) => this.sendStreamChunk(reqId, streamId, pickFinal(buf, finalText), true),
      /** 当前已累积并展示的内容（收尾转主动推送时取此作为终稿）。 */
      get buffer(): string {
        return buf
      },
      /** 是否已推送过流式内容（用于错误收尾判断）。 */
      get pushed(): boolean {
        return pushed
      },
    }
  }

  private sendStreamChunk(reqId: string, streamId: string, content: string, finish: boolean) {
    // 官方流式回复格式：msgtype=stream，内容在 stream.content（非 markdown.content）
    return this.send({
      cmd: 'aibot_respond_msg',
      headers: { req_id: reqId },
      body: {
        msgtype: 'stream',
        stream: { id: streamId, finish, content },
      },
    })
  }
}
