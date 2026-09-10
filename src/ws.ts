// @dsh-version 0.1.2-rc1
// @protocol 企微智能机器人长连接 developer.work.weixin.qq.com/document/path/101463
import { EventEmitter } from 'node:events'
import { WebSocket } from 'ws'
import { randomUUID } from 'node:crypto'

const WS_URL = 'wss://openws.work.weixin.qq.com'
const HEARTBEAT_INTERVAL_MS = 30_000   // 以官方文档「保持心跳」一节为准
const HEARTBEAT_TIMEOUT_MS  = 10_000
const MAX_BACKOFF_MS        = 60_000
const STREAM_THROTTLE_MS    = 500      // 流式推送节流

export interface WecomConfig {
  botId: string
  secret: string
}

export interface WecomCallbackPacket {
  cmd: string
  headers: { req_id: string }
  body: any
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
    clearTimeout(this.hbTimer)
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

  // ── 订阅帧（字段名以文档「订阅请求」一节为准）──────────
  private buildSubscribe() {
    return {
      cmd: 'aibot_subscribe',
      headers: { req_id: randomUUID() },
      body: {
        aibotid: this.cfg.botId,
        secret: this.cfg.secret,   // 仅此帧携带 secret，日志严禁打印 body
      },
    }
  }

  // ── 分发 ───────────────────────────────────────────────
  private dispatch(pkt: WecomCallbackPacket) {
    switch (pkt.cmd) {
      case 'aibot_subscribe_response':   // 订阅结果（字段名以实际响应为准）
        this.retry = 0
        this.logger.info('subscribed ok')
        this.emit('subscribed')
        break

      case 'aibot_msg_callback':
        this.logger.debug('msg received: msgtype=%s msgid=%s', pkt.body?.msgtype, pkt.body?.msgid)
        this.emit('message', pkt)
        break

      case 'aibot_event_callback':       // 进入会话/点赞点踩等事件
        this.logger.debug('event received: event_type=%s', pkt.body?.event_type)
        this.emit('event', pkt)
        break

      default:
        // pong / 未知 cmd → 交给 raw 监听（订阅失败时用于排查）
        this.emit('raw', pkt)
    }
  }

  // ── 心跳 ───────────────────────────────────────────────
  private startHeartbeat() {
    clearInterval(this.hbTimer)
    this.hbTimer = setInterval(() => {
      if (this.ws?.readyState !== WebSocket.OPEN) return
      this.send({ cmd: 'aibot_heartbeat', headers: { req_id: randomUUID() }, body: {} })
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

  /** 欢迎语（⚠️ cmd 名以文档「发送欢迎语」一节为准） */
  respondWelcome(reqId: string, markdown: string) {
    return this.send({
      cmd: 'aibot_respond_welcome_msg',
      headers: { req_id: reqId },
      body: { msgtype: 'markdown', markdown: { content: markdown } },
    })
  }

  /** 流式回复句柄：content 全量累积，finish 结束，内置节流 */
  createStreamResponder(reqId: string) {
    const streamId = randomUUID()
    let buf = ''
    let lastPush = 0
    let pushed = false
    return {
      append: (delta: string) => {
        buf += delta
        const now = Date.now()
        if (now - lastPush >= STREAM_THROTTLE_MS) {
          lastPush = now
          pushed = true
          this.sendStreamChunk(reqId, streamId, buf, false)
        }
      },
      finish: () => this.sendStreamChunk(reqId, streamId, buf, true),
      /** 是否已推送过流式内容（用于错误收尾判断）。 */
      get pushed(): boolean {
        return pushed
      },
    }
  }

  private sendStreamChunk(reqId: string, streamId: string, content: string, finish: boolean) {
    return this.send({
      cmd: 'aibot_respond_msg',
      headers: { req_id: reqId },
      body: {
        msgtype: 'markdown',
        markdown: { content },
        stream: { id: streamId, finish },
      },
    })
  }
}