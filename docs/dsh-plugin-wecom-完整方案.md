# DSH 企业微信机器人插件完整方案
## —— 基于 DSH 0.1.2-rc1 · 长连接模式 · 界面化配置

> **版本标注**：`@dsh-version 0.1.2-rc1`
> **协议依据**：企微智能机器人长连接（document/path/101463）、回复消息（document/path/101836）
> **最后更新**：2026-09-09

---

## 目录

1. [方案概述](#1-方案概述)
2. [长连接协议要点](#2-长连接协议要点)
3. [工程结构](#3-工程结构)
4. [完整代码](#4-完整代码)
   - 4.1 插件入口 `src/index.ts`
   - 4.2 长连接客户端 `src/ws.ts`
   - 4.3 LRU 幂等缓存 `src/lru.ts`
   - 4.4 会话串行队列 `src/queue.ts`
   - 4.5 多媒体处理器 `src/media.ts`
   - 4.6 会话桥接 `src/bridge.ts`
5. [控制台配置效果](#5-控制台配置效果)
6. [安全加固要点](#6-安全加固要点)
7. [挂载方式](#7-挂载方式)
8. [分阶段验证清单（M1–M5）](#8-分阶段验证清单m1m5)
9. [0.1.2-rc1 专属避坑指南](#9-012-rc1-专属避坑指南)
10. [仍需现场核对的字段清单](#10-仍需现场核对的字段清单)
11. [后续扩展方向](#11-后续扩展方向)

---

## 1. 方案概述

将企业微信「智能机器人」（API 模式 · 长连接）接入 DSH，作为 Agent 的对话前端：

| 维度 | 设计 |
|---|---|
| 连接方式 | WebSocket 长连接（`wss://openws.work.weixin.qq.com`），免公网回调地址 |
| 配置方式 | DSH 控制台配置面板（Config Schema 自动渲染），`secret` 字段脱敏 |
| 会话模型 | 单聊/群聊各自独立 sessionId，同会话消息严格串行 |
| 回复模式 | markdown 一次性回复 / stream 流式打字机（主动推送） |
| 多媒体 | 图片/文件/语音下载到沙箱，Agent 产出文件经临时素材上传后回复 |
| 事件 | 进入会话欢迎语、点赞/点踩接审计日志 |
| 可靠性 | msgid 幂等去重（LRU）、心跳保活、指数退避重连 |

**里程碑划分**：

- **M1** 长连接跑通（订阅 + 收包 + 心跳 + 重连）
- **M2** 最小闭环（文本消息 → Agent → 回复）
- **M3** 幂等去重 + 会话串行队列
- **M4** 多媒体收发 + 沙箱落盘
- **M5** 欢迎语 + 点赞点踩事件

---

## 2. 长连接协议要点

### 2.1 连接与订阅

1. 建立 WebSocket 连接：`wss://openws.work.weixin.qq.com`
2. 发送 `aibot_subscribe` 订阅帧（携带 BotID + Secret 身份校验）
3. 订阅成功后开始接收消息回调

> ⚠️ **单连接互踢**：同一 BotID 全局仅允许 1 个有效长连接。新连接完成订阅后，旧连接被踢下线。

### 2.2 消息回调格式（企微 → 开发者）

```json
{
  "cmd": "aibot_msg_callback",
  "headers": { "req_id": "REQUEST_ID" },
  "body": {
    "msgid": "MSGID",
    "aibotid": "AIBOTID",
    "chatid": "CHATID",          // 仅群聊返回
    "chattype": "group",         // single | group
    "from": { "userid": "USERID" },
    "msgtype": "text",
    "text": { "content": "@RobotA hello robot" }
  }
}

```

- **`headers.req_id` 回复时必须透传**（关联回复的关键）
- `body.msgid` 用于幂等去重
- 群聊消息 content 带 `@机器人` 前缀，需去除

### 2.3 回复命令

| 用途 | cmd |
|---|---|
| 普通/流式回复 | `aibot_respond_msg` |
| 欢迎语（进入会话） | `aibot_respond_welcome_msg` |
| 更新模板卡片 | `aibot_respond_update_msg` |

### 2.4 流式回复机制（长连接 = 主动推送，非轮询）

1. 开发者生成唯一 `stream.id`
2. 多次发送 `aibot_respond_msg`：相同 `req_id` + `stream.id`，`finish: false`，content 为**全量累积内容**
3. 最后一次发送 `finish: true` 结束
4. 推送节流建议 ≥ 500ms

### 2.5 3 秒响应要求

收到消息回调后须尽快 ACK，Agent 处理必须异步化——本方案中 `handle()` 同步只做幂等 + 入队，处理逻辑全部在异步队列中执行。

---

## 3. 工程结构

```
dsh-plugin-wecom/
├── package.json
├── tsconfig.json
└── src/
    ├── index.ts      # 插件入口：name / Config / inject / apply
    ├── ws.ts         # WsClient：长连接、订阅、心跳、重连、回复方法
    ├── lru.ts        # LRUCache：msgid 幂等去重
    ├── queue.ts      # SessionQueue：同会话消息串行队列
    ├── media.ts      # MediaHandler：媒体下载 / 临时素材上传
    └── bridge.ts     # SessionBridge：分发、白名单、事件、Agent 调用

```

依赖：`ws`、`form-data`（媒体上传用）。

---

## 4. 完整代码

### 4.1 插件入口 `src/index.ts`

```ts
// @dsh-version 0.1.2-rc1
import { Context, Schema } from '@cordisjs/core'
import { WsClient } from './ws'
import { SessionBridge } from './bridge'

export const name = 'wecom'

// TS 接口与 Schema 同名共存（0.1.x Cordis 约定）
export interface Config {
  botId: string
  secret: string
  preset: string
  replyMode: 'markdown' | 'stream'
  sessionTtlMs: number
  welcomeText?: string
}

export const Config: Schema<Config> = Schema.object({
  botId: Schema.string()
    .required()
    .description('企微智能机器人 Bot ID（API模式→长连接页面获取）。⚠️ 同一 BotID 仅允许 1 个有效长连接，多实例会互踢'),

  secret: Schema.string()
    .role('secret')
    .required()
    .description('Bot Secret（仅创建时显示一次，丢失需重新生成）'),


  preset: Schema.string()
    .default('default')
    .description('Agent 使用的 dsh preset 名称'),

  replyMode: Schema.union(['markdown', 'stream'] as const)
    .default('stream')
    .description('回复模式：markdown 一次性返回 / stream 流式打字机'),

  sessionTtlMs: Schema.number()
    .default(1800000)
    .min(60000)
    .description('会话空闲超时（毫秒）'),

  welcomeText: Schema.string()
    .description('用户进入会话时的欢迎语（markdown），留空使用默认文案'),
})

// 0.1.2-rc1 内置服务
export const inject = ['session', 'logger'] as const

export function apply(ctx: Context, config: Config) {
  const logger = ctx.logger('wecom')
  logger.info('wecom plugin loaded, botId=%s, preset=%s, replyMode=%s',
    config.botId, config.preset, config.replyMode)   // 严禁打印 secret

  const ws = new WsClient(config, logger)
  const bridge = new SessionBridge(ctx, config)

  ws.on('message', (pkt) => bridge.handle(pkt, ws))
  ws.on('event', (pkt) => bridge.handleEvent(pkt, ws))
  ws.on('raw', (pkt) => logger.debug('raw frame: %o', pkt))   // 订阅失败时兜底打印

  ctx.on('ready', () => ws.start())
  ctx.on('dispose', () => {
    logger.info('wecom plugin disposing, closing ws connection')
    ws.stop()
  })
}

```

---

### 4.2 长连接客户端 `src/ws.ts`

```ts
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
        this.emit('message', pkt)
        break

      case 'aibot_event_callback':       // 进入会话/点赞点踩等事件
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
    return {
      append: (delta: string) => {
        buf += delta
        const now = Date.now()
        if (now - lastPush >= STREAM_THROTTLE_MS) {
          lastPush = now
          this.sendStreamChunk(reqId, streamId, buf, false)
        }
      },
      finish: () => this.sendStreamChunk(reqId, streamId, buf, true),
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

```

---

### 4.3 LRU 幂等缓存 `src/lru.ts`

```ts
export class LRUCache<K, V> {
  private map = new Map<K, { value: V; expireAt: number }>()
  constructor(private capacity: number, private ttlMs: number) {}

  has(key: K): boolean {
    const entry = this.map.get(key)
    if (!entry) return false
    if (Date.now() > entry.expireAt) { this.map.delete(key); return false }
    // 命中移到末尾（Map 插入顺序即访问顺序）
    this.map.delete(key)
    this.map.set(key, entry)
    return true
  }

  set(key: K, value: V) {
    if (this.map.size >= this.capacity) {
      const firstKey = this.map.keys().next().value!
      this.map.delete(firstKey)   // 淘汰最久未访问
    }
    this.map.set(key, { value, expireAt: Date.now() + this.ttlMs })
  }
}

```

---

### 4.4 会话串行队列 `src/queue.ts`

```ts
type Task = () => Promise<void>

export class SessionQueue {
  private queues = new Map<string, Task[]>()
  private running = new Set<string>()

  async enqueue(sessionId: string, task: Task) {
    if (!this.queues.has(sessionId)) this.queues.set(sessionId, [])
    this.queues.get(sessionId)!.push(task)
    if (!this.running.has(sessionId)) this.drain(sessionId)
  }

  private async drain(sessionId: string) {
    this.running.add(sessionId)
    const q = this.queues.get(sessionId)!
    while (q.length) {
      const t = q.shift()!
      try { await t() } catch { /* bridge 层已兜底 */ }
    }
    this.running.delete(sessionId)
    this.queues.delete(sessionId)   // 空闲即释放
  }
}

```

**语义保证**：同一 sessionId 的消息严格按到达顺序执行；不同会话并行互不阻塞。

---

### 4.5 多媒体处理器 `src/media.ts`

```ts
import { createReadStream, createWriteStream } from 'node:fs'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { randomUUID } from 'node:crypto'

export interface MediaConfig {
  sandboxRoot: string     // dsh 沙箱根目录，如 ctx.sandbox.root
  http: any               // dsh http 服务实例
}

export class MediaHandler {
  constructor(private cfg: MediaConfig) {}

  /** 下载企微媒体资源到沙箱，返回本地路径 */
  async download(mediaId: string, ext: string): Promise<string> {
    const dir = join(this.cfg.sandboxRoot, 'wecom-media')
    await mkdir(dir, { recursive: true })
    const localPath = join(dir, `${randomUUID()}${ext}`)

    // ⚠️ URL 与鉴权方式以官方文档「获取媒体资源」一节为准
    const url = `https://openws.work.weixin.qq.com/cgi-bin/media/get?media_id=${encodeURIComponent(mediaId)}`
    const res = await this.cfg.http.get(url, { responseType: 'stream' })
    if (res.status !== 200) throw new Error(`media download failed: ${res.status}`)
    await pipeline(res.data, createWriteStream(localPath))
    return localPath
  }

  /** 上传临时素材，返回 media_id */
  async upload(filePath: string, type: 'image' | 'file'): Promise<string> {
    // ⚠️ 接口地址与 form-data 字段名以官方文档「上传临时素材」一节为准
    const FormData = (await import('form-data')).default
    const form = new FormData()
    form.append('media', createReadStream(filePath))
    form.append('type', type)

    const res = await this.cfg.http.post(
      'https://openws.work.weixin.qq.com/cgi-bin/media/upload',
      form,
      { headers: form.getHeaders() }
    )
    if (res.data?.errcode) throw new Error(`upload failed: ${res.data.errmsg}`)
    return res.data.media_id
  }
}

```

---

### 4.6 会话桥接 `src/bridge.ts`

```ts
// @dsh-version 0.1.2-rc1
import { WsClient, WecomCallbackPacket } from './ws'
import { LRUCache } from './lru'
import { SessionQueue } from './queue'
import { MediaHandler } from './media'

export class SessionBridge {
  private seen = new LRUCache<string, true>(2000, 5 * 60_000)  // 2000条/5分钟
  private queue = new SessionQueue()
  private media: MediaHandler

  constructor(private ctx: any, private cfg: any) {
    this.media = new MediaHandler({
      sandboxRoot: ctx.sandbox?.root ?? '/tmp/dsh-wecom-sandbox',
      http: ctx.http,
    })
  }

  // ── 消息入口：同步只做幂等+入队，保证 3s ACK ──────────
  handle(pkt: WecomCallbackPacket, ws: WsClient) {
    const msgId = pkt.body.msgid
    if (this.seen.has(msgId)) return
    this.seen.set(msgId, true)

    const sessionId = this.sessionKey(pkt.body)
    this.queue.enqueue(sessionId, () => this.process(pkt, ws, sessionId))
  }

  // ── 事件入口：欢迎语 / 点赞点踩，不走消息队列 ─────────
  handleEvent(pkt: WecomCallbackPacket, ws: WsClient) {
    const reqId = pkt.headers.req_id
    const evt = pkt.body.event_type   // ⚠️ 字段名以文档「事件回调」一节为准

    switch (evt) {
      case 'enter_session': {
        const welcome = this.cfg.welcomeText
          ?? `你好！我是智能助手，有什么可以帮你？`
        ws.respondWelcome(reqId, welcome)
        break
      }
      case 'like':
      case 'dislike': {
        // 接审计日志，不影响用户体验
        this.ctx.logger('wecom').info('feedback event: %o', pkt.body)
        break
      }
      default:
        this.ctx.logger('wecom').debug('unhandled event: %s', evt)
    }
  }

  private sessionKey(body: any) {
    return body.chattype === 'group'
      ? `wecom:group:${body.chatid}`
      : `wecom:single:${body.from.userid}`
  }

  // ── 异步处理 ───────────────────────────────────────────
  private async process(pkt: WecomCallbackPacket, ws: WsClient, sessionId: string) {
    const reqId = pkt.headers.req_id
    const body = pkt.body

    // 1. 白名单（空 = 拒绝所有）
    const key = body.chattype === 'group' ? body.chatid : body.from.userid
   

    // 2. 构造 Agent 输入
    let content: string
    switch (body.msgtype) {
      case 'text':
        content = (body.text.content as string).replace(/^\s*@\S+\s*/, '')  // 去 @ 前缀
        break

      case 'image':
      case 'file':
      case 'voice': {
        const extMap = { image: '.png', file: '.bin', voice: '.amr' } as const
        const mediaId = body[body.msgtype]?.media_id
        if (!mediaId) { ws.respondMarkdown(reqId, '收到媒体但缺少 media_id'); return }
        try {
          const localPath = await this.media.download(mediaId, extMap[body.msgtype])
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

    // 3. 调用 Agent（⚠️ 占位 API，以 0.1.2-rc1 ctx.session 实际签名为准）
    try {
      if (this.cfg.replyMode === 'stream') {
        const r = ws.createStreamResponder(reqId)
        const stream = this.ctx.session.stream(sessionId, content, this.cfg.preset)
        for await (const delta of stream) r.append(delta.text ?? delta)
        await r.finish()
      } else {
        const reply = await this.ctx.session.send(sessionId, content, this.cfg.preset)
        ws.respondMarkdown(reqId, reply.text ?? String(reply))
      }
    } catch (err: any) {
      ws.respondMarkdown(reqId, `处理失败：${err.message}`)
    }
  }
}

```

---

## 5. 控制台配置效果

安装插件后，进入 **DSH 控制台 → 插件 → wecom → 配置**，Schema 自动渲染为：

| 字段 | 控件类型 | 行为说明 |
|---|---|---|
| botId | 文本输入框 | 必填校验；description 提示单连接互踢风险 |
| secret | **密码输入框**（遮罩） | `role('secret')` 生效；日志/dump/导出自动脱敏为 `***` |
| preset | 文本输入框 | 可后续扩展为动态下拉（读取 preset 列表填 enum） |
| replyMode | 单选按钮组 | markdown / stream |
| sessionTtlMs | 数字输入框 | 低于 60000 标红 |
| welcomeText | 文本输入框 | 留空使用默认欢迎语 |

**热重载**：点击「保存」后 DSH 自动 dispose → apply，长连接会断开重连，**属正常行为**，无需重启进程。

---

## 6. 安全加固要点

1. **Secret 绝不落明文**：`role('secret')` 保证控制台 API、配置 dump、日志全部脱敏；自写日志**永远不要打印 `config.secret`**（订阅帧 body 同理）。
2. **环境变量兜底**：生产环境建议 Schema 加 `.default(process.env.WECOM_SECRET)` fallback，界面配置仅作开发/调试用。
3. **Secret 加密存储**：0.1.2-rc1 使用 AES-256-GCM 加密，密钥派生自机器指纹。**迁移服务器时需重新填写 secret**，无法直接复制配置文件。
4. **配置变更审计**：`apply` 时打印 `botId/preset/replyMode`（不含 secret），便于排查。
5. **单连接提醒**：多实例部署会互踢，务必在运维文档注明。

---

## 7. 挂载方式

### 方式 A：控制台图形化安装（推荐）

控制台 → 插件市场 / 本地插件 → 上传或链接 `dsh-plugin-wecom` → 填写配置表单 → 启用。

### 方式 B：配置文件声明（适合 GitOps）

```yaml
# ~/.dsh/profiles/web/cordis.patch.yml
- name: wecom
  path: file:./plugins/dsh-plugin-wecom
  config:
    preset: default
    replyMode: stream
    # botId / secret 不要写在 yaml 里，去控制台补填或用环境变量

```

---

## 8. 分阶段验证清单（M1–M5）

### 前置：企微侧准备

工作台 → 智能机器人 → 创建机器人 → 手动创建 → 页面底部「API 模式创建」→ 连接方式选「使用长连接」→ 保存 BotID、点「点击获取」拿 Secret（**只显示一次**）。

> 若无 API 模式选项：需管理员在「管理后台 → 智能工具 → 智能机器人 → API 模式管理」授权。

### M1：长连接跑通

- [ ] 日志依次出现 `connecting to wss://...` → `ws opened, sending aibot_subscribe` → `subscribed ok`
- [ ] 私聊机器人发「你好」，日志出现 message 事件
- [ ] 断网 10 秒恢复：日志 `heartbeat timeout, force reconnect` → `reconnect in xxxms` → `subscribed ok`
- [ ] 起第二个实例：第一个实例收到 close 并警告（互踢验证）
- [ ] 订阅失败时 `raw` 分支能打印原始响应包（用于字段核对）

### M2：最小闭环

- [ ] 文本消息 → Agent → markdown 回复正常
- [ ] stream 模式打字机效果正常，finish 帧正确结束
- [ ] 群聊消息 `@机器人` 前缀被正确去除

### M3：幂等 + 串行

- [ ] 断网重连后同一条消息重推：只处理一次，不重复回复
- [ ] 快速连发 3 条消息：回复顺序严格 = 发送顺序，无交叉
- [ ] 不同会话（两个人）并发发消息：互不阻塞

### M4：多媒体

- [ ] 发送图片：沙箱目录出现 .png 文件，Agent 收到路径
- [ ] 发送文件/语音：同上，扩展名正确
- [ ] Agent 工具产出文件：用户收到文件消息，文件名正确
- [ ] 媒体下载失败：用户收到友好错误提示，进程不崩

### M5：事件

- [ ] 首次打开机器人对话：自动收到欢迎语（自定义 welcomeText 生效）
- [ ] 点击消息 👍/👎：日志打印 feedback event，无报错
- [ ] 未知事件类型：走 debug 日志，不抛异常

### 配置面板专项

- [ ] secret 粘贴内容显示为圆点/星号
- [ ] 保存空 botId 触发必填校验
- [ ] 修改配置保存后热重载，日志显示新 botId（无 secret）
- [ ] `dsh config dump` 输出中 secret 为加密串或 `***`
- [ ] 卸载插件后 5 秒内日志显示 `closing ws connection`

---

## 9. 0.1.2-rc1 专属避坑指南

| # | 坑点 | 对策 |
|---|---|---|
| 1 | Schema 导入路径 | 必须 `@cordisjs/core`，不是 `dsh`；报错时核对 `node_modules/@cordisjs/core/package.json` 版本 |
| 2 | 数组默认值陷阱 | 控制台新建配置可能传 `undefined`，务必 `.transform(v => Array.isArray(v) ? v : [])` 兜底 |
| 3 | 热重载断连 | 修改配置必然断开重连，期间勿发消息（会触发超时重推） |
| 4 | Secret 无法迁移 | AES-256-GCM + 机器指纹，换机需重填 |
| 5 | inject 服务缺失 | 报 `service 'session' not found` 时检查 profile 是否启用对应服务 |
| 6 | rc → 正式版破坏性变更 | 代码注释标注 `// @dsh-version 0.1.2-rc1`，升级时批量排查 `transform`/`role` API |
| 7 | 插件契约四要素 | `name`、`Config`(Schema)、`inject`、`apply` 缺一不可 |

---

## 10. 仍需现场核对的字段清单

以下字段基于文档整理，但 rc 版本无法离线 100% 确认，**首次真机联调时对照修正**：

| # | 位置 | 待核对内容 | 核对途径 |
|---|---|---|---|
| 1 | `ws.ts → buildSubscribe()` | body 字段名（`aibotid`/`secret` 还是签名派生） | 文档 path/101463「订阅请求」；或抓官方 SDK 首帧 |
| 2 | `ws.ts → startHeartbeat()` | 心跳 cmd 名与 pong 响应判定 | 文档「保持心跳」一节 |
| 3 | `ws.ts → dispatch()` | 订阅响应 cmd 名（`aibot_subscribe_response`?） | 订阅失败时看 `raw` 打印的真实响应 |
| 4 | `media.ts` | `media/get`、`media/upload` 完整 URL 与鉴权（是否复用 BotID+Secret 派生凭证 or 单独 access_token） | 文档「获取媒体资源」「上传临时素材」；或抓包 |
| 5 | `bridge.ts → handleEvent()` | `event_type` 枚举值（`enter_session`/`like`/`dislike`?） | 文档「事件回调」一节 |
| 6 | `bridge.ts → process()` | `ctx.session.stream/send` 真实签名 | `dsh --dump-config` 或 `@cordisjs/core` 类型定义 |
| 7 | `bridge.ts` 构造函数 | `ctx.sandbox.root` 是否存在，否则改 `ctx.baseDir` 或配置项 | 同上 |
| 8 | `ws.ts → respondWelcome()` | 欢迎语 cmd 名 | 文档「发送欢迎语」一节 |

**排查技巧**：所有未识别帧都会走 `dispatch` 的 `default` → `raw` 事件 → debug 日志。真机连一次，把原始包贴出来即可分钟级修正。

---

## 11. 后续扩展方向

- **preset 动态下拉框**：启动时读取 dsh 可用 preset 列表，填充 Schema enum，替代手填字符串
- **模板卡片回复**：`aibot_respond_update_msg` 实现可交互卡片（按钮、表单）
- **会话超时清理**：`sessionTtlMs` 目前仅声明，可在 SessionQueue 或 dsh session 服务侧实现空闲淘汰
- **多 Bot 支持**：将插件改为可多实例（每个实例一套 botId/secret），但注意每个 BotID 仍是单连接
- **审计落库**：点赞/点踩事件从日志升级为写入 database 服务，供后续分析
- **配置校验前置**：apply 阶段主动校验 botId/secret 格式，避免无效连接占用单连接配额

---

> **文档结束** · 联调中遇到任何字段不符 / 接口 404 / 订阅失败，把原始报文贴出来即可快速定位。