# AGENTS.md — dsh-plugin-wecom

企业微信（WeCom）智能机器人插件。通过企微 API 模式的长连接（WebSocket）将 WeCom 消息对接到 DeepSeek Harness（dsh）的 Agent，免公网回调地址。

本插件是**独立项目**（不进入 harness monorepo），采用 `tsc` 编译的标准 DSH 插件构建工作流。任何改动都应遵循本文件约定。

---

## 仓库结构

| 路径 | 作用 |
| --- | --- |
| `src/index.ts` | **插件入口**。导出 `name / inject / Config / apply`，注入 `agents / agentPresets / sandboxPolicy / logger`。 |
| `src/ws.ts` | `WsClient`（extends EventEmitter）。企微长连接协议实现：建立 WebSocket、发送 `aibot_subscribe` 订阅帧、心跳保活、指数退避重连、`respondMarkdown / respondImage / respondFile / respondWelcome / createStreamResponder` 等回复方法。 |
| `src/lru.ts` | `LRUCache<K,V>` — msgid 幂等去重（Map + TTL 淘汰），2000 条 / 5 分钟窗口。 |
| `src/queue.ts` | `SessionQueue` — 同 sessionId 消息严格串行，不同会话并发不阻塞。 |
| `src/media.ts` | `MediaHandler` — 使用 Node 原生 `fetch` 下载企微媒体资源到沙箱，使用 `openAsBlob` + `FormData` 上传临时素材。**无第三方 form-data 依赖**。 |
| `src/bridge.ts` | `SessionBridge` — 消息分发、白名单、幂等去重、会话队列、Agent 创建与事件驱动回复协调。核心桥接层。 |
| `src/settings.ts` | `WecomSettings` + `WecomSettingsSchema`（schemastery），由 `ctx.settings.register('wecom', ..., { applies: 'live' })` 注册，改配置即热重启服务。 |
| `src/rpc.ts` | **宿主半 RPC**：`registerRpcHandler()` 用 `connection.fetch.register()` 挂 `/api/wecom-rpc/{get,update}`，供浏览器配置面板读写设置。 |
| `src/client.ts` | **浏览器半**（经 `scripts/wrap-client.mjs` 包成 CJS 闭包工厂）。侧边栏入口「⚙ 企微」→ 配置面板。 |
| `src/debuglog.ts` | `debugLog` / `tee` — 受 `DSH_WECOM_DEBUG` 控制的落盘诊断日志。 |
| `lib/` | tsc 构建产物（.js + .d.ts），`package.json` 的 `files` 仅包含 `lib`。 |
| `docs/dsh-plugin-wecom-完整方案.md` | 方案设计文档（M1–M5 分阶段验证清单、协议要点、待核对字段）。 |

---

## 常用命令

```bash
npm install                     # 安装依赖（ws / cordis / schemastery 等）
npm run build                   # tsc -p tsconfig.json
npm run typecheck               # tsc --noEmit
npm run check:rpc               # 构建 + 用假 connection 驱动宿主 RPC（信封/错误/反注册）
npm run check:agent             # 构建 + 假 ctx 驱动 ensureAgent 的会话冲突分支
npm run check:reply             # 构建 + 假 ws 驱动完整消息链路（占位帧/收尾/错误分支）
npm run check                   # 以上三个全跑
npx @deepseek-ai/dsh plugin --profile web add .   # 链接进 web profile
```

> **改任何源码后必须重启 host。** 插件集在 boot 时扫描并缓存。

---

## 核心约定（违反即破坏构建/运行）

1. **ESM + NodeNext。** `tsconfig.json` 使用 `module: NodeNext`、`moduleResolution: NodeNext`；`package.json` 有 `"type": "module"`；所有相对导入**必须**加 `.js` 后缀（如 `'./ws.js'`），否则 tsc 报 ESM/CJS 冲突。

2. **插件元数据用具名导出。** `name` / `inject` / `Config` / `apply` 均为具名导出，禁止 `export default`（会丢失 `inject` 元数据）。

3. **`Config` 用 schemastery 默认导入。** `import z from '@deepseek-ai/schemastery'`（非 `Schema` 命名导出）。`union` 用法：`z.union(['markdown', 'stream'])`。

4. **服务注入用 `inject` 声明，避免 `ctx.get()` 运行时猜测。** 本插件注入的服务：`agents` / `agentPresets` / `agentDefaultModel` / `sandboxPolicy`。**`logger` 是 cordis 内置服务，不能放进 `inject`**（loader 会永久等待其 provide，报 `pending (waiting for service: logger)`），直接 `ctx.logger('wecom')` 使用；`agentDefaultModel` 类型上容缺。

5. **生命周期用 `ctx.effect`，不用 `ready` / `dispose` 事件。** cordis 4 无全局 `ready` 事件；`dispose` 在 HMR 时不保证触发。示例：
   ```ts
   ws.start()
   ctx.effect(() => () => { ws.stop(); bridge.dispose() }, 'wecom: lifecycle')
   ```

6. **所有注册可逆。** 用 `ctx.on` / `ctx.effect` 管理副作用与清理，`ctx.on` 返回 disposer 函数，在清理时调用。

7. **不臆造 API。** harness 服务签名以 `../deepseek-harness/packages/...` 源码为准，不以方案文档中的占位签名为准。

8. **`Config` 不应在代码中硬编码 secret / botId。** 配置只写在 Schema 里，通过控制台表单编辑，日志严禁打印 secret。

9. **插件 RPC 只能用 `connection.fetch.register()`，禁用 `connection.rpc.handle()`。** harness 0.1.5 起 `rpc.handle` 内部执行 `owner.webServer.register(route)`，而 connection 插件的 fiber 只 inject 了 `credentials`（`/api` 改由 `ctx.inject(['webServer'])` 延迟挂载），cordis 4 严格属性访问抛 `cannot get property "webServer" without inject`，错误被 catch 吞掉 → 浏览器只看到 `HTTP 405`。正解是注册 `/api/<route>` 的 **exact Fetch 路由**（在 `/api` prefix 之前命中，仍经过信任栅栏与浏览器鉴权），浏览器侧固定 `connection.rpc.call('/api', ...)`。

10. **RPC 错误信封必须带齐 `code` / `message` / `details`。** 浏览器 `parseConnectionResponse` 三者缺一即抛 `invalid server-response failure`。业务拒绝用 `code: 'gateway/bad-request'`，**不要用 `internal`**（部分客户端会触发重试），HTTP 状态恒为 200。

11. **企微回复必须先占位、后收尾（5 秒规则）。** 企微要求「收到 `aibot_msg_callback` 后 5 秒内回复」，否则 req_id 失效、后续帧全被丢弃（用户侧永远停在 "…"）。因此 `process()` 一进来就 `ws.createStreamResponder(reqId)`（该方法**创建即发首帧占位**），之后所有出口——媒体失败、不支持类型、turn/end 成功、turn 出错、超时——统一用 `responder.finish(...)`，且 `finish` 的内容优先级是「显式文案 > 累积 buf > 兜底」。**任何分支都不要再退回 `respondMarkdown()`**（那时早已超窗口）。另外首帧起 10 分钟企微强制结束流式消息，`TURN_TIMEOUT_MS` 取 5 分钟留余量。

12. **会话 id 冲突（`session "<id>" already exists`）的两条纪律。** `sessions.prepare()` / `enter()` 发现同名会话直接抛错，而 `AgentHandle.dispose()` 是**异步**的（要把会话从 store 移除）：
    - 重启服务（配置热更新）前**必须 `await` 旧 `SessionBridge.dispose()`**，否则新 bridge 会撞上尚未释放的同名会话；`startServices` 已用一个 promise 链串行化启停。
    - `ensureAgent` 的五级自愈：缓存 → `ctx.agents.get(sid)` **借用**（包 `dispose` 为空的伪 handle，标 `owned:false` 不释放）→ `create` → 撞车后**再借一次** → **同 id 重试一次**（瞬时竞争）→ 仍失败则用 `${sessionId}#${Date.now()}` 开新会话。任何一步都不要抛给用户在企微里看到「处理失败」。
    - 定位手段：冲突时会打 `wecom: create session <id> 冲突（...）；诊断: agent=... session=... sessions服务=... 存活会话=N`。`sessions服务=false` 说明当前 ctx 拿不到 session store（隔离 scope 问题）；`session=true` 说明会话残留且无 agent。SessionStore **没有公开删除接口**（`detachEntered` 是私有的），残留会话只能绕开、不能清理。

---

## API 契约速查

### 插件注入的服务

```ts
export const inject = ['agents', 'agentPresets', 'agentDefaultModel', 'sandboxPolicy', 'connection', 'settings'] as const
```

- `agents`（`ctx.agents`）：`create(options)` / `resume(options)` → `Promise<AgentHandle>`
- `agentPresets`（`ctx.agentPresets`）：`resolve(id)` → `Promise<Preset>`；`mount(agentCtx, id)` — 在 agent setup 中挂载 preset
- `agentDefaultModel`（`ctx.agentDefaultModel`，类型容缺）：`currentSelection()` → `{ provider, model }`
- `sandboxPolicy`（`ctx.sandboxPolicy`，类型容缺）：`resolve({ session?, mode? }).workspaceRoot` → 沙箱工作目录
- `logger`（cordis 内置，**不注入**）：`ctx.logger('wecom')` → 带 scope 的 Logger
- `settings`（`ctx.settings`）：`register(name, schema, { applies })` → `SettingsScope`（`get()` / `update(patch)` / `watch(cb)`）
- `connection`（`ctx.get('connection')`）：只用到 `fetch.register(route)`，见约定 9

### Agent 创建与回合驱动

```ts
const handle = await ctx.agents.create({
  sessionId: brandString<SessionId>(`wecom:group:${chatid}`),
  meta: { cwd: sandboxPolicy.resolve({}).workspaceRoot, agentPreset: preset.id },
  agentOptions: { provider, model },
  setup: async (agentCtx) => {
    await ctx.agentPresets.mount(agentCtx, preset.id)
  },
})
// → { agent, dispose() }

// 发消息（同步排队，不等待）
handle.agent.followup(createUserMessage({
  content: [{ type: 'text', text: '用户消息' }],
  source: { kind: 'plugin', plugin: 'wecom' },
}))

// 等待回合结束（在事件监听中完成转发，无需直接调用 whenIdle）
```

### 会话事件监听

```ts
// session/event 是全局 durable 事件流
ctx.on('session/event', (session: Session, event: SessionEvent) => {
  if (session.id !== targetSessionId) return
  switch (event.type) {
    case 'turn/start': { /* event.data.turn */ }
    case 'assistant/chunk': { /* event.data.chunk.type === 'text-delta' → { text } */ }
    case 'assistant/message': { /* event.data.message.content: ContentBlock[] → extractText */ }
    case 'turn/end': { /* event.data.reason: TurnEndReason */ }
  }
})
```

企微事件（进入会话/点赞点踩）由 WsClient 的 `event` 事件分发，不走 session/event 总线。

### 企微长连接协议（WsClient）

```
ws://openws.work.weixin.qq.com（WSS）
→ aibot_subscribe（body: { aibotid, secret }）
→ aibot_subscribe_response（订阅成功）
← aibot_msg_callback（用户消息）
← aibot_event_callback（事件）
→ aibot_respond_msg（回复：markdown / image / file / stream）
→ aibot_respond_welcome_msg（欢迎语）
```

- 心跳：`aibot_heartbeat` / 30s 间隔，10s 超时终止重连。
- 重连：指数退避 + 随机抖动（1s → 60s cap，`2^n` 倍）。
- 流式回复：`stream: { id: string, finish: boolean }`，content 全量累积，节流间隔 ≥ 500ms。
- `headers.req_id` 回复时必须透传（关联回复的关键）。
- `body.msgid` 用于幂等去重。

### MediaHandler

```ts
// 下载媒体到沙箱
const localPath = await media.download(mediaId, ext)  // ext = .png | .bin | .amr

// 上传临时素材
const mediaId = await media.upload(filePath, 'image' | 'file')
```

使用 Node 原生 fetch（>=22）/ FormData / `openAsBlob`（>=19.8），**无第三方 form-data 依赖**。

---

## 如何扩展

### 新增消息类型

1. 在 `src/bridge.ts` 的 `process()` 方法的 `switch (body.msgtype)` 中添加 case。
2. 若需下载新类型媒体，在 `extMap` 中注册扩展名。
3. 若 Agent 需理解该类型内容，在 `content` 字符串中表达。
4. 运行 `npm run typecheck` → `npm run build` → 重启 host。

### 新增企微事件类型

1. 在 `src/ws.ts` 的 `dispatch()` 中加 `case` 分支（或走 `default` → `raw` 分支兜底打印）。
2. 在 `src/bridge.ts` 的 `handleEvent()` 中增加事件枚举的处理分支。
3. 编译后重启 host，真机验证。

### 新增回复类型

1. 在 `src/ws.ts` 中增加 `respondXxx()` 方法，构造对应 `aibot_respond_msg` 载荷。
2. 在 `src/bridge.ts` 的 `process()` 中调用新方法。

### preset 动态下拉框（后续计划）

读取 DSH 可用 preset 列表，在 Config Schema 中用 `z.string()` + 运行时验证填充 enum 选项。暂不实现。

---

## 需现场核对的字段（联调时对照 `docs/dsh-plugin-wecom-完整方案.md` 第 10 节）

| # | 位置 | 待核对内容 |
| --- | --- | --- |
| 1 | `ws.ts → buildSubscribe()` | body 字段名（`aibotid` / `secret` 还是签名派生） |
| 2 | `ws.ts → startHeartbeat()` | 心跳 cmd 名与 pong 响应判定 |
| 3 | `ws.ts → dispatch()` | 订阅响应 cmd 名（`aibot_subscribe_response`?） |
| 4 | `media.ts` | `media/get`、`media/upload` 完整 URL 与鉴权 |
| 5 | `bridge.ts → handleEvent()` | `event_type` 枚举值 |
| 6 | `bridge.ts → ensureAgent()` | `create()` setup 签名验证 |
| 8 | `ws.ts → respondWelcome()` | 欢迎语 cmd 名 |

**排查技巧**：所有未识别帧走 `dispatch` 的 `default` → `raw` 事件 → debug 日志。真机连一次，把原始包贴出来即可分钟级修正。

---

## 已知坑（踩过）

1. **`@cordisjs/core` → `@deepseek-ai/cordis`**：方案文档写 `@cordisjs/core`，实际 DSH 使用 `@deepseek-ai/cordis` + `@deepseek-ai/schemastery`。不更正则 tsc 找不到 `Schema` 导出。

2. **`ctx.session.stream / send` 不存在**：真实 DSH 中 Agent 调用是 `ctx.agents.create()` + `followup()` + 事件驱动。回复需通过 `session/event` 监听 `assistant/chunk` / `assistant/message` 获取。

3. **`ctx.http` 不存在**：DSH 无 axios 等 HTTP 客户端注入。使用 Node 全局 `fetch`（>=22）发 HTTP 请求。

4. **`ctx.sandbox.root` 不存在**：沙箱根目录的正确来源是 `ctx.sandboxPolicy.resolve({}).workspaceRoot`。注意 `sandboxPolicy` 是服务名。

5. **cordis 4 无全局 `ready` / `dispose` 事件**：生命周期清理必须挂 `ctx.effect`。插件无需等任何事件即可在 `apply()` 中同步启动 `ws.start()`。

6. **`z.union` 的用法**：schemastery 的 `z.union` 接受 `readonly X[]`。传字符串数组 `z.union(['markdown', 'stream'])` 可行，返回值类型为 `string`（非字面量 union），在 `z<Config>` 检查中不通过时需改为 `z.union([z.const('markdown'), z.const('stream')])`。

7. **`errorChain`**：从 `@deepseek-ai/dsh-llm` 导入的 `errorChain` 用于格式化错误链。非 `@deepseek-ai/dsh-util-values`。

8. **`Application` vs `Context`**：`apply()` 的参数是 `ctx: Context`（来自 cordis），不是 application 或其他对象。`ctx.logger` 是 callable。


9. **`@deepseek-ai/dsh-agent-default-model` 不存在**：`inject` 中已声明 `agentDefaultModel`（服务运行时确实存在），但 npm 上无此类型定义包。用 `(ctx as any).agentDefaultModel?.currentSelection?.()` 编译时容缺。

10. **`logger` 不能放进 `inject`**：报 `dsh-plugin-wecom: pending (waiting for service: logger)`，整个 profile 启动失败（`1 entry did not activate`）。`logger` 是 cordis 内置属性而非 service provider，直接 `ctx.logger('wecom')`。

11. **Schema 必填字段未配置会拖垮整个 profile**：`botId`/`secret` 加 `.required()` 后，未配置时 loader 报 `invalid config: $.botId missing required value`，`dsh web` 起不来。对策：bundle patch 的 insert 提供空串占位 `config: { botId: '', secret: '' }`（schemastery required 只拒绝 undefined，空串通过），`apply()` 开头检测 `!config.botId || !config.secret` 则打印警告并 `return`（不启动 ws），用户到控制台填真值后保存即热重载接入。

---

## 参考

- 方案设计文档：`docs/dsh-plugin-wecom-完整方案.md`（需求基准 + M1-M5 验证清单）
- harness 源码（只读参考，不要改）：`../deepseek-harness/packages/`
  - Agent 创建：`packages/core/agent/src/index.ts`
  - Session 事件类型：`packages/core/session/src/types.ts`
  - 消息构造：`packages/llm/llm/src/message.ts`
  - Webhook 外部渠道示例：`packages/webhook/webhook/src/session.ts`
- 参考插件（同类独立构建）：`../dsh-role-manager/`