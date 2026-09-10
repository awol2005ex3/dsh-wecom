import type { SettingsScope } from '@deepseek-ai/dsh-settings'
import type { WecomSettings } from './settings.js'

/**
 * 插件 RPC 的挂载点：0.1.5 起只能用 `connection.fetch.register()`。
 *
 * 为什么不用 `connection.rpc.handle()`：它内部执行 `owner.webServer.register(route)`，
 * 而 connection 插件 0.1.5 把 `inject` 缩成了 `['credentials']`（`/api` 改由
 * `ctx.inject(['webServer'])` 延迟挂载），cordis 4 的严格属性访问随即抛
 * `cannot get property "webServer" without inject`；该错误被 connection 的 catch 吞掉，
 * host 无日志，浏览器只看到 `transport failure ... HTTP 405`。
 *
 * exact Fetch 路由在 `/api` prefix 之前命中（见 `HostConnectionService.createSharedFetchHandler`），
 * 所以 `/api/wecom-rpc/*` 能合法压过通用分发，且依然经过 `/api` 的
 * Host/Origin 信任栅栏与浏览器鉴权。
 */
const API_PATH = '/api'
const RPC_ROUTE = 'wecom-rpc'

/** 与 `packages/client/connection/src/rpc.ts` 的 wire 契约保持一致。 */
interface ClientRequestEnvelope {
  type?: unknown
  rpcId?: unknown
  method?: unknown
  payload?: unknown
}

interface RpcFailure {
  code: string
  message: string
  details: object
}

/** 浏览器端 `parseConnectionResponse` 要求 error 同时具备 code / message / details。 */
interface RpcResult {
  ok: boolean
  value?: unknown
  error?: RpcFailure
}

type FetchMethod = 'GET' | 'HEAD' | 'POST'

interface ConnectionFetchRoute {
  readonly path: string
  readonly methods: readonly FetchMethod[]
  readonly requestBody: 'buffered' | 'streaming'
  readonly fetch: (request: Request) => Promise<Response>
}

interface HostConnectionFetch {
  register(route: ConnectionFetchRoute): () => Promise<void>
}

/** 只声明用到的部分，避免对私有包类型的硬依赖。 */
export interface WecomHostConnection {
  fetch?: HostConnectionFetch
}

type Endpoint = 'get' | 'update'

const ENDPOINTS: readonly Endpoint[] = ['get', 'update']

function failure(err: unknown): RpcResult {
  const message = err instanceof Error ? err.message : String(err)
  // code 用 bad-request 而非 internal：部分客户端遇到 internal 会触发重试。
  return { ok: false, error: { code: 'gateway/bad-request', message, details: {} } }
}

function serverResponse(rpcId: string, result: RpcResult): Response {
  return Response.json({ type: 'server-response', rpcId, result })
}

/**
 * 注册 wecom 配置面板的两个 host 端 RPC 端点。
 * @param connection - 宿主 `connection` 服务（只需 `fetch` 注册能力，不依赖 webServer）。
 * @param scope - wecom 设置作用域。
 * @param onUpdate - 配置被浏览器改写后的回调。
 * @param warn - 注册失败时的留痕回调（用插件 logger，不要 console）。
 * @returns 反注册函数；未拿到 `fetch` 时返回空实现（例如非 web profile）。
 */
export function registerRpcHandler(
  connection: WecomHostConnection | undefined,
  scope: SettingsScope<WecomSettings>,
  onUpdate: () => void,
  warn: (message: string) => void,
): () => void {
  const fetchRegistry = connection?.fetch
  if (!fetchRegistry) {
    warn('wecom: connection.fetch 不可用（非 web profile？），配置面板 RPC 未注册')
    return () => {}
  }

  const disposers: Array<() => Promise<void>> = []

  for (const endpoint of ENDPOINTS) {
    const path = `${API_PATH}/${RPC_ROUTE}/${endpoint}`
    try {
      disposers.push(fetchRegistry.register({
        path,
        methods: ['POST'],
        requestBody: 'buffered',
        fetch: (request: Request) => dispatch(endpoint, request, scope, onUpdate),
      }))
    } catch (err) {
      // 路径被占用时 register 会同步抛错；单个端点失败不应拖垮插件加载，
      // 但必须留痕，否则浏览器端只会看到 404。
      const message = err instanceof Error ? err.message : String(err)
      warn(`wecom: 注册 RPC 路由 ${path} 失败：${message}`)
    }
  }

  return () => {
    void Promise.allSettled(disposers.map((dispose) => dispose()))
  }
}

async function dispatch(
  endpoint: Endpoint,
  request: Request,
  scope: SettingsScope<WecomSettings>,
  onUpdate: () => void,
): Promise<Response> {
  let rpcId = ''
  try {
    const body = (await request.json()) as ClientRequestEnvelope
    if (body?.type !== 'client-request' || typeof body.rpcId !== 'string') {
      // 信封不对时没法回填 rpcId，只能走 HTTP 层。
      return new Response('malformed envelope', { status: 400 })
    }
    rpcId = body.rpcId
    const payload = (body.payload ?? {}) as { args?: Record<string, unknown> }
    const args = payload.args ?? {}
    return serverResponse(rpcId, await handle(endpoint, args, scope, onUpdate))
  } catch (err) {
    return serverResponse(rpcId, failure(err))
  }
}

async function handle(
  endpoint: Endpoint,
  args: Record<string, unknown>,
  scope: SettingsScope<WecomSettings>,
  onUpdate: () => void,
): Promise<RpcResult> {
  switch (endpoint) {
    case 'get': {
      const cfg = scope.get()
      // secret 不回传明文，用占位符提示"已配置"。
      return { ok: true, value: { ...cfg, secret: cfg.secret ? '***' : '' } }
    }
    case 'update': {
      await scope.update(args)
      onUpdate()
      return { ok: true, value: scope.get() }
    }
  }
}
