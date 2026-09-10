import type { SettingsScope } from '@deepseek-ai/dsh-settings'
import type { WecomSettings } from './settings.js'

const RPC_CHANNEL = '/wecom-rpc'
const RPC_PREFIX = 'wecom/'

interface RpcResult {
  ok: boolean
  value?: unknown
  error?: { message: string }
}

interface HostRpc {
  handle(
    channel: string,
    handler: (endpoint: string, payload: unknown, signal: AbortSignal) => Promise<RpcResult>,
    options: { authority: 'loopback' | 'trusted-host' },
  ): () => Promise<void>
}

export function registerRpcHandler(connection: { rpc?: HostRpc }, scope: SettingsScope<WecomSettings>, onUpdate: () => void): () => void {
  if (!connection?.rpc) return () => {}

  const remove = connection.rpc.handle(RPC_CHANNEL, async (endpoint, _payload): Promise<RpcResult> => {
    if (!endpoint.startsWith(RPC_PREFIX)) {
      return { ok: false, error: { message: `unknown endpoint ${endpoint}` } }
    }
    try {
      switch (endpoint.slice(RPC_PREFIX.length)) {
        case 'get': {
          const cfg = scope.get()
          return {
            ok: true,
            value: {
              ...cfg,
              secret: cfg.secret ? '***' : '',
            },
          }
        }
        case 'update': {
          const payload = _payload as { args?: Record<string, unknown> } | undefined
          const patch = payload?.args ?? {}
          await scope.update(patch)
          onUpdate()
          return { ok: true, value: scope.get() }
        }
        default:
          return { ok: false, error: { message: `unknown endpoint ${endpoint}` } }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      return { ok: false, error: { message } }
    }
  }, { authority: 'loopback' })

  return () => { void remove() }
}