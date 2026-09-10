// @dsh-version 0.1.2-rc.1
import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-agent-presets'
import type {} from '@deepseek-ai/dsh-sandbox-policy'

import { WsClient } from './ws.js'
import { SessionBridge } from './bridge.js'
import { WecomSettingsSchema, type WecomSettings } from './settings.js'
import { registerRpcHandler, type WecomHostConnection } from './rpc.js'
import { debugLog, tee } from './debuglog.js'

export const name = 'wecom'

export interface Config {}

export const Config: z<Config> = z.object({})

export const inject = ['agents', 'agentPresets', 'agentDefaultModel', 'sandboxPolicy', 'connection', 'settings'] as const

export function apply(ctx: Context, _config: Config) {
  const logger = ctx.logger('wecom')

  const scope = ctx.settings.register('wecom', WecomSettingsSchema, { applies: 'live' })

  let cfg = scope.get()
  let ws: WsClient | undefined
  let bridge: SessionBridge | undefined

  /**
   * 停止并**彻底释放**上一轮服务。
   *
   * agent 的 dispose 是异步的（会话要从 session store 移除），不 await 就建新 bridge
   * 会让新 agent 撞上尚未释放的同名会话 → `session "<id>" already exists`。
   */
  async function stopServices() {
    if (ws) { ws.stop(); ws = undefined }
    if (bridge) {
      const old = bridge
      bridge = undefined
      await old.dispose()
    }
  }

  /** 串行化所有启停：并发重启是 session 冲突的主要来源。 */
  let pending: Promise<void> = Promise.resolve()

  function startServices(settings: WecomSettings): Promise<void> {
    const run = async (): Promise<void> => {
      await stopServices()

      if (!settings.botId || !settings.secret) {
        logger.warn('wecom: botId/secret 未配置，跳过长连接。请在 DSH 控制台 → 插件 → wecom 中填写。')
        return
      }

      logger.info('wecom plugin started, botId=%s, preset=%s, replyMode=%s',
        settings.botId, settings.preset, settings.replyMode)
      debugLog(`[svc] start: botId=${settings.botId} preset=${settings.preset} replyMode=${settings.replyMode}`)

      const nextWs = new WsClient(settings, tee(logger))
      const nextBridge = new SessionBridge(ctx, settings)
      ws = nextWs
      bridge = nextBridge

      nextWs.on('message', (pkt: any) => nextBridge.handle(pkt, nextWs))
      nextWs.on('event', (pkt: any) => nextBridge.handleEvent(pkt, nextWs))
      nextWs.on('raw', (pkt: any) => {
        // 心跳回包每次都会走 raw，降噪跳过；其余未知帧（含订阅失败原因）必须可见
        const cmd = pkt?.cmd ?? ''
        if (cmd.includes('heartbeat')) return
        logger.info('raw frame: cmd=%s body=%j', cmd, pkt?.body)
        debugLog(`[raw] ${JSON.stringify(pkt)}`)
      })

      nextWs.start()
    }

    pending = pending.catch(() => {}).then(run)
    // 无人 await 时也别让 rejection 变成 unhandled
    pending.catch((err) => logger.warn('wecom: startServices failed: %s', String(err)))
    return pending
  }

  void startServices(cfg)

  const disposeWatch = scope.watch((next) => {
    cfg = next
    logger.info('wecom: settings updated, restarting services')
    startServices(next)
  })

  // 用 ctx.get 拿 connection：不做 inject 检查，plugin 的 inject 已声明它，
  // 而 get 在服务缺席时返回 undefined 而不是抛错。
  const connection = ctx.get('connection') as WecomHostConnection | undefined
  const disposeRpc = registerRpcHandler(connection, scope, () => {
    logger.info('wecom: settings updated via RPC')
  }, (message) => logger.warn(message))

  ctx.effect(() => async () => {
    logger.info('wecom plugin disposing')
    disposeWatch()
    disposeRpc()
    // 先等排队中的启停跑完，再释放，避免卸载时又建出孤儿 agent
    await pending.catch(() => {})
    await stopServices()
  }, 'wecom: lifecycle')
}