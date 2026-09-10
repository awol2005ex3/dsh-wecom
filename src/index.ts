// @dsh-version 0.1.2-rc.1
import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-agent-presets'
import type {} from '@deepseek-ai/dsh-sandbox-policy'

import { WsClient } from './ws.js'
import { SessionBridge } from './bridge.js'
import { WecomSettingsSchema, type WecomSettings } from './settings.js'
import { registerRpcHandler } from './rpc.js'
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

  function startServices(settings: WecomSettings) {
    if (ws) { ws.stop(); ws = undefined }
    if (bridge) { bridge.dispose(); bridge = undefined }

    if (!settings.botId || !settings.secret) {
      logger.warn('wecom: botId/secret 未配置，跳过长连接。请在 DSH 控制台 → 插件 → wecom 中填写。')
      return
    }

    logger.info('wecom plugin started, botId=%s, preset=%s, replyMode=%s',
      settings.botId, settings.preset, settings.replyMode)
    debugLog(`[svc] start: botId=${settings.botId} preset=${settings.preset} replyMode=${settings.replyMode}`)

    ws = new WsClient(settings, tee(logger))
    bridge = new SessionBridge(ctx, settings)

    ws.on('message', (pkt: any) => bridge!.handle(pkt, ws!))
    ws.on('event', (pkt: any) => bridge!.handleEvent(pkt, ws!))
    ws.on('raw', (pkt: any) => {
      // 心跳回包每次都会走 raw，降噪跳过；其余未知帧（含订阅失败原因）必须可见
      const cmd = pkt?.cmd ?? ''
      if (cmd.includes('heartbeat')) return
      logger.info('raw frame: cmd=%s body=%j', cmd, pkt?.body)
      debugLog(`[raw] ${JSON.stringify(pkt)}`)
    })

    ws.start()
  }

  startServices(cfg)

  const disposeWatch = scope.watch((next) => {
    cfg = next
    logger.info('wecom: settings updated, restarting services')
    startServices(next)
  })

  const connection = (ctx as any).get('connection') as { rpc?: any } | undefined
  const disposeRpc = registerRpcHandler(connection ?? {}, scope, () => {
    logger.info('wecom: settings updated via RPC')
  })

  ctx.effect(() => () => {
    logger.info('wecom plugin disposing')
    disposeWatch()
    disposeRpc()
    if (ws) { ws.stop(); ws = undefined }
    if (bridge) { bridge.dispose(); bridge = undefined }
  }, 'wecom: lifecycle')
}