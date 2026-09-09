// @dsh-version 0.1.2-rc.1
import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-agent-presets'

import type {} from '@deepseek-ai/dsh-sandbox-policy'
import { WsClient } from './ws.js'
import { SessionBridge } from './bridge.js'

export const name = 'wecom'

export interface Config {
  /** 企微智能机器人 Bot ID（API 模式 → 长连接页面获取）。同一 BotID 仅允许 1 个有效长连接，多实例会互踢。 */
  botId: string
  /** Bot Secret（仅创建时显示一次，丢失需重新生成），role('secret') 脱敏。 */
  secret: string
  /** 允许的 userid/chatid 白名单，留空拒绝所有消息。 */
  allowFrom: string[]
  /** Agent 使用的 dsh preset 名称。 */
  preset: string
  /** 回复模式：markdown 一次性返回 / stream 流式打字机。 */
  replyMode: 'markdown' | 'stream'
  /** 会话空闲超时（毫秒）。 */
  sessionTtlMs: number
  /** 用户进入会话时的欢迎语（markdown），留空使用默认文案。 */
  welcomeText?: string
}

export const Config: z<Config> = z.object({
  botId: z.string().required().description(
    '企微智能机器人 Bot ID（API模式→长连接页面获取）。⚠️ 同一 BotID 仅允许 1 个有效长连接，多实例会互踢',
  ),

  secret: z.string().role('secret').required().description(
    'Bot Secret（仅创建时显示一次，丢失需重新生成）',
  ),

  allowFrom: z.array(z.string()).default([]).description(
    '允许的 userid/chatid 白名单，留空拒绝所有消息',
  ),

  preset: z.string().default('default').description(
    'Agent 使用的 dsh preset 名称',
  ),

  replyMode: z.union(['markdown', 'stream']).default('stream').description(
    '回复模式：markdown 一次性返回 / stream 流式打字机',
  ),

  sessionTtlMs: z.number().default(1800000).min(60000).description(
    '会话空闲超时（毫秒）',
  ),

  welcomeText: z.string().description(
    '用户进入会话时的欢迎语（markdown），留空使用默认文案',
  ),
})

/** 声明的宿主服务（base bundle 默认全部装载）。 */
export const inject = ['agents', 'agentPresets', 'agentDefaultModel', 'sandboxPolicy', 'logger'] as const

export function apply(ctx: Context, config: Config) {
  const logger = ctx.logger('wecom')
  logger.info('wecom plugin loaded, botId=%s, preset=%s, replyMode=%s',
    config.botId, config.preset, config.replyMode)   // 严禁打印 secret

  const ws = new WsClient(config, logger)
  const bridge = new SessionBridge(ctx, config)

  ws.on('message', (pkt) => bridge.handle(pkt, ws))
  ws.on('event', (pkt) => bridge.handleEvent(pkt, ws))
  ws.on('raw', (pkt) => logger.debug('raw frame: %o', pkt))   // 订阅失败时兜底打印

  ws.start()

  ctx.effect(() => () => {
    logger.info('wecom plugin disposing, closing ws connection')
    ws.stop()
    bridge.dispose()
  }, 'wecom: lifecycle')
}