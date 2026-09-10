import z from '@deepseek-ai/schemastery'

export interface WecomSettings {
  botId: string
  secret: string
  allowFrom: string[]
  preset: string
  replyMode: 'markdown' | 'stream'
  sessionTtlMs: number
  welcomeText?: string
}

export const WecomSettingsSchema: z<WecomSettings> = z.object({
  botId: z.string().default('').description(
    '企微智能机器人 Bot ID（API模式→长连接页面获取）。⚠️ 同一 BotID 仅允许 1 个有效长连接，多实例会互踢',
  ),
  secret: z.string().role('secret').default('').description(
    'Bot Secret（仅创建时显示一次，丢失需重新生成）',
  ),
  allowFrom: z.array(z.string()).default([]).description(
    '允许的 userid/chatid 白名单，留空或 ["*"] 则表示允许所有',
  ),
  preset: z.string().default('default').description(
    'Agent 使用的 dsh preset 名称',
  ),
  replyMode: z.union([z.const('markdown'), z.const('stream')]).default('stream').description(
    '回复模式：markdown 一次性返回 / stream 流式打字机',
  ),
  sessionTtlMs: z.number().default(1800000).min(60000).description(
    '会话空闲超时（毫秒）',
  ),
  welcomeText: z.string().description(
    '用户进入会话时的欢迎语（markdown），留空使用默认文案',
  ),
})