// 联调期诊断日志：独立于宿主 logger 配置，把 wecom 关键日志镜像到文件，
// 便于在不依赖 host 终端的情况下核对协议字段（AGENTS.md「需现场核对的字段」）。
import { appendFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

const file = join(homedir(), '.dsh', 'wecom-debug.log')

export function debugLog(msg: string): void {
  void appendFile(file, `[${new Date().toISOString()}] ${msg}\n`).catch(() => {})
}

/** 打包一个 logger 形状的对象，把 info/warn/error/debug 同步镜像到 debugLog。 */
export function tee(logger: object): {
  info: (...args: unknown[]) => unknown
  warn: (...args: unknown[]) => unknown
  error: (...args: unknown[]) => unknown
  debug: (...args: unknown[]) => unknown
} {
  const mirror = (method: 'info' | 'warn' | 'error' | 'debug') =>
    (...args: unknown[]) => {
      debugLog(`[${method}] ${args.map((a) => typeof a === 'string' ? a : JSON.stringify(a)).join(' ')}`)
      return (logger as any)[method]?.(...args)
    }
  return { info: mirror('info'), warn: mirror('warn'), error: mirror('error'), debug: mirror('debug') }
}
