/**
 * 企业微信机器人插件 · 浏览器端 bundle 模块体。
 *
 * 构建契约同 dsh-role-manager/src/client.ts：
 * 本文件经 tsc 编译后，由 scripts/wrap-client.mjs 包上闭包工厂外壳。
 *
 * 功能：通过 ctx.connection.rpc 调用宿主端 /rpc 端点，提供
 * wecom 插件的配置面板（botId/secret/allowFrom/preset/replyMode 等）。
 * 启动器挂入侧边栏 [data-slot="sidebar.footer.action"] 插槽，缺失时回退为浮动按钮。
 */

const PLUGIN_ID = 'dsh-plugin-wecom'
const RPC_CHANNEL = '/wecom-rpc'
const RPC_PREFIX = 'wecom/'

declare const module: { exports: unknown }

const win = window as unknown as { __dshWecomMounted?: boolean }
const doc = document

/* ── 工具函数 ── */

type ElProps = { style?: string; [key: string]: unknown }

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props?: ElProps,
  children?: (Node | string)[],
): HTMLElementTagNameMap[K] {
  const node = doc.createElement(tag)
  if (props) {
    for (const [key, value] of Object.entries(props)) {
      if (key === 'style') node.setAttribute('style', value as string)
      else (node as unknown as Record<string, unknown>)[key] = value
    }
  }
  if (children) {
    for (const c of children) node.append(typeof c === 'string' ? doc.createTextNode(c) : c)
  }
  return node
}

/* ── RPC 调用 ── */

interface RpcResponse {
  ok: boolean
  value?: unknown
  error?: { message: string }
}

async function callRpc(conn: any, endpoint: string, args?: Record<string, unknown>): Promise<unknown> {
  const res = (await conn.rpc.call(RPC_CHANNEL, `${RPC_PREFIX}${endpoint}`, args ? { args } : {})) as RpcResponse
  if (!res || res.ok !== true) {
    throw new Error(res?.error?.message ?? 'rpc error')
  }
  return res.value
}

/* ── 配置面板 ── */

const PANEL_CSS = [
  'position:fixed;left:16px;bottom:64px;z-index:2147483646;width:380px;max-height:80vh;',
  'overflow:auto;background:#fff;color:#1f2328;border:1px solid #d0d7de;border-radius:12px;',
  'box-shadow:0 8px 28px rgba(0,0,0,.18);font:13px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;',
  'padding:16px;',
].join('')

const FIELD_CSS = 'width:100%;box-sizing:border-box;padding:5px 8px;margin-bottom:4px;border:1px solid #d0d7de;border-radius:6px;font-size:13px;'
const LABEL_CSS = 'display:block;margin-top:10px;font-weight:600;color:#24292f;'
const HINT_CSS = 'display:block;font-size:11px;color:#57606a;margin-bottom:4px;'

interface WecomConfig {
  botId: string
  secret: string
  allowFrom: string[]
  preset: string
  replyMode: 'markdown' | 'stream'
  sessionTtlMs: number
  welcomeText?: string
}

function buildPanel(conn: any): { root: HTMLElement; refresh: () => void } {
  const status = el('div', { style: 'margin:4px 0;min-height:16px;font-size:12px;' })
  let savedSecret = ''

  /* 表单字段 */
  const botIdInput = el('input', { placeholder: '必填', style: FIELD_CSS }) as HTMLInputElement
  const secretInput = el('input', { type: 'password', placeholder: '留空则不修改', style: FIELD_CSS }) as HTMLInputElement

  const presetInput = el('input', { placeholder: 'standard', style: FIELD_CSS }) as HTMLInputElement
  const sessionTtlInput = el('input', { type: 'number', placeholder: '1800000', min: '60000', style: FIELD_CSS }) as HTMLInputElement
  const welcomeTextInput = el('textarea', { placeholder: '用户进入会话时的欢迎语（支持 Markdown），留空使用默认文案', style: FIELD_CSS + 'min-height:60px;resize:vertical;' }) as HTMLTextAreaElement

  /* 单选项：回复模式 */
  const streamRadio = el('input', { type: 'radio', name: 'replyMode', value: 'stream', id: 'rm-stream' }) as HTMLInputElement
  const markdownRadio = el('input', { type: 'radio', name: 'replyMode', value: 'markdown', id: 'rm-markdown' }) as HTMLInputElement

  /* 表单保存 */
  const saveBtn = el('button', {
    textContent: '保存配置',
    style: 'margin-top:12px;padding:6px 16px;font-size:13px;cursor:pointer;border:1px solid #1f6feb;background:#1f6feb;color:#fff;border-radius:6px;',
  }) as HTMLButtonElement

  function getFormValues(): Record<string, unknown> {
    const patch: Record<string, unknown> = {
      botId: botIdInput.value.trim(),

      preset: presetInput.value.trim() || 'default',
      replyMode: streamRadio.checked ? 'stream' : 'markdown',
      sessionTtlMs: parseInt(sessionTtlInput.value, 10) || 1800000,
      welcomeText: welcomeTextInput.value.trim() || undefined,
    }
    const secret = secretInput.value
    if (secret && secret !== savedSecret) {
      patch.secret = secret
    }
    return patch
  }

  function setFormValues(cfg: WecomConfig): void {
    botIdInput.value = cfg.botId || ''
    savedSecret = cfg.secret || ''
    secretInput.value = ''
    secretInput.placeholder = cfg.secret === '***' ? '已配置（输入新值覆盖）' : '必填'

    presetInput.value = cfg.preset || ''
    if (cfg.replyMode === 'markdown') markdownRadio.checked = true; else streamRadio.checked = true
    sessionTtlInput.value = String(cfg.sessionTtlMs ?? 1800000)
    welcomeTextInput.value = cfg.welcomeText ?? ''
  }

  async function refresh(): Promise<void> {
    try {
      const cfg = (await callRpc(conn, 'get', {})) as WecomConfig
      setFormValues(cfg)
      status.textContent = '✓ 已加载'
      status.style.color = '#1a7f37'
    } catch (err) {
      status.textContent = err instanceof Error ? `加载失败：${err.message}` : '加载失败'
      status.style.color = '#cf222e'
    }
  }

  saveBtn.addEventListener('click', async () => {
    try {
      const patch = getFormValues()
      if (!(patch.botId as string)) {
        status.textContent = 'botId 为必填项'
        status.style.color = '#cf222e'
        return
      }
      if (!savedSecret && !(patch.secret as string)) {
        status.textContent = 'secret 为必填项'
        status.style.color = '#cf222e'
        return
      }
      await callRpc(conn, 'update', patch)
      status.textContent = '✓ 已保存'
      status.style.color = '#1a7f37'
      void refresh()
    } catch (err) {
      status.textContent = err instanceof Error ? `保存失败：${err.message}` : '保存失败'
      status.style.color = '#cf222e'
    }
  })

  /* 关闭按钮 */
  const closeBtn = el('button', {
    type: 'button',
    textContent: '✕',
    ariaLabel: '关闭',
    title: '关闭',
    style: 'margin-left:8px;padding:2px 8px;font-size:13px;line-height:1;cursor:pointer;border:1px solid #d0d7de;background:#f6f8fa;color:#57606a;border-radius:6px;',
  }) as HTMLButtonElement

  const root = el('div', { style: PANEL_CSS }, [
    el('div', { style: 'display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;' }, [
      el('div', { style: 'font-weight:700;font-size:14px;', textContent: '⚙ 企微机器人配置' }),
      closeBtn,
    ]),
    status,

    /* botId */
    el('label', { style: LABEL_CSS, textContent: 'Bot ID' }),
    el('span', { style: HINT_CSS, textContent: '企微智能机器人 Bot ID（API 模式 → 长连接页面获取）' }),
    botIdInput,

    /* secret */
    el('label', { style: LABEL_CSS, textContent: 'Bot Secret' }),
    el('span', { style: HINT_CSS, textContent: '仅创建时显示一次，丢失需重新生成。保持为空则不修改当前密钥。' }),
    secretInput,


    /* preset */
    el('label', { style: LABEL_CSS, textContent: 'Preset' }),
    el('span', { style: HINT_CSS, textContent: 'Agent 使用的 dsh preset 名称' }),
    presetInput,

    /* replyMode */
    el('label', { style: LABEL_CSS, textContent: '回复模式' }),
    el('div', { style: 'margin:4px 0;' }, [
      el('label', { style: 'margin-right:16px;cursor:pointer;' }, [
        streamRadio, el('span', { textContent: ' 流式（stream）' }),
      ]),
      el('label', { style: 'cursor:pointer;' }, [
        markdownRadio, el('span', { textContent: ' 一次性（markdown）' }),
      ]),
    ]),

    /* sessionTtlMs */
    el('label', { style: LABEL_CSS, textContent: '会话超时（毫秒）' }),
    el('span', { style: HINT_CSS, textContent: '最小 60000（1 分钟），默认 1800000（30 分钟）' }),
    sessionTtlInput,

    /* welcomeText */
    el('label', { style: LABEL_CSS, textContent: '欢迎语' }),
    el('span', { style: HINT_CSS, textContent: '用户进入会话时展示的 Markdown 文本，留空使用默认文案' }),
    welcomeTextInput,

    saveBtn,
  ])

  closeBtn.addEventListener('click', () => { root.style.display = 'none' })

  void refresh()
  return { root, refresh }
}

/* ── 客户端插件契约 ── */

const SIDEBAR_SLOT = 'sidebar.footer.action'
const SIDEBAR_BTN_CSS =
  'display:flex;align-items:center;gap:6px;width:100%;box-sizing:border-box;' +
  'margin:4px 0;padding:8px 10px;font-size:13px;cursor:pointer;' +
  'border:1px solid rgba(127,127,127,.25);background:transparent;color:inherit;' +
  'border-radius:8px;'
const FLOAT_BTN_CSS =
  'position:fixed;left:16px;bottom:16px;z-index:2147483647;padding:9px 14px;' +
  'font-size:13px;background:#1f6feb;color:#fff;border:none;border-radius:8px;' +
  'box-shadow:0 2px 8px rgba(0,0,0,.3);margin:0;'

function mountLauncher(launcher: HTMLButtonElement): void {
  const styleSidebar = (): void => { launcher.style.cssText = SIDEBAR_BTN_CSS }
  const styleFloat = (): void => { launcher.style.cssText = FLOAT_BTN_CSS }

  function sidebarHost(): Element | null {
    return doc.querySelector(`[data-slot="${SIDEBAR_SLOT}"]`)
  }
  function ensureMounted(): void {
    const host = sidebarHost()
    if (host) {
      if (launcher.parentElement !== host) { host.append(launcher); styleSidebar() }
    } else if (launcher.parentElement !== doc.body) {
      doc.body.append(launcher); styleFloat()
    }
  }
  ensureMounted()
  const observer = new MutationObserver(() => ensureMounted())
  observer.observe(doc.documentElement, { childList: true, subtree: true })
}

function apply(ctx: any): void {
  const conn = ctx.connection
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return
  if (win.__dshWecomMounted === true) return
  win.__dshWecomMounted = true

  const handle = buildPanel(conn)
  const panel = handle.root
  panel.style.display = 'none'
  panel.id = 'dsh-wecom-panel'
  doc.body.append(panel)

  const launcher = el('button', {
    textContent: '⚙ 企微',
    id: 'dsh-wecom-launcher',
  }) as HTMLButtonElement

  launcher.addEventListener('click', () => {
    if (panel.style.display === 'none') {
      panel.style.display = 'block'
      handle.refresh()
    } else {
      panel.style.display = 'none'
    }
  })

  mountLauncher(launcher)
}

module.exports = { name: PLUGIN_ID, inject: ['connection'], apply }