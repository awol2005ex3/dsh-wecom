/**
 * 企业微信机器人插件 · 浏览器端 bundle 模块体。
 *
 * 构建契约同 dsh-role-manager/src/client.ts：
 * 本文件经 tsc 编译后，由 scripts/wrap-client.mjs 包上闭包工厂外壳。
 *
 * 功能：通过 ctx.connection.rpc 调用宿主端 /api/wecom-rpc/* 端点，提供
 * wecom 插件的配置面板（botId/secret/preset/replyMode 等）。
 *
 * 挂载策略（参考 dsh-logo-custom 的 settings 插槽写法）：
 * 1. 主路径：用浏览器端 slots 服务把面板注册进 dsh 自身设置页
 *    「设置 → 插件 → 插件配置」（slot `settings.plugin.item`，key 必须等于
 *    宿主 ctx.settings.register 的命名空间 'wecom'，tab-store 按命名空间交集派发）；
 *    失败再退 `settings.section` 独立分区。面板经 React 壳组件挂载真实 DOM。
 * 2. 回退：slots/React 不可用时保留旧行为——侧边栏启动器 + 浮动面板。
 *
 * ⚠ 通道固定为 /api：宿主端是 connection.fetch.register() 注册的 exact 路由
 * （0.1.5 起 rpc.handle 对插件失效），只有 /api 前缀才会被路由表命中。
 */

const PLUGIN_ID = 'dsh-plugin-wecom'
const RPC_CHANNEL = '/api'
const RPC_PREFIX = 'wecom-rpc/'

declare const module: { exports: unknown }
declare function require(id: string): unknown

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
  /** 0.1.5 起宿主必须回 code / message / details，否则浏览器校验信封失败。 */
  error?: { code: string; message: string; details?: unknown }
}

async function callRpc(conn: any, endpoint: string, args?: Record<string, unknown>): Promise<unknown> {
  const res = (await conn.rpc.call(RPC_CHANNEL, `${RPC_PREFIX}${endpoint}`, args ? { args } : {})) as RpcResponse
  if (!res || res.ok !== true) {
    throw new Error(res?.error?.message ?? 'rpc error')
  }
  return res.value
}

/* ── 配置面板 ── */

/** 设置页内嵌模式：随卡片容器布局，继承对话框配色。 */
const PANEL_CSS = [
  'position:relative;width:100%;max-width:520px;box-sizing:border-box;',
  'overflow:auto;background:transparent;color:inherit;border:1px solid rgba(127,127,127,.25);',
  'border-radius:12px;font:13px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;',
  'padding:14px;margin:8px 0;',
].join('')

/** 浮动面板回退模式：固定在视口左下。 */
const PANEL_FLOAT_CSS = [
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
  preset: string
  replyMode: 'markdown' | 'stream'
  sessionTtlMs: number
  welcomeText?: string
}

interface PanelHandle {
  root: HTMLElement
  refresh: () => void
  /** true = 浮动面板（显示关闭按钮）；false = 设置页内嵌卡片。 */
  setChrome: (floating: boolean) => void
}

function buildPanel(conn: any): PanelHandle {
  const status = el('div', { style: 'margin:4px 0;min-height:16px;font-size:12px;' })
  let savedSecret = ''

  function say(node: HTMLElement, text: string, ok: boolean): void {
    node.textContent = text
    node.style.color = ok ? '#1a7f37' : '#cf222e'
  }

  /* 表单字段 */
  const botIdInput = el('input', { placeholder: '必填', style: FIELD_CSS }) as HTMLInputElement
  const secretInput = el('input', { type: 'password', placeholder: '留空则不修改', style: FIELD_CSS }) as HTMLInputElement

  const presetInput = el('input', { placeholder: 'standard', style: FIELD_CSS }) as HTMLInputElement
  const sessionTtlInput = el('input', { type: 'number', placeholder: '1800000', min: '60000', style: FIELD_CSS }) as HTMLInputElement
  const welcomeTextInput = el('textarea', { placeholder: '用户进入会话时的欢迎语（支持 Markdown），留空使用默认文案', style: FIELD_CSS + 'min-height:60px;resize:vertical;' }) as HTMLTextAreaElement

  /* 单选项：回复模式 */
  const streamRadio = el('input', { type: 'radio', name: 'replyMode', value: 'stream', id: 'rm-stream' }) as HTMLInputElement
  const markdownRadio = el('input', { type: 'radio', name: 'replyMode', value: 'markdown', id: 'rm-markdown' }) as HTMLInputElement

  /* 表单保存（保存反馈放在按钮旁，面板顶部状态在设置页里可能滚出视口） */
  const saveBtn = el('button', {
    type: 'button',
    textContent: '保存配置',
    style: 'margin-top:12px;padding:6px 16px;font-size:13px;cursor:pointer;border:1px solid #1f6feb;background:#1f6feb;color:#fff;border-radius:6px;',
  }) as HTMLButtonElement
  const saveStatus = el('span', { style: 'margin-left:10px;font-size:12px;' })

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

  async function refresh(showMsg = true): Promise<void> {
    try {
      const cfg = (await callRpc(conn, 'get', {})) as WecomConfig
      setFormValues(cfg)
      if (showMsg) say(status, '✓ 已加载', true)
    } catch (err) {
      if (showMsg) say(status, err instanceof Error ? `加载失败：${err.message}` : '加载失败', false)
    }
  }

  saveBtn.addEventListener('click', async () => {
    const patch = getFormValues()
    if (!(patch.botId as string)) {
      say(saveStatus, 'botId 为必填项', false)
      return
    }
    if (!savedSecret && !(patch.secret as string)) {
      say(saveStatus, 'secret 为必填项', false)
      return
    }
    saveBtn.disabled = true
    saveBtn.textContent = '保存中…'
    try {
      await callRpc(conn, 'update', patch)
      say(saveStatus, '✓ 已保存', true)
      void refresh(false)
    } catch (err) {
      say(saveStatus, err instanceof Error ? `保存失败：${err.message}` : '保存失败', false)
    } finally {
      saveBtn.disabled = false
      saveBtn.textContent = '保存配置'
    }
  })

  /* 关闭按钮（仅浮动模式显示） */
  const closeBtn = el('button', {
    type: 'button',
    textContent: '✕',
    ariaLabel: '关闭',
    title: '关闭',
    style: 'margin-left:8px;padding:2px 8px;font-size:13px;line-height:1;cursor:pointer;border:1px solid #d0d7de;background:#f6f8fa;color:#57606a;border-radius:6px;display:none;',
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

    el('div', { style: 'display:flex;align-items:center;' }, [saveBtn, saveStatus]),
  ])

  closeBtn.addEventListener('click', () => { root.style.display = 'none' })

  function setChrome(floating: boolean): void {
    root.style.cssText = floating ? PANEL_FLOAT_CSS : PANEL_CSS
    closeBtn.style.display = floating ? '' : 'none'
    if (!floating) root.style.display = ''
  }

  void refresh()
  return { root, refresh, setChrome }
}

/* ── 挂载进 dsh 设置页（主路径） ── */

/** 宿主 ctx.settings.register 用的命名空间；必须与 settings.plugin.item 的 key 一致。 */
const SETTINGS_NS = 'wecom'
const SECTION_LABEL = '企微机器人'

function createSettingsComponent(React: any, handle: PanelHandle): any {
  return function WecomSettings() {
    const ref = React.useRef(null)
    React.useEffect(function () {
      const node = ref.current as HTMLElement | null
      if (!node) return
      handle.setChrome(false)
      node.appendChild(handle.root)
      handle.refresh()
    }, [])
    return React.createElement('div', { ref, 'data-dsh-wecom-settings': 'true' })
  }
}

function createNavIcon(React: any): any {
  return function NavIcon(props: Record<string, unknown>) {
    return React.createElement(
      'svg',
      Object.assign({ width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, 'aria-hidden': true }, props),
      React.createElement('path', { d: 'M21 11.5a8.38 8.38 0 0 1-8.5 8.5 9.26 9.26 0 0 1-3.8-.8L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 8.5-8.5 8.38 8.38 0 0 1 8.5 8.5z' }),
    )
  }
}

/**
 * 把面板注册进 dsh 设置页。优先 `settings.plugin.item`（卡片出现在
 * 设置 → 插件 → 插件配置，key 命中宿主注册的 'wecom' 命名空间才会被派发），
 * 退而求其次 `settings.section`（设置导航里的独立分区）。
 * ctx.inject 的服务回调在服务已就绪时同步触发；若稍后才就绪，
 * onSlotted 负责撤掉已挂出的回退启动器。
 */
function tryRegisterSettingsSlot(ctx: any, handle: PanelHandle, onSlotted: () => void): boolean {
  let registered = false
  const register = (slots: any): void => {
    if (registered || !slots) return
    let React: any
    try { React = require('react') } catch { return }
    if (!React || typeof React.createElement !== 'function') return
    const Comp = createSettingsComponent(React, handle)

    const tryOne = (slotName: string, opts: Record<string, unknown>) => {
      try {
        if (typeof slots.inject === 'function') {
          slots.inject(slotName, function () {
            return slots.register({ name: slotName, ...opts }, Comp)
          })
          return true
        }
        slots.register({ name: slotName, ...opts }, Comp)
        return true
      } catch {
        return false
      }
    }

    const ok = tryOne('settings.plugin.item', { key: SETTINGS_NS, label: SECTION_LABEL })
      || tryOne('settings.section', { id: PLUGIN_ID, label: SECTION_LABEL, title: SECTION_LABEL, icon: createNavIcon(React) })
    if (ok) {
      registered = true
      onSlotted()
    }
  }

  try {
    if (typeof ctx?.inject === 'function') ctx.inject(['slots'], (scope: any) => { register(scope?.slots) })
    if (!registered) register(ctx?.get?.('slots') ?? ctx?.slots)
  } catch { /* 回退到侧边栏启动器 */ }
  return registered
}

/* ── 回退挂载：侧边栏启动器 + 浮动面板 ── */

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

function mountLauncher(launcher: HTMLButtonElement): () => void {
  let dead = false
  const styleSidebar = (): void => { launcher.style.cssText = SIDEBAR_BTN_CSS }
  const styleFloat = (): void => { launcher.style.cssText = FLOAT_BTN_CSS }

  function sidebarHost(): Element | null {
    return doc.querySelector(`[data-slot="${SIDEBAR_SLOT}"]`)
  }
  function ensureMounted(): void {
    if (dead) return
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
  return () => { dead = true; observer.disconnect(); launcher.remove() }
}

/* ── 客户端插件契约 ── */

function apply(ctx: any): void {
  const conn = ctx.connection
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return
  if (win.__dshWecomMounted === true) return
  win.__dshWecomMounted = true

  const handle = buildPanel(conn)
  const panel = handle.root
  panel.id = 'dsh-wecom-panel'

  let disposeLauncher: (() => void) | undefined
  if (tryRegisterSettingsSlot(ctx, handle, () => { disposeLauncher?.() })) return

  // 回退：设置页插槽不可用时保留侧边栏启动器 + 浮动面板
  handle.setChrome(true)
  panel.style.display = 'none'
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

  disposeLauncher = mountLauncher(launcher)
}

module.exports = { name: PLUGIN_ID, inject: ['connection'], apply }