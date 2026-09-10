/**
 * 用最小假 cordis 上下文驱动 SessionBridge.ensureAgent，覆盖 session 冲突的三条分支：
 *   1. 进程内存活 agent → 借用，不重复 create
 *   2. create 报 already exists 且会话残留无 agent → 换新 id 重建
 *   3. create 报 already exists 但查无会话 → 抛错
 */
import assert from 'node:assert/strict'
import { SessionBridge } from '../lib/bridge.js'

function makeCtx({ agents, sessions }) {
  return {
    logger: () => ({ info() {}, warn() {}, error() {}, debug() {} }),
    agents,
    agentPresets: { resolve: async (id) => ({ id }), mount: async () => {} },
    agentDefaultModel: { currentSelection: () => ({ provider: 'p', model: 'm' }) },
    sandboxPolicy: { resolve: () => ({ workspaceRoot: process.cwd() }) },
    get: (name) => (name === 'sessions' ? sessions : undefined),
    on: () => () => {},
  }
}

const SID = 'wecom:single:user-1'

// 1. 借用存活 agent
{
  const live = { session: { id: SID }, followup() {} }
  const agents = {
    get: (id) => (id === SID ? live : undefined),
    create: async () => { throw new Error('create must not be called') },
  }
  const bridge = new SessionBridge(makeCtx({ agents, sessions: undefined }), { preset: 'standard' })
  const handle = await bridge.ensureAgent(SID)
  assert.equal(handle.agent, live, '应借用同一个 agent')
  assert.equal(await bridge.ensureAgent(SID), handle, '第二次应命中缓存')
  await handle.dispose()  // 借用句柄的 dispose 是空实现，不能动别人的会话
  assert.equal(agents.get(SID), live, '借用的 agent 仍然存活')
  await bridge.dispose()
  assert.equal(agents.get(SID), live, 'bridge 释放后借用的 agent 仍在')
}

// 2. 会话残留但无 agent → 换新 id
{
  const created = []
  const agents = {
    get: () => undefined,
    create: async (opts) => {
      created.push(opts.sessionId)
      if (opts.sessionId === SID) throw new Error(`session "${SID}" already exists`)
      return { agent: { session: { id: opts.sessionId } }, dispose: async () => {} }
    },
  }
  const sessions = { get: (id) => (id === SID ? { id } : undefined) }
  const bridge = new SessionBridge(makeCtx({ agents, sessions }), { preset: 'standard' })
  const handle = await bridge.ensureAgent(SID)
  assert.equal(created.length, 2, '应重试一次')
  assert.ok(handle.agent.session.id.startsWith(`${SID}#`), `新会话 id 应带后缀：${handle.agent.session.id}`)
  await bridge.dispose()
}

// 3. 无会话可查 → 抛错
{
  const agents = {
    get: () => undefined,
    create: async () => { throw new Error(`session "${SID}" already exists`) },
  }
  const bridge = new SessionBridge(makeCtx({ agents, sessions: { get: () => undefined } }), { preset: 'standard' })
  await assert.rejects(() => bridge.ensureAgent(SID), /无法恢复/)
}

// 4. 正常创建 → 拥有所有权，dispose 会释放
{
  let disposed = 0
  const agents = {
    get: () => undefined,
    create: async (opts) => ({
      agent: { session: { id: opts.sessionId } },
      dispose: async () => { disposed += 1 },
    }),
  }
  const bridge = new SessionBridge(makeCtx({ agents, sessions: { get: () => undefined } }), { preset: 'standard' })
  await bridge.ensureAgent(SID)
  await bridge.dispose()
  assert.equal(disposed, 1, '自有的 agent 必须被释放')
}

console.log('check-ensure-agent: OK')
