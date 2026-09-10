import assert from 'node:assert/strict'
import { registerRpcHandler } from '../lib/rpc.js'

const routes = new Map()
const connection = {
  fetch: {
    register(route) {
      if (routes.has(route.path)) throw new Error(`already registered: ${route.path}`)
      routes.set(route.path, route)
      return async () => { routes.delete(route.path) }
    },
  },
}

let updated = null
let notified = 0
const scope = {
  get: () => ({ botId: 'bot-1', secret: 's3cr3t', preset: 'standard', replyMode: 'stream', sessionTtlMs: 1800000 }),
  update: async (patch) => { updated = patch },
}
const warnings = []

const dispose = registerRpcHandler(connection, scope, () => { notified += 1 }, (m) => warnings.push(m))

assert.deepEqual([...routes.keys()].sort(), ['/api/wecom-rpc/get', '/api/wecom-rpc/update'])
assert.equal(warnings.length, 0)

async function post(path, body) {
  const route = routes.get(path)
  assert.ok(route, `route missing: ${path}`)
  assert.deepEqual(route.methods, ['POST'])
  assert.equal(route.requestBody, 'buffered')
  return route.fetch(new Request(`http://dsh.internal${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }))
}

// 1. get：secret 必须脱敏
{
  const res = await post('/api/wecom-rpc/get', { type: 'client-request', rpcId: 'r1', method: 'wecom-rpc/get', payload: {} })
  assert.equal(res.status, 200)
  const json = await res.json()
  assert.equal(json.type, 'server-response')
  assert.equal(json.rpcId, 'r1')
  assert.equal(json.result.ok, true)
  assert.equal(json.result.value.secret, '***')
  assert.equal(json.result.value.botId, 'bot-1')
}

// 2. update：patch 落到 scope，回调触发
{
  const res = await post('/api/wecom-rpc/update', {
    type: 'client-request', rpcId: 'r2', method: 'wecom-rpc/update',
    payload: { args: { botId: 'bot-2', sessionTtlMs: 60000 } },
  })
  const json = await res.json()
  assert.equal(json.result.ok, true)
  assert.deepEqual(updated, { botId: 'bot-2', sessionTtlMs: 60000 })
  assert.equal(notified, 1)
}

// 3. 烂信封 → 400（无 rpcId 可回填）
{
  const res = await post('/api/wecom-rpc/get', { type: 'nope' })
  assert.equal(res.status, 400)
}

// 4. 业务抛错 → 200 + 完整错误信封（code/message/details 缺一不可）
{
  const errRoutes = new Map()
  const bad = registerRpcHandler(
    { fetch: { register(route) { errRoutes.set(route.path, route); return async () => {} } } },
    { get: () => { throw new Error('boom') }, update: async () => {} },
    () => {}, () => {},
  )
  const route = errRoutes.get('/api/wecom-rpc/get')
  const res = await route.fetch(new Request('http://dsh.internal/api/wecom-rpc/get', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'client-request', rpcId: 'r4', method: 'wecom-rpc/get', payload: {} }),
  }))
  const json = await res.json()
  assert.equal(json.result.ok, false)
  assert.equal(json.result.error.code, 'gateway/bad-request')
  assert.equal(json.result.error.message, 'boom')
  assert.deepEqual(json.result.error.details, {})
  void bad
}

// 5. 反注册
await dispose()
assert.equal(routes.size, 0)

console.log('rpc-check: OK')
