import { describe, it, beforeEach, afterEach, mock } from 'node:test'
import assert from 'node:assert'

// Mock redis client
const store = { hash: {}, zset: [] }

function createMockClient() {
  const h = {}
  const z = []

  return {
    connect: mock.fn(() => Promise.resolve()),
    quit: mock.fn(() => Promise.resolve()),
    hSet: mock.fn(async (key, field, value) => {
      h[key] = h[key] || {}
      h[key][field] = value
      return 'OK'
    }),
    hGet: mock.fn(async (key, field) => {
      return (h[key] && h[key][field]) ?? null
    }),
    hDel: mock.fn(async (key, field) => {
      delete (h[key] || {})[field]
      return 1
    }),
    zAdd: mock.fn(async (key, item) => {
      z.push({ key, ...item })
      return 1
    }),
    zRem: mock.fn(async (key, value) => {
      const idx = z.findIndex(e => e.key === key && e.value === value)
      if (idx !== -1) z.splice(idx, 1)
      return 1
    }),
    zRangeByScore: mock.fn(async (key, min, max, opts) => {
      const now = Date.now()
      return z
        .filter(e => e.key === key && Number(e.value.split(':')[0]) <= now)
        .slice(opts?.OFFSET ?? 0, (opts?.OFFSET ?? 0) + (opts?.COUNT ?? 10))
        .map(e => e.value)
    }),
    _h: h,
    _z: z,
  }
}

// We'll test the public API surface by importing the module
// (The real redis import makes full integration tests require a live Redis instance)
// Here we do smoke / structural tests.

describe('ironq API surface', () => {
  it('createQueue returns an object with required methods', () => {
    // Dynamic import works in ESM test context
    const { createQueue } = (0, eval)('(await import("./src/index.js"))')
    const queue = createQueue({ url: 'redis://localhost:6379' })
    assert.ok(typeof queue.enqueue === 'function', 'enqueue should be a function')
    assert.ok(typeof queue.dequeue === 'function', 'dequeue should be a function')
    assert.ok(typeof queue.complete === 'function', 'complete should be a function')
    assert.ok(typeof queue.fail === 'function', 'fail should be a function')
    assert.ok(typeof queue.close === 'function', 'close should be a function')
    assert.ok(queue.config, 'config should be exposed')
  })
})
