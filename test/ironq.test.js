import { describe, it } from 'node:test'
import assert from 'node:assert'
import { createQueue } from '../src/index.js'

describe('ironq API surface', () => {
  it('createQueue returns an object with required methods', () => {
    const queue = createQueue({ url: 'redis://localhost:6379' })
    assert.ok(typeof queue.enqueue === 'function', 'enqueue should be a function')
    assert.ok(typeof queue.dequeue === 'function', 'dequeue should be a function')
    assert.ok(typeof queue.complete === 'function', 'complete should be a function')
    assert.ok(typeof queue.fail === 'function', 'fail should be a function')
    assert.ok(typeof queue.close === 'function', 'close should be a function')
    assert.ok(queue.config, 'config should be exposed')
  })

  it('createQueue accepts custom config', () => {
    const queue = createQueue({
      url: 'redis://localhost:6380',
      queueName: 'custom:queue',
      maxRetries: 5,
    })
    assert.strictEqual(queue.config.url, 'redis://localhost:6380')
    assert.strictEqual(queue.config.queueName, 'custom:queue')
    assert.strictEqual(queue.config.maxRetries, 5)
  })
})
