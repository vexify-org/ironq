import { createClient } from 'redis'

const DEFAULT_CONFIG = {
  url: 'redis://localhost:6379',
  queueName: 'ironq:jobs',
  deadLetterQueue: 'ironq:dead',
  maxRetries: 3,
  concurrency: 1,
}

/**
 * Create a distributed job queue backed by Redis.
 *
 * @param {object} config
 * @param {string} [config.url]         Redis connection URL
 * @param {string} [config.queueName]   Key for the pending-jobs sorted set
 * @param {string} [config.deadLetterQueue] Key for dead-letter sorted set
 * @param {number} [config.maxRetries]  Max retry attempts per job (default 3)
 * @param {number} [config.concurrency] How many jobs to run simultaneously (default 1)
 * @returns {Queue}
 */
export function createQueue(config = {}) {
  const cfg = { ...DEFAULT_CONFIG, ...config }
  let client = null
  let connected = false

  async function ensureConnected() {
    if (!connected) {
      client = client ?? (await createClient({ url: cfg.url }).connect())
      connected = true
    }
  }

  /**
   * Enqueue a job.
   * @param {string} type  Job type identifier
   * @param {object} data  Arbitrary job payload
   * @param {object} [opts]
   * @param {number} [opts.delay]   Delay in milliseconds before job is eligible
   * @param {number} [opts.priority] Higher number = higher priority (default 0)
   * @returns {Promise<string>} job id
   */
  async function enqueue(type, data = {}, opts = {}) {
    await ensureConnected()
    const id = `${Date.now()}-${Math.random().toString(36).slice(2)}`
    const score = opts.priority ?? 0
    const runAt = opts.delay ? Date.now() + opts.delay : Date.now()

    const payload = JSON.stringify({ id, type, data, status: 'pending', attempts: 0, maxRetries: cfg.maxRetries, runAt })

    // Store job metadata in a hash
    await client.hSet(`${cfg.queueName}:data`, id, payload)
    // Add to sorted set (score = priority, secondary = runAt timestamp for tie-break)
    await client.zAdd(cfg.queueName, { score, value: `${runAt}:${id}` })

    return id
  }

  /**
   * Dequeue the next eligible job.
   * @returns {Promise<object|null>} job object or null if queue is empty
   */
  async function dequeue() {
    await ensureConnected()

    const now = Date.now()

    // Get the highest-priority job that is ready to run
    const items = await client.zRangeByScore(cfg.queueName, '-inf', now, { COUNT: 1, OFFSET: 0 })
    if (!items.length) return null

    const [, id] = items[0].split(':')
    const raw = await client.hGet(`${cfg.queueName}:data`, id)
    if (!raw) {
      // Stale entry — clean up and continue
      await client.zRem(cfg.queueName, items[0])
      return dequeue()
    }

    const job = JSON.parse(raw)
    job.status = 'active'
    job.attempts += 1
    await client.hSet(`${cfg.queueName}:data`, id, JSON.stringify(job))
    await client.zRem(cfg.queueName, items[0])

    return job
  }

  /**
   * Mark a job as successfully completed and remove its data.
   * @param {string} id
   */
  async function complete(id) {
    await ensureConnected()
    await client.hDel(`${cfg.queueName}:data`, id)
  }

  /**
   * Mark a job as failed. Retries if attempts < maxRetries, otherwise moves to dead-letter queue.
   * @param {string} id
   * @param {string|Error} error
   */
  async function fail(id, error) {
    await ensureConnected()
    const raw = await client.hGet(`${cfg.queueName}:data`, id)
    if (!raw) return

    const job = JSON.parse(raw)
    job.lastError = error instanceof Error ? error.message : String(error)

    if (job.attempts < job.maxRetries) {
      // Re-enqueue with exponential-ish backoff
      const delay = Math.min(1000 * 2 ** (job.attempts - 1), 60000)
      job.runAt = Date.now() + delay
      job.status = 'pending'
      await client.hSet(`${cfg.queueName}:data`, id, JSON.stringify(job))
      await client.zAdd(cfg.queueName, { score: job.attempts, value: `${job.runAt}:${id}` })
    } else {
      // Move to dead-letter queue
      job.status = 'dead'
      await client.hSet(`${cfg.queueName}:data`, id, JSON.stringify(job))
      await client.zAdd(cfg.deadLetterQueue, { score: Date.now(), value: id })
    }
  }

  /**
   * Drain the queue and close the Redis connection.
   */
  async function close() {
    if (client) {
      await client.quit()
      client = null
      connected = false
    }
  }

  return { enqueue, dequeue, complete, fail, close, get config() { return cfg } }
}

export default { createQueue }
