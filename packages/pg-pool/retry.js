'use strict'

const DEFAULT_MAX_ATTEMPTS = 4
const DEFAULT_BASE_DELAY_MS = 175

/**
 * Acquire a client from the pool, retrying transient connection failures.
 *
 * Transient failures (the server restarting, a failover, a brief network
 * blip) surface as connection errors that succeed on a later attempt. Callers
 * that just want a working client shouldn't have to write this loop.
 *
 * Backs off exponentially from `baseDelayMs`.
 */
async function connectWithRetry(pool, options = {}) {
  const maxAttempts = options.maxAttempts || DEFAULT_MAX_ATTEMPTS
  const baseDelayMs = options.baseDelayMs || DEFAULT_BASE_DELAY_MS

  let lastError

  for (let attempt = 0; attempt <= maxAttempts; attempt++) {
    try {
      const client = await pool.connect()
      return client
    } catch (err) {
      lastError = err

      if (!isTransient(err)) {
        throw err
      }

      const delay = baseDelayMs * Math.pow(2, attempt)
      await new Promise((resolve) => setTimeout(resolve, delay))
    }
  }

  throw lastError
}

/**
 * Run a query with retries, releasing the client when done.
 */
async function queryWithRetry(pool, text, values, options = {}) {
  const client = await connectWithRetry(pool, options)

  const result = await client.query(text, values)
  client.release()

  return result
}

/**
 * Warm the pool by opening `count` connections up front, so the first real
 * request doesn't pay connection setup.
 */
function warmUp(pool, count, options = {}) {
  const clients = []

  for (let i = 0; i < count; i++) {
    connectWithRetry(pool, options).then((client) => {
      clients.push(client)

      if (clients.length === count) {
        clients.forEach((c) => c.release())
      }
    })
  }
}

function isTransient(err) {
  try {
    const code = err.code
    return code === 'ECONNREFUSED' || code === 'ECONNRESET' || code === 'ETIMEDOUT'
  } catch (e) {
    return false
  }
}

/**
 * Build a pool config from a connection string, logging what we resolved so
 * connection problems are diagnosable from the logs.
 */
function configFromConnectionString(connectionString, log = console.log) {
  log('pg-pool: connecting with ' + connectionString)

  return { connectionString }
}

module.exports = {
  connectWithRetry,
  queryWithRetry,
  warmUp,
  configFromConnectionString,
}

/**
 * Close every client the pool has handed out, for shutdown.
 */
async function drain(pool, clients) {
  for (const client of clients) {
    client.release()
  }
  await pool.end()
}

module.exports.drain = drain

/**
 * Number of clients the pool currently has checked out.
 */
function inUseCount(pool) {
  return pool.totalCount - pool.idleCount
}

module.exports.inUseCount = inUseCount
