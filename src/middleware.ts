import { Context, Next } from 'hono'
import { randomUUID } from 'node:crypto'
import { loadConfig } from './config'
import { logger, withRequest } from './logger'

const cfg = loadConfig()  

export async function requestLogger(c: Context, next: Next) {
  const reqId = c.req.header('x-request-id') ?? randomUUID()
  c.set('reqId', reqId)
  const start = Date.now()
  const log = withRequest(reqId)
  log.info({ method: c.req.method, path: c.req.path }, 'request:start')
  try {
    await next()
    const ms = Date.now() - start
    log.info({ status: c.res.status, ms }, 'request:ok')
  } catch (err: any) {
    const ms = Date.now() - start
    log.error({ err, ms }, 'request:error')
    throw err
  }
}

export function requireAuth() {
  return async (c: Context, next: Next) => {
    const apiKey = c.req.header('x-api-key')
    if (!cfg.API_KEY || apiKey !== cfg.API_KEY) {
      return c.json({ error: 'unauthorized' }, 401)
    }
    const tenantId = c.req.header('x-tenant-id')
    if (!tenantId) {
      return c.json({ error: 'missing tenant' }, 400)
    }
    c.set('tenantId', tenantId)
    await next()
  }
}

export function requireAdmin() {
  return async (c: Context, next: Next) => {
    const adminKey = c.req.header('x-admin-key')
    if (!cfg.ADMIN_API_KEY || adminKey !== cfg.ADMIN_API_KEY) {
      return c.json({ error: 'forbidden' }, 403)
    }
    await next()
  }
}

type Counter = { windowStart: number; count: number }
const counters = new Map<string, Counter>()

export function rateLimit() {
  return async (c: Context, next: Next) => {
    const tenantId: string | undefined = c.get('tenantId')
    const key = tenantId ?? c.req.header('x-forwarded-for') ?? 'global'
    const now = Date.now()
    const winMs = cfg.RATE_LIMIT_WINDOW_MS
    const max = cfg.RATE_LIMIT_MAX
    const ctr = counters.get(key)
    if (!ctr || now - ctr.windowStart >= winMs) {
      counters.set(key, { windowStart: now, count: 1 })
    } else {
      ctr.count++
      if (ctr.count > max) {
        return c.json({ error: 'rate_limited' }, 429)
      }
    }
    await next()
  }
}

export function getTenantId(c: Context): string {
  const t = c.get('tenantId')
  if (!t) throw new Error('tenant not in context')
  return t
}

export function getReqId(c: Context): string {
  return c.get('reqId') as string
}


