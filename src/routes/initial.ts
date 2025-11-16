import { Hono } from 'hono'
import { getTenantId } from '../middleware'
import { getDb, recentSample } from '../db'

export const initial = new Hono()

initial.get('/', async (c) => {
  const tenantId = getTenantId(c)
  const { conn } = getDb()
  const sample = await recentSample(conn, tenantId, 50)
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
  const kpi = await new Promise<{ last24h: number }>((resolve, reject) => {
    conn.all(
      `SELECT COUNT(*)::INT AS cnt FROM events_staging WHERE tenant_id = ? AND ts > ?`,
      [tenantId, since],
      (err: any, rows: any[]) => (err ? reject(err) : resolve({ last24h: rows?.[0]?.cnt ?? 0 }))
    )
  })
  return c.json({ tenantId, kpi, sample })
})


