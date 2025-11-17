import { Hono } from 'hono'
import { getDb, recentSample } from '../db'
import type { TableData } from '../db'

export const initial = new Hono()

initial.get('/', async (c) => {
  const userId = c.req.query('user_id')
  const { conn } = await getDb()
  const sample = await recentSample(conn, userId ?? undefined, 50)
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
  const rows = (await conn.runAndReadAll(
    userId
      ? `SELECT COUNT(*)::INT AS cnt FROM events WHERE user_id = ? AND occurred_at > ?`
      : `SELECT COUNT(*)::INT AS cnt FROM events WHERE occurred_at > ?`,
    userId ? [userId, since] : [since]
  )).getRowObjects() as TableData
  const kpi = { last24h: (rows?.[0]?.cnt as number | undefined) ?? 0 }
  return c.json({ userId: userId ?? null, kpi, sample })
})


