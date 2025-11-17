import { Hono } from 'hono'
import { countByUser, recentSample, getDb } from '../db'
import type { Row } from '../db'

export const debugdb = new Hono()

function normalizeRow(row: Row): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(row)) {
    if (typeof value === 'bigint') {
      out[key] = value.toString()
    } else {
      out[key] = value
    }
  }
  return out
}

debugdb.get('/', async (c) => {
  const { conn } = await getDb()
  const rawCounts = await countByUser(conn)
  const counts = rawCounts.map((c) => ({
    user_id: String(c.user_id),
    cnt: Number(c.cnt),
  }))
  const userId = c.req.query('user_id')
  const sample = await recentSample(conn, userId, 10)
  const redacted = sample.map((r: Row) => {
    const { ip_hash, ...rest } = r
    return normalizeRow(rest)
  })
  const payload = { counts, sample: redacted }
  const body = JSON.stringify(payload, (_key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  )
  return c.text(body, 200, { 'content-type': 'application/json; charset=utf-8' })
})


