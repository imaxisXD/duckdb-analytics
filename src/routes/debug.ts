import { Hono } from 'hono'
import { countByTenant, recentSample, getDb } from '../db'
import { getTenantId } from '../middleware'

export const debugdb = new Hono()

debugdb.get('/', async (c) => {
  const { conn } = getDb()
  const counts = await countByTenant(conn)
  const tenantId = getTenantId(c)
  const sample = await recentSample(conn, tenantId, 10)
  const redacted = sample.map((r) => ({ ...r, properties: undefined }))
  return c.json({ counts, sample: redacted })
})


