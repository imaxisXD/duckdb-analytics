import { Hono } from 'hono'
import { IngestBodySchema } from '../types'
import { getDb, insertEvents } from '../db'
import { getTenantId } from '../middleware'
import { logger } from '../logger'

export const ingest = new Hono()

ingest.post('/', async (c) => {
  const tenantId = getTenantId(c)
  const body = await c.req.json().catch(() => null)
  const parsed = IngestBodySchema.safeParse(body)
  if (!parsed.success) {
    return c.json({ error: 'invalid_body', details: parsed.error.flatten() }, 400)
  }
  const events = parsed.data.events.map((e: any) => ({
    tenant_id: tenantId,
    event_id: e.eventId,
    ts: e.ts,
    type: e.type,
    properties: e.properties,
  }))
  const { conn } = getDb()
  await insertEvents(conn, events)
  logger.info({ tenantId, count: events.length }, 'ingest:inserted')
  return c.json({ ok: true, inserted: events.length })
})


