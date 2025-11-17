import { Hono } from 'hono'
import { ClickEventSchema } from '../types'
import { getDb, insertClickEvent } from '../db'
import { logger } from '../logger'

export const ingest = new Hono()

ingest.post('/', async (c) => {
  const body = await c.req.json().catch(() => null)
  const parsed = ClickEventSchema.safeParse(body)
  if (!parsed.success) {
    return c.json({ error: 'invalid_body', details: parsed.error.flatten() }, 400)
  }
  const { conn } = await getDb()
  await insertClickEvent(conn, parsed.data)
  logger.info({ userId: parsed.data.user_id, idk: parsed.data.idempotency_key }, 'ingest:inserted')
  return c.json({ ok: true })
})


