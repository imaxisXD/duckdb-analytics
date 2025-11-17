import { Hono } from 'hono'
import { getDb } from '../db'

export const health = new Hono()

health.get('/', (c) => c.text('ok'))

health.get('/ready', async (c) => {
  try {
    const { conn } = await getDb()
    await conn.run('SELECT 1')
    return c.text('ready')
  } catch {
    return c.text('not-ready', 500)
  }
})


