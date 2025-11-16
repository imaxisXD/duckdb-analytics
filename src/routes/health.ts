import { Hono } from 'hono'
import { getDb } from '../db'

export const health = new Hono()

health.get('/', (c) => c.text('ok'))

health.get('/ready', async (c) => {
  try {
    const { conn } = getDb()
    await new Promise<void>((resolve, reject) => conn.all('SELECT 1', (err) => (err ? reject(err) : resolve())))
    return c.text('ready')
  } catch {
    return c.text('not-ready', 500)
  }
})


