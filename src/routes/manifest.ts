import { Hono } from 'hono'
import { listPrefix, presignGet, parsePartitionsFromKey } from '../r2'

export const manifest = new Hono()

manifest.get('/', async (c) => {
  const userId = c.req.query('user_id')
  if (!userId) {
    return c.json({ error: 'missing user_id' }, 400)
  }
  const prefix = `user_id=${userId}/`
  const objs = await listPrefix(prefix)
  const items = await Promise.all(
    objs
      .filter((o) => o.Key && o.Key.endsWith('.parquet'))
      .map(async (o) => {
        const key = o.Key as string
        const url = await presignGet(key)
        return {
          key,
          url,
          size: o.Size,
          etag: o.ETag,
          lastModified: o.LastModified?.toISOString(),
          partitions: parsePartitionsFromKey(key),
        }
      })
  )
  return c.json({ userId, items })
})


