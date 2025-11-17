import * as fs from 'node:fs'
import { randomUUID } from 'node:crypto'
import { loadConfig } from './config'
import { logger } from './logger'
import { getDb, listDistinctUsers, getOffset, getMaxTsSince, copyUserSinceToParquet, getTempFile, upsertOffset } from './db'
import type { DbConnection, TableData } from './db'
import { uploadFile, buildUserKey } from './r2'

const cfg = loadConfig()
const log = logger.child({ mod: 'exporter' })

async function countNewRows(conn: DbConnection, userId: string, sinceTs: string): Promise<number> {
  const reader = await conn.runAndReadAll(
    `SELECT COUNT(*)::INT AS cnt FROM events WHERE user_id = ? AND occurred_at > ?`,
    [userId, sinceTs]
  )
  const rows = reader.getRowObjects() as TableData
  return (rows?.[0]?.cnt as number | undefined) ?? 0
}

export async function runExportOnce() {
  const { conn } = await getDb()
  const users = await listDistinctUsers(conn)
  if (users.length === 0) {
    log.debug('no users to export')
    return
  }
  for (const userId of users) {
    try {
      const sinceTs = await getOffset(conn, userId)
      const pending = await countNewRows(conn, userId, sinceTs)
      if (pending === 0) {
        log.debug({ userId }, 'no new rows')
        continue
      }
      const outfile = getTempFile('export', 'parquet')
      await copyUserSinceToParquet(conn, userId, sinceTs, outfile)
      const maxTs = await getMaxTsSince(conn, userId, sinceTs)
      if (!maxTs) {
        log.warn({ userId }, 'no max ts after export')
        fs.rmSync(outfile, { force: true })
        continue
      }
      const dt = maxTs.slice(0, 10) // YYYY-MM-DD
      const hr = maxTs.slice(11, 13) // HH
      const filename = `part-${randomUUID()}.parquet`
      const key = buildUserKey(userId, dt, hr, filename)
      const buf = fs.readFileSync(outfile)
      await uploadFile(key, buf, 'application/octet-stream')
      fs.rmSync(outfile, { force: true })
      await upsertOffset(conn, userId, maxTs)
      log.info({ userId, rows: pending, key }, 'exported parquet')
    } catch (err) {
      log.error({ userId, err }, 'export error')
    }
  }
}

let exportTimer: any = null

export function startExporter() {
  if (exportTimer) return
  exportTimer = setInterval(() => {
    runExportOnce().catch((err) => log.error({ err }, 'exporter loop error'))
  }, cfg.EXPORT_INTERVAL_MS) as unknown as Timer
  log.info({ intervalMs: cfg.EXPORT_INTERVAL_MS }, 'exporter: started')
}


