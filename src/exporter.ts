import * as fs from 'node:fs'
import { randomUUID } from 'node:crypto'
import { loadConfig } from './config'
import { logger } from './logger'
import { getDb, listDistinctTenants, getOffset, getMaxTsSince, copyTenantSinceToParquet, getTempFile, upsertOffset } from './db'
import { uploadFile, buildTenantKey } from './r2'

const cfg = loadConfig()
const log = logger.child({ mod: 'exporter' })

async function countNewRows(conn: any, tenantId: string, sinceTs: string): Promise<number> {
  return await new Promise((resolve, reject) => {
    conn.all(
      `SELECT COUNT(*)::INT AS cnt FROM events_staging WHERE tenant_id = ? AND ts > ?`,
      [tenantId, sinceTs],
      (err: any, rows: any[]) => (err ? reject(err) : resolve(rows?.[0]?.cnt ?? 0))
    )
  })
}

export async function runExportOnce() {
  const { conn } = getDb()
  const tenants = await listDistinctTenants(conn)
  if (tenants.length === 0) {
    log.debug('no tenants to export')
    return
  }
  for (const tenantId of tenants) {
    try {
      const sinceTs = await getOffset(conn, tenantId)
      const pending = await countNewRows(conn, tenantId, sinceTs)
      if (pending === 0) {
        log.debug({ tenantId }, 'no new rows')
        continue
      }
      const outfile = getTempFile('export', 'parquet')
      await copyTenantSinceToParquet(conn, tenantId, sinceTs, outfile)
      const maxTs = await getMaxTsSince(conn, tenantId, sinceTs)
      if (!maxTs) {
        log.warn({ tenantId }, 'no max ts after export')
        fs.rmSync(outfile, { force: true })
        continue
      }
      const dt = maxTs.slice(0, 10) // YYYY-MM-DD
      const hr = maxTs.slice(11, 13) // HH
      const filename = `part-${randomUUID()}.parquet`
      const key = buildTenantKey(tenantId, dt, hr, filename)
      const buf = fs.readFileSync(outfile)
      await uploadFile(key, buf, 'application/octet-stream')
      fs.rmSync(outfile, { force: true })
      await upsertOffset(conn, tenantId, maxTs)
      log.info({ tenantId, rows: pending, key }, 'exported parquet')
    } catch (err) {
      log.error({ tenantId, err }, 'export error')
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


