import * as fs from 'node:fs'
import * as path from 'node:path'
import * as os from 'node:os'
import duckdb from 'duckdb'
import { loadConfig } from './config'
import { logger as baseLogger } from './logger'

const cfg = loadConfig()
const log = baseLogger.child({ mod: 'db' })

let database: any = null
let connection: any = null

const ensureDir = (p: string) => {
  const dir = path.dirname(p)
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
}

export type EventRow = {
  tenant_id: string
  event_id: string
  ts: string
  type?: string
  properties?: unknown
}

const toSqlString = (s: string) => `'${s.replace(/'/g, "''")}'`

export function getDb(): { db: any; conn: any } {
  if (database && connection) {
    return { db: database, conn: connection }
  }
  ensureDir(cfg.DUCKDB_PATH)
  database = new duckdb.Database(cfg.DUCKDB_PATH)
  connection = database.connect()

  log.info({ dbPath: cfg.DUCKDB_PATH }, 'duckdb: connected')
  initializeSchema(connection)
  return { db: database, conn: connection }
}

function initializeSchema(conn: any) {
  conn.run(`
    CREATE TABLE IF NOT EXISTS events_staging (
      tenant_id TEXT,
      event_id TEXT,
      ts TIMESTAMP,
      type TEXT,
      properties TEXT
    );
  `)
  conn.run(`
    CREATE INDEX IF NOT EXISTS idx_events_staging_tenant_ts
    ON events_staging(tenant_id, ts);
  `)
  conn.run(`
    CREATE TABLE IF NOT EXISTS export_offsets (
      tenant_id TEXT,
      last_event_ts TIMESTAMP
    );
  `)
  conn.run(`
    CREATE INDEX IF NOT EXISTS idx_export_offsets_tenant
    ON export_offsets(tenant_id);
  `)
}

export async function insertEvents(conn: any, rows: EventRow[]) {
  await new Promise<void>((resolve, reject) => {
    conn.run('BEGIN TRANSACTION', (err: unknown) => {
      if (err) return reject(err)
      resolve()
    })
  })

  try {
    // Delete existing IDs (idempotency), then insert
    const del = conn.prepare('DELETE FROM events_staging WHERE tenant_id = ? AND event_id = ?')
    const ins = conn.prepare('INSERT INTO events_staging (tenant_id, event_id, ts, type, properties) VALUES (?, ?, ?, ?, ?)')
    for (const r of rows) {
      const props = r.properties == null ? null : JSON.stringify(r.properties)
      del.run([r.tenant_id, r.event_id])
      ins.run([r.tenant_id, r.event_id, r.ts, r.type ?? null, props])
    }
    del.finalize()
    ins.finalize()
    await new Promise<void>((resolve, reject) => {
      conn.run('COMMIT', (err: unknown) => (err ? reject(err) : resolve()))
    })
  } catch (e) {
    await new Promise<void>((resolve) => conn.run('ROLLBACK', (_err: unknown) => resolve()))
    throw e
  }
}

export function getTempFile(prefix: string, ext: string) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-export-'))
  const fname = `${prefix}.${ext}`
  return path.join(dir, fname)
}

export async function copyTenantSinceToParquet(conn: any, tenantId: string, sinceTs: string, outfile: string) {
  const sql = `
    COPY (
      SELECT tenant_id, event_id, ts, type, properties
      FROM events_staging
      WHERE tenant_id = ? AND ts > ?
      ORDER BY ts
    )
    TO ${toSqlString(outfile)} (FORMAT PARQUET, COMPRESSION ZSTD);
  `
  await new Promise<void>((resolve, reject) => conn.run(sql, [tenantId, sinceTs], (err: unknown) => (err ? reject(err) : resolve())))
}

export async function getMaxTsSince(conn: any, tenantId: string, sinceTs: string): Promise<string | null> {
  return await new Promise((resolve, reject) => {
    conn.all(
      `SELECT MAX(ts) AS max_ts
       FROM events_staging
       WHERE tenant_id = ? AND ts > ?`,
      [tenantId, sinceTs],
      (err: unknown, rows: any[]) => {
        if (err) return reject(err)
        const maxTs = rows?.[0]?.max_ts ?? null
        resolve(maxTs)
      }
    )
  })
}

export async function upsertOffset(conn: any, tenantId: string, lastTs: string) {
  // DuckDB lacks standard UPSERT syntax; emulate with delete+insert in a tx
  await new Promise<void>((resolve, reject) => conn.run('BEGIN TRANSACTION', (err: unknown) => (err ? reject(err) : resolve())))
  try {
    conn.run('DELETE FROM export_offsets WHERE tenant_id = ?', [tenantId])
    conn.run('INSERT INTO export_offsets (tenant_id, last_event_ts) VALUES (?, ?)', [tenantId, lastTs])
    await new Promise<void>((resolve, reject) => conn.run('COMMIT', (err: unknown) => (err ? reject(err) : resolve())))
  } catch (e) {
    await new Promise<void>((resolve) => conn.run('ROLLBACK', (_err: unknown) => resolve()))
    throw e
  }
}

export async function getOffset(conn: any, tenantId: string): Promise<string> {
  const fallback = '1970-01-01 00:00:00'
  return await new Promise((resolve, reject) => {
    conn.all(
      'SELECT last_event_ts FROM export_offsets WHERE tenant_id = ?',
      [tenantId],
      (err: unknown, rows: any[]) => {
        if (err) return reject(err)
        resolve(rows?.[0]?.last_event_ts ?? fallback)
      }
    )
  })
}

export async function listDistinctTenants(conn: any): Promise<string[]> {
  return await new Promise((resolve, reject) => {
    conn.all('SELECT DISTINCT tenant_id FROM events_staging', (err: unknown, rows: any[]) => {
      if (err) return reject(err)
      resolve(rows.map((r) => r.tenant_id))
    })
  })
}

export async function recentSample(conn: any, tenantId: string, limit = 50): Promise<any[]> {
  return await new Promise((resolve, reject) => {
    conn.all(
      `SELECT tenant_id, event_id, ts, type, properties
       FROM events_staging
       WHERE tenant_id = ?
       ORDER BY ts DESC
       LIMIT ${limit}`,
      [tenantId],
      (err: unknown, rows: any[]) => (err ? reject(err) : resolve(rows))
    )
  })
}

export async function countByTenant(conn: any): Promise<Array<{ tenant_id: string; cnt: number }>> {
  return await new Promise((resolve, reject) => {
    conn.all(
      `SELECT tenant_id, COUNT(*)::INT AS cnt
       FROM events_staging
       GROUP BY tenant_id
       ORDER BY tenant_id`,
      (err: unknown, rows: any[]) => (err ? reject(err) : resolve(rows))
    )
  })
}


