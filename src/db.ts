import * as fs from 'node:fs'
import * as path from 'node:path'
import * as os from 'node:os'
import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api'
import type { DuckDBValue } from '@duckdb/node-api'
import { loadConfig } from './config'
import { logger as baseLogger } from './logger'
import type { ClickEvent } from './types'

const cfg = loadConfig()
const log = baseLogger.child({ mod: 'db' })

export type DbConnection = DuckDBConnection
export type Row = Record<string, unknown>
export type TableData = Row[]

let instancePromise: Promise<DuckDBInstance> | null = null
let connectionPromise: Promise<DuckDBConnection> | null = null

const ensureDir = (p: string) => {
  const dir = path.dirname(p)
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
}

const toSqlString = (s: string) => `'${s.replace(/'/g, "''")}'`

export async function getDb(): Promise<{ db: DuckDBInstance; conn: DbConnection }> {
  if (!instancePromise) {
    ensureDir(cfg.DUCKDB_PATH)
    instancePromise = DuckDBInstance.fromCache(cfg.DUCKDB_PATH)
    instancePromise
      .then(() => log.info({ dbPath: cfg.DUCKDB_PATH }, 'duckdb: connected'))
      .catch((err) => log.error({ err, dbPath: cfg.DUCKDB_PATH }, 'duckdb: connect error'))
  }
  const db = await instancePromise
  if (!connectionPromise) {
    connectionPromise = db.connect().then(async (conn) => {
      await initializeSchema(conn)
      return conn
    })
  }
  const conn = await connectionPromise
  return { db, conn }
}

async function initializeSchema(conn: DuckDBConnection) {
  await conn.run(`
    CREATE TABLE IF NOT EXISTS events (
      idempotency_key TEXT NOT NULL,
      occurred_at TIMESTAMP NOT NULL,
      link_slug TEXT NOT NULL,
      short_url TEXT NOT NULL,
      link_id TEXT,
      user_id TEXT NOT NULL,
      destination_url TEXT NOT NULL,
      redirect_status INTEGER NOT NULL,
      tracking_enabled BOOLEAN NOT NULL,
      latency_ms_worker INTEGER NOT NULL,
      session_id TEXT,
      first_click_of_session BOOLEAN NOT NULL,
      request_id TEXT NOT NULL,
      worker_datacenter TEXT NOT NULL,
      worker_version TEXT NOT NULL,
      user_agent TEXT NOT NULL,
      device_type TEXT,
      browser TEXT,
      os TEXT,
      ip_hash TEXT NOT NULL,
      country TEXT NOT NULL,
      region TEXT,
      city TEXT,
      referer TEXT,
      utm_source TEXT,
      utm_medium TEXT,
      utm_campaign TEXT,
      utm_term TEXT,
      utm_content TEXT,
      is_bot BOOLEAN NOT NULL,
      language TEXT,
      timezone TEXT
    );
  `)
  await conn.run(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_events_user_idem
    ON events(user_id, idempotency_key);
  `)
  await conn.run(`
    CREATE INDEX IF NOT EXISTS idx_events_user_time
    ON events(user_id, occurred_at);
  `)
  await conn.run(`
    DROP TABLE IF EXISTS export_offsets;
  `)
  await conn.run(`
    CREATE TABLE IF NOT EXISTS export_offsets (
      user_id TEXT,
      last_event_ts TIMESTAMP
    );
  `)
  await conn.run(`
    CREATE INDEX IF NOT EXISTS idx_export_offsets_user
    ON export_offsets(user_id);
  `)
}

async function queryAll(
  conn: DuckDBConnection,
  sql: string,
  values?: DuckDBValue[] | Record<string, DuckDBValue>
): Promise<TableData> {
  const reader =
    values !== undefined ? await conn.runAndReadAll(sql, values) : await conn.runAndReadAll(sql)
  const rows = reader.getRowObjects()
  return rows as TableData
}

export async function insertClickEvent(conn: DbConnection, ev: ClickEvent) {
  // Idempotency: (user_id, idempotency_key)
  await conn.run('BEGIN TRANSACTION')
  try {
    await conn.run('DELETE FROM events WHERE user_id = ? AND idempotency_key = ?', [
      ev.user_id,
      ev.idempotency_key,
    ])
    await conn.run(
      `
      INSERT INTO events (
        idempotency_key, occurred_at, link_slug, short_url, link_id, user_id, destination_url,
        redirect_status, tracking_enabled, latency_ms_worker, session_id, first_click_of_session,
        request_id, worker_datacenter, worker_version, user_agent, device_type, browser, os,
        ip_hash, country, region, city, referer, utm_source, utm_medium, utm_campaign, utm_term,
        utm_content, is_bot, language, timezone
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
      [
        ev.idempotency_key,
        ev.occurred_at,
        ev.link_slug,
        ev.short_url,
        ev.link_id ?? null,
        ev.user_id,
        ev.destination_url,
        ev.redirect_status,
        ev.tracking_enabled,
        ev.latency_ms_worker,
        ev.session_id ?? null,
        ev.first_click_of_session,
        ev.request_id,
        ev.worker_datacenter,
        ev.worker_version,
        ev.user_agent,
        ev.device_type ?? null,
        ev.browser ?? null,
        ev.os ?? null,
        ev.ip_hash,
        ev.country,
        ev.region ?? null,
        ev.city ?? null,
        ev.referer ?? null,
        ev.utm_source ?? null,
        ev.utm_medium ?? null,
        ev.utm_campaign ?? null,
        ev.utm_term ?? null,
        ev.utm_content ?? null,
        ev.is_bot,
        ev.language ?? null,
        ev.timezone ?? null,
      ]
    )
    await conn.run('COMMIT')
  } catch (e) {
    await conn.run('ROLLBACK')
    throw e
  }
}

export function getTempFile(prefix: string, ext: string) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-export-'))
  const fname = `${prefix}.${ext}`
  return path.join(dir, fname)
}

export async function copyUserSinceToParquet(conn: DbConnection, userId: string, sinceTs: string, outfile: string) {
  const sql = `
    COPY (
      SELECT *
      FROM events
      WHERE user_id = ? AND occurred_at > ?
      ORDER BY occurred_at
    )
    TO ${toSqlString(outfile)} (FORMAT PARQUET, COMPRESSION ZSTD);
  `
  await conn.run(sql, [userId, sinceTs])
}

export async function getMaxTsSince(conn: DbConnection, userId: string, sinceTs: string): Promise<string | null> {
  const rows = await queryAll(
    conn,
    `SELECT MAX(occurred_at) AS max_ts
     FROM events
     WHERE user_id = ? AND occurred_at > ?`,
    [userId, sinceTs]
  )
  const maxTs = (rows?.[0]?.max_ts as string | null | undefined) ?? null
  return maxTs
}

export async function upsertOffset(conn: DbConnection, userId: string, lastTs: string) {
  // DuckDB lacks standard UPSERT syntax; emulate with delete+insert in a tx
  await conn.run('BEGIN TRANSACTION')
  try {
    await conn.run('DELETE FROM export_offsets WHERE user_id = ?', [userId])
    await conn.run('INSERT INTO export_offsets (user_id, last_event_ts) VALUES (?, ?)', [userId, lastTs])
    await conn.run('COMMIT')
  } catch (e) {
    await conn.run('ROLLBACK')
    throw e
  }
}

export async function getOffset(conn: DbConnection, userId: string): Promise<string> {
  const fallback = '1970-01-01 00:00:00'
  const rows = await queryAll(conn, 'SELECT last_event_ts FROM export_offsets WHERE user_id = ?', [
    userId,
  ])
  return (rows?.[0]?.last_event_ts as string | undefined) ?? fallback
}

export async function listDistinctUsers(conn: DbConnection): Promise<string[]> {
  const rows = await queryAll(conn, 'SELECT DISTINCT user_id FROM events')
  return rows.map((r) => String(r.user_id))
}

export async function recentSample(conn: DbConnection, userId?: string, limit = 50): Promise<TableData> {
  if (userId) {
    return await queryAll(
      conn,
      `SELECT *
       FROM events
       WHERE user_id = ?
       ORDER BY occurred_at DESC
       LIMIT ${limit}`,
      [userId]
    )
  }
  return await queryAll(
    conn,
    `SELECT *
     FROM events
     ORDER BY occurred_at DESC
     LIMIT ${limit}`
  )
}

export async function countByUser(conn: DbConnection): Promise<Array<{ user_id: string; cnt: number }>> {
  const rows = await queryAll(
    conn,
    `SELECT user_id, COUNT(*)::INT AS cnt
     FROM events
     GROUP BY user_id
     ORDER BY user_id`
  )
  return rows.map((r) => ({ user_id: String(r.user_id), cnt: Number(r.cnt) }))
}
