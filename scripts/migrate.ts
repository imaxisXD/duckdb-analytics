import * as fs from 'node:fs'
import * as path from 'node:path'
import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api'
import type { DuckDBValue } from '@duckdb/node-api'
import { loadConfig } from '../src/config'

type MigrationRecord = { version: string }

function ensureDir(filePath: string) {
  const dir = path.dirname(filePath)
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
}

async function runAsync(conn: DuckDBConnection, sql: string, params?: DuckDBValue[]): Promise<void> {
  if (params && params.length > 0) {
    await conn.run(sql, params)
  } else {
    await conn.run(sql)
  }
}

async function allAsync<T = unknown>(
  conn: DuckDBConnection,
  sql: string,
  params?: DuckDBValue[]
): Promise<T[]> {
  const reader =
    params && params.length > 0 ? await conn.runAndReadAll(sql, params) : await conn.runAndReadAll(sql)
  const rows = reader.getRowObjects()
  return rows as T[]
}

async function main() {
  const cfg = loadConfig()
  ensureDir(cfg.DUCKDB_PATH)
  const instance = await DuckDBInstance.create(cfg.DUCKDB_PATH)
  const conn = await instance.connect()

  await runAsync(
    conn,
    `
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY,
      applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `
  )

  const migrationsDir = path.join(process.cwd(), 'migrations')
  if (!fs.existsSync(migrationsDir)) {
    console.log('No migrations directory found, skipping.')
    conn.disconnectSync()
    instance.closeSync()
    return
  }

  const files = fs
    .readdirSync(migrationsDir)
    .filter((f) => f.endsWith('.sql'))
    .sort((a, b) => a.localeCompare(b))

  const applied = new Set<string>(
    (await allAsync<MigrationRecord>(conn, 'SELECT version FROM schema_migrations')).map((r) => r.version)
  )

  for (const file of files) {
    const version = path.basename(file, '.sql')
    if (applied.has(version)) {
      console.log(`Skipping ${version} (already applied)`)
      continue
    }
    const fullPath = path.join(migrationsDir, file)
    const sql = fs.readFileSync(fullPath, 'utf8')
    console.log(`Applying ${version} ...`)
    await runAsync(conn, 'BEGIN')
    try {
      await runAsync(conn, sql)
      await runAsync(conn, 'INSERT INTO schema_migrations (version) VALUES (?)', [version])
      await runAsync(conn, 'COMMIT')
      console.log(`Applied ${version}`)
    } catch (err) {
      await runAsync(conn, 'ROLLBACK')
      console.error(`Failed ${version}`, err)
      throw err
    }
  }

  conn.disconnectSync()
  instance.closeSync()
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})


