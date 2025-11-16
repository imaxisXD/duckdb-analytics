import * as fs from 'node:fs'
import * as path from 'node:path'
import duckdb from 'duckdb'
import { loadConfig } from '../src/config'

type MigrationRecord = { version: string }

function ensureDir(filePath: string) {
  const dir = path.dirname(filePath)
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true })
}

function runAsync(conn: any, sql: string, params?: any[]): Promise<void> {
  return new Promise((resolve, reject) => {
    if (params && params.length > 0) {
      conn.run(sql, params, (err: unknown) => (err ? reject(err) : resolve()))
    } else {
      conn.run(sql, (err: unknown) => (err ? reject(err) : resolve()))
    }
  })
}

function allAsync<T = any>(conn: any, sql: string, params?: any[]): Promise<T[]> {
  return new Promise((resolve, reject) => {
    if (params && params.length > 0) {
      conn.all(sql, params, (err: unknown, rows: T[]) => (err ? reject(err) : resolve(rows)))
    } else {
      conn.all(sql, (err: unknown, rows: T[]) => (err ? reject(err) : resolve(rows)))
    }
  })
}

async function main() {
  const cfg = loadConfig()
  ensureDir(cfg.DUCKDB_PATH)
  const db = new duckdb.Database(cfg.DUCKDB_PATH)
  const conn = db.connect()

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
    conn.close()
    db.close()
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

  conn.close()
  db.close()
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})


