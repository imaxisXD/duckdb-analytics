import * as fs from 'node:fs'
import * as path from 'node:path'
import * as os from 'node:os'
import { randomUUID } from 'node:crypto'
import { loadConfig } from './config'
import { logger } from './logger'
import { getDb, listDistinctUsers } from './db'
import { listPrefix, parsePartitionsFromKey, uploadFile, deleteKeys, buildUserKey, presignGet } from './r2'


const cfg = loadConfig()
const log = logger.child({ mod: 'compactor' })

type Obj = { Key?: string; Size?: number; LastModified?: Date; ETag?: string }

function groupByDtHr(objs: Obj[]) {
  const groups = new Map<string, Obj[]>()
  for (const o of objs) {
    if (!o.Key) continue
    const parts = parsePartitionsFromKey(o.Key)
    const key = `${parts['dt'] ?? 'unknown'}|${parts['hr'] ?? 'unknown'}`
    const list = groups.get(key) ?? []
    list.push(o)
    groups.set(key, list)
  }
  return groups
}

export async function runCompactionOnce() {
  const { conn } = await getDb()
  const users = await listDistinctUsers(conn)
  if (users.length === 0) return
  for (const userId of users) {
    try {
      const prefix = `user_id=${userId}/`
      const objs = await listPrefix(prefix)
      if (objs.length === 0) continue
      const groups = groupByDtHr(objs)
      for (const entry of Array.from(groups.entries())) {
        const [gk, files] = entry
        const totalSize = files.reduce((s: number, f: Obj) => s + (f.Size ?? 0), 0)
        const smallFiles = files.filter((f: Obj) => (f.Size ?? 0) < cfg.TARGET_PARQUET_MB * 1024 * 1024 * 0.5)
        if (smallFiles.length < 2) continue
        if (totalSize < cfg.TARGET_PARQUET_MB * 1024 * 1024) continue
        // compact: read remote parquet via presigned HTTPS URLs using httpfs
        const [dt, hr] = gk.split('|')
        const urls: string[] = []
        for (const f of smallFiles) {
          if (!f.Key) continue
          const url = await presignGet(f.Key)
          urls.push(url)
        }
        const { conn } = await getDb()
        // Enable httpfs to read https urls
        await conn.run('INSTALL httpfs; LOAD httpfs;')
        const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-compact-'))
        const outfile = path.join(tmpdir, `compact-${randomUUID()}.parquet`)
        const urlArgs = urls.map((u) => `'${u.replace(/'/g, "''")}'`).join(', ')
        const sql = `
          COPY (
            SELECT * FROM read_parquet(${urlArgs})
          ) TO '${outfile.replace(/'/g, "''")}' (FORMAT PARQUET, COMPRESSION ZSTD);
        `
        await conn.run(sql)
        const data = fs.readFileSync(outfile)
        const newKey = buildUserKey(userId, dt, hr, `part-compact-${randomUUID()}.parquet`)
        await uploadFile(newKey, data, 'application/octet-stream')
        // delete old small files
        const delKeys = smallFiles.map((f) => f.Key!).filter(Boolean)
        await deleteKeys(delKeys)
        log.info({ userId, dt, hr, filesMerged: smallFiles.length, newKey }, 'compaction:completed')
        try { fs.rmSync(tmpdir, { recursive: true, force: true }) } catch (_err: unknown) {}
      }
    } catch (err) {
      log.error({ userId, err }, 'compaction error')
    }
  }
}

let compactTimer: any = null
export function startCompactor() {
  if (compactTimer) return
  compactTimer = setInterval(() => {
    runCompactionOnce().catch((err) => log.error({ err }, 'compactor loop error'))
  }, cfg.COMPACTION_INTERVAL_MS) as unknown as Timer
  log.info({ intervalMs: cfg.COMPACTION_INTERVAL_MS }, 'compactor: started')
}


