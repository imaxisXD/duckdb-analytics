/* Demo script: ingest → wait → manifest → debug */
import { randomUUID } from 'node:crypto'
const BASE = `http://localhost:${process.env.PORT ?? 3000}`
const API_KEY = process.env.API_KEY ?? 'dev-api-key'
const ADMIN_API_KEY = process.env.ADMIN_API_KEY ?? 'dev-admin-key'
const TENANT = process.env.TENANT_ID ?? 'demo-tenant'

function headers(extra?: Record<string, string>) {
  return {
    'content-type': 'application/json',
    'x-api-key': API_KEY,
    'x-tenant-id': TENANT,
    ...extra,
  }
}

async function main() {
  // Ingest a few events
  const now = new Date()
  const events = Array.from({ length: 5 }).map((_, i) => ({
    eventId: randomUUID(),
    ts: new Date(now.getTime() - i * 1000).toISOString(),
    type: 'demo_event',
    properties: { n: i },
  }))
  let res = await fetch(`${BASE}/ingest`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify({ events }),
  })
  console.log('ingest', res.status, await res.json())

  // Initial
  res = await fetch(`${BASE}/initial`, { headers: headers() })
  console.log('initial', res.status, await res.json())

  // Wait for exporter interval
  const delayMs = Number(process.env.EXPORT_WAIT_MS ?? '2000')
  await new Promise((r) => setTimeout(r, delayMs))

  // Manifest
  res = await fetch(`${BASE}/manifest`, { headers: headers() })
  const man = await res.json()
  console.log('manifest', res.status, man.items?.slice(0, 2))

  // Debug (admin)
  res = await fetch(`${BASE}/debug/db`, {
    headers: headers({ 'x-admin-key': ADMIN_API_KEY }),
  })
  console.log('debug/db', res.status, await res.json())
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})


