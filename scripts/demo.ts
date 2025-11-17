/* Demo script: ingest → read from DuckDB via debug endpoint */
import { randomUUID } from 'node:crypto'
const BASE = `http://localhost:${process.env.PORT ?? 3000}`
const API_KEY = process.env.API_KEY ?? 'dev-api-key'
const ADMIN_API_KEY = process.env.ADMIN_API_KEY ?? 'dev-admin-key'
const USER_ID = process.env.USER_ID ?? 'demo-user'

function headers(extra?: Record<string, string>) {
  return {
    'content-type': 'application/json',
    'x-api-key': API_KEY,
    ...extra,
  }
}

async function main() {
  const now = new Date().toISOString()
  const event = {
    idempotency_key: randomUUID(),
    occurred_at: now,
    link_slug: 'demo',
    short_url: 'https://sho.rt/demo',
    link_id: null,
    user_id: USER_ID,
    destination_url: 'https://example.com/',
    redirect_status: 302,
    tracking_enabled: true,
    latency_ms_worker: 12,
    session_id: null,
    first_click_of_session: true,
    request_id: randomUUID(),
    worker_datacenter: 'iad',
    worker_version: 'demo',
    user_agent: 'demo/1.0',
    device_type: null,
    browser: null,
    os: null,
    ip_hash: 'abc123',
    country: 'US',
    region: null,
    city: null,
    referer: null,
    utm_source: null,
    utm_medium: null,
    utm_campaign: null,
    utm_term: null,
    utm_content: null,
    is_bot: false,
    language: null,
    timezone: null,
  }
  let res = await fetch(`${BASE}/ingest`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(event),
  })
  console.log('ingest', res.status, await res.json())

  res = await fetch(`${BASE}/debug/db?user_id=${encodeURIComponent(USER_ID)}`, {
    headers: headers({ 'x-admin-key': ADMIN_API_KEY }),
  })
  console.log('debug/db', res.status, await res.text())
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})


