import { z } from 'zod'

export const ClickEventSchema = z.object({
  idempotency_key: z.string().min(1),
  occurred_at: z.iso.datetime(), // ISO 8601
  link_slug: z.string().min(1),
  short_url: z.string().min(1),
  link_id: z.string().nullable().optional(),
  user_id: z.string().min(1),
  destination_url: z.string().min(1),
  redirect_status: z.number().int(),
  tracking_enabled: z.boolean(),
  latency_ms_worker: z.number().int(),
  session_id: z.string().nullable().optional(),
  first_click_of_session: z.boolean(),
  request_id: z.string().min(1),
  worker_datacenter: z.string().min(1),
  worker_version: z.string().min(1),
  user_agent: z.string().min(1),
  device_type: z.string().nullable().optional(),
  browser: z.string().nullable().optional(),
  os: z.string().nullable().optional(),
  ip_hash: z.string().min(1),
  country: z.string().min(1),
  region: z.string().nullable().optional(),
  city: z.string().nullable().optional(),
  referer: z.string().nullable().optional(),
  utm_source: z.string().nullable().optional(),
  utm_medium: z.string().nullable().optional(),
  utm_campaign: z.string().nullable().optional(),
  utm_term: z.string().nullable().optional(),
  utm_content: z.string().nullable().optional(),
  is_bot: z.boolean(),
  language: z.string().nullable().optional(),
  timezone: z.string().nullable().optional(),
})

export type ClickEvent = z.output<typeof ClickEventSchema>

