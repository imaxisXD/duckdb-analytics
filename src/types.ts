import { z } from 'zod'

export const EventSchema = z.object({
  eventId: z.string().min(1),
  ts: z.string().datetime(), // ISO string
  type: z.string().optional(),
  properties: z.record(z.any()).optional(),
})

export const IngestBodySchema = z.object({
  events: z.array(EventSchema).min(1),
})

export type IngestBody = z.infer<typeof IngestBodySchema>


