import { z } from 'zod'

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),

  // DuckDB
  DUCKDB_PATH: z.string().default('./data/main.duckdb'),

  // Auth
  API_KEY: z.string().min(16).optional(),
  ADMIN_API_KEY: z.string().min(16).optional(),

  // R2 (S3-compatible)
  R2_ENDPOINT: z.string().url().optional(),
  R2_REGION: z.string().default('auto'),
  R2_BUCKET: z.string().optional(),
  R2_ACCESS_KEY_ID: z.string().optional(),
  R2_SECRET_ACCESS_KEY: z.string().optional(),
  PRESIGN_TTL_SECONDS: z.coerce.number().int().positive().default(900),

  // Export / Compaction
  EXPORT_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  COMPACTION_INTERVAL_MS: z.coerce.number().int().positive().default(3_600_000),
  TARGET_PARQUET_MB: z.coerce.number().int().positive().default(128),

  // Rate limit (simple in-memory)
  RATE_LIMIT_WINDOW_MS: z.coerce.number().int().positive().default(60_000),
  RATE_LIMIT_MAX: z.coerce.number().int().positive().default(120),
})

export type AppConfig = any

export const loadConfig = (): AppConfig => {
  const parsed = envSchema.safeParse(process.env)
  if (!parsed.success) {
    // eslint-disable-next-line no-console
    console.error('Invalid environment configuration', parsed.error.flatten().fieldErrors)
    throw new Error('Invalid environment configuration')
  }
  const cfg = parsed.data
  return cfg
}


