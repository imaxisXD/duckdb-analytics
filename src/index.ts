import { Hono } from 'hono'
import { loadConfig } from './config'
import { logger } from './logger'
import { requestLogger, rateLimit, requireAuth, requireAdmin } from './middleware'
import { ingest } from './routes/ingest'
import { manifest } from './routes/manifest'
import { initial } from './routes/initial'
import { debugdb } from './routes/debug'
import { health } from './routes/health'
import { getDb } from './db'
import { startExporter } from './exporter'
import { startCompactor } from './compactor'

const cfg = loadConfig()
const app = new Hono()

// Global middleware
app.use('*', requestLogger)

// Public health
app.route('/health', health)

// Info
app.get('/', (c) => c.json({ name: 'duckdb-analytics', status: 'ok' }))

// Authenticated routes (scoped middleware)
app.use('/ingest/*', requireAuth())
app.use('/ingest/*', rateLimit())
app.route('/ingest', ingest)

app.use('/initial/*', requireAuth())
app.route('/initial', initial)

app.use('/manifest/*', requireAuth())
app.route('/manifest', manifest)

app.use('/debug/db/*', requireAuth())
app.use('/debug/db/*', requireAdmin())
app.route('/debug/db', debugdb)

// Initialize DB and start background workers
getDb()
startExporter()
startCompactor()

// Start server (Bun)
logger.info({ port: cfg.PORT }, 'server: listening')

export default app
