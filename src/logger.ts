import pino from 'pino'
import { loadConfig } from './config'

const cfg = loadConfig()

export const logger = pino({
  level: cfg.NODE_ENV === 'production' ? 'info' : 'debug',
  base: undefined,
})

export const withRequest = (reqId: string) => logger.child({ reqId })


