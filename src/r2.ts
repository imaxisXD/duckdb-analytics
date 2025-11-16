import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectsCommand, _Object } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { loadConfig } from './config'
import { logger } from './logger'

const cfg = loadConfig()
const log = logger.child({ mod: 'r2' })

export const s3 = new S3Client({
  region: cfg.R2_REGION,
  endpoint: cfg.R2_ENDPOINT,
  forcePathStyle: true,
  credentials: cfg.R2_ACCESS_KEY_ID && cfg.R2_SECRET_ACCESS_KEY
    ? { accessKeyId: cfg.R2_ACCESS_KEY_ID, secretAccessKey: cfg.R2_SECRET_ACCESS_KEY }
    : undefined,
})

export type ManifestEntry = {
  key: string
  url: string
  size?: number
  etag?: string
  lastModified?: string
  partitions?: Record<string, string>
}

export async function uploadFile(bucketKey: string, data: Uint8Array | Buffer, contentType = 'application/octet-stream') {
  if (!cfg.R2_BUCKET) throw new Error('R2_BUCKET not configured')
  await s3.send(new PutObjectCommand({
    Bucket: cfg.R2_BUCKET,
    Key: bucketKey,
    Body: data,
    ContentType: contentType,
  }))
  log.info({ key: bucketKey, size: data.byteLength }, 'r2: uploaded')
}

export async function getObjectStream(bucketKey: string): Promise<any> {
  if (!cfg.R2_BUCKET) throw new Error('R2_BUCKET not configured')
  const res: any = await s3.send(new GetObjectCommand({ Bucket: cfg.R2_BUCKET, Key: bucketKey }))
  return res.Body as unknown as ReadableStream
}

export async function listPrefix(prefix: string): Promise<_Object[]> {
  if (!cfg.R2_BUCKET) throw new Error('R2_BUCKET not configured')
  const out: _Object[] = []
  let token: string | undefined = undefined
  do {
    const resp: any = await s3.send(new ListObjectsV2Command({
      Bucket: cfg.R2_BUCKET,
      Prefix: prefix,
      ContinuationToken: token,
    }))
    if (resp.Contents) out.push(...resp.Contents)
    token = resp.IsTruncated ? resp.NextContinuationToken : undefined
  } while (token)
  return out
}

export async function presignGet(bucketKey: string): Promise<string> {
  if (!cfg.R2_BUCKET) throw new Error('R2_BUCKET not configured')
  const cmd = new GetObjectCommand({ Bucket: cfg.R2_BUCKET, Key: bucketKey })
  const url = await getSignedUrl(s3, cmd, { expiresIn: cfg.PRESIGN_TTL_SECONDS })
  return url
}

export async function deleteKeys(keys: string[]) {
  if (!cfg.R2_BUCKET || keys.length === 0) return
  await s3.send(new DeleteObjectsCommand({
    Bucket: cfg.R2_BUCKET,
    Delete: { Objects: keys.map((k) => ({ Key: k })) },
  }))
  log.info({ deleted: keys.length }, 'r2: deleted objects')
}

export function buildTenantKey(tenantId: string, dt: string, hr: string, filename: string) {
  return `tenant_id=${tenantId}/dt=${dt}/hr=${hr}/${filename}`
}

export function parsePartitionsFromKey(key: string): Record<string, string> {
  // Expect keys like: tenant_id=T/dt=YYYY-MM-DD/hr=HH/part-uuid.parquet
  const parts: Record<string, string> = {}
  const segs = key.split('/')
  for (const s of segs) {
    const [k, v] = s.split('=')
    if (v) parts[k] = v
  }
  return parts
}


