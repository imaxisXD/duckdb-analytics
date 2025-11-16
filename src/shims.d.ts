declare module 'duckdb' {
  const anyExport: any
  export default anyExport
}

declare module '@aws-sdk/client-s3' {
  export const S3Client: any
  export const PutObjectCommand: any
  export const GetObjectCommand: any
  export const ListObjectsV2Command: any
  export const DeleteObjectsCommand: any
  export type _Object = any
}

declare module '@aws-sdk/s3-request-presigner' {
  export const getSignedUrl: any
}

declare module 'zod' {
  export const z: any
  export default z
}


