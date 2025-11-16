-- Create initial tables and indexes (match current initializeSchema)
CREATE TABLE IF NOT EXISTS events_staging (
  tenant_id TEXT,
  event_id TEXT,
  ts TIMESTAMP,
  type TEXT,
  properties TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_staging_tenant_ts
ON events_staging(tenant_id, ts);

CREATE TABLE IF NOT EXISTS export_offsets (
  tenant_id TEXT,
  last_event_ts TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_export_offsets_tenant
ON export_offsets(tenant_id);


