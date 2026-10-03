CREATE TABLE IF NOT EXISTS hm_app_state (
  org_id text NOT NULL,
  key text NOT NULL,
  value text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by text NOT NULL DEFAULT '',
  PRIMARY KEY (org_id, key)
);

CREATE INDEX IF NOT EXISTS hm_app_state_updated_idx
  ON hm_app_state (org_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS hm_files (
  id text PRIMARY KEY,
  org_id text NOT NULL,
  lead_id text NOT NULL DEFAULT '',
  type text NOT NULL DEFAULT 'document',
  category text NOT NULL DEFAULT '',
  file_name text NOT NULL,
  mime_type text NOT NULL DEFAULT 'application/octet-stream',
  size_bytes bigint NOT NULL DEFAULT 0,
  bucket text NOT NULL DEFAULT 'hail-money-files',
  object_key text NOT NULL,
  note text NOT NULL DEFAULT '',
  uploaded_by text NOT NULL DEFAULT '',
  uploaded_by_email text NOT NULL DEFAULT '',
  uploaded_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL DEFAULT 'pending',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS hm_files_org_lead_idx
  ON hm_files (org_id, lead_id, uploaded_at DESC);

CREATE INDEX IF NOT EXISTS hm_files_org_type_idx
  ON hm_files (org_id, type, uploaded_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS hm_files_object_key_idx
  ON hm_files (org_id, object_key);

CREATE TABLE IF NOT EXISTS hm_audit_events (
  id bigserial PRIMARY KEY,
  org_id text NOT NULL,
  actor_uid text NOT NULL DEFAULT '',
  actor_email text NOT NULL DEFAULT '',
  action text NOT NULL,
  entity_type text NOT NULL DEFAULT '',
  entity_id text NOT NULL DEFAULT '',
  detail jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS hm_audit_events_org_created_idx
  ON hm_audit_events (org_id, created_at DESC);
