-- Creates the table required by scripts/ingest_lsr_hail.mjs

CREATE TABLE IF NOT EXISTS public.hail_lsr_raw (
  id text PRIMARY KEY,
  event_time timestamptz NOT NULL,
  event_date date NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  hail_in double precision NULL,
  state text NULL,
  county text NULL,
  source text NOT NULL,
  raw jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
