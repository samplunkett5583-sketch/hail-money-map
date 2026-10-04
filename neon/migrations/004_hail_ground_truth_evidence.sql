create table if not exists public.hail_ground_truth_evidence (
  id text primary key,
  event_date date not null,
  lat double precision not null,
  lon double precision not null,
  hail_in double precision not null check (hail_in >= 0 and hail_in <= 10),
  confidence double precision not null default 0.8 check (confidence >= 0 and confidence <= 1),
  accepted boolean not null default false,
  allow_seed boolean not null default false,
  seed_radius_miles double precision not null default 2.0 check (seed_radius_miles > 0 and seed_radius_miles <= 25),
  evidence_type text not null default 'geographic_coverage',
  source text,
  label text,
  source_ref text,
  metadata_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists hail_ground_truth_evidence_date_idx
  on public.hail_ground_truth_evidence (event_date, accepted, confidence desc);
