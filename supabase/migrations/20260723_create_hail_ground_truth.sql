-- Auditable web-grounded hail observations used to supplement NOAA/NWS LSR data.
-- Accepted observations are also copied into hail_lsr_raw so every existing
-- summary, ranking, property lookup, and swath renderer consumes them.

create table if not exists public.hail_ground_truth_evidence (
  id text primary key,
  event_date date not null,
  event_time timestamptz not null,
  lat double precision not null,
  lon double precision not null,
  hail_in double precision not null check (hail_in between 0.50 and 8.00),
  city text,
  state text,
  county text,
  source_url text not null,
  source_title text,
  source_kind text not null,
  observation_text text,
  confidence numeric not null check (confidence between 0 and 1),
  accepted boolean not null default false,
  rejection_reason text,
  google_citations jsonb not null default '[]'::jsonb,
  raw jsonb not null default '{}'::jsonb,
  verified_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists hail_ground_truth_evidence_date_idx
  on public.hail_ground_truth_evidence (event_date desc);

create index if not exists hail_ground_truth_evidence_location_idx
  on public.hail_ground_truth_evidence (event_date, lat, lon);

create table if not exists public.hail_ground_truth_runs (
  event_date date primary key,
  status text not null,
  searched_regions integer not null default 0,
  found_reports integer not null default 0,
  accepted_reports integer not null default 0,
  message text,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  updated_at timestamptz not null default now()
);

alter table public.hail_ground_truth_evidence enable row level security;
alter table public.hail_ground_truth_runs enable row level security;

grant all on public.hail_ground_truth_evidence to service_role;
grant all on public.hail_ground_truth_runs to service_role;

