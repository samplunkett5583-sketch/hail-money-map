create table if not exists public.hail_lsr_raw (
  id text primary key,
  event_time timestamptz not null,
  event_date date not null,
  lat double precision not null,
  lon double precision not null,
  hail_in double precision null,
  state text null,
  county text null,
  source text not null,
  raw jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists hail_lsr_raw_event_date_idx on public.hail_lsr_raw (event_date);
create index if not exists hail_lsr_raw_event_time_idx on public.hail_lsr_raw (event_time);
create index if not exists hail_lsr_raw_state_idx on public.hail_lsr_raw (state);