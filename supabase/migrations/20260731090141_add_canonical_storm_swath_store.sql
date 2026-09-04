create table if not exists public.storm_swaths_canonical (
  id uuid not null default gen_random_uuid(),
  event_date date not null,
  storm_type text not null default 'hail',
  source text not null,
  source_product text not null,
  source_priority smallint not null default 1,
  quality_status text,
  swath_index integer not null,
  polygon_geojson jsonb not null,
  centroid_lat double precision,
  centroid_lon double precision,
  area_sq_mi double precision,
  threshold_value double precision,
  band_min numeric,
  band_max numeric,
  band_label text,
  event_start_utc timestamptz,
  event_end_utc timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint storm_swaths_canonical_pkey
    primary key (event_date, source, source_product, swath_index),
  constraint storm_swaths_canonical_id_key unique (id)
);

create index if not exists idx_storm_swaths_canonical_date_priority
  on public.storm_swaths_canonical (event_date, source_priority, swath_index);

alter table public.storm_swaths_canonical enable row level security;

grant select on public.storm_swaths_canonical to anon, authenticated;
grant select, insert, update, delete on public.storm_swaths_canonical to service_role;

drop policy if exists "Public weather swaths are readable"
  on public.storm_swaths_canonical;

create policy "Public weather swaths are readable"
  on public.storm_swaths_canonical
  for select
  to anon, authenticated
  using (true);
