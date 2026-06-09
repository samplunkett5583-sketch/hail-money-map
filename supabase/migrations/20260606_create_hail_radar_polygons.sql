-- hail_radar_polygons: stores MRMS/MESH/NEXRAD radar-derived hail coverage polygons.
-- Separate from storm_polygons so radar data can be read as a fast base layer.
-- The frontend renders this table as the broad pale-yellow hail base before
-- LSR-derived or swath-render storm_polygons cores.

create table if not exists public.hail_radar_polygons (
  id              bigserial    primary key,
  event_date      date         not null,
  source          text         not null,  -- 'mrms_mesh', 'nexrad_iem', etc.
  source_product  text,                   -- 'MESH_Max_1440min', etc.
  band_min        numeric,                -- hail size lower bound (inches)
  band_max        numeric,                -- hail size upper bound (inches, null = unbounded)
  polygon_geojson jsonb        not null,
  centroid_lat    numeric,
  centroid_lon    numeric,
  area_sq_mi      numeric,
  swath_index     integer,
  created_at      timestamptz  not null default now()
);

create index if not exists hail_radar_polygons_event_date_idx
  on public.hail_radar_polygons (event_date);

create index if not exists hail_radar_polygons_source_event_date_idx
  on public.hail_radar_polygons (source, event_date);

alter table public.hail_radar_polygons enable row level security;

create policy "Public read hail_radar_polygons"
  on public.hail_radar_polygons
  for select
  using (true);

grant select on public.hail_radar_polygons to anon;
grant select on public.hail_radar_polygons to authenticated;
grant all    on public.hail_radar_polygons to service_role;
grant usage  on sequence public.hail_radar_polygons_id_seq to service_role;
