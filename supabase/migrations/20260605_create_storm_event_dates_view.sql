-- Lightweight date-list view for the Most Recent Storms panel.
-- Returns one row per unique storm event date from all three source tables.
-- The frontend reads this view instead of paginating through raw storm report rows,
-- making date list load fast (one small query vs. thousands of raw rows).

create or replace view public.storm_event_dates as
select distinct event_date::date as event_date
from (
  select event_date from public.hail_lsr_raw   where event_date is not null
  union
  select event_date from public.storm_lsr_raw  where event_date is not null
  union
  select event_date from public.storm_polygons where event_date is not null
) d;

grant select on public.storm_event_dates to anon;
grant select on public.storm_event_dates to authenticated;
grant select on public.storm_event_dates to service_role;
