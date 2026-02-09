alter table public.matches_to_download
  add column if not exists coach_tip text,
  add column if not exists tip_sent boolean default false;
