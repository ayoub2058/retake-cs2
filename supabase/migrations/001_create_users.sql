create table if not exists public.users (
  steam_id bigint primary key,
  username text not null,
  avatar_url text,
  created_at timestamptz not null default now()
);
