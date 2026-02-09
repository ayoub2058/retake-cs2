create table if not exists public.rounds (
  id bigserial primary key,
  match_id bigint not null references public.matches(id) on delete cascade,
  round_number integer not null,
  winner_side text,
  reason text,
  ct_score integer,
  t_score integer,
  created_at timestamptz not null default now()
);

create index if not exists rounds_match_id_idx on public.rounds (match_id);
create unique index if not exists rounds_match_round_idx on public.rounds (match_id, round_number);
