-- Persist each account's scanner alert preferences on its app_users row.
alter table public.app_users
  add column if not exists scanner_settings jsonb not null default '{}'::jsonb;
