module Migrations where

import Database.PostgreSQL.Simple as PGS
import Data.Functor (void)

createJobTableQuery :: Query
createJobTableQuery = "CREATE TABLE jobs (" <>
  "  id serial primary key" <>
  ", created_at timestamp with time zone default now() not null" <>
  ", updated_at timestamp with time zone default now() not null" <>
  ", run_at timestamp with time zone default now() not null" <>
  ", status text not null" <>
  ", payload jsonb not null" <>
  ", last_error jsonb null" <>
  ", attempts int not null default 0" <>
  ", locked_at timestamp with time zone null" <>
  ", locked_by text null" <>
  ", constraint incorrect_locking_info CHECK ((locked_at is null and locked_by is null) or (locked_at is not null and locked_by is not null))" <>
  ");" <>
  "create index idx_jobs_created_at on jobs(created_at);" <>
  "create index idx_jobs_updated_at on jobs(updated_at);" <>
  "create index idx_jobs_locked_at on jobs(locked_at);" <>
  "create index idx_jobs_locked_by on jobs(locked_by);" <>
  "create index idx_jobs_status on jobs(status);" <>
  "create index idx_jobs_run_at on jobs(run_at);"

createNotificationTrigger :: Query
createNotificationTrigger = "create or replace function notify_job_monitor() returns trigger as $$" <>
  "begin \n" <>
  "  perform pg_notify('job_created_event', row_to_json(new)::text); \n" <>
  "  return new; \n" <>
  "end; \n" <>
  "$$ language plpgsql;" <>
  "create trigger trg_notify_job_monitor after insert on jobs for each row execute procedure notify_job_monitor();"


createJobTable :: Connection -> IO ()
createJobTable conn = void $ do
  PGS.execute_ conn createJobTableQuery
  PGS.execute_ conn createNotificationTrigger
