module Migrations
  ( module Migrations
  , module Types
  )
where

import Database.PostgreSQL.Simple as PGS
import Data.Functor (void)
import Types

createJobTableQuery :: TableName -> Query
createJobTableQuery tname = "CREATE TABLE " <> tname <>
  "( id serial primary key" <>
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
  "create index idx_" <> tname <> "_created_at on " <> tname <> "(created_at);" <>
  "create index idx_" <> tname <> "_updated_at on " <> tname <> "(updated_at);" <>
  "create index idx_" <> tname <> "_locked_at on " <> tname <> "(locked_at);" <>
  "create index idx_" <> tname <> "_locked_by on " <> tname <> "(locked_by);" <>
  "create index idx_" <> tname <> "_status on " <> tname <> "(status);" <>
  "create index idx_" <> tname <> "_run_at on " <> tname <> "(run_at);"

createNotificationTrigger :: TableName -> Query
createNotificationTrigger tname = "create or replace function " <> fnName <> "() returns trigger as $$" <>
  "begin \n" <>
  "  perform pg_notify('" <> pgEventName tname <> "', row_to_json(new)::text); \n" <>
  "  return new; \n" <>
  "end; \n" <>
  "$$ language plpgsql;" <>
  "create trigger " <> trgName <> " after insert on " <> tname <> " for each row execute procedure " <> fnName <> "();"
  where
    fnName = "notify_job_monitor_for_" <> tname
    trgName = "trg_notify_job_monitor_for_" <> tname


createJobTable :: Connection -> TableName -> IO ()
createJobTable conn tname = void $ do
  PGS.execute_ conn (createJobTableQuery tname)
  PGS.execute_ conn (createNotificationTrigger tname)
