module OddJobs.Migrations
  ( module OddJobs.Migrations
  , module OddJobs.Types
  )
where

import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Types as PGS
import Data.Functor (void)
import OddJobs.Types

createJobTableQuery :: Query
createJobTableQuery = "CREATE TABLE IF NOT EXISTS ?" <>
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
  ", constraint incorrect_locking_info CHECK ((status <> 'locked' and locked_at is null and locked_by is null) or (status = 'locked' and locked_at is not null and locked_by is not null))" <>
  ");" <>
  "create index if not exists ? on ?(created_at);" <>
  "create index if not exists ? on ?(updated_at);" <>
  "create index if not exists ? on ?(locked_at);" <>
  "create index if not exists ? on ?(locked_by);" <>
  "create index if not exists ? on ?(status);" <>
  "create index if not exists ? on ?(run_at);"

createNotificationTrigger :: Query
createNotificationTrigger = "create or replace function ?() returns trigger as $$" <>
  "begin \n" <>
  "  perform pg_notify('?', \n" <>
  "    json_build_object('id', new.id, 'run_at', new.run_at, 'locked_at', new.locked_at)::text); \n" <>
  "  return new; \n" <>
  "end; \n" <>
  "$$ language plpgsql;" <>
  "create trigger ? after insert on ? for each row execute procedure ?();"

createJobTable :: Connection -> TableName -> IO ()
createJobTable conn tname = void $ do
  let tnameTxt = getTnameTxt tname
  PGS.execute conn createJobTableQuery
    ( tname
    , PGS.Identifier $ "idx_" <> tnameTxt <> "_created_at"
    , tname
    , PGS.Identifier $ "idx_" <> tnameTxt <> "_updated_at"
    , tname
    , PGS.Identifier $ "idx_" <> tnameTxt <> "_locked_at"
    , tname
    , PGS.Identifier $ "idx_" <> tnameTxt <> "_locked_by"
    , tname
    , PGS.Identifier $ "idx_" <> tnameTxt <> "_status"
    , tname
    , PGS.Identifier $ "idx_" <> tnameTxt <> "_run_at"
    , tname
    )
  PGS.execute conn createNotificationTrigger
    ( fnName
    , pgEventName tname
    , trgName
    , tname
    , fnName
    )
  where
    fnName = PGS.Identifier $ "notify_job_monitor_for_" <> (getTnameTxt tname)
    trgName = PGS.Identifier $ "trg_notify_job_monitor_for_" <> (getTnameTxt tname)
    getTnameTxt (PGS.QualifiedIdentifier _ tname') = tname'
