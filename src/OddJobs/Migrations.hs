module OddJobs.Migrations
  ( module OddJobs.Migrations
  , module OddJobs.Types
  )
where

import Database.PostgreSQL.Simple as PGS
import Data.Functor (void)
import OddJobs.Types

-- | Create database objects based on the name given for the primary job table,
-- with the names of other objects being generated autmatically.
createJobTable :: Connection -> TableName -> IO ()
createJobTable conn tname = createJobTables conn $ simpleTableNames tname

-- | Create database objects based on the name given for the primary job table
-- and the associated resource table. The names of indexes, functions, and
-- triggers are generated automatically.
createJobTables :: Connection -> TableNames -> IO ()
createJobTables conn tnames = do
  void $ PGS.execute_ conn (createJobTableQuery tnames)
  void $ PGS.execute_ conn (createResourceTableQuery tnames)
  void $ PGS.execute_ conn (createNotificationTrigger tnames)

-- | Remove all Odd Jobs objects from the database
dropJobTables :: Connection -> TableNames -> IO ()
dropJobTables conn tnames = do
  void $ PGS.execute_ conn $ dropObject "table" $ tnResource tnames
  void $ PGS.execute_ conn $ dropObject "table" $ tnJob tnames
  void $ PGS.execute_ conn $ dropObject "function" $ notifyFunctionName tnames
  where
    dropObject typ obj = "drop " <> typ <> " if exists " <> obj <> ";"

createJobTableQuery :: TableNames -> Query
createJobTableQuery (TableNames tname _) = "CREATE TABLE " <> tname <>
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
  ", resource_id text null" <>
  ", constraint incorrect_locking_info CHECK ((status <> 'locked' and locked_at is null and locked_by is null) or (status = 'locked' and locked_at is not null and locked_by is not null))" <>
  ");" <>
  "create index idx_" <> tname <> "_created_at on " <> tname <> "(created_at);" <>
  "create index idx_" <> tname <> "_updated_at on " <> tname <> "(updated_at);" <>
  "create index idx_" <> tname <> "_locked_at on " <> tname <> "(locked_at);" <>
  "create index idx_" <> tname <> "_locked_by on " <> tname <> "(locked_by);" <>
  "create index idx_" <> tname <> "_status on " <> tname <> "(status);" <>
  "create index idx_" <> tname <> "_run_at on " <> tname <> "(run_at);" <>
  "create index idx_" <> tname <> "_resource_id on " <> tname <>"(resource_id) where resource_id is not null;"

createResourceTableQuery :: TableNames -> Query
createResourceTableQuery tnames = "create table " <> tnResource tnames <>
  "( resource_id text primary key" <>
  ", resource_limit int" <>
  ");"

createNotificationTrigger :: TableNames -> Query
createNotificationTrigger tnames = "create or replace function " <> notifyFunctionName tnames <> "() returns trigger as $$" <>
  "begin \n" <>
  "  perform pg_notify('" <> pgEventName tnames <> "', \n" <>
  "    json_build_object('id', new.id, 'run_at', new.run_at, 'locked_at', new.locked_at)::text); \n" <>
  "  return new; \n" <>
  "end; \n" <>
  "$$ language plpgsql;" <>
  "create trigger " <> trgName <> " after insert on " <> tnJob tnames <> " for each row execute procedure " <> notifyFunctionName tnames <> "();"
  where
    trgName = "trg_notify_job_monitor_for_" <> tnJob tnames

notifyFunctionName :: TableNames -> Query
notifyFunctionName tnames = "notify_job_monitor_for_" <> tnJob tnames
