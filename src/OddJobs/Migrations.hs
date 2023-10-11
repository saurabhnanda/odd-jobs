{-# LANGUAGE RecordWildCards #-}
module OddJobs.Migrations
  ( module OddJobs.Migrations
  , module OddJobs.Types
  )
where

import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Types as PGS
import Database.PostgreSQL.Simple.ToRow as PGS
import Data.Functor (void)
import OddJobs.Types

createJobTableQuery :: Query
createJobTableQuery = createJobTableQueryInternal False

createJobTable :: Connection -> TableName -> IO ()
createJobTable = createJobTableInternal False

createJobTableQueryInternal :: 
  Bool ->
  -- ^ whether to enable job-results and job-workflow features
  Query
createJobTableQueryInternal enableWorkflows = "CREATE TABLE IF NOT EXISTS ?" <>
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
  if enableWorkflows then ", result jsonb, parent_job_id int references ?(id)" else "" <>
  ", constraint incorrect_locking_info CHECK (" <>
    "(locked_at is null and locked_by is null and status <> 'locked') or " <>
    "(locked_at is not null and locked_by is not null and (status = 'locked' or status = 'cancelled')))" <>
  ");" <>
  "create index if not exists ? on ?(created_at);" <>
  "create index if not exists ? on ?(updated_at);" <>
  "create index if not exists ? on ?(locked_at);" <>
  "create index if not exists ? on ?(locked_by);" <>
  "create index if not exists ? on ?(status);" <>
  "create index if not exists ? on ?(run_at);" <>
  if enableWorkflows then "create index if not exists ? on ?(parent_job_id);" else ""

createNotificationTrigger :: Query
createNotificationTrigger = "create or replace function ?() returns trigger as $$" <>
  "begin \n" <>
  "  perform pg_notify('?', \n" <>
  "    json_build_object('id', new.id, 'run_at', new.run_at, 'locked_at', new.locked_at)::text); \n" <>
  "  return new; \n" <>
  "end; \n" <>
  "$$ language plpgsql;" <>
  "drop trigger if exists ? on ?;" <>
  "create trigger ? after insert on ? for each row execute procedure ?();"

createJobTableInternal :: 
  Bool ->
  -- ^ whether to enable job-results and job-workflow features
  Connection ->
  TableName ->
  IO ()
createJobTableInternal enableWorkflows conn tname = void $ do
  let tnameTxt = getTnameTxt tname
  let a1 =  ( PGS.Identifier $ "idx_" <> tnameTxt <> "_created_at"
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
      finalFields = if enableWorkflows
                    then PGS.toRow $ (tname, tname) PGS.:. a1 PGS.:. (tname, PGS.Identifier $ "idx_" <> tnameTxt <> "_parent_job_id")
                    else PGS.toRow $ (Only tname) PGS.:. a1

  _ <- PGS.execute conn (createJobTableQueryInternal enableWorkflows) finalFields
  PGS.execute conn createNotificationTrigger
    ( fnName
    , pgEventName tname
    , trgName
    , tname
    , trgName
    , tname
    , fnName
    )
  where
    fnName = PGS.Identifier $ "notify_job_monitor_for_" <> getTnameTxt tname
    trgName = PGS.Identifier $ "trg_notify_job_monitor_for_" <> getTnameTxt tname
    getTnameTxt (PGS.QualifiedIdentifier _ tname') = tname'

createResourceTableQuery :: Query
createResourceTableQuery = "CREATE TABLE IF NOT EXISTS ?" <>
  "( id text primary key" <>
  ", usage_limit int not null" <>
  ")";

createUsageTableQuery :: Query
createUsageTableQuery = "CREATE TABLE IF NOT EXISTS ?" <>
  "( job_id serial not null REFERENCES ? ON DELETE CASCADE" <>
  ", resource_id text not null REFERENCES ? ON DELETE CASCADE" <>
  ", usage int not null" <>
  ", PRIMARY KEY (job_id, resource_id)" <>
  ");"

createUsageFunction :: Query
createUsageFunction = "CREATE OR REPLACE FUNCTION ?(resourceId text) RETURNS int as $$" <>
  " SELECT coalesce(sum(usage), 0) FROM ? AS j INNER JOIN ? AS jr ON j.id = jr.job_id " <>
  " WHERE jr.resource_id = $1 AND j.status = ? " <>
  " $$ LANGUAGE SQL;"

createCheckResourceFunction :: Query
createCheckResourceFunction = "CREATE OR REPLACE FUNCTION ?(jobId int) RETURNS bool as $$" <>
  " SELECT coalesce(bool_and(?(resource.id) + job_resource.usage <= resource.usage_limit), true) FROM " <>
  " ? AS job_resource INNER JOIN ? AS resource ON job_resource.resource_id = resource.id " <>
  " WHERE job_resource.job_id = $1" <>
  " $$ LANGUAGE SQL;"

createResourceTables :: 
  Connection -> 
  TableName ->
  -- ^ Name of the jobs table
  ResourceCfg ->
  IO ()
createResourceTables conn jobTableName ResourceCfg{..} = do
  void $ PGS.execute conn createResourceTableQuery (PGS.Only resCfgResourceTable)
  void $ PGS.execute conn createUsageTableQuery
    ( resCfgUsageTable
    , jobTableName
    , resCfgResourceTable
    )
  void $ PGS.execute conn createUsageFunction
    ( usageFnName
    , jobTableName
    , resCfgUsageTable
    , Locked
    )
  void $ PGS.execute conn createCheckResourceFunction
    ( resCfgCheckResourceFunction
    , usageFnName
    , resCfgUsageTable
    , resCfgResourceTable
    )
  where
    usageFnName = PGS.Identifier $ "calculate_usage_for_resource_from_" <> getTnameTxt resCfgUsageTable
    getTnameTxt (PGS.QualifiedIdentifier _ tname') = tname'
