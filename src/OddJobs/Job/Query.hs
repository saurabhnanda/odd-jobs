
-- | Split out the queries because the multiline strings conflict
--   with CPP.
--   see https://gitlab.haskell.org/ghc/ghc/-/issues/16520
module OddJobs.Job.Query
  ( defaultJobOrdering
  , jobPollingSql
  , jobPollingWithResourceSql
  , killJobPollingSql
  , qWithResources
  , createJobQuery
  , ensureResource
  , registerResourceUsage
  , concatJobDbColumns
  , jobDbColumns
  , rescheduleJobSql
  , fetchJobByIdForUpdateSql
  )
where

import Database.PostgreSQL.Simple(Query)
import Data.String

-- | Default job ordering: jobs with fewer attempts first (prevents failed jobs from blocking),
-- then FIFO within same attempt count.
--
-- This is the historical default behavior. Use 'cfgJobOrdering' in 'Config' to override.
defaultJobOrdering :: Query
defaultJobOrdering = "attempts ASC, run_at ASC"

-- | Create job polling SQL with custom ordering and optional job type filter.
-- The ordering parameter should be just the ORDER BY expression without "ORDER BY" keywords.
-- The filter parameter is an optional WHERE clause fragment to filter jobs by type.
jobPollingSql :: Maybe Query  -- ^ Optional filter clause (e.g., "payload->>'tag' IN (?, ?)")
              -> Query        -- ^ ORDER BY expression
              -> Query
jobPollingSql mFilter ordering =
  "update ? set status = ?, locked_at = ?, locked_by = ?, attempts=attempts+1 \
  \ WHERE id in (select id from ? where (run_at<=? AND ((status in ?) OR (status = ? and locked_at<?)))"
  <> maybe "" (\f -> " AND (" <> f <> ")") mFilter
  <> " ORDER BY " <> ordering <> " LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id"

-- | Create job polling SQL with resources and custom ordering.
-- The filter parameter is an optional WHERE clause fragment to filter jobs by type.
jobPollingWithResourceSql :: Maybe Query  -- ^ Optional filter clause
                          -> Query        -- ^ ORDER BY expression
                          -> Query
jobPollingWithResourceSql mFilter ordering =
  " UPDATE ? SET status = ?, locked_at = ?, locked_by = ?, attempts = attempts + 1 \
  \ WHERE id in (select id from ? where (run_at<=? AND ((status in ?) OR (status = ? and locked_at<?)))"
  <> maybe "" (\f -> " AND (" <> f <> ")") mFilter
  <> " AND ?(id) \
  \ ORDER BY " <> ordering <> " LIMIT 1) \
  \ RETURNING id"

-- | Ref: 'killJobPoller'
killJobPollingSql :: Query
killJobPollingSql =
  "UPDATE ? SET locked_at = NULL, locked_by = NULL \
  \ WHERE id IN (SELECT id FROM ? WHERE status = ? AND locked_by = ? AND locked_at <= ? \
  \ ORDER BY locked_at ASC LIMIT 1 FOR UPDATE \
  \ ) RETURNING id"

qWithResources :: Query
qWithResources =
              "UPDATE ? SET status=?, locked_at=now(), locked_by=?, attempts=attempts+1 \
              \ WHERE id=? AND status in ? AND ?(id) RETURNING id"

createJobQuery :: Query
createJobQuery = "INSERT INTO ? (run_at, status, payload, last_error, attempts, locked_at, locked_by) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING " <> concatJobDbColumns

ensureResource :: Query
ensureResource = "INSERT INTO ? (id, usage_limit) VALUES (?, ?) ON CONFLICT DO NOTHING"

registerResourceUsage :: Query
registerResourceUsage = "INSERT INTO ? (job_id, resource_id, usage) VALUES (?, ?, ?)"

-- | All 'jobDbColumns' joined together with commas. Useful for constructing SQL
-- queries, eg:
--
-- @'query_' conn $ "SELECT " <> concatJobDbColumns <> "FROM jobs"@
concatJobDbColumns :: (IsString s, Semigroup s) => s
concatJobDbColumns = concatJobDbColumns_ jobDbColumns ""
  where
    concatJobDbColumns_ [] x = x
    concatJobDbColumns_ [col] x = x <> col
    concatJobDbColumns_ (col:cols) x = concatJobDbColumns_ cols (x <> col <> ", ")

-- | Ref: 'rescheduleJob'
fetchJobByIdForUpdateSql :: Query
fetchJobByIdForUpdateSql = "select " <> concatJobDbColumns <> " from ? where id = ? for update"

-- | Ref: 'rescheduleJob'
rescheduleJobSql :: Query
rescheduleJobSql = "update ? set status = ?, attempts = ?, run_at = ? where id = ? returning " <> concatJobDbColumns

-- | If you are writing SQL queries where you want to return ALL columns from
-- the jobs table it is __recommended__ that you do not issue a @SELECT *@ or
-- @RETURNIG *@. List out specific DB columns using 'jobDbColumns' and
-- 'concatJobDbColumns' instead. This will insulate you from runtime errors
-- caused by addition of new columns to 'cfgTableName' in future versions of
-- OddJobs.
jobDbColumns :: (IsString s, Semigroup s) => [s]
jobDbColumns =
  [ "id"
  , "created_at"
  , "updated_at"
  , "run_at"
  , "status"
  , "payload"
  , "last_error"
  , "attempts"
  , "locked_at"
  , "locked_by"
  ]
