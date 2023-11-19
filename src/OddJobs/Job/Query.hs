
-- | Split out the queries because the multiline strings conflict
--   with CPP.
--   see https://gitlab.haskell.org/ghc/ghc/-/issues/16520
module OddJobs.Job.Query
  ( jobPollingSql
  , jobPollingWithResourceSql
  , killJobPollingSql
  , qWithResources
  , createJobQuery
  , ensureResource
  , registerResourceUsage
  , concatJobDbColumns
  , jobDbColumns
  , saveJobQuery
  )
where

import Database.PostgreSQL.Simple(Query)
import Data.String

-- | Ref: 'jobPoller'
jobPollingSql :: Query
jobPollingSql =
  "update ? set status = ?, locked_at = ?, locked_by = ?, attempts=attempts+1 \
  \ WHERE id in (select id from ? where (run_at<=? AND ((status in ?) OR (status = ? and locked_at<?))) \
  \ ORDER BY attempts ASC, run_at ASC LIMIT 1 FOR UPDATE) RETURNING id"

jobPollingWithResourceSql :: Query
jobPollingWithResourceSql =
  " UPDATE ? SET status = ?, locked_at = ?, locked_by = ?, attempts = attempts + 1 \
  \ WHERE id in (select id from ? where (run_at<=? AND ((status in ?) OR (status = ? and locked_at<?))) \
  \ AND ?(id) \
  \ ORDER BY attempts ASC, run_at ASC LIMIT 1) \
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
{-# INLINE concatJobDbColumns #-}
concatJobDbColumns :: (IsString s, Semigroup s) => s
concatJobDbColumns = concatJobDbColumnsInternal jobDbColumns

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
  , "result"
  , "parent_id"
  ]

{-# INLINE concatJobDbColumnsInternal #-}
concatJobDbColumnsInternal :: (IsString s, Semigroup s) => [s] -> s
concatJobDbColumnsInternal ys = go ys ""
  where
    go [] x = x
    go [col] x = x <> col
    go (col:cols) x = go cols (x <> col <> ", ")

-- | TODO
{-# INLINE concatJobDbColumnsWorkflow #-}
concatJobDbColumnsWorkflow :: (IsString s, Semigroup s) => s
concatJobDbColumnsWorkflow = concatJobDbColumnsInternal jobDbColumnsWorkflow
  where

-- | TODO
jobDbColumnsWorkflow :: (IsString s, Semigroup s) => [s]
jobDbColumnsWorkflow =
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
  , "result"
  , "parent_job_id"
  ]

saveJobQuery :: Query
saveJobQuery = 
  "UPDATE ? set run_at = ?, status = ?, payload = ?, last_error = ?, attempts = ?, locked_at = ?, locked_by = ?, result = ? WHERE id = ? return " <> concatJobDbColumns
