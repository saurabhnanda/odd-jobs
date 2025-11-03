-- | Internal functions for testing. Not part of the public API.
--
-- These functions are exported for use in the test suite but are not
-- intended for external use. They may change or be removed without notice
-- in minor version updates.
module OddJobs.Job.Internal
  ( jobPollingIO
  ) where

import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Types as PGS (In(..))
import Data.Time.Clock
import OddJobs.Types
import OddJobs.Job.Query (jobPollingSql, defaultJobOrdering)

-- | Low-level helper for polling a single job. Uses default ordering.
--
-- This function was created for testing purposes to allow tests to directly
-- invoke the polling SQL without starting the full job monitor.
--
-- __Not for production use.__ Use 'OddJobs.Job.Config' with 'cfgJobOrdering'
-- for custom job ordering in production.
jobPollingIO :: Connection -> String -> TableName -> Seconds -> IO [Only JobId]
jobPollingIO pollerDbConn processName tname lockTimeout = do
  t <- getCurrentTime
  PGS.query pollerDbConn (jobPollingSql defaultJobOrdering)
             ( tname
             , Locked
             , t
             , processName
             , tname
             , t
             , In [Queued, Retry]
             , Locked
             , addUTCTime (fromIntegral $ negate $ unSeconds lockTimeout) t)
