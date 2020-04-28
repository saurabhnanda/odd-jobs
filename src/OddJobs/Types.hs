module OddJobs.Types where

import Database.PostgreSQL.Simple as PGS
import UnliftIO (MonadIO)
import UnliftIO.Concurrent (threadDelay)

-- | An alias for 'Query' type. Since this type has an instance of 'IsString'
-- you do not need to do anything special to create a value for this type. Just
-- ensure you have the @OverloadedStrings@ extention enabled. For example:
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
--
-- myJobsTable :: TableName
-- myJobsTable = "my_jobs"
-- @
type TableName = PGS.Query

pgEventName :: TableName -> Query
pgEventName tname = "job_created_" <> tname

newtype Seconds = Seconds { unSeconds :: Int } deriving (Eq, Show, Ord, Num, Read)

-- | Convenience wrapper on-top of 'threadDelay' which takes 'Seconds' as an
-- argument, instead of micro-seconds.
delaySeconds :: (MonadIO m) => Seconds -> m ()
delaySeconds (Seconds s) = threadDelay $ oneSec * s

oneSec :: Int
oneSec = 1000000

