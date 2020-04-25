module OddJobs.Types where

import Database.PostgreSQL.Simple as PGS
import UnliftIO (MonadIO)
import UnliftIO.Concurrent (threadDelay)

type TableName = PGS.Query

pgEventName :: TableName -> Query
pgEventName tname = "job_created_" <> tname

newtype Seconds = Seconds { unSeconds :: Int } deriving (Eq, Show, Ord, Num, Read)

delaySeconds :: (MonadIO m) => Seconds -> m ()
delaySeconds (Seconds s) = threadDelay $ oneSec * s

oneSec :: Int
oneSec = 1000000

