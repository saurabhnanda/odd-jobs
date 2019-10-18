module OddJobs.Types where

import Database.PostgreSQL.Simple as PGS

type TableName = PGS.Query


pgEventName :: TableName -> Query
pgEventName tname = "job_created_" <> tname

newtype Seconds = Seconds { unSeconds :: Int } deriving (Eq, Show, Ord, Num)
