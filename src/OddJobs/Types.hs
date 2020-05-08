{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE RankNTypes #-}

module OddJobs.Types where

import Database.PostgreSQL.Simple as PGS
import UnliftIO (MonadIO)
import UnliftIO.Concurrent (threadDelay)
import Data.Text.Conversions
import Database.PostgreSQL.Simple.FromField as FromField
import Database.PostgreSQL.Simple.ToField as ToField
import Database.PostgreSQL.Simple.FromRow as FromRow
import Data.Time
import UnliftIO.Exception
import Data.Text (Text)
import GHC.Generics
import Data.Aeson as Aeson hiding (Success)
import Data.String.Conv
import Lucid (Html(..))
import Data.Pool (Pool)
import Control.Monad.Logger (LogLevel)

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


data LogEvent
  -- | Emitted when a job starts execution
  = LogJobStart !Job
  -- | Emitted when a job succeeds along with the time taken for execution.
  | LogJobSuccess !Job !NominalDiffTime
  -- | Emitted when a job fails (but will be retried) along with the time taken for
  -- /this/ attempt
  | LogJobFailed !Job !SomeException !FailureMode !NominalDiffTime
  -- | Emitted when a job times out and is picked-up again for execution
  | LogJobTimeout !Job
  -- | Emitted whenever 'jobPoller' polls the DB table
  | LogPoll
  -- | Emitted whenever any other event occurs
  | LogText !Text
  deriving (Show, Generic)

data FailureMode = FailWithRetry | FailPermanent deriving (Eq, Show)

data JobErrHandler a = forall e . (Exception e) => JobErrHandler (e -> Job -> FailureMode -> IO a)

data ConcurrencyControl
  -- | The maximum number of concurrent jobs that /this instance/ of the
  -- job-runner can execute. TODO: Link-off to tutorial.
  = MaxConcurrentJobs Int
  -- | __Not recommended:__ Please do not use this in production unless you know
  -- what you're doing. No machine can support unlimited concurrency. If your
  -- jobs are doing anything worthwhile, running a sufficiently large number
  -- concurrently is going to max-out /some/ resource of the underlying machine,
  -- such as, CPU, memory, disk IOPS, or network bandwidth.
  | UnlimitedConcurrentJobs

  -- | Use this to dynamically determine if the next job should be picked-up, or
  -- not. This is useful to write custom-logic to determine whether a limited
  -- resource is below a certain usage threshold (eg. CPU usage is below 80%).
  -- __Caveat:__ This feature has not been tested in production, yet. TODO:
  -- Link-off to tutorial.
  | DynamicConcurrency (IO Bool)

instance Show ConcurrencyControl where
  show cc = case cc of
    MaxConcurrentJobs n -> "MaxConcurrentJobs " <> show n
    UnlimitedConcurrentJobs -> "UnlimitedConcurrentJobs"
    DynamicConcurrency _ -> "DynamicConcurrency (IO Bool)"

type JobId = Int

data Status = Success
            | Queued
            | Failed
            | Retry
            | Locked
            deriving (Eq, Show, Generic, Enum, Bounded)

instance Ord Status where
  compare x y = compare (toText x) (toText y)

instance ToJSON Status where
  toJSON s = toJSON $ toText s

instance FromJSON Status where
  parseJSON = withText "Expecting text to convert into Job.Status" $ \t -> do
    case (fromText t :: Either String Status) of
      Left e -> fail e
      Right r -> pure r


newtype JobRunnerName = JobRunnerName { unJobRunnerName :: Text } deriving (Eq, Show, FromField, ToField, Generic, ToJSON, FromJSON)

data Job = Job
  { jobId :: JobId
  , jobCreatedAt :: UTCTime
  , jobUpdatedAt :: UTCTime
  , jobRunAt :: UTCTime
  , jobStatus :: Status
  , jobPayload :: Aeson.Value
  , jobLastError :: Maybe Value
  , jobAttempts :: Int
  , jobLockedAt :: Maybe UTCTime
  , jobLockedBy :: Maybe JobRunnerName
  } deriving (Eq, Show)

instance ToText Status where
  toText s = case s of
    Success -> "success"
    Queued -> "queued"
    Retry -> "retry"
    Failed -> "failed"
    Locked -> "locked"

instance (StringConv Text a) => FromText (Either a Status) where
  fromText t = case t of
    "success" -> Right Success
    "queued" -> Right Queued
    "failed" -> Right Failed
    "retry" -> Right Retry
    "locked" -> Right Locked
    x -> Left $ toS $ "Unknown job status: " <> x

instance FromField Status where
  fromField f mBS = (fromText <$> (fromField f mBS)) >>= \case
    Left e -> FromField.returnError PGS.ConversionFailed f e
    Right s -> pure s

instance ToField Status where
  toField s = toField $ toText s

instance FromRow Job where
  fromRow = Job
    <$> field -- jobId
    <*> field -- createdAt
    <*> field -- updatedAt
    <*> field -- runAt
    <*> field -- status
    <*> field -- payload
    <*> field -- lastError
    <*> field -- attempts
    <*> field -- lockedAt
    <*> field -- lockedBy

-- TODO: Add a sum-type for return status which can signal the monitor about
-- whether the job needs to be retried, marked successfull, or whether it has
-- completed failed.
type JobRunner = Job -> IO ()

data AllJobTypes
  = AJTFixed [Text]
  | AJTSql (Connection -> IO [Text])
  | AJTCustom (IO [Text])




-- | While odd-jobs is highly configurable and the 'Config' data-type might seem
-- daunting at first, it is not necessary to tweak every single configuration
-- parameter by hand. Please start-off by using the sensible defaults provided
-- by the [configuration helpers](#configHelpers), and tweaking
-- config parameters on a case-by-case basis.
data Config = Config
  { -- | The DB table which holds your jobs. Please note, this should have been
    -- created by the 'OddJobs.Migrations.createJobTable' function.
    cfgTableName :: TableName

    -- | The actualy "job-runner" that __you__ need to provide. Please look at
    -- the examples/tutorials if your applicaton's code is not in the @IO@
    -- monad.
  , cfgJobRunner :: Job -> IO ()

    -- | The number of times a failing job is retried before it is considered is
    -- "permanently failed" and ignored by the job-runner. This config parameter
    -- is called "/default/ max attempts" because, in the future, it would be
    -- possible to specify the number of retry-attemps on a per-job basis
    -- (__Note:__ per-job retry-attempts has not been implemented yet)
  , cfgDefaultMaxAttempts :: Int

    -- | Controls how many jobs can be run concurrently by /this instance/ of
    -- the job-runner. __Please note,__ this is NOT the global concurrency of
    -- entire job-queue. It is possible to have job-runners running on multiple
    -- machines, and each will apply the concurrency control independnt of other
    -- job-runners. TODO: Link-off to relevant section in the tutorial.
  , cfgConcurrencyControl :: ConcurrencyControl

    -- | The DB connection-pool to use for the job-runner. __Note:__ in case
    -- your jobs require a DB connection, please create a separate
    -- connection-pool for them. This pool will be used ONLY for monitoring jobs
    -- and changing their status. We need to have __at least 4 connections__ in
    -- this connection-pool for the job-runner to work as expected. (TODO:
    -- Link-off to tutorial)
  , cfgDbPool :: Pool Connection

    -- | How frequently should the 'jobPoller' check for jobs where the Job's
    -- 'jobRunAt' field indicates that it's time for the job to be executed.
    -- TODO: link-off to the tutorial.
  , cfgPollingInterval :: Seconds

  -- | User-defined callback function that is called whenever a job succeeds.
  , cfgOnJobSuccess :: Job -> IO ()

  -- | User-defined callback function that is called whenever a job fails. This
  -- does not indicate permanent failure and means the job will be retried. It
  -- is a good idea to log the failures to Airbrake, NewRelic, Sentry, or some
  -- other error monitoring tool.
  , cfgOnJobFailed :: forall a . [JobErrHandler a]

  -- | User-defined callback function that is called whenever a job starts
  -- execution.
  , cfgOnJobStart :: Job -> IO ()

  -- | User-defined callback function that is called whenever a job times-out.
  -- Also check 'cfgDefaultJobTimeout'
  , cfgOnJobTimeout :: Job -> IO ()

  -- | File to store the PID of the job-runner process. This is used only when
  -- invoking the job-runner as an independent background deemon (the usual mode
  -- of deployment). (TODO: Link-off to tutorial).
  , cfgPidFile :: Maybe FilePath

  -- | A "structured logging" function that __you__ need to provide. The
  -- @odd-jobs@ library does NOT use the standard logging interface provided by
  -- 'monad-logger' on purpose. TODO: link-off to tutorial. Also look at
  -- 'cffJobType' and 'defaultLogStr'
  , cfgLogger :: LogLevel -> LogEvent -> IO ()

  -- | How to extract the "job type" from a 'Job'. Related: 'defaultJobType'
  , cfgJobType :: Job -> Text

    -- | How long can a job run after which it is considered to be "crashed" and
    -- picked up for execution again
  , cfgDefaultJobTimeout :: Seconds

    -- | How to convert a list of 'Job's to a list of HTML fragments. This is
    -- used in the Web\/Admin UI. This function accepts a /list/ of jobs and
    -- returns a /list/ of 'Html' fragments is because, in case, you need to
    -- query another table to fetch some metadata (eg. convert a primary-key to
    -- a human-readable name), you can do it efficiently instead of resulting in
    -- an N+1 SQL bug. Ref: defaultJobToHtml
  , cfgJobToHtml :: [Job] -> IO [Html ()]

    -- | How to get a list of all known job-types?
  , cfgAllJobTypes :: AllJobTypes

    -- | TODO
  , cfgJobTypeSql :: PGS.Query
  }
