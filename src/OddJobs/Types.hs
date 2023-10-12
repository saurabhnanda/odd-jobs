{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE CPP #-}

module OddJobs.Types where

import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Types as PGS
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
import Lucid (Html)
import Data.Pool (Pool)
import Control.Monad.Logger (LogLevel)
import Data.Int (Int64)

-- | An alias for 'QualifiedIdentifier' type. It is used for the job table name.
-- Since this type has an instance of 'IsString',
-- you do not need to do anything special to create a value for this type. Just
-- ensure you have the @OverloadedStrings@ extention enabled. For example:
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
--
-- myJobsTable :: TableName
-- myJobsTable = "my_jobs"
-- @
--
-- This should also work for table names qualified by the schema name. For example:
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
--
-- myJobsTable :: TableName
-- myJobsTable = "odd_jobs.jobs"
-- @

type TableName = PGS.QualifiedIdentifier

pgEventName :: TableName -> PGS.Identifier
pgEventName (PGS.QualifiedIdentifier Nothing tname) = PGS.Identifier $ "jobs_created_" <> tname
pgEventName (PGS.QualifiedIdentifier (Just schema) tname) = PGS.Identifier $ "jobs_created_" <> schema <> "_" <> tname

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
  -- | Emitted when user kills a job and the job thread sucessfully cancelled thereafter.
  | LogKillJobSuccess !Job
  -- | Emitted when user kills a job and the job thread is not found in the threadRef
  -- | (most likely the job has either got completed or timed out).
  | LogKillJobFailed !Job
  -- | Emitted whenever 'OddJobs.Job.jobPoller' polls the DB table
  | LogPoll
  -- | Emitted whenever 'OddJobs.Job.jobPoller' polls the DB table
  | LogDeletionPoll !Int64
  -- | TODO
  | LogWebUIRequest
  -- | Emitted whenever any other event occurs
  | LogText !Text
  deriving (Show, Generic)

-- | Used by 'JobErrHandler' and 'LogEvent' to indicate the nature of failure.
data FailureMode
  -- The job failed, but will be retried in the future.
  = FailWithRetry
  -- | The job failed and will no longer be retried (probably because it has
  -- been tried 'cfgDefaultMaxAttempts' times already).
  | FailPermanent deriving (Eq, Show, Generic)

-- | Exception handler for jobs. This is conceptually very similar to how
-- 'Control.Exception.Handler' and 'Control.Exception.catches' (from
-- 'Control.Exception') work in-tandem. Using 'cfgOnJobFailed' you can install
-- /multiple/ exception handlers, where each handler is responsible for one type
-- of exception. OddJobs will execute the correct exception handler on the basis
-- of the type of runtime exception raised. For example:
--
-- @
-- cfgOnJobFailed =
--   [ JobErrHandler $ \(e :: HttpException) job failMode -> ...
--   , JobErrHandler $ \(e :: SqlException) job failMode -> ...
--   , JobErrHandler $ \(e :: ) job failMode -> ...
--   ]
-- @
--
-- __Note:__ Please refer to the section on [alerts and
-- notifications](https://www.haskelltutorials.com/odd-jobs/guide.html#alerts)
-- in the implementation guide to understand how to use the machinery provided
-- by 'JobErrHandler' and 'cfgOnJobFailed'.
data JobErrHandler = forall a e . (Exception e) => JobErrHandler (e -> Job -> FailureMode -> IO a)

type FunctionName = PGS.Identifier

data ResourceCfg = ResourceCfg
  { resCfgResourceTable :: TableName
  -- ^ Table to use for tracking resources and their limits. Both this and
  -- 'resCfgUsageTable' should be created by 'OddJobs.Migrations.createResourceTables'.

  , resCfgUsageTable :: TableName
  -- ^ Table to use for tracking how jobs use resources.

  , resCfgCheckResourceFunction :: FunctionName
  -- ^ Name of the function that checks that the resources required to run a
  -- job are all available (i.e. that the total usage of each resource, plus
  -- the usage the job needs to run, does not exceed the resource limit). The
  -- function should have the signature @(int) RETURNS bool@, and return @TRUE@
  -- if the job with the given ID has its resources available.

  , resCfgDefaultLimit :: Int
  -- ^ When a job requires a resource not already in 'resCfgResourceTable',
  -- what should its limit be set to?
  } deriving (Show)

newtype ResourceId = ResourceId { rawResourceId :: Text }
  deriving (Show)

-- | __Note:__ Please read the section on [controlling
-- concurrency](https://www.haskelltutorials.com/odd-jobs/guide.html#controlling-concurrency)
-- in the implementation guide to understand the implications of each option
-- given by the data-type.
data ConcurrencyControl
  -- | The maximum number of concurrent jobs that /this instance/ of the
  -- job-runner can execute.
  = MaxConcurrentJobs Int
  -- | __Not recommended:__ Please do not use this in production unless you know
  -- what you're doing. No machine can support unlimited concurrency. If your
  -- jobs are doing anything worthwhile, running a sufficiently large number
  -- concurrently is going to max-out /some/ resource of the underlying machine,
  -- such as, CPU, memory, disk IOPS, or network bandwidth.
  | UnlimitedConcurrentJobs

  -- | Limit jobs according to their access to resources, as tracked in the DB
  -- according to the 'ResourceCfg'.
  --
  -- __Warning:__ without sufficient limits, this can easily hit the same problems
  -- as 'UnlimitedConcurrentJobs' where jobs are able to exhaust system resources.
  -- It is therefore recommended that all jobs in your system use /some/ DB-tracked
  -- resource.
  | ResourceLimits ResourceCfg

  -- | Use this to dynamically determine if the next job should be picked-up, or
  -- not. This is useful to write custom-logic to determine whether a limited
  -- resource is below a certain usage threshold (eg. CPU usage is below 80%).
  -- __Caveat:__ This feature has not been tested in production, yet.
  | DynamicConcurrency (IO Bool)

instance Show ConcurrencyControl where
  show cc = case cc of
    MaxConcurrentJobs n -> "MaxConcurrentJobs " <> show n
    UnlimitedConcurrentJobs -> "UnlimitedConcurrentJobs"
    ResourceLimits cfg -> "ResourceLimits " <> show cfg
    DynamicConcurrency _ -> "DynamicConcurrency (IO Bool)"

type JobId = Int

data Status
  -- | In the current version of odd-jobs you /should not/ find any jobs having
  -- the 'Success' status, because successful jobs are immediately deleted.
  -- However, in the future, we may keep them around for a certain time-period
  -- before removing them from the jobs table.
  = Success
  -- | Jobs in 'Queued' status /may/ be picked up by the job-runner on the basis
  -- of the 'jobRunAt' field.
  | Queued
  -- | Jobs in 'Failed' status will not be retried by the job-runner.
  | Failed
  -- | Jobs with 'Cancelled' status are cancelled by the user and will not be
  -- retried by the job-runner
  | Cancelled
  -- | Jobs in 'Retry' status will be retried by the job-runner on the basis of
  -- the 'jobRunAt' field.
  | Retry
  -- | Jobs in 'Locked' status are currently being executed by a job-runner,
  -- which is identified by the 'jobLockedBy' field. The start of job-execution
  -- is indicated by the 'jobLocketAt' field.
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
  , jobResult :: Maybe Aeson.Value
  , jobParentId :: Maybe JobId
  } deriving (Eq, Show, Generic)

instance ToText Status where
  toText s = case s of
    Success -> "success"
    Queued -> "queued"
    Retry -> "retry"
    Failed -> "failed"
    Cancelled -> "cancelled"
    Locked -> "locked"

instance (StringConv Text a) => FromText (Either a Status) where
  fromText t = case t of
    "success" -> Right Success
    "queued" -> Right Queued
    "failed" -> Right Failed
    "cancelled" -> Right Cancelled
    "retry" -> Right Retry
    "locked" -> Right Locked
    x -> Left $ toS $ "Unknown job status: " <> x

instance FromField Status where
  fromField f mBS = fromField f mBS >>= (\case
    Left e -> FromField.returnError PGS.ConversionFailed f e
    Right s -> pure s)
    . fromText

instance ToField Status where
  toField s = toField $ toText s

jobRowParserSimple :: RowParser Job
jobRowParserSimple = jobRowParserInternal (pure Nothing) (pure Nothing)

jobRowParserWithWorkflow :: RowParser Job
jobRowParserWithWorkflow = jobRowParserInternal field field

{-# INLINE jobRowParserInternal #-}
jobRowParserInternal :: 
  RowParser (Maybe Aeson.Value) -> 
  RowParser (Maybe JobId) ->
  RowParser Job
jobRowParserInternal resultParser parentJobIdParser = Job
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
  <*> resultParser -- job result
  <*> parentJobIdParser -- parentJobId

instance FromRow Job where
  -- "Please do not depend on the FromRow instance of the Job type. It does not handle optional features, such as job-results and job-workflows. Depending upon your scenario, use 'jobRowParserSimple' or 'jobRowParserWithWorkflow' directly." #-}
  fromRow = jobRowParserSimple

-- TODO: Add a sum-type for return status which can signal the monitor about
-- whether the job needs to be retried, marked successfull, or whether it has
-- completed failed.
type JobRunner = Job -> IO (Maybe Aeson.Value)

-- | The web\/admin UI needs to know a \"master list\" of all job-types to be
-- able to power the \"filter by job-type\" feature. This data-type helps in
-- letting odd-jobs know /how/ to get such a master-list. The function specified
-- by this type is run once when the job-runner starts (and stored in an
-- internal @IORef@). After that the list of job-types needs to be updated
-- manually by pressing the appropriate \"refresh\" link in the admin\/web UI.
data AllJobTypes
  -- | A fixed-list of job-types. If you don't want to increase boilerplate,
  -- consider using 'OddJobs.ConfigBuilder.defaultConstantJobTypes' which will
  -- automatically generate the list of available job-types based on a sum-type
  -- that represents your job payload.
  = AJTFixed [Text]
  -- | Construct the list of job-types dynamically by looking at the actual
  -- payloads in 'cfgTableName' (using an SQL query).
  | AJTSql (Connection -> IO [Text])
  -- | A custom 'IO' action for fetching the list of job-types.
  | AJTCustom (IO [Text])


-- | While odd-jobs is highly configurable and the 'Config' data-type might seem
-- daunting at first, it is not necessary to tweak every single configuration
-- parameter by hand.
--
-- __Recommendation:__ Please start-off by building a 'Config' by using the
-- 'OddJobs.ConfigBuilder.mkConfig' function (to get something with sensible
-- defaults) and then tweaking config parameters on a case-by-case basis.
data Config = Config
  { -- | The DB table which holds your jobs. Please note, this should have been
    -- created by the 'OddJobs.Migrations.createJobTable' function.
    cfgTableName :: TableName

    -- | The actualy "job-runner" that __you__ need to provide. If this function
    -- throws a runtime exception, the job will be retried
    -- 'cfgDefaultMaxAttempts' times. Please look at the examples/tutorials if
    -- your applicaton's code is not in the @IO@ monad. Your job-runner can return
    -- a @Maybe Aeson.Value@, which will be stored as a 'jobResult'. Setting a non-@Nothing@
    -- return value makes sense only if you don't delete successful jobs 
    -- immediately (see: 'cfgDeleteSuccessfulJobs'), else you can use the 'OddJobs.Job.noJobResult' 
    -- utility function to always return a @Nothing@ value from this function.
  , cfgJobRunner :: Job -> IO (Maybe Aeson.Value)

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
    -- job-runners. __Ref:__ Section on [controllng
    -- concurrency](https://www.haskelltutorials.com/odd-jobs/guide.html#controlling-concurrency)
    -- in the implementtion guide.
  , cfgConcurrencyControl :: ConcurrencyControl

    -- | The DB connection-pool to use for the job-runner. __Note:__ in case
    -- your jobs require a DB connection, please create a separate
    -- connection-pool for them. This pool will be used ONLY for monitoring jobs
    -- and changing their status. We need to have __at least 4 connections__ in
    -- this connection-pool for the job-runner to work as expected.
  , cfgDbPool :: Pool Connection

    -- | How frequently should the 'jobPoller' check for jobs where the Job's
    -- 'jobRunAt' field indicates that it's time for the job to be executed.
    -- __Ref:__ Please read the section on [how Odd Jobs works
    -- (architecture)](https://www.haskelltutorials.com/odd-jobs/guide.html#architecture)
    -- to find out more.
  , cfgPollingInterval :: Seconds

  -- | User-defined callback function that is called whenever a job succeeds.
  , cfgOnJobSuccess :: Job -> IO ()

  -- | User-defined error-handler that is called whenever a job fails (indicated
  -- by 'cfgJobRunner' throwing an unhandled runtime exception). Please refer to
  -- 'JobErrHandler' for documentation on how to use this.
  , cfgOnJobFailed :: [JobErrHandler]

  -- | User-defined callback function that is called whenever a job starts
  -- execution.
  , cfgOnJobStart :: Job -> IO ()

  -- | User-defined callback function that is called whenever a job times-out.
  -- Also check 'cfgDefaultJobTimeout'
  , cfgOnJobTimeout :: Job -> IO ()

  -- | File to store the PID of the job-runner process. This is used only when
  -- invoking the job-runner as an independent background deemon (the usual mode
  -- of deployment).
  -- , cfgPidFile :: Maybe FilePath

  -- | A "structured logging" function that __you__ need to provide. The
  -- @odd-jobs@ library does NOT use the standard logging interface provided by
  -- 'monad-logger' on purpose. Also look at 'cfgJobType' and 'defaultLogStr'
  --
  -- __Note:__ Please take a look at the section on [structured
  -- logging](https://www.haskelltutorials.com/odd-jobs/guide.html#structured-logging)
  -- to find out how to use this to log in JSON.
  , cfgLogger :: LogLevel -> LogEvent -> IO ()

  -- | How to extract the "job type" from a 'Job'. If you are overriding this,
  -- please consider overriding 'uicfgJobTypeSql' as well. Related:
  -- 'OddJobs.ConfigBuilder.defaultJobType'
  , cfgJobType :: Job -> Text

    -- | How long can a job run after which it is considered to be "crashed" and
    -- picked up for execution again
  , cfgDefaultJobTimeout :: Seconds

    -- | After a job attempt, should it be immediately deleted to save table space? The default
    -- behaviour, as defined by 'OddJobs.ConfigBuilder.defaultImmediateJobDeletion' is to delete
    -- successful jobs immediately (and retain everything else). If you are providing your 
    -- own implementation here, __be careful__ to check for the job's status before deciding
    -- whether to delete it, or not.
    --
    -- A /possible/ use-case for non-successful jobs could be to immediately delete a failed
    -- job depending upon the 'jobResult' or 'jobLastError' if there is no use retrying the job.
  , cfgImmediateJobDeletion :: Job -> IO Bool

    -- | A funciton which will be run every 'cfgPollingInterval' seconds to delete
    -- old jobs that may be hanging around in the @jobs@ table (eg. failed jobs, cancelled jobs, or even
    -- successful jobs whose deletion has been delayed via a custom 'cfgImmediateJobDeletion' function).
    --
    -- Ref: 'OddJobs.ConfigBuilder.defaultDelayedJobDeletionSql'
  , cfgDelayedJobDeletion :: Maybe (PGS.Connection -> IO Int64)

    -- | How far into the future should jobs which can be retried be queued for?
    --
    -- The 'Int' argument is the number of times the job has been attepted. It will
    -- always be at least 1, since the job will have to have started at least once
    -- in order to fail and be retried. The default implementation is an exponential
    -- backoff of @'Seconds' $ 2 ^ 'jobAttempts'@.
  , cfgDefaultRetryBackoff :: Int -> IO Seconds

  -- | Controls whether to enable the job-results and job-workflow features. If this is
  --  @True@ then functions like 'OddJobs.Job.saveJobIO', etc. will start using the 
  -- 'jobResult' and 'jobParentJobId' fields as well.
  --
  -- __WARNING:__ Before enabling this, please ensure that you have created your jobs table
  -- via the `OddJobs.Migrations.createJobTableWithWorkflow' function, else your jobs table
  -- will not have the @results@ and @parent_job_id@ fields, and ALL your jobs will start
  -- failing at runtime.
  , cfgEnableWorkflows :: Bool
  }


data UIConfig = UIConfig
  { -- | The DB table which holds your jobs. Please note, this should have been
    -- created by the 'OddJobs.Migrations.createJobTable' function.
    uicfgTableName :: TableName

    -- | The DB connection-pool to use for the web UI. __Note:__ the same DB
    -- pool used by your job-runner can be passed here if it has a sufficient
    -- number of connections to satisfy both use-scases. Else create a separate
    -- DB pool to be used only by the web UI (this DB pool can have just 1-3
    -- connection, because __typically__ the web UI doesn't serve too many
    -- concurrent user in most real-life cases)
  , uicfgDbPool :: Pool Connection

    -- | How to extract the "job type" from a 'Job'. If you are overriding this,
    -- please consider overriding 'cfgJobTypeSql' as well. __Note:__ Usually
    -- 'cfgJobType' and 'uicfgJobType' would use the same value. Related:
    -- 'OddJobs.ConfigBuilder.defaultJobType'
  , uicfgJobType :: Job -> Text

    -- | How to extract the \"job type\" directly in SQL. There are many places,
    -- especially in the web\/admin UI, where we need to know a job's type
    -- directly in SQL (because transferrring the entire @payload@ column to
    -- Haskell, and then parsing it into JSON, and then applying the
    -- 'cfgJobType' function on it would be too inefficient). Ref:
    -- 'OddJobs.ConfigBuilder.defaultJobTypeSql' and 'uicfgJobType'
  , uicfgJobTypeSql :: PGS.Query

    -- | How to convert a list of 'Job's to a list of HTML fragments. This is
    -- used in the Web\/Admin UI. This function accepts a /list/ of jobs and
    -- returns a /list/ of 'Html' fragments, because, in case, you need to query
    -- another table to fetch some metadata (eg. convert a primary-key to a
    -- human-readable name), you can do it efficiently instead of resulting in
    -- an N+1 SQL bug. Ref: 'defaultJobToHtml'
  , uicfgJobToHtml :: [Job] -> IO [Html ()]

    -- | How to get a list of all known job-types? This is used by the
    -- Web\/Admin UI to power the \"filter by job-type\" functionality. The
    -- default value for this is 'OddJobs.ConfigBuilder.defaultDynamicJobTypes'
    -- which does a @SELECT DISTINCT payload ->> ...@ to get a list of job-types
    -- directly from the DB.
  , uicfgAllJobTypes :: AllJobTypes

    -- | A "structured logging" function that __you__ need to provide. The
    -- @odd-jobs@ library does NOT use the standard logging interface provided by
    -- 'monad-logger' on purpose. Also look at 'cfgJobType' and 'defaultLogStr'
    --
    -- __Note:__ Please take a look at the section on [structured
    -- logging](https://www.haskelltutorials.com/odd-jobs/guide.html#structured-logging)
    -- to find out how to use this to log in JSON.
  , uicfgLogger :: LogLevel -> LogEvent -> IO ()
  }
