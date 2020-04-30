{-# LANGUAGE RankNTypes, FlexibleInstances, FlexibleContexts, PartialTypeSignatures, TupleSections, DeriveGeneric, UndecidableInstances #-}
{-# LANGUAGE ExistentialQuantification #-}
module OddJobs.Job
  (
    -- * Starting the job-runner
    --
    -- $startRunner
    startJobRunner

    -- * Configuring the job-runner
    --
    -- $config
  ,  Config(..)
  , ConcurrencyControl(..)

    -- ** Configuration helpers
    --
    -- $configHelpers
  , defaultConfig
  , defaultJobToText
  , defaultJobType
  , defaultTimedLogger
  , defaultLogStr
  , defaultPollingInterval
  , defaultLockTimeout
  , withConnectionPool

  -- * Creating/scheduling jobs
  --
  -- $createJobs
  , createJob
  , scheduleJob

  -- * @Job@ and associated data-types
  --
  -- $dataTypes
  , Job(..)
  , JobId
  , Status(..)
  , TableName
  , delaySeconds
  , Seconds(..)
  -- ** Structured logging
  --
  -- $logging
  , LogEvent(..)
  , LogLevel(..)

  -- * Job-runner interals
  --
  -- $internals
  , jobMonitor
  , jobEventListener
  , jobPoller
  , jobPollingSql
  , JobRunner
  , HasJobRunner (..)

  -- * Database helpers
  --
  -- $dbHelpers
  , findJobById
  , findJobByIdIO
  , saveJob
  , saveJobIO
  , jobDbColumns
  , concatJobDbColumns

  -- * JSON helpers
  --
  -- $jsonHelpers
  , eitherParsePayload
  , throwParsePayload
  , eitherParsePayloadWith
  , throwParsePayloadWith

  , JobErrHandler(..)
  )
where

import OddJobs.Types
import Data.Pool
import Data.Text as T
import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Notification
import Database.PostgreSQL.Simple.FromField as FromField
import Database.PostgreSQL.Simple.ToField as ToField
import Database.PostgreSQL.Simple.FromRow as FromRow
import UnliftIO.Async
import UnliftIO.Concurrent (threadDelay, myThreadId)
import Data.String
import System.Posix.Process (getProcessID)
import Network.HostName (getHostName)
import UnliftIO.MVar
import Debug.Trace
import Control.Monad.Logger as MLogger (LogLevel(..), LogStr, toLogStr)
import UnliftIO.IORef
import UnliftIO.Exception ( SomeException(..), try, catch, finally
                          , catchAny, bracket, Exception(..), throwIO
                          , catches, Handler(..), mask_, throwString
                          )
import Data.Proxy
import Control.Monad.Trans.Control
import Control.Monad.IO.Unlift (MonadUnliftIO, withRunInIO, liftIO)
import Data.Text.Conversions
import Data.Time
import Data.Aeson hiding (Success)
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson (Parser, parseMaybe)
import Data.String.Conv (StringConv(..), toS)
import Data.Functor (void)
import Control.Monad (forever)
import Data.Maybe (isNothing, maybe, fromMaybe)
import Data.Either (either)
import Control.Monad.Reader
import GHC.Generics
import qualified Data.HashMap.Strict as HM
import qualified Data.List as DL
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import System.FilePath (FilePath)
import qualified System.Directory as Dir
import Data.Aeson.Internal (iparse, IResult(..), formatError)
import qualified System.Log.FastLogger as FLogger
import Prelude hiding (log)


-- | The documentation of odd-jobs currently promotes 'startJobRunner', which
-- expects a fairly detailed 'Config' record, as a top-level function for
-- initiating a job-runner. However, internally, this 'Config' record is used as
-- an enviroment for a 'ReaderT', and almost all functions are written in this
-- 'ReaderT' monad which impleents an instance of the 'HasJobRunner' type-class.
--
-- __In future,__ this /internal/ implementation detail will allow us to offer a
-- type-class based interface as well (similar to what
-- 'Yesod.JobQueue.YesodJobQueue' provides).
class (MonadUnliftIO m, MonadBaseControl IO m) => HasJobRunner m where
  getPollingInterval :: m Seconds
  onJobSuccess :: Job -> m ()
  onJobFailed :: forall a . m [JobErrHandler a]
  getJobRunner :: m (Job -> IO ())
  getDbPool :: m (Pool Connection)
  getTableName :: m TableName
  onJobStart :: Job -> m ()
  getDefaultMaxAttempts :: m Int
  getRunnerEnv :: m RunnerEnv
  getConcurrencyControl :: m ConcurrencyControl
  getPidFile :: m (Maybe FilePath)
  getJobToText :: m (Job -> Text)
  log :: LogLevel -> LogEvent -> m ()


-- $logging
--
-- TODO: Complete the prose here


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
  , cfgOnJobTimeout :: Job -> IO ()

  -- | File to store the PID of the job-runner process. This is used only when
  -- invoking the job-runner as an independent background deemon (the usual mode
  -- of deployment). (TODO: Link-off to tutorial).
  , cfgPidFile :: Maybe FilePath

  -- | A "structured logging" function that __you__ need to provide. The
  -- @odd-jobs@ library does NOT use the standard logging interface provided by
  -- 'monad-logger' on purpose. TODO: link-off to tutorial. Please also read
  -- 'cfgJobToText' and 'cfgJobType'
  , cfgLogger :: LogLevel -> LogEvent -> IO ()

  -- | When emitting certain text messages in logs, how should the 'Job' be
  -- summarized in a textual format? Related: 'defaultJobToText'
  , cfgJobToText :: Job -> Text

  -- | How to extract the "job type" from a 'Job'. Related: 'defaultJobType'
  , cfgJobType :: Job -> Text
  }

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


-- $configHelpers
--
-- #configHelpers#

-- | This function gives you a 'Config' with a bunch of sensible defaults
-- already applied. It requies the bare minimum arguments that this library
-- cannot assume on your behalf.
--
-- It makes a few __important assumptions__ about your 'jobPayload 'JSON, which
-- are documented in 'defaultJobType'.
defaultConfig :: (LogLevel -> LogEvent -> IO ())  -- ^ "Structured logging" function. Ref: 'cfgLogger'
              -> TableName                        -- ^ DB table which holds your jobs. Ref: 'cfgTableName'
              -> Pool Connection                  -- ^ DB connection-pool to be used by job-runner. Ref: 'cfgDbPool'
              -> ConcurrencyControl               -- ^ Concurrency configuration. Ref: 'cfgConcurrencyControl'
              -> (Job -> IO ())                   -- ^ The actual "job runner" which contains your application code. Ref: 'cfgJobRunner'
              -> Config
defaultConfig logger tname dbpool ccControl jrunner =
  let cfg = Config
            { cfgPollingInterval = defaultPollingInterval
            , cfgOnJobSuccess = (const $ pure ())
            , cfgOnJobFailed = []
            , cfgJobRunner = jrunner
            , cfgLogger = logger
            , cfgDbPool = dbpool
            , cfgOnJobStart = (const $ pure ())
            , cfgDefaultMaxAttempts = 10
            , cfgTableName = tname
            , cfgOnJobTimeout = (const $ pure ())
            , cfgConcurrencyControl = ccControl
            , cfgPidFile = Nothing
            , cfgJobToText = defaultJobToText (cfgJobType cfg)
            , cfgJobType = defaultJobType
            }
  in cfg

-- | Used only by 'defaultLogStr' now. TODO: Is this even required anymore?
-- Should this be removed?
defaultJobToText :: (Job -> Text) -> Job -> Text
defaultJobToText jobTypeFn job@Job{jobId} =
  "JobId=" <> (toS $ show jobId) <> " JobType=" <> jobTypeFn job

-- | This makes __two important assumptions__. First, this /assumes/ that jobs
-- in your app are represented by a sum-type. For example:
--
-- @
-- data MyJob = SendWelcomeEmail Int
--            | SendPasswordResetEmail Text
--            | SetupSampleData Int
-- @
--
-- Second, it /assumes/ that the JSON representatin of this sum-type is
-- "tagged". For example, the following...
--
-- > let pload = SendWelcomeEmail 10
--
-- ...when converted to JSON, would look like...
--
-- > {"tag":"SendWelcomeEmail", "contents":10}
--
-- It uses this assumption to extract the "job type" from a 'Data.Aeson.Value'
-- (which would be @SendWelcomeEmail@ in the example given above). This is used
-- in logging and the admin UI.
--
-- Even if tihs assumption is violated, the job-runner /should/ continue to
-- function. It's just that you won't get very useful log messages.
defaultJobType :: Job -> Text
defaultJobType Job{jobPayload} =
  case jobPayload of
    Aeson.Object hm -> case HM.lookup "tag" hm of
      Just (Aeson.String t) -> t
      _ -> "unknown"
    _ -> "unknown"



data RunnerEnv = RunnerEnv
  { envConfig :: !Config
  , envJobThreadsRef :: !(IORef [Async ()])
  }

type RunnerM = ReaderT RunnerEnv IO

logCallbackErrors :: (HasJobRunner m) => JobId -> Text -> m () -> m ()
logCallbackErrors jid msg action = catchAny action $ \e -> log LevelError $ LogText $ msg <> " Job ID=" <> toS (show jid) <> ": " <> toS (show e)

instance HasJobRunner RunnerM where
  getPollingInterval = cfgPollingInterval . envConfig <$> ask
  onJobFailed = cfgOnJobFailed . envConfig  <$> ask
  onJobSuccess job = do
    fn <- cfgOnJobSuccess . envConfig <$> ask
    logCallbackErrors (jobId job) "onJobSuccess" $ liftIO $ fn job
  getJobRunner = cfgJobRunner . envConfig <$> ask
  getDbPool = cfgDbPool . envConfig <$> ask
  getTableName = cfgTableName . envConfig <$> ask
  onJobStart job = do
    fn <- cfgOnJobStart . envConfig <$> ask
    logCallbackErrors (jobId job) "onJobStart" $ liftIO $ fn job

  getDefaultMaxAttempts = cfgDefaultMaxAttempts . envConfig <$> ask

  getRunnerEnv = ask

  getConcurrencyControl = (cfgConcurrencyControl . envConfig <$> ask)

  getPidFile = cfgPidFile . envConfig <$> ask
  getJobToText = cfgJobToText . envConfig <$> ask

  log logLevel logEvent = do
    loggerFn <- cfgLogger . envConfig <$> ask
    liftIO $ loggerFn logLevel logEvent

-- | Start the job-runner in the /current/ thread, i.e. you'll need to use
-- 'forkIO' or 'async' manually, if you want the job-runner to run in the
-- background. Consider using 'OddJobs.Cli' to rapidly build your own
-- standalone daemon.
startJobRunner :: Config -> IO ()
startJobRunner jm = do
  r <- newIORef []
  let monitorEnv = RunnerEnv
                   { envConfig = jm
                   , envJobThreadsRef = r
                   }
  runReaderT jobMonitor monitorEnv

-- | Convenience function to create a DB connection-pool with some sensible
-- defaults. Please see the source-code of this function to understand what it's
-- doing. TODO: link-off to tutorial.
withConnectionPool :: (MonadUnliftIO m)
                   => Either BS.ByteString PGS.ConnectInfo
                   -> (Pool PGS.Connection -> m a)
                   -> m a
withConnectionPool connConfig action = withRunInIO $ \runInIO -> do
  bracket poolCreator destroyAllResources (runInIO . action)
  where
    poolCreator = liftIO $
      case connConfig of
        Left connString ->
          createPool (PGS.connectPostgreSQL connString) PGS.close 1 (fromIntegral $ 2 * (unSeconds defaultPollingInterval)) 5
        Right connInfo ->
          createPool (PGS.connect connInfo) PGS.close 1 (fromIntegral $ 2 * (unSeconds defaultPollingInterval)) 5

-- | As the name says. Ref: 'cfgPollingInterval'
defaultPollingInterval :: Seconds
defaultPollingInterval = Seconds 5

type JobId = Int

data Status = Success
            | Queued
            | Failed
            | Retry
            | Locked
            deriving (Eq, Show, Generic, Enum)

instance Ord Status where
  compare x y = compare (toText x) (toText y)

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
  , jobLockedBy :: Maybe Text
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


jobWorkerName :: IO String
jobWorkerName = do
  pid <- getProcessID
  hname <- getHostName
  pure $ hname ++ ":" ++ (show pid)

-- | TODO: Make this configurable for the job-runner, why is this still
-- hard-coded?
defaultLockTimeout :: Seconds
defaultLockTimeout = Seconds 600

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

concatJobDbColumns :: (IsString s, Semigroup s) => s
concatJobDbColumns = concatJobDbColumns_ jobDbColumns ""
  where
    concatJobDbColumns_ [] x = x
    concatJobDbColumns_ (col:[]) x = x <> col
    concatJobDbColumns_ (col:cols) x = concatJobDbColumns_ cols (x <> col <> ", ")


findJobByIdQuery :: TableName -> PGS.Query
findJobByIdQuery tname = "SELECT " <> concatJobDbColumns <> " FROM " <> tname <> " WHERE id = ?"

withDbConnection :: (HasJobRunner m)
                 => (Connection -> m a)
                 -> m a
withDbConnection action = do
  pool <- getDbPool
  withResource pool action

findJobById :: (HasJobRunner m)
            => JobId
            -> m (Maybe Job)
findJobById jid = do
  tname <- getTableName
  withDbConnection $ \conn -> liftIO $ findJobByIdIO conn tname jid

findJobByIdIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
findJobByIdIO conn tname jid = PGS.query conn (findJobByIdQuery tname) (Only jid) >>= \case
  [] -> pure Nothing
  [j] -> pure (Just j)
  js -> Prelude.error $ "Not expecting to find multiple jobs by id=" <> (show jid)


saveJobQuery :: TableName -> PGS.Query
saveJobQuery tname = "UPDATE " <> tname <> " set run_at = ?, status = ?, payload = ?, last_error = ?, attempts = ?, locked_at = ?, locked_by = ? WHERE id = ? RETURNING " <> concatJobDbColumns


saveJob :: (HasJobRunner m) => Job -> m Job
saveJob j = do
  tname <- getTableName
  withDbConnection $ \conn -> liftIO $ saveJobIO conn tname j

saveJobIO :: Connection -> TableName -> Job -> IO Job
saveJobIO conn tname Job{jobRunAt, jobStatus, jobPayload, jobLastError, jobAttempts, jobLockedBy, jobLockedAt, jobId} = do
  rs <- PGS.query conn (saveJobQuery tname)
        ( jobRunAt
        , jobStatus
        , jobPayload
        , jobLastError
        , jobAttempts
        , jobLockedAt
        , jobLockedBy
        , jobId
        )
  case rs of
    [] -> Prelude.error $ "Could not find job while updating it id=" <> (show jobId)
    [j] -> pure j
    js -> Prelude.error $ "Not expecting multiple rows to ber returned when updating job id=" <> (show jobId)

data TimeoutException = TimeoutException deriving (Eq, Show)
instance Exception TimeoutException

runJobWithTimeout :: (HasJobRunner m)
                  => Seconds
                  -> Job
                  -> m ()
runJobWithTimeout timeoutSec job = do
  threadsRef <- envJobThreadsRef <$> getRunnerEnv
  jobRunner_ <- getJobRunner

  a <- async $ liftIO $ jobRunner_ job

  x <- atomicModifyIORef' threadsRef $ \threads -> (a:threads, DL.map asyncThreadId (a:threads))
  -- liftIO $ putStrLn $ "Threads: " <> show x
  log LevelDebug $ LogText $ toS $ "Spawned job in " <> show (asyncThreadId a)

  t <- async $ do
    delaySeconds timeoutSec
    uninterruptibleCancel a
    throwIO TimeoutException

  void $ finally
    (waitEitherCancel a t)
    (atomicModifyIORef' threadsRef $ \threads -> (DL.delete a threads, ()))


runJob :: (HasJobRunner m) => JobId -> m ()
runJob jid = do
  (findJobById jid) >>= \case
    Nothing -> Prelude.error $ "Could not find job id=" <> show jid
    Just job -> do
      startTime <- liftIO getCurrentTime
      (flip catches) [Handler $ timeoutHandler job startTime, Handler $ exceptionHandler job startTime] $ do
        runJobWithTimeout defaultLockTimeout job
        endTime <- liftIO getCurrentTime
        newJob <- saveJob job{jobStatus=Success, jobLockedBy=Nothing, jobLockedAt=Nothing}
        log LevelInfo $ LogJobSuccess newJob (diffUTCTime endTime startTime)
        onJobSuccess newJob
        pure ()
  where
    timeoutHandler job startTime (e :: TimeoutException) = retryOrFail (toException e) job startTime
    exceptionHandler job startTime (e :: SomeException) = retryOrFail (toException e) job startTime
    retryOrFail e job@Job{jobAttempts} startTime = do
      endTime <- liftIO getCurrentTime
      defaultMaxAttempts <- getDefaultMaxAttempts
      let runTime = diffUTCTime endTime startTime
          (newStatus, failureMode, logLevel) = if jobAttempts >= defaultMaxAttempts
                                               then ( Failed, FailPermanent, LevelError )
                                               else ( Retry, FailWithRetry, LevelWarn )
      t <- liftIO getCurrentTime
      newJob <- saveJob job{ jobStatus=newStatus
                           , jobLockedBy=Nothing
                           , jobLockedAt=Nothing
                           , jobLastError=(Just $ toJSON $ show e) -- TODO: convert errors to json properly
                           , jobRunAt=(addUTCTime (fromIntegral $ (2::Int) ^ jobAttempts) t)
                           }
      case fromException e :: Maybe TimeoutException of
        Nothing -> log logLevel $ LogJobFailed newJob e failureMode runTime
        Just _ -> log logLevel $ LogJobTimeout newJob

      let tryHandler (JobErrHandler handler) res = case fromException e of
            Nothing -> res
            Just e_ -> handler e_ newJob failureMode
      handlers <- onJobFailed
      liftIO $ void $ Prelude.foldr tryHandler (throwIO e) handlers
      pure ()

restartUponCrash :: (HasJobRunner m, Show a) => Text -> m a -> m ()
restartUponCrash name_ action = do
  a <- async action
  finally (waitCatch a >>= fn) $ do
    (log LevelInfo $ LogText $ "Received shutdown: " <> toS name_)
    cancel a
  where
    fn x = do
      case x of
        Left (e :: SomeException) -> log LevelError $ LogText $ name_ <> " seems to have exited with an error. Restarting: " <> toS (show e)
        Right r -> log LevelError $ LogText $ name_ <> " seems to have exited with the folloing result: " <> toS (show r) <> ". Restaring."
      restartUponCrash name_ action

-- | Spawns 'jobPoller' and 'jobEventListener' in separate threads and restarts
-- them in the off-chance they happen to crash. Also responsible for
-- implementing graceful shutdown, i.e. waiting for all jobs already being
-- executed to finish execution before exiting the main thread.
jobMonitor :: forall m . (HasJobRunner m) => m ()
jobMonitor = do
  a1 <- async $ restartUponCrash "Job poller" jobPoller
  a2 <- async $ restartUponCrash "Job event listener" jobEventListener
  finally (void $ waitAnyCatch [a1, a2]) $ do
    log LevelInfo (LogText "Stopping jobPoller and jobEventListener threads.")
    cancel a2
    cancel a1
    log LevelInfo (LogText "Waiting for jobs to complete.")
    waitForJobs
    getPidFile >>= \case
      Nothing -> pure ()
      Just f -> do
        log LevelInfo $ LogText $ "Removing PID file: " <> toS f
        liftIO $ Dir.removePathForcibly f

-- | Ref: 'jobPoller'
jobPollingSql :: TableName -> Query
jobPollingSql tname = "update " <> tname <> " set status = ?, locked_at = ?, locked_by = ?, attempts=attempts+1 WHERE id in (select id from " <> tname <> " where (run_at<=? AND ((status in ?) OR (status = ? and locked_at<?))) ORDER BY run_at ASC LIMIT 1 FOR UPDATE) RETURNING id"

waitForJobs :: (HasJobRunner m)
            => m ()
waitForJobs = do
  threadsRef <- envJobThreadsRef <$> getRunnerEnv
  readIORef threadsRef >>= \case
    [] -> log LevelInfo $ LogText "All job-threads exited"
    as -> do
      tid <- myThreadId
      void $ waitAnyCatch as
      log LevelDebug $ LogText $ toS $ "Waiting for " <> show (DL.length as) <> " jobs to complete before shutting down. myThreadId=" <> (show tid)
      delaySeconds (Seconds 1)
      waitForJobs

getConcurrencyControlFn :: (HasJobRunner m)
                        => m (m Bool)
getConcurrencyControlFn = getConcurrencyControl >>= \case
  UnlimitedConcurrentJobs -> pure $ pure True
  MaxConcurrentJobs maxJobs -> pure $ do
    curJobs <- getRunnerEnv >>= (readIORef . envJobThreadsRef)
    pure $ (DL.length curJobs) < maxJobs
  DynamicConcurrency fn -> pure $ liftIO fn

-- | Executes 'jobPollingSql' every 'cfgPollingInterval' seconds to pick up jobs
-- for execution. Uses @UPDATE@ along with @SELECT...FOR UPDATE@ to efficiently
-- find a job that matches /all/ of the following conditions:
--
--   * 'jobRunAt' should be in the past
--   * /one of the following/ conditions match:
--
--       * 'jobStatus' should be 'Queued' or 'Retry'
--
--       * 'jobStatus' should be 'Locked' and 'jobLockedAt' should be
--         'defaultLockTimeout' seconds in the past, thus indicating that the
--         job was picked up execution, but didn't complete on time (possible
--         because the thread/process executing it crashed without being able to
--         update the DB)

jobPoller :: (HasJobRunner m) => m ()
jobPoller = do
  processName <- liftIO jobWorkerName
  pool <- getDbPool
  tname <- getTableName
  log LevelInfo $ LogText $ toS $ "Starting the job monitor via DB polling with processName=" <> processName
  concurrencyControlFn <- getConcurrencyControlFn
  withResource pool $ \pollerDbConn -> forever $ concurrencyControlFn >>= \case
    False -> log LevelWarn $ LogText $ "NOT polling the job queue due to concurrency control"
    True -> do
      nextAction <- mask_ $ do
        log LevelDebug $ LogText $ toS $ "[" <> processName <> "] Polling the job queue.."
        t <- liftIO getCurrentTime
        r <- liftIO $
             PGS.query pollerDbConn (jobPollingSql tname)
             (Locked, t, processName, t, (In [Queued, Retry]), Locked, (addUTCTime (fromIntegral $ negate $ unSeconds defaultLockTimeout) t))
        case r of
          -- When we don't have any jobs to run, we can relax a bit...
          [] -> pure delayAction

          -- When we find a job to run, fork and try to find the next job without any delay...
          [Only (jid :: JobId)] -> do
            void $ async $ runJob jid
            pure noDelayAction

          x -> error $ "WTF just happened? I was supposed to get only a single row, but got: " ++ (show x)
      nextAction
  where
    delayAction = delaySeconds =<< getPollingInterval
    noDelayAction = pure ()

-- | Uses PostgreSQL's LISTEN/NOTIFY to be immediately notified of newly created
-- jobs.
jobEventListener :: (HasJobRunner m)
                 => m ()
jobEventListener = do
  log LevelInfo $ LogText "Starting the job monitor via LISTEN/NOTIFY..."
  pool <- getDbPool
  tname <- getTableName
  jwName <- liftIO jobWorkerName
  concurrencyControlFn <- getConcurrencyControlFn

  let tryLockingJob jid = do
        let q = "UPDATE " <> tname <> " SET status=?, locked_at=now(), locked_by=?, attempts=attempts+1 WHERE id=? AND status in ? RETURNING id"
        (withDbConnection $ \conn -> (liftIO $ PGS.query conn q (Locked, jwName, jid, In [Queued, Retry]))) >>= \case
          [] -> do
            log LevelDebug $ LogText $ toS $ "Job was locked by someone else before I could start. Skipping it. JobId=" <> show jid
            pure Nothing
          [Only (_ :: JobId)] -> pure $ Just jid
          x -> error $ "WTF just happned? Was expecting a single row to be returned, received " ++ (show x)

  withResource pool $ \monitorDbConn -> do
    void $ liftIO $ PGS.execute monitorDbConn ("LISTEN " <> pgEventName tname) ()
    forever $ do
      log LevelDebug $ LogText "[LISTEN/NOFIFY] Event loop"
      notif <- liftIO $ getNotification monitorDbConn
      concurrencyControlFn >>= \case
        False -> log LevelWarn $ LogText "Received job event, but ignoring it due to concurrency control"
        True -> do
          let pload = notificationData notif
          log LevelDebug $ LogText $ toS $ "NOTIFY | " <> show pload
          case (eitherDecode $ toS pload) of
            Left e -> log LevelError $ LogText $ toS $  "Unable to decode notification payload received from Postgres. Payload=" <> show pload <> " Error=" <> show e

            -- Checking if job needs to be fired immediately AND it is not already
            -- taken by some othe thread, by the time it got to us
            Right (v :: Value) -> case (Aeson.parseMaybe parser v) of
              Nothing -> log LevelError $ LogText $ toS $ "Unable to extract id/run_at/locked_at from " <> show pload
              Just (jid, runAt_, mLockedAt_) -> do
                t <- liftIO getCurrentTime
                if (runAt_ <= t) && (isNothing mLockedAt_)
                  then do log LevelDebug $ LogText $ toS $ "Job needs needs to be run immediately. Attempting to fork in background. JobId=" <> show jid
                          void $ async $ do
                            -- Let's try to lock the job first... it is possible that it has already
                            -- been picked up by the poller by the time we get here.
                            tryLockingJob jid >>= \case
                              Nothing -> pure ()
                              Just lockedJid -> runJob lockedJid
                  else log LevelDebug $ LogText $ toS $ "Job is either for future, or is already locked. Skipping. JobId=" <> show jid
  where
    parser :: Value -> Aeson.Parser (JobId, UTCTime, Maybe UTCTime)
    parser = withObject "expecting an object to parse job.run_at and job.locked_at" $ \o -> do
      runAt_ <- o .: "run_at"
      mLockedAt_ <- o .:? "locked_at"
      jid <- o .: "id"
      pure (jid, runAt_, mLockedAt_)



createJobQuery :: TableName -> PGS.Query
createJobQuery tname = "INSERT INTO " <> tname <> "(run_at, status, payload, last_error, attempts, locked_at, locked_by) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING " <> concatJobDbColumns

-- $createJobs
--
-- Ideally you'd want to create wrappers for 'createJob' and 'scheduleJob' in
-- your application so that instead of being in @IO@ they can be in your
-- application's monad @m@ instead (this saving you from a @liftIO@ every time
-- you want to enqueue a job

-- | Create a job for immediate execution.
--
-- Internally calls 'scheduleJob' passing it the current time. Read
-- 'scheduleJob' for further documentation.
createJob :: ToJSON p
          => Connection
          -> TableName
          -> p
          -> IO Job
createJob conn tname payload = do
  t <- getCurrentTime
  scheduleJob conn tname payload t

-- | Create a job for execution at the given time.
--
--  * If time has already past, 'jobEventListener' is going to pick this up
--    for execution immediately.
--
--  * If time is in the future, 'jobPoller' is going to pick this up with an
--    error of +/- 'cfgPollingInterval' seconds. Please do not expect very high
--    accuracy of when the job is actually executed.
scheduleJob :: ToJSON p
            => Connection   -- ^ DB connection to use. __Note:__ This should
                            -- /ideally/ come out of your application's DB pool,
                            -- not the 'cfgDbPool' you used in the job-runner.
            -> TableName    -- ^ DB-table which holds your jobs
            -> p            -- ^ Job payload
            -> UTCTime      -- ^ when should the job be executed
            -> IO Job
scheduleJob conn tname payload runAt = do
  let args = ( runAt, Queued, toJSON payload, Nothing :: Maybe Value, 0 :: Int, Nothing :: Maybe Text, Nothing :: Maybe Text )
      queryFormatter = toS <$> (PGS.formatQuery conn (createJobQuery tname) args)
  rs <- PGS.query conn (createJobQuery tname) args
  case rs of
    [] -> (Prelude.error . (<> "Not expecting a blank result set when creating a job. Query=")) <$> queryFormatter
    [r] -> pure r
    _ -> (Prelude.error . (<> "Not expecting multiple rows when creating a single job. Query=")) <$> queryFormatter 


-- getRunnerEnv :: (HasJobRunner m) => m RunnerEnv
-- getRunnerEnv = ask

eitherParsePayload :: (FromJSON a)
                   => Job
                   -> Either String a
eitherParsePayload job =
  eitherParsePayloadWith parseJSON job

throwParsePayload :: (FromJSON a)
                  => Job
                  -> IO a
throwParsePayload job =
  throwParsePayloadWith parseJSON job

eitherParsePayloadWith :: (Aeson.Value -> Aeson.Parser a)
                       -> Job
                       -> Either String a
eitherParsePayloadWith parser Job{jobPayload} = do
  case iparse parser jobPayload of
    -- TODO: throw a custom exception so that error reporting is better
    IError jpath e -> Left $ formatError jpath e
    ISuccess r -> Right r

throwParsePayloadWith :: (Aeson.Value -> Aeson.Parser a)
                      -> Job
                      -> IO a
throwParsePayloadWith parser job =
  either throwString (pure . Prelude.id) (eitherParsePayloadWith parser job)


-- | TODO: Should the library be doing this?
defaultTimedLogger :: FLogger.TimedFastLogger
                   -> (LogLevel -> LogEvent -> LogStr)
                   -> LogLevel
                   -> LogEvent
                   -> IO ()
defaultTimedLogger logger logStrFn logLevel logEvent =
  if logLevel == LevelDebug
  then pure ()
  else logger $ \t -> (toLogStr t) <> " | " <>
                      (logStrFn logLevel logEvent) <>
                      "\n"

  -- TODO; Should the library be doing this?
defaultLogStr :: (Job -> Text)
              -> LogLevel
              -> LogEvent
              -> LogStr
defaultLogStr jobToText logLevel logEvent =
  (toLogStr $ show logLevel) <> " | " <> str
  where
    jobToLogStr j = toLogStr $ jobToText j
    str = case logEvent of
      LogJobStart j ->
        "Started | " <> jobToLogStr j
      LogJobFailed j e fm t ->
        let tag = case fm of
                    FailWithRetry -> "Failed (retry)"
                    FailPermanent -> "Failed (permanent)"
        in tag <> " | " <> jobToLogStr j <> " | runtime=" <> (toLogStr $ show t) <> " | error=" <> (toLogStr $ show e)
      LogJobSuccess j t ->
        "Success | " <> (jobToLogStr j) <> " | runtime=" <> (toLogStr $ show t)
      LogJobTimeout j@Job{jobLockedAt, jobLockedBy} ->
        "Timeout | " <> jobToLogStr j <> " | lockedBy=" <> (toLogStr $ fromMaybe "unknown" jobLockedBy) <>
        " lockedAt=" <> (toLogStr $ maybe "unknown" show jobLockedAt)
      LogPoll ->
        "Polling jobs table"
      LogText t ->
        toLogStr t
