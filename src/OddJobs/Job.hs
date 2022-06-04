{-# LANGUAGE RankNTypes, FlexibleInstances, FlexibleContexts, PartialTypeSignatures, TupleSections, DeriveGeneric, UndecidableInstances #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}

module OddJobs.Job
  (
    -- * Starting the job-runner
    --
    -- $startRunner
    startJobRunner

    -- * Configuring the job-runner
    --
    -- $config
  , Config(..)
  , ConcurrencyControl(..)

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
  , JobRunnerName(..)
  , TableName
  , delaySeconds
  , Seconds(..)
  , JobErrHandler(..)
  , AllJobTypes(..)

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
  , findJobByIdIO
  , saveJobIO
  , runJobNowIO
  , unlockJobIO
  , cancelJobIO
  , jobDbColumns
  , jobPollingIO
  , concatJobDbColumns
  , fetchAllJobTypes
  , fetchAllJobRunners

  -- * JSON helpers
  --
  -- $jsonHelpers
  , eitherParsePayload
  , throwParsePayload
  , eitherParsePayloadWith
  , throwParsePayloadWith
  )
where

import OddJobs.Types
import qualified Data.Pool as Pool
import Data.Pool(Pool)
import Data.Text as T
import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Notification
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
import Data.Maybe (isNothing, maybe, fromMaybe, listToMaybe, mapMaybe)
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
import Prelude hiding (log)
import GHC.Exts (toList)
import Database.PostgreSQL.Simple.Types as PGS (Identifier(..))
import Database.PostgreSQL.Simple.ToField as PGS (toField)

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
  deleteSuccessfulJobs :: m Bool
  onJobFailed :: m [JobErrHandler]
  getJobRunner :: m (Job -> IO ())
  getDbPool :: m (Pool Connection)
  getTableName :: m TableName
  onJobStart :: Job -> m ()
  getDefaultMaxAttempts :: m Int
  getRunnerEnv :: m RunnerEnv
  getConcurrencyControl :: m ConcurrencyControl
  log :: LogLevel -> LogEvent -> m ()
  getDefaultJobTimeout :: m Seconds
  getDefaultRetryBackoff :: Int -> m Seconds

-- $logging
--
-- OddJobs uses \"structured logging\" for important events that occur during
-- the life-cycle of a job-runner. This is useful if you're using JSON\/XML for
-- aggegating logs of your entire application to something like Kibana, AWS
-- CloudFront, GCP StackDriver Logging, etc.
--
-- If you're not interested in using structured logging, look at
-- 'OddJobs.ConfigBuilder.defaultLogStr' to output plain-text logs (or you can
-- write your own function, as well).
--


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
  deleteSuccessfulJobs = cfgDeleteSuccessfulJobs . envConfig <$> ask

  getJobRunner = cfgJobRunner . envConfig <$> ask
  getDbPool = cfgDbPool . envConfig <$> ask
  getTableName = cfgTableName . envConfig <$> ask
  onJobStart job = do
    fn <- cfgOnJobStart . envConfig <$> ask
    logCallbackErrors (jobId job) "onJobStart" $ liftIO $ fn job

  getDefaultMaxAttempts = cfgDefaultMaxAttempts . envConfig <$> ask

  getRunnerEnv = ask

  getConcurrencyControl = (cfgConcurrencyControl . envConfig <$> ask)

  log logLevel logEvent = do
    loggerFn <- cfgLogger . envConfig <$> ask
    liftIO $ loggerFn logLevel logEvent

  getDefaultJobTimeout = cfgDefaultJobTimeout . envConfig <$> ask

  getDefaultRetryBackoff attempts = do
    retryFn <- cfgDefaultRetryBackoff . envConfig <$> ask
    liftIO $ retryFn attempts

-- | Start the job-runner in the /current/ thread, i.e. you'll need to use
-- 'forkIO' or 'async' manually, if you want the job-runner to run in the
-- background. Consider using 'OddJobs.Cli' to rapidly build your own
-- standalone daemon.
startJobRunner :: Config -> IO ()
startJobRunner jm = do
  traceM "startJobRunner top"
  r <- newIORef []
  let monitorEnv = RunnerEnv
                   { envConfig = jm
                   , envJobThreadsRef = r
                   }
  traceM "before runReaderT"
  runReaderT jobMonitor monitorEnv

jobWorkerName :: IO String
jobWorkerName = do
  pid <- getProcessID
  hname <- getHostName
  pure $ hname ++ ":" ++ (show pid)

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

-- | All 'jobDbColumns' joined together with commas. Useful for constructing SQL
-- queries, eg:
--
-- @'query_' conn $ "SELECT " <> concatJobDbColumns <> "FROM jobs"@

concatJobDbColumns :: (IsString s, Semigroup s) => s
concatJobDbColumns = concatJobDbColumns_ jobDbColumns ""
  where
    concatJobDbColumns_ [] x = x
    concatJobDbColumns_ (col:[]) x = x <> col
    concatJobDbColumns_ (col:cols) x = concatJobDbColumns_ cols (x <> col <> ", ")


findJobByIdQuery :: PGS.Query
findJobByIdQuery = "SELECT " <> concatJobDbColumns <> " FROM ? WHERE id = ?"

withResource :: MonadUnliftIO m => Pool a -> (a -> m b) -> m b
withResource pool fa =
  withRunInIO $ \runInIO -> Pool.withResource pool (runInIO . fa)

withDbConnection :: (HasJobRunner m)
                 => (Connection -> m a)
                 -> m a
withDbConnection action = do
  pool <- getDbPool
  withResource pool action

--
-- $dbHelpers
--
-- A bunch of functions that help you query 'cfgTableName' and change the status
-- of individual jobs. Most of these functions are in @IO@ and you /might/ want
-- to write wrappers that lift them into you application's custom monad.
--
-- __Note:__ When passing a 'Connection' to these function, it is
-- __recommended__ __to not__ take a connection from 'cfgDbPool'. Use your
-- application\'s database pool instead.
--

findJobById :: (HasJobRunner m)
            => JobId
            -> m (Maybe Job)
findJobById jid = do
  tname <- getTableName
  withDbConnection $ \conn -> liftIO $ findJobByIdIO conn tname jid

findJobByIdIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
findJobByIdIO conn tname jid = PGS.query conn findJobByIdQuery (tname, jid) >>= \case
  [] -> pure Nothing
  [j] -> pure (Just j)
  js -> Prelude.error $ "Not expecting to find multiple jobs by id=" <> (show jid)


saveJobQuery :: PGS.Query
saveJobQuery = "UPDATE ? set run_at = ?, status = ?, payload = ?, last_error = ?, attempts = ?, locked_at = ?, locked_by = ? WHERE id = ? RETURNING " <> concatJobDbColumns

deleteJobQuery :: PGS.Query
deleteJobQuery = "DELETE FROM ? WHERE id = ?"

saveJob :: (HasJobRunner m) => Job -> m Job
saveJob j = do
  tname <- getTableName
  withDbConnection $ \conn -> liftIO $ saveJobIO conn tname j

saveJobIO :: Connection -> TableName -> Job -> IO Job
saveJobIO conn tname Job{jobRunAt, jobStatus, jobPayload, jobLastError, jobAttempts, jobLockedBy, jobLockedAt, jobId} = do
  rs <- PGS.query conn saveJobQuery
        ( tname 
        , jobRunAt
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

deleteJob :: (HasJobRunner m) => JobId -> m ()
deleteJob jid = do
  tname <- getTableName
  withDbConnection $ \conn -> liftIO $ deleteJobIO conn tname jid

deleteJobIO :: Connection -> TableName -> JobId -> IO ()
deleteJobIO conn tname jid = do
  void $ PGS.execute conn deleteJobQuery (tname, jid)

runJobNowIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
runJobNowIO conn tname jid = do
  t <- getCurrentTime
  updateJobHelper tname conn (Queued, [Queued, Retry, Failed], Just t, jid)

-- | TODO: First check in all job-runners if this job is still running, or not,
-- and somehow send an uninterruptibleCancel to that thread.
unlockJobIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
unlockJobIO conn tname jid = do
  fmap listToMaybe $ PGS.query conn q (tname, Retry, jid, In [Locked])
  where
    q = "update ? set status=?, run_at=now(), locked_at=null, locked_by=null where id=? and status in ? returning " <> concatJobDbColumns

cancelJobIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
cancelJobIO conn tname jid =
  updateJobHelper tname conn (Failed, [Queued, Retry], Nothing, jid)

updateJobHelper :: TableName
                -> Connection
                -> (Status, [Status], Maybe UTCTime, JobId)
                -> IO (Maybe Job)
updateJobHelper tname conn (newStatus, existingStates, mRunAt, jid) =
  fmap listToMaybe $ PGS.query conn q (tname, newStatus, runAt, jid, PGS.In existingStates)
  where
    q = "update ? set attempts=0, status=?, run_at=? where id=? and status in ? returning " <> concatJobDbColumns
    runAt = case mRunAt of
      Nothing -> PGS.toField $ PGS.Identifier "run_at"
      Just t -> PGS.toField t


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
      lockTimeout <- getDefaultJobTimeout
      log LevelInfo $ LogJobStart job
      (flip catches) [Handler $ timeoutHandler job startTime, Handler $ exceptionHandler job startTime] $ do
        runJobWithTimeout lockTimeout job
        endTime <- liftIO getCurrentTime
        shouldDeleteJob <- deleteSuccessfulJobs
        let newJob = job{jobStatus=Success, jobLockedBy=Nothing, jobLockedAt=Nothing, jobUpdatedAt = endTime}
        if shouldDeleteJob
          then deleteJob jid
          else void $ saveJob newJob
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
      backoffInSeconds <- getDefaultRetryBackoff jobAttempts
      newJob <- saveJob job{ jobStatus=newStatus
                           , jobLockedBy=Nothing
                           , jobLockedAt=Nothing
                           , jobLastError=(Just $ toJSON $ show e) -- TODO: convert errors to json properly
                           , jobRunAt=(addUTCTime (fromIntegral $ unSeconds backoffInSeconds) t)
                           }
      case fromException e :: Maybe TimeoutException of
        Nothing -> log logLevel $ LogJobFailed newJob e failureMode runTime
        Just _ -> log logLevel $ LogJobTimeout newJob

      let tryHandler (JobErrHandler handler) res = case fromException e of
            Nothing -> res
            Just e_ -> void $ handler e_ newJob failureMode
      handlers <- onJobFailed
      liftIO $ void $ Prelude.foldr tryHandler (throwIO e) handlers
      pure ()

-- TODO: This might have a resource leak.
restartUponCrash :: (HasJobRunner m, Show a) => Text -> m a -> m ()
restartUponCrash name_ action = do
  traceM "restartUponCrash top"
  a <- async action
  finally (waitCatch a >>= fn) $ do
    (log LevelInfo $ LogText $ "Received shutdown: " <> toS name_)
    cancel a
  where
    fn x = do
      case x of
        Left (e :: SomeException) -> log LevelError $ LogText $ name_ <> " seems to have exited with an error. Restarting: " <> toS (show e)
        Right r -> log LevelError $ LogText $ name_ <> " seems to have exited with the folloing result: " <> toS (show r) <> ". Restaring."
      traceM "CRASH OCCURRED"
      restartUponCrash name_ action

-- | Spawns 'jobPoller' and 'jobEventListener' in separate threads and restarts
-- them in the off-chance they happen to crash. Also responsible for
-- implementing graceful shutdown, i.e. waiting for all jobs already being
-- executed to finish execution before exiting the main thread.
jobMonitor :: forall m . (HasJobRunner m) => m ()
jobMonitor = do
  traceM "jobMonitor top"
  a1 <- async $ restartUponCrash "Job poller" jobPoller
  a2 <- async $ restartUponCrash "Job event listener" jobEventListener
  traceM "jobMonitor after async"
  finally (void $ waitAnyCatch [a1, a2]) $ do
    log LevelInfo (LogText "Stopping jobPoller and jobEventListener threads.")
    cancel a2
    cancel a1
    log LevelInfo (LogText "Waiting for jobs to complete.")
    waitForJobs

-- | Ref: 'jobPoller'
jobPollingSql :: Query
jobPollingSql = "update ? set status = ?, locked_at = ?, locked_by = ?, attempts=attempts+1 WHERE id in (select id from ? where (run_at<=? AND ((status in ?) OR (status = ? and locked_at<?))) ORDER BY attempts ASC, run_at ASC LIMIT 1 FOR UPDATE) RETURNING id"

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


jobPollingIO :: Connection -> String -> TableName -> Seconds -> IO [Only JobId]
jobPollingIO pollerDbConn processName tname lockTimeout = do
  t <- getCurrentTime
  PGS.query pollerDbConn jobPollingSql
             ( tname
             , Locked
             , t
             , processName
             , tname
             , t
             , (In [Queued, Retry])
             , Locked
             , (addUTCTime (fromIntegral $ negate $ unSeconds lockTimeout) t))

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
  lockTimeout <- getDefaultJobTimeout
  traceM "jobPoller just before log statement"
  log LevelInfo $ LogText $ toS $ "Starting the job monitor via DB polling with processName=" <> processName
  concurrencyControlFn <- getConcurrencyControlFn
  withResource pool $ \pollerDbConn -> forever $ concurrencyControlFn >>= \case
    False -> do
      log LevelWarn $ LogText $ "NOT polling the job queue due to concurrency control"
      -- If we can't run any jobs ATM, relax and wait for resources to free up
      delayAction
    True -> do
      nextAction <- mask_ $ do
        log LevelDebug $ LogText $ toS $ "[" <> processName <> "] Polling the job queue.."
        r <- liftIO $ jobPollingIO pollerDbConn processName tname lockTimeout
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
        let q = "UPDATE ? SET status=?, locked_at=now(), locked_by=?, attempts=attempts+1 WHERE id=? AND status in ? RETURNING id"
        (withDbConnection $ \conn -> (liftIO $ PGS.query conn q (tname, Locked, jwName, jid, In [Queued, Retry]))) >>= \case
          [] -> do
            log LevelDebug $ LogText $ toS $ "Job was locked by someone else before I could start. Skipping it. JobId=" <> show jid
            pure Nothing
          [Only (_ :: JobId)] -> pure $ Just jid
          x -> error $ "WTF just happned? Was expecting a single row to be returned, received " ++ (show x)

  withResource pool $ \monitorDbConn -> do
    void $ liftIO $ PGS.execute monitorDbConn ("LISTEN ?") (Only $ pgEventName tname)
    forever $ do
      log LevelDebug $ LogText "[LISTEN/NOTIFY] Event loop"
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



createJobQuery :: PGS.Query
createJobQuery = "INSERT INTO ? (run_at, status, payload, last_error, attempts, locked_at, locked_by) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING " <> concatJobDbColumns

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
  let args = ( tname, runAt, Queued, toJSON payload, Nothing :: Maybe Value, 0 :: Int, Nothing :: Maybe Text, Nothing :: Maybe Text )
      queryFormatter = toS <$> (PGS.formatQuery conn createJobQuery args)
  rs <- PGS.query conn createJobQuery args
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


-- | Used by the web\/admin UI to fetch a \"master list\" of all known
-- job-types. Ref: 'cfgAllJobTypes'
fetchAllJobTypes :: (MonadIO m)
                 => UIConfig
                 -> m [Text]
fetchAllJobTypes UIConfig{uicfgAllJobTypes, uicfgDbPool} = liftIO $ do
  case uicfgAllJobTypes of
    AJTFixed jts -> pure jts
    AJTSql fn -> withResource uicfgDbPool fn
    AJTCustom fn -> fn

-- | Used by web\/admin IO to fetch a \"master list\" of all known job-runners.
-- There is a known issue with the way this has been implemented:
--
--   * Since this looks at the 'jobLockedBy' column of 'cfgTableName', it will
--     discover only those job-runners that are actively executing at least one
--     job at the time this function is executed.
fetchAllJobRunners :: (MonadIO m)
                   => UIConfig
                   -> m [JobRunnerName]
fetchAllJobRunners UIConfig{uicfgTableName, uicfgDbPool} = liftIO $ withResource uicfgDbPool $ \conn -> do
  fmap (mapMaybe fromOnly) $ PGS.query conn "select distinct locked_by from ?" (Only uicfgTableName)

