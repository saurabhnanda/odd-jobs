{-# LANGUAGE RankNTypes, FlexibleInstances, FlexibleContexts, PartialTypeSignatures, UndecidableInstances #-}
{-# LANGUAGE ExistentialQuantification, RecordWildCards, ScopedTypeVariables, CPP #-}

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
  , ResourceCfg(..)

  -- * Creating/scheduling jobs
  --
  -- $createJobs
  , createJob
  , scheduleJob

  , createJobWithResources
  , scheduleJobWithResources

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
  , ResourceId(..)
  , FunctionName
  , RunnerEnv(..)

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
  , pollRunJob
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
  , killJobIO
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
import UnliftIO.Async hiding (poll)
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
                          , catches, Handler(..), mask_, onException
                          , throwString
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
import Data.Functor ((<&>), void)
import Control.Monad (forever, forM_, join)
import Data.Maybe (isNothing, maybe, fromMaybe, listToMaybe, mapMaybe, maybeToList)
import Data.Either (either)
import Control.Monad.Reader
import GHC.Generics
import Data.Map (Map)
import qualified Data.HashMap.Strict as HM
import qualified Data.List as DL
import qualified Data.Map as DM
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import System.FilePath (FilePath)
import qualified System.Directory as Dir
import Prelude hiding (log)
import GHC.Exts (toList)
import Database.PostgreSQL.Simple.Types as PGS (Identifier(..))
import Database.PostgreSQL.Simple.ToField as PGS (toField)
import OddJobs.Job.Query
import Data.Int (Int64)
#if MIN_VERSION_aeson(2,2,0)
import Data.Aeson.Types
#else
import Data.Aeson.Internal (iparse, IResult(..), formatError)
#endif

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
  immediateJobDeletion :: m (Job -> IO Bool)
  delayedJobDeletion :: m (Maybe (PGS.Connection -> IO Int64))
  onJobFailed :: m [JobErrHandler]
  getJobRunner :: m (Job -> IO (Maybe Aeson.Value))
  getDbPool :: m (Pool Connection)
  getTableName :: m TableName
  onJobStart :: Job -> m ()
  getDefaultMaxAttempts :: m Int
  getRunnerEnv :: m RunnerEnv
  getConcurrencyControl :: m ConcurrencyControl
  log :: LogLevel -> LogEvent -> m ()
  getDefaultJobTimeout :: m Seconds
  onJobTimeout :: Job -> m ()
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
  , envJobThreadsRef :: !(IORef (Map JobId (Async ())))
  }

type RunnerM = ReaderT RunnerEnv IO

logCallbackErrors :: (HasJobRunner m) => JobId -> Text -> m () -> m ()
logCallbackErrors jid msg action = catchAny action $ \e -> log LevelError $ LogText $ msg <> " Job ID=" <> toS (show jid) <> ": " <> toS (show e)

instance HasJobRunner RunnerM where
  getPollingInterval = asks (cfgPollingInterval . envConfig)
  onJobFailed = asks (cfgOnJobFailed . envConfig)
  onJobSuccess job = do
    fn <- asks (cfgOnJobSuccess . envConfig)
    logCallbackErrors (jobId job) "onJobSuccess" $ liftIO $ fn job
  immediateJobDeletion = asks (cfgImmediateJobDeletion . envConfig)
  delayedJobDeletion = asks (cfgDelayedJobDeletion . envConfig)

  getJobRunner = asks (cfgJobRunner . envConfig)
  getDbPool = asks (cfgDbPool . envConfig)
  getTableName = asks (cfgTableName . envConfig)
  onJobStart job = do
    fn <- asks (cfgOnJobStart . envConfig)
    logCallbackErrors (jobId job) "onJobStart" $ liftIO $ fn job

  getDefaultMaxAttempts = asks (cfgDefaultMaxAttempts . envConfig)

  getRunnerEnv = ask

  getConcurrencyControl = asks (cfgConcurrencyControl . envConfig)

  log logLevel logEvent = do
    loggerFn <- asks (cfgLogger . envConfig)
    liftIO $ loggerFn logLevel logEvent

  getDefaultJobTimeout = asks (cfgDefaultJobTimeout . envConfig)
  onJobTimeout job = do
    fn <- asks (cfgOnJobTimeout . envConfig)
    logCallbackErrors (jobId job) "onJobTimeout" $ liftIO $ fn job

  getDefaultRetryBackoff attempts = do
    retryFn <- asks (cfgDefaultRetryBackoff . envConfig)
    liftIO $ retryFn attempts

-- | Start the job-runner in the /current/ thread, i.e. you'll need to use
-- 'forkIO' or 'async' manually, if you want the job-runner to run in the
-- background. Consider using 'OddJobs.Cli' to rapidly build your own
-- standalone daemon.
startJobRunner :: Config -> IO ()
startJobRunner jm = do
  r <- newIORef DM.empty
  let monitorEnv = RunnerEnv
                   { envConfig = jm
                   , envJobThreadsRef = r
                   }
  runReaderT jobMonitor monitorEnv

jobWorkerName :: IO String
jobWorkerName = do
  pid <- getProcessID
  hname <- getHostName
  pure $ hname ++ ":" ++ show pid

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
  _js -> Prelude.error $ "Not expecting to find multiple jobs by id=" <> show jid


saveJobQuery :: PGS.Query
saveJobQuery = "UPDATE ? set run_at = ?, status = ?, payload = ?, last_error = ?, attempts = ?, locked_at = ?, locked_by = ?, result = ? WHERE id = ? RETURNING " <> concatJobDbColumns

deleteJobQuery :: PGS.Query
deleteJobQuery = "DELETE FROM ? WHERE id = ?"

saveJob :: (HasJobRunner m) => Job -> m Job
saveJob j = do
  tname <- getTableName
  withDbConnection $ \conn -> liftIO $ saveJobIO conn tname j

saveJobIO :: Connection -> TableName -> Job -> IO Job
saveJobIO conn tname Job{jobRunAt, jobStatus, jobPayload, jobLastError, jobAttempts, jobLockedBy, jobLockedAt, jobResult, jobId} = do
  rs <- PGS.query conn saveJobQuery
        ( tname
        , jobRunAt
        , jobStatus
        , jobPayload
        , jobLastError
        , jobAttempts
        , jobLockedAt
        , jobLockedBy
        , jobResult
        , jobId
        )
  case rs of
    [] -> Prelude.error $ "Could not find job while updating it id=" <> show jobId
    [j] -> pure j
    _js -> Prelude.error $ "Not expecting multiple rows to ber returned when updating job id=" <> show jobId

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
  updateJobHelper tname conn (Queued, [Queued, Retry, Failed, Cancelled], Just t, jid)

-- | TODO: First check in all job-runners if this job is still running, or not,
-- and somehow send an uninterruptibleCancel to that thread.
unlockJobIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
unlockJobIO conn tname jid = do
  listToMaybe <$> PGS.query conn q (tname, Retry, jid, In [Locked])
  where
    q = "update ? set status=?, run_at=now(), locked_at=null, locked_by=null where id=? and status in ? returning " <> concatJobDbColumns

cancelJobIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
cancelJobIO conn tname jid =
  updateJobHelper tname conn (Cancelled, [Queued, Retry], Nothing, jid)

killJobIO :: Connection -> TableName -> JobId -> IO (Maybe Job)
killJobIO conn tname jid =
  updateJobHelper tname conn (Cancelled, [Locked], Nothing, jid)

updateJobHelper :: TableName
                -> Connection
                -> (Status, [Status], Maybe UTCTime, JobId)
                -> IO (Maybe Job)
updateJobHelper tname conn (newStatus, existingStates, mRunAt, jid) =
  listToMaybe <$> PGS.query conn q (tname, newStatus, runAt, jid, PGS.In existingStates)
  where
    q = "update ? set attempts=0, status=?, run_at=? where id=? and status in ? returning " <> concatJobDbColumns
    runAt = case mRunAt of
      Nothing -> PGS.toField $ PGS.Identifier "run_at"
      Just t -> PGS.toField t


data TimeoutException = TimeoutException deriving (Eq, Show)
instance Exception TimeoutException

-- | this spawns the job on a a seperate thread, and
--   also spawns a monitoring thread, whichever finishes
--   first kills the other.
runJobWithTimeout :: (HasJobRunner m)
                  => Seconds
                  -> Job
                  -> m (Maybe Aeson.Value)
runJobWithTimeout timeoutSec job@Job{jobId} = do
  threadsRef <- envJobThreadsRef <$> getRunnerEnv
  jobRunner_ <- getJobRunner

  a <- async $ liftIO $ jobRunner_ job

  atomicModifyIORef' threadsRef $ \threads -> ( DM.insert jobId (void a) threads, () )
    -- ( DM.insert jobId a threads
    -- , DL.map asyncThreadId $ DM.elems $ DM.insert jobId a threads
    -- )

  -- liftIO $ putStrLn $ "Threads: " <> show x
  log LevelDebug $ LogText $ toS $ "Spawned job in " <> show (asyncThreadId a)

  t <- async $ delaySeconds timeoutSec

  
  finally
    (waitEitherCancel t a >>= either (const $ throwIO TimeoutException) pure) 
    (atomicModifyIORef' threadsRef $ \threads -> (DM.delete jobId threads, ()))

  


-- | runs a job, blocks for as long as it's in progress
runJob :: (HasJobRunner m) => JobId -> m ()
runJob jid = do
  findJobById jid >>= \case
    Nothing -> Prelude.error $ "Could not find job id=" <> show jid
    Just job -> do
      startTime <- liftIO getCurrentTime
      lockTimeout <- getDefaultJobTimeout
      log LevelInfo $ LogJobStart job
      flip catch (exceptionHandler job startTime) $ do
        onJobStart job
        jresult <- runJobWithTimeout lockTimeout job
        endTime <- liftIO getCurrentTime
        let newJob = job{jobStatus=OddJobs.Types.Success, jobLockedBy=Nothing, jobLockedAt=Nothing, jobUpdatedAt = endTime, jobResult = jresult}
        shouldDeleteJob <- immediateJobDeletion >>= (\fn -> liftIO $ fn newJob)
        if shouldDeleteJob
          then deleteJob jid
          else void $ saveJob newJob
        -- case jobParentJobId job of
        --   Nothing -> do
        --   Just _ -> do

        log LevelInfo $ LogJobSuccess newJob (diffUTCTime endTime startTime)
        onJobSuccess newJob
        pure ()
  where
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
                           , jobLastError=Just $ toJSON $ show e -- TODO: convert errors to json properly
                           , jobRunAt=addUTCTime (fromIntegral $ unSeconds backoffInSeconds) t
                           }
      case fromException e :: Maybe TimeoutException of
        Nothing -> do
          log logLevel $ LogJobFailed newJob e failureMode runTime
          let tryHandler (JobErrHandler handler) res = case fromException e of
                Nothing -> res
                Just e_ -> void $ handler e_ newJob failureMode
          handlers <- onJobFailed
          liftIO $ void $ Prelude.foldr tryHandler (throwIO e) handlers

        Just _ -> do
          log logLevel $ LogJobTimeout newJob
          onJobTimeout newJob

      pure ()

killJob :: (HasJobRunner m) => JobId -> m ()
killJob jid = do
  threadsRef <- envJobThreadsRef <$> getRunnerEnv
  threads <- liftIO $ readIORef threadsRef
  mJob <- findJobById jid

  case (mJob, jid `DM.lookup` threads) of
    (Just job, Just thread) -> do
      log LevelInfo $ LogKillJobSuccess job
      void $ finally
        (uninterruptibleCancel thread)
        (atomicModifyIORef' threadsRef $ \threads' -> (DM.delete jid threads', ()))

    (Just job, Nothing) -> do
      log LevelInfo $ LogKillJobFailed job

    (Nothing, _) ->
      log LevelError $ LogText $ "Unable to find job in db to kill, jobId = " <> toS (show jid)

-- TODO: This might have a resource leak.
restartUponCrash :: (HasJobRunner m, Show a) => Text -> m a -> m ()
restartUponCrash name_ action = do
  a <- async action
  finally (waitCatch a >>= fn) $ do
    log LevelInfo $ LogText $ "Received shutdown: " <> toS name_
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
  a3 <- async $ restartUponCrash "Job Kill poller" killJobPoller
  a4 <- delayedJobDeletion >>= \case
    Nothing -> pure Nothing
    Just deletionFn -> fmap Just $ async $ restartUponCrash "job deletion poller" (jobDeletionPoller deletionFn)
  let asyncThreads = [a1, a2, a3] <> maybeToList a4
  finally (void $ waitAnyCatch asyncThreads) $ do
    log LevelInfo (LogText "Stopping jobPoller and jobEventListener threads.")
    cancel a3
    cancel a2
    cancel a1
    maybe (pure ()) cancel a4
    log LevelInfo (LogText "Waiting for jobs to complete.")
    waitForJobs

waitForJobs :: (HasJobRunner m)
            => m ()
waitForJobs = do
  curJobs <- getRunnerEnv >>= (readIORef . envJobThreadsRef) <&> DM.elems
  case curJobs of
    [] -> log LevelInfo $ LogText "All job-threads exited"
    as -> do
      tid <- myThreadId
      void $ waitAnyCatch as
      log LevelDebug $ LogText $ toS $ "Waiting for " <> show (DL.length as) <> " jobs to complete before shutting down. myThreadId=" <> show tid
      delaySeconds (Seconds 1)
      waitForJobs

data ConcurrencyAction
  = DontPoll
  | PollAny
  | PollWithResources ResourceCfg

getConcurrencyControlFn :: (HasJobRunner m)
                        => m (Connection -> m ConcurrencyAction)
getConcurrencyControlFn = getConcurrencyControl >>= \case
  UnlimitedConcurrentJobs -> pure $ const $ pure PollAny
  MaxConcurrentJobs maxJobs -> pure $ const $ do
    curJobs <- getRunnerEnv >>= (readIORef . envJobThreadsRef) <&> DM.elems
    pure $ pollIf $ DL.length curJobs < maxJobs
  ResourceLimits resCfg -> pure $ const $ pure $ PollWithResources resCfg
  DynamicConcurrency fn -> pure $ const $ pollIf <$> liftIO fn

  where
    pollIf cond = if cond then PollAny else DontPoll

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
             , In [Queued, Retry]
             , Locked
             , addUTCTime (fromIntegral $ negate $ unSeconds lockTimeout) t)

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
  log LevelInfo $ LogText $ toS $ "Starting the job monitor via DB polling with processName=" <> processName
  concurrencyControlFn <- getConcurrencyControlFn
  pool <- getDbPool
  forever $ do
    concurencyPolicy <- withResource pool concurrencyControlFn
    case concurencyPolicy of
      DontPoll -> log LevelWarn $ LogText "NOT polling the job queue due to concurrency control"
      PollAny -> void $ pollRunJob processName Nothing
      PollWithResources resCfg -> void $ pollRunJob processName (Just resCfg)

-- | Polls a job and runs it, or executes a delay action if no job was found
--
--   returns an async to the task that was started, allowing you
--   to block until it's finished
pollRunJob :: (HasJobRunner m) => String -> Maybe ResourceCfg -> m (Maybe (Async ()))
pollRunJob processName mResCfg = do
    tname <- getTableName
    -- note it's better to use fine grained pool connection,
    -- since data.pool already has it's internal resource cache.
    -- this way the user can configure how long a connection
    -- needs to remain open.
    pool <- getDbPool
    lockTimeout <- getDefaultJobTimeout
    join $ withResource pool $ \pollerDbConn -> mask_ $ do
      log LevelDebug $ LogText $ toS $ "[" <> processName <> "] Polling the job queue.."
      t <- liftIO getCurrentTime
      r <- case mResCfg of
        Nothing -> liftIO $
           PGS.query pollerDbConn jobPollingSql
           ( tname
           , Locked
           , t
           , processName
           , tname
           , t
           , In [Queued, Retry]
           , Locked
           , addUTCTime (fromIntegral $ negate $ unSeconds lockTimeout) t)
        Just ResourceCfg{..} -> liftIO $
           PGS.query pollerDbConn jobPollingWithResourceSql
           ( tname
           , Locked
           , t
           , processName
           , tname
           , t
           , In [Queued, Retry]
           , Locked
           , addUTCTime (fromIntegral $ negate $ unSeconds lockTimeout) t
           , resCfgCheckResourceFunction
           )
      case r of
        -- When we don't have any jobs to run, we can relax a bit...
        [] -> pure (Nothing <$ delayAction)

        -- When we find a job to run, fork and try to find the next job without any delay...
        [Only (jid :: JobId)] -> do
          x <- async $ runJob jid
          pure $ Just x <$ noDelayAction

        x -> error $ "WTF just happened? I was supposed to get only a single row, but got: " ++ show x
  where
    delayAction = delaySeconds =<< getPollingInterval
    noDelayAction = pure ()

-- | Executes 'killJobPollingSql' every 'cfgPollingInterval' seconds to pick up jobs
-- that are cancelled and need to be killed. Uses @UPDATE@ along with @SELECT...
-- ..FOR UPDATE@ to efficiently find a job that matches /all/ of the following
-- conditions:
--
--   * 'jobStatus' should be 'cancelled'
--   * 'jobLockedAt' should be in the past
--   * 'jobLockedBy' should be the current job worker name

killJobPoller :: (HasJobRunner m) => m ()
killJobPoller = do
  processName <- liftIO jobWorkerName
  pool <- getDbPool
  tname <- getTableName

  let pollJobToKill conn = join $ mask_ $ do
        currentTime <- liftIO getCurrentTime
        result <- liftIO $ PGS.query conn killJobPollingSql
          (tname, tname, Cancelled, processName, currentTime)

        case result of
          [] ->
            pure delayAction

          [Only (jobId :: JobId)] -> do
            void $ async $ killJob jobId
            pure noDelayAction

          x ->
            error $ "I was supposed to get only a single row, but got: " ++ show x

  withResource pool (forever . pollJobToKill)

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

  let tryLockingJob jid mResCfg = withDbConnection $ \conn -> do
        let q = "UPDATE ? SET status=?, locked_at=now(), locked_by=?, attempts=attempts+1 WHERE id=? AND status in ? RETURNING id"
        result <- case mResCfg of
          Nothing -> liftIO $ PGS.query conn q (tname, Locked, jwName, jid, In [Queued, Retry])
          Just ResourceCfg{..} -> liftIO $ PGS.query conn qWithResources
              (tname, Locked, jwName, jid, In [Queued, Retry], resCfgCheckResourceFunction)

        case result of
          [] -> do
            log LevelDebug $ LogText $ toS $ "Job was locked by someone else before I could start. Skipping it. JobId=" <> show jid
            pure Nothing
          [Only (_ :: JobId)] -> pure $ Just jid
          x -> error $ "WTF just happned? Was expecting a single row to be returned, received " ++ show x

  withResource pool $ \monitorDbConn -> do
    void $ liftIO $ PGS.execute monitorDbConn "LISTEN ?" (Only $ pgEventName tname)
    forever $ do
      log LevelDebug $ LogText "[LISTEN/NOTIFY] Event loop"
      notif <- liftIO $ getNotification monitorDbConn

      let pload = notificationData notif
          runNotifWithFilter :: HasJobRunner m => Maybe ResourceCfg -> m ()
          runNotifWithFilter mResCfg = do
            log LevelDebug $ LogText $ toS $ "NOTIFY | " <> show pload
            case eitherDecode $ toS pload of
              Left e -> log LevelError $ LogText $ toS $  "Unable to decode notification payload received from Postgres. Payload=" <> show pload <> " Error=" <> show e

              -- Checking if job needs to be fired immediately AND it is not already
              -- taken by some othe thread, by the time it got to us
              Right (v :: Value) -> case Aeson.parseMaybe parser v of
                Nothing -> log LevelError $ LogText $ toS $ "Unable to extract id/run_at/locked_at from " <> show pload
                Just (jid, runAt_, mLockedAt_) -> do
                  t <- liftIO getCurrentTime
                  if (runAt_ <= t) && isNothing mLockedAt_
                    then do log LevelDebug $ LogText $ toS $ "Job needs needs to be run immediately. Attempting to fork in background. JobId=" <> show jid
                            void $ async $ do
                              -- Let's try to lock the job first... it is possible that it has already
                              -- been picked up by the poller by the time we get here.
                              tryLockingJob jid mResCfg >>= \case
                                Nothing -> pure ()
                                Just lockedJid -> runJob lockedJid
                    else log LevelDebug $ LogText $ toS $ "Job is either for future, is already locked, or would violate concurrency constraints. Skipping. JobId=" <> show jid

      concurrencyControlFn monitorDbConn >>= \case
        DontPoll -> log LevelWarn $ LogText "Received job event, but ignoring it due to concurrency control"
        PollAny -> runNotifWithFilter Nothing
        PollWithResources resCfg -> runNotifWithFilter (Just resCfg)
  where
    parser :: Value -> Aeson.Parser (JobId, UTCTime, Maybe UTCTime)
    parser = withObject "expecting an object to parse job.run_at and job.locked_at" $ \o -> do
      runAt_ <- o .: "run_at"
      mLockedAt_ <- o .:? "locked_at"
      jid <- o .: "id"
      pure (jid, runAt_, mLockedAt_)


jobDeletionPoller :: (HasJobRunner m) => (Connection -> IO Int64) -> m ()
jobDeletionPoller deletionFn = do
  i <- getPollingInterval
  dbPool <- getDbPool
  withDbConnection $ \conn -> do
    forever $ do
      n <- liftIO $ deletionFn conn
      log LevelDebug $ LogDeletionPoll n
      delaySeconds i

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
      queryFormatter = toS <$> PGS.formatQuery conn createJobQuery args
  rs <- PGS.query conn createJobQuery args
  case rs of
    [] -> Prelude.error . (<> "Not expecting a blank result set when creating a job. Query=") <$> queryFormatter
    [r] -> pure r
    _ -> Prelude.error . (<> "Not expecting multiple rows when creating a single job. Query=") <$> queryFormatter

type ResourceList = [(ResourceId, Int)]

createJobWithResources :: 
  ToJSON p =>
  Connection ->
  TableName ->
  ResourceCfg ->
  p ->
  ResourceList ->
  IO Job
createJobWithResources conn tname resCfg payload resources = do
  t <- getCurrentTime
  scheduleJobWithResources conn tname resCfg payload resources t

scheduleJobWithResources
  :: ToJSON p
  => Connection
  -> TableName
  -> ResourceCfg
  -> p
  -> ResourceList
  -> UTCTime
  -> IO Job
scheduleJobWithResources conn tname ResourceCfg{..} payload resources runAt = PGS.withTransaction conn $ do
  -- We insert everything in a single transaction to delay @NOTIFY@ calls,
  -- so a job isn't picked up before its resources are inserted.
  let args = ( tname, runAt, Queued, toJSON payload, Nothing :: Maybe Value, 0 :: Int, Nothing :: Maybe Text, Nothing :: Maybe Text )
      queryFormatter = toS <$> PGS.formatQuery conn createJobQuery args
  rs <- PGS.query conn createJobQuery args

  job <- flip onException (PGS.rollback conn) $ case rs of
    [] -> Prelude.error . (<> "Not expecting a blank result set when creating a job. Query=") <$> queryFormatter
    [r] -> pure r
    _ -> Prelude.error . (<> "Not expecting multiple rows when creating a single job. Query=") <$> queryFormatter

  forM_ resources $ \(resourceId, usage) -> do
    void $ PGS.execute conn ensureResource (resCfgResourceTable, rawResourceId resourceId, resCfgDefaultLimit)
    void $ PGS.execute conn registerResourceUsage (resCfgUsageTable, jobId job, rawResourceId resourceId, usage)

  pure job

-- getRunnerEnv :: (HasJobRunner m) => m RunnerEnv
-- getRunnerEnv = ask

eitherParsePayload :: (FromJSON a)
                   => Job
                   -> Either String a
eitherParsePayload =
  eitherParsePayloadWith parseJSON

throwParsePayload :: (FromJSON a)
                  => Job
                  -> IO a
throwParsePayload =
  throwParsePayloadWith parseJSON

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
  either throwString pure (eitherParsePayloadWith parser job)


-- | If you aren't interesting in storing 'jobResults' then use this in every 
-- branch of the @case@ statement in your 'jobRunner', like this:
--
-- @
-- myJobRunner :: Job -> IO (Maybe Aeson.Value)
-- myJobRunner job = do
-- 
-- throwParsePayload job >>= \case
--   SendConfirmationEmail uid -> noJobResult $ sendConfirmationEmail uid
--   SetupCustomerAccount cid -> noJobResult $ setupCustomerAccount cid
-- @
--
noJobResult :: (Functor f) => f a -> f (Maybe Aeson.Value)
noJobResult = (Nothing <$)


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
  mapMaybe fromOnly <$> PGS.query conn "select distinct locked_by from ?" (Only uicfgTableName)
