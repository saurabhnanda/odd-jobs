{-# LANGUAGE RankNTypes, FlexibleInstances, FlexibleContexts, PartialTypeSignatures, TupleSections, DeriveGeneric, UndecidableInstances #-}
module PGQueue.Job
  ( jobMonitor
  , jobEventListener
  , jobPoller
  , createJob
  , scheduleJob
  , Job(..)
  , JobRunner
  , HasJobMonitor(..)
  , Status(..)
  , findJobById
  , JobId
  , saveJob
  , defaultPollingInterval
  , JobMonitor(..)
  , defaultJobMonitor
  , runJobMonitor
  , TableName
  , jobDbColumns
  , concatJobDbColumns
  , jobType
  )
where

import PGQueue.Types
import Data.Pool
import Data.Text as T
import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Notification
import Database.PostgreSQL.Simple.FromField as FromField
import Database.PostgreSQL.Simple.ToField as ToField
import Database.PostgreSQL.Simple.FromRow as FromRow
import UnliftIO.Async
import Control.Concurrent.Async (AsyncCancelled(..))
import UnliftIO.Concurrent (threadDelay)
import Data.String
import System.Posix.Process (getProcessID)
import Network.HostName (getHostName)
import UnliftIO.MVar
import Debug.Trace
import Control.Monad.Logger as MLogger
import UnliftIO.IORef
import UnliftIO.Exception (SomeException(..), try, catch, finally, catchAny)
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
import Data.Maybe (isNothing)
import Data.Either (either)
import System.Log.FastLogger (fromLogStr, newTimedFastLogger, LogType(..), defaultBufSize, FastLogger, FileLogSpec(..), TimedFastLogger)
import System.Log.FastLogger.Date (newTimeCache, simpleTimeFormat')
import Control.Monad.Reader
import GHC.Generics
import qualified Data.HashMap.Strict as HM

class (MonadUnliftIO m, MonadBaseControl IO m, MonadLogger m) => HasJobMonitor m where
  getPollingInterval :: m Int
  onJobRetry :: Job -> m ()
  onJobSuccess :: Job -> m ()
  onJobPermanentlyFailed :: Job -> m ()
  getJobRunner :: m (Job -> IO ())
  getMaxAttempts :: m Int
  getDbPool :: m (Pool Connection)
  getTableName :: m TableName
  onJobStart :: Job -> m ()
  getDefaultMaxAttempts :: m Int

data JobMonitor = JobMonitor
  { monitorPollingInterval :: Int
  , monitorOnJobSuccess :: Job -> IO ()
  , monitorOnJobRetry :: Job -> IO ()
  , monitorOnJobPermanentlyFailed :: Job -> IO ()
  , monitorJobRunner :: Job -> IO ()
  , monitorMaxAttempts :: Int
  , monitorLogger :: (forall msg . ToLogStr msg => Loc -> LogSource -> LogLevel -> msg -> IO ())
  , monitorDbPool :: Pool Connection
  , monitorTableName :: TableName
  , monitorOnJobStart :: Job -> IO ()
  , monitorDefaultMaxAttempts :: Int
  }

type JobMonitorM = ReaderT JobMonitor IO

instance {-# OVERLAPS #-} MonadLogger JobMonitorM where
  monadLoggerLog loc logsource loglevel msg = do
    fn <- monitorLogger <$> ask
    liftIO $ fn loc logsource loglevel msg

instance HasJobMonitor JobMonitorM where
  getPollingInterval = monitorPollingInterval <$> ask
  onJobRetry job = do
    fn <- monitorOnJobRetry <$> ask
    liftIO $ fn job
  onJobSuccess job = do
    fn <- monitorOnJobSuccess <$> ask
    liftIO $ fn job
  onJobPermanentlyFailed job = do
    fn <- monitorOnJobPermanentlyFailed <$> ask
    liftIO $ fn job
  getJobRunner = monitorJobRunner <$> ask
  getMaxAttempts = monitorMaxAttempts <$> ask
  getDbPool = monitorDbPool <$> ask
  getTableName = monitorTableName <$> ask
  onJobStart job = do
    fn <- monitorOnJobStart <$> ask
    liftIO $ fn job

  getDefaultMaxAttempts = monitorDefaultMaxAttempts <$> ask


runJobMonitor :: JobMonitor -> IO ()
runJobMonitor jm = runReaderT jobMonitor jm

defaultLogger :: IO (TimedFastLogger, IO ())
defaultLogger = do
  tcache <- newTimeCache simpleTimeFormat'
  newTimedFastLogger tcache (LogStdout defaultBufSize)

defaultJobMonitor :: TableName -> Pool Connection -> IO (JobMonitor, IO ())
defaultJobMonitor tname dbpool = do
  (logger, cleanup) <- defaultLogger
  pure $ (, cleanup) JobMonitor
    { monitorPollingInterval = defaultPollingInterval
    , monitorOnJobSuccess = (const $ pure ())
    , monitorOnJobRetry = (const $ pure ())
    , monitorOnJobPermanentlyFailed = (const $ pure ())
    , monitorJobRunner = (const $ pure ())
    , monitorMaxAttempts = 25
    , monitorLogger = \ loc logsource loglevel msg -> logger $ \t ->
        toLogStr t <> " | " <>
        defaultLogStr loc logsource loglevel (toLogStr msg)
    , monitorDbPool = dbpool
    , monitorOnJobStart = (const $ pure ())
    , monitorDefaultMaxAttempts = 10
    , monitorTableName = tname
  }


oneSec :: Int
oneSec = 1000000

defaultPollingInterval :: Int
defaultPollingInterval = (oneSec * 5)

type JobId = Int

data Status = Success
            | Queued
            | Failed
            | Retry
            deriving (Eq, Show, Generic, Enum)

instance Ord Status where
  compare x y = compare (toText x) (toText y)

data Job = Job
  { jobId :: JobId
  , jobCreatedAt :: UTCTime
  , jobUpdatedAt :: UTCTime
  , jobRunAt :: UTCTime
  , jobStatus :: Status
  , jobPayload :: Value
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

instance (StringConv Text a) => FromText (Either a Status) where
  fromText t = case t of
    "success" -> Right Success
    "queued" -> Right Queued
    "failed" -> Right Failed
    "retry" -> Right Retry
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
  pure $ (show hname) ++ ":" ++ (show pid)

-- TODO: Make this configurable based on a per-job basis
lockTimeout :: Int
lockTimeout = 600

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

findJobById :: Connection -> TableName -> JobId -> IO (Maybe Job)
findJobById conn tname jid = PGS.query conn (findJobByIdQuery tname) (Only jid) >>= \case
  [] -> pure Nothing
  [j] -> pure (Just j)
  js -> Prelude.error $ "Not expecting to find multiple jobs by id=" <> (show jid)


saveJobQuery :: TableName -> PGS.Query
saveJobQuery tname = "UPDATE " <> tname <> " set run_at = ?, status = ?, payload = ?, last_error = ?, attempts = ?, locked_at = ?, locked_by = ? WHERE id = ? RETURNING " <> concatJobDbColumns

saveJob :: Connection -> TableName -> Job -> IO Job
saveJob conn tname Job{jobRunAt, jobStatus, jobPayload, jobLastError, jobAttempts, jobLockedBy, jobLockedAt, jobId} = do
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


logCallbackErrors :: (HasJobMonitor m) => JobId -> Text -> m () -> m ()
logCallbackErrors jid msg action = catchAny action $ \e -> logErrorN $ msg <> " Job ID=" <> toS (show jid) <> ": " <> toS (show e)


runJob :: (HasJobMonitor m) => Connection -> JobId -> m ()
runJob conn jid = do
  tname <- getTableName
  (liftIO $ findJobById conn tname jid) >>= \case
    Nothing -> Prelude.error $ "Could not find job id=" <> show jid
    Just job -> do
      jobRunner_ <- getJobRunner
      (try $ liftIO $ jobRunner_ job) >>= \case
        Right () -> do
          -- TODO: save job result for future
          newJob <- liftIO $ saveJob conn tname job{jobStatus=Success, jobLockedBy=Nothing, jobLockedAt=Nothing}
          logCallbackErrors (jobId newJob) "onSucces" (onJobSuccess newJob)
          pure ()

        Left (SomeException e) -> do
          defaultMaxAttempts <- getDefaultMaxAttempts
          let newStatus = if (jobAttempts job) >= defaultMaxAttempts
                          then Failed
                          else Retry
          t <- liftIO getCurrentTime
          newJob <- liftIO $ saveJob conn tname job{ jobStatus=newStatus
                                                   , jobLockedBy=Nothing
                                                   , jobLockedAt=Nothing
                                                   , jobLastError=(Just $ toJSON $ show e) -- TODO: convert errors to json properly
                                                   , jobRunAt=(addUTCTime (fromIntegral $ (1::Int) ^ (jobAttempts job)) t)
                                                   }
          case (jobStatus newJob) of
            Failed -> logCallbackErrors (jobId newJob) "onJobPermanentlyFailed" (onJobPermanentlyFailed newJob)
            Retry -> logCallbackErrors (jobId newJob) "onJobRetry" (onJobRetry newJob)
            x -> Prelude.error $ "Unexpected job status = " <> show x
          pure ()

jobMonitor :: forall m . (HasJobMonitor m) => m ()
jobMonitor = jobMonitor_ Nothing Nothing
  where
    jobMonitor_ :: Maybe (Async ()) -> Maybe (Async ()) -> m ()
    jobMonitor_ mPollerAsync mEventAsync = do
      pollerAsync <- maybe (async jobPoller) pure mPollerAsync
      eventAsync <- maybe (async jobEventListener) pure mEventAsync
      finally
        (restartUponCrash pollerAsync eventAsync)
        (do logInfoN "Received shutdown event. Cancelling job-poller and event-listener threads"
            cancel eventAsync
            cancel pollerAsync
        )


    restartUponCrash pollerAsync eventAsync = do
      waitEitherCatch pollerAsync eventAsync >>= \case
        Left pollerResult -> do
          either
            (\(SomeException e) -> logErrorN $ "Job poller seems to have crashed. Respawning: " <> toS (show e))
            (\x -> logErrorN $ "Job poller seems to have escaped the `forever` loop. Respawning: " <> toS (show x))
            pollerResult
          jobMonitor_ Nothing (Just eventAsync)
        Right eventResult -> do
          either
            (\(SomeException e) -> logErrorN $ "Event listened seems to have crashed. Respawning: " <> toS (show e))
            (\x -> logErrorN $ "Event listener seems to have escaped the `forever` loop. Respawning: " <> toS (show x))
            eventResult
          jobMonitor_ (Just pollerAsync) Nothing


jobPollingSql :: TableName -> Query
jobPollingSql tname = "update " <> tname <> " set locked_at = ?, locked_by = ?, attempts=attempts+1 WHERE id in (select id from " <> tname <> " where (run_at<=? AND ((locked_at is null AND locked_by is null AND status in ?) OR (locked_at<?))) ORDER BY run_at ASC LIMIT 1 FOR UPDATE) RETURNING id"


jobPoller :: (HasJobMonitor m) => m ()
jobPoller = do
  processName <- liftIO jobWorkerName
  pool <- getDbPool
  tname <- getTableName
  logInfoN $ toS $ "Starting the job monitor via DB polling with processName=" <> show processName
  withResource pool $ \pollerDbConn -> forever $ do
    logInfoN $ toS $ "[" <> show processName <> "] Polling the job queue.."
    t <- liftIO getCurrentTime
    (liftIO $ PGS.query pollerDbConn (jobPollingSql tname) (t, processName, t, (In [Queued, Retry]), (addUTCTime (fromIntegral (-lockTimeout)) t))) >>= \case
      -- When we don't have any jobs to run, we can relax a bit...
      [] -> threadDelay defaultPollingInterval

      -- When we find a job to run, fork and try to find the next job without any delay...
      [Only (jid :: JobId)] -> do
        logInfoN $ toS $ "Job poller found a job. Forking another thread in the background. JobId=" <> show jid

        -- NOTE: If we don't have any more connections in the pool, the
        -- following statements will block, which is a good thing. Because, if we
        -- don't have any more DB connections, there's no point in polling for
        -- more jobs...

        jobReadyToRun <- newEmptyMVar
        void $ async $ withResource pool $ \jobConn -> do
          logInfoN $ toS $ "DB connection acquired. Signalling the job-poller to continue polling. JobId=" <> show jid
          putMVar jobReadyToRun True
          runJob jobConn jid

        -- Block the polling till the job-runner is ready to run....
        void $ readMVar jobReadyToRun

      x -> error $ "WTF just happened? I was supposed to get only a single row, but got: " ++ (show x)



jobEventListener :: (HasJobMonitor m) => m ()
jobEventListener = do
  logInfoN "Starting the job monitor via LISTEN/NOTIFY..."
  pool <- getDbPool
  tname <- getTableName
  withResource pool $ \monitorDbConn -> forever $ do
    logInfoN "[LISTEN/NOFIFY] Event loop"
    _ <- liftIO $ PGS.execute monitorDbConn ("LISTEN " <> pgEventName tname) ()
    notif <- liftIO $ getNotification monitorDbConn
    let pload = notificationData notif
    logDebugN $ toS $ "NOTIFY | " <> show pload

    case (eitherDecode $ toS pload) of
      Left e -> logErrorN $ toS $  "Unable to decode notification payload received from Postgres. Payload=" <> show pload <> " Error=" <> show e

      -- Checking if job needs to be fired immediately AND it is not already
      -- taken by the time it got to us
      Right (v :: Value) -> case (Aeson.parseMaybe parser v) of
        Nothing -> logErrorN $ toS $ "Unable to extract id/run_at/locked_at from " <> show pload
        Just (jid, runAt_, mLockedAt_) -> do
          t <- liftIO getCurrentTime
          if (runAt_ <= t) && (isNothing mLockedAt_)
            then do logDebugN $ toS $ "Job needs needs to be run immediately. Attempting to fork in background. JobId=" <> show jid
                    void $ async $ withResource pool $ \conn -> do
                      -- Let's try to lock the job first... it is possible that it has already
                      -- been picked up by the poller by the time we get here.
                      t2 <- liftIO getCurrentTime
                      jwName <- liftIO jobWorkerName
                      let q = "UPDATE " <> tname <> " SET locked_at=?, locked_by=?, attempts=attempts+1 WHERE id=? AND locked_at IS NULL AND locked_by IS NULL RETURNING id"
                      (liftIO $ PGS.query conn q (t2, jwName, jid)) >>= \case
                        [] -> logDebugN $ toS $ "Job was locked by someone else before I could start. Skipping it. JobId=" <> show jid
                        [Only (_ :: JobId)] -> do
                          logDebugN $ "Attempting to run JobId=" <> (toS $ show jid)
                          runJob conn jid
                        x -> error $ "WTF just happned? Was expecting a single row to be returned, received " ++ (show x)
            else logDebugN $ toS $ "Job is either for future, or is already locked. Skipping. JobId=" <> show jid
  where
    parser :: Value -> Aeson.Parser (JobId, UTCTime, Maybe UTCTime)
    parser = withObject "expecting an object to parse job.run_at and job.locked_at" $ \o -> do
      runAt_ <- o .: "run_at"
      mLockedAt_ <- o .:? "locked_at"
      jid <- o .: "id"
      pure (jid, runAt_, mLockedAt_)


createJobQuery :: TableName -> PGS.Query
createJobQuery tname = "INSERT INTO " <> tname <> "(run_at, status, payload, last_error, attempts, locked_at, locked_by) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING " <> concatJobDbColumns

createJob :: ToJSON p => Connection -> TableName -> p -> IO Job
createJob conn tname payload = do
  t <- getCurrentTime
  scheduleJob conn tname payload t

scheduleJob :: ToJSON p => Connection -> TableName -> p -> UTCTime -> IO Job
scheduleJob conn tname payload runAt = do
  let args = ( runAt, Queued, toJSON payload, Nothing :: Maybe Value, 0 :: Int, Nothing :: Maybe Text, Nothing :: Maybe Text )
      queryFormatter = toS <$> (PGS.formatQuery conn (createJobQuery tname) args)
  rs <- PGS.query conn (createJobQuery tname) args
  case rs of
    [] -> (Prelude.error . (<> "Not expecting a blank result set when creating a job. Query=")) <$> queryFormatter
    [r] -> pure r
    _ -> (Prelude.error . (<> "Not expecting multiple rows when creating a single job. Query=")) <$> queryFormatter 


jobType :: Job -> T.Text
jobType Job{jobPayload} = case jobPayload of
  Aeson.Object hm -> case HM.lookup "tag" hm of
    Just (Aeson.String t) -> t
    _ -> ""
  _ -> ""
