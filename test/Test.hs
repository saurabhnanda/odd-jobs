{-# LANGUAGE FlexibleInstances, NamedFieldPuns, DeriveGeneric, FlexibleContexts, TypeFamilies, StandaloneDeriving, RankNTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-missing-signatures -Wno-partial-type-signatures #-}
module Test where

import Control.Monad.Trans.Control
import Test.Tasty as Tasty
import qualified OddJobs.Migrations as Migrations
import qualified OddJobs.Job as Job
import Database.PostgreSQL.Simple as PGS
import Data.Functor (void)
import Data.Pool as Pool
import Test.Tasty.HUnit
import Debug.Trace
-- import Control.Exception.Lifted (finally, catch, bracket)
import Control.Monad (void, forM, forM_, replicateM, when)
import Control.Monad.Logger
import Control.Monad.Reader
import Data.Aeson as Aeson
import Data.Aeson.TH as Aeson
-- import Control.Concurrent.Lifted
-- import Control.Concurrent.Async.Lifted
import OddJobs.Job (Job(..), JobId, delaySeconds, Seconds(..))
import System.Log.FastLogger ( fromLogStr, withFastLogger, LogType'(..)
                             , defaultBufSize, FastLogger, FileLogSpec(..), newTimedFastLogger
                             , withTimedFastLogger)
import System.Log.FastLogger.Date (newTimeCache, simpleTimeFormat')
import Data.String.Conv (toS)
import Data.Time
import GHC.Generics
import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Test.Tasty.Hedgehog
import qualified System.Random as R
import Data.String (fromString)
import qualified Data.IntMap.Strict as Map
import Control.Monad.Morph (hoist)
import Data.List as DL
import OddJobs.Web as Web
import qualified Data.Time.Convenience as Time
import qualified Data.Text as T
import Data.Ord (comparing, Down(..))
import Data.Maybe (fromMaybe)
import qualified OddJobs.ConfigBuilder as Job
import UnliftIO
import Control.Exception (ArithException)
import Data.Bifunctor(first)
import System.Environment (lookupEnv)

$(Aeson.deriveJSON Aeson.defaultOptions ''Seconds)

main :: IO ()
main = do
  bracket createAppPool destroyAllResources $ \appPool -> do
    bracket createJobPool destroyAllResources $ \jobPool -> do
      defaultMain $ tests appPool jobPool
  where
    getConnInfo = do
      connectHost <- fromMaybe "localhost" <$> lookupEnv "PGHOST"
      connectUser <- fromMaybe "jobs_test" <$> lookupEnv "PGUSER"
      connectPassword <- fromMaybe "jobs_test" <$> lookupEnv "PGPASSWORD"
      connectDatabase <- fromMaybe "jobs_test" <$> lookupEnv "PGDATABASE"
      connectPort <- maybe (fromIntegral (5432 :: Int)) read <$> lookupEnv "PGPORT"
      pure ConnectInfo{..}

    createAppPool = do
      connInfo <- getConnInfo
      Pool.newPool $ Pool.defaultPoolConfig
        (PGS.connect connInfo)  -- create a new resource
        PGS.close               -- destroy resource
        (fromRational 10)       -- number of seconds unused resources are kept around
        45

    createJobPool = do
      connInfo <- getConnInfo
      Pool.newPool $ Pool.defaultPoolConfig
        (PGS.connect connInfo)  -- create a new resource
        PGS.close               -- destroy resource
        (fromRational 10)       -- number of seconds unused resources are kept around
        45

tests appPool jobPool = testGroup "All tests"
  [
    testGroup "simple tests" [ testJobCreation appPool jobPool
                             , testJobScheduling appPool jobPool
                             , testJobFailure appPool jobPool
                             , testJobResults appPool jobPool
                             , testEnsureShutdown appPool jobPool
                             , testGracefulShutdown appPool jobPool
                             , testPushFailedJobEndQueue jobPool
                             , testRetryBackoff appPool jobPool
                             , testJobDeletion appPool jobPool
                             , testGroup "callback tests"
                               [ testOnJobFailed appPool jobPool
                               , testOnJobStart appPool jobPool
                               , testOnJobSuccess appPool jobPool
                               , testOnJobTimeout appPool jobPool
                               ]
                             , testResourceLimitedScheduling appPool jobPool
                             , testKillJob appPool jobPool
                             ]
  -- , testGroup "property tests" [ testEverything appPool jobPool
  --                              -- , propFilterJobs appPool jobPool
  --                              ]
  ]

myTestCase
  :: TestName
  -> (Connection -> Assertion)
  -> Pool Connection
  -> TestTree
myTestCase tname actualtest pool = testCase tname $ Pool.withResource pool actualtest
-- myTestCase tname actualtest pool = Tasty.withResource
--   (traceShow "connection taken" $ Pool.takeResource pool)
--   (\(conn, lpool) -> traceShow "connection released" $ Pool.putResource lpool conn)
--   (\res -> testCase tname $ do
--       (conn, _) <- res
--       (actualtest conn))

data Env = Env
  { envDbPool :: Pool Connection
  , envLogger :: FastLogger
  }

type TestM = ReaderT Env IO

-- instance Job.HasJobMonitor TestM where
--   getDbPool = envDbPool <$> ask

--   onJobFailed Job{jobId} = logInfoN $ "Failed JobId=" <> toS (show jobId)
--   onJobSuccess Job{jobId} = logInfoN $ "Success JobId=" <> toS (show jobId)

-- instance {-# OVERLAPPING #-} MonadLogger TestM where
--   monadLoggerLog _ _ _ msg = do
--     flogger <- envLogger <$> ask
--     liftIO $ flogger $ toLogStr msg <> toLogStr ("\n" :: String)

testPayload :: Value
testPayload = toJSON (10 :: Int)

jobRunner :: Job.Job -> IO (Maybe Aeson.Value)
jobRunner Job{jobPayload, jobAttempts} = case fromJSON jobPayload of
  Aeson.Error e -> error e
  Success (j :: JobPayload) ->
    let recur pload idx =  case pload of
          PayloadAlwaysFail delay -> delaySeconds delay >> error ("Forced error after " <> show delay <> " seconds")
          PayloadSucceed delay jresult -> delaySeconds delay >> pure (fmap Aeson.toJSON jresult)
          PayloadFail delay innerpload -> if idx<jobAttempts
                                          then recur innerpload (idx + 1)
                                          else delaySeconds delay >> error ("Forced error after " <> show delay <> " seconds. step=" <> show idx)
          PayloadThrowStringException s -> throwString s
          PayloadThrowDivideByZero -> seq (1 `div` 0 :: Integer) (pure Nothing)
    in recur j 0

data JobPayload = PayloadSucceed Seconds (Maybe String)
                | PayloadFail Seconds JobPayload
                | PayloadAlwaysFail Seconds
                | PayloadThrowStringException String
                | PayloadThrowDivideByZero
                deriving (Eq, Show, Generic)

instance ToJSON JobPayload where
  toJSON = genericToJSON Aeson.defaultOptions

instance FromJSON JobPayload where
  parseJSON = genericParseJSON Aeson.defaultOptions


logEventToJob :: Job.LogEvent -> Maybe Job.Job
logEventToJob le = case le of
  Job.LogJobStart j -> Just j
  Job.LogJobSuccess j _ -> Just j
  Job.LogJobFailed j _ _ _ -> Just j
  Job.LogJobTimeout j -> Just j
  Job.LogKillJobSuccess j -> Just j
  Job.LogKillJobFailed j -> Just j
  Job.LogPoll -> Nothing
  Job.LogDeletionPoll _ -> Nothing
  Job.LogWebUIRequest -> Nothing
  Job.LogText _ -> Nothing

assertJobIdStatus :: HasCallStack
                  => Connection
                  -> Job.TableName
                  -> IORef [Job.LogEvent]
                  -> String
                  -> Job.Status
                  -> JobId
                  -> Assertion
assertJobIdStatus conn tname logRef msg st jid = do
  logs <- readIORef logRef
  let _mjid = Just jid
  case st of
    Job.Success ->
      assertBool (msg <> ": Success event not found in job-logs for JobId=" <> show jid) $
      flip DL.any logs $ \case
        Job.LogJobSuccess j _ -> jid == Job.jobId j
        _ -> False

    Job.Queued ->
      assertBool (msg <> ": Not expecting to find a queued job in the the job-logs JobId=" <> show jid) $
      not $ flip DL.any logs $ \case
        Job.LogJobStart j -> jid == Job.jobId j
        Job.LogJobSuccess j _ -> jid == Job.jobId j
        Job.LogJobFailed j _ _ _ -> jid == Job.jobId j
        Job.LogJobTimeout j -> jid == Job.jobId j
        Job.LogKillJobSuccess _ -> False
        Job.LogKillJobFailed _  -> False
        Job.LogWebUIRequest -> False
        Job.LogText _ -> False
        Job.LogPoll -> False
        Job.LogDeletionPoll _ -> False

    Job.Failed ->
      assertBool (msg <> ": Failed event not found in job-logs for JobId=" <> show jid) $
      flip DL.any logs $ \case
        Job.LogJobFailed j _ _ _ -> jid == Job.jobId j
        _ -> False

    Job.Cancelled ->
      assertBool (msg <> ": Job killed event not found in job-logs for JobId=" <> show jid) $
      flip DL.any logs $ \case
        Job.LogKillJobSuccess j -> jid == Job.jobId j
        _ -> False

    Job.Retry ->
      assertBool (msg <> ": Failed event not found in job-logs for JobId=" <> show jid) $
      flip DL.any logs $ \case
        Job.LogJobFailed j _ _ _ -> jid == Job.jobId j
        _ -> False

    Job.Locked ->
      assertBool (msg <> ": Start event should be present in the log for a locked job JobId=" <> show jid) $
      flip DL.any logs $ \case
        Job.LogJobStart j -> jid == Job.jobId j
        _ -> False

  when (st /= Job.Success) $ do
    Job.findJobByIdIO conn tname jid >>= \case
      Nothing -> assertFailure $ "Not expecting job to be deleted. JobId=" <> show jid
      Just (Job{jobStatus}) -> assertEqual msg st jobStatus

ensureJobId :: HasCallStack => Connection -> Job.TableName -> JobId -> IO Job
ensureJobId conn tname jid = Job.findJobByIdIO conn tname jid >>= \case
  Nothing -> error $ "Not expecting job to be deleted. JobId=" <> show jid
  Just j -> pure j

withRandomTable :: (MonadBaseControl
                          IO m, MonadUnliftIO m) => Pool Connection -> (Job.TableName -> m a) -> m a
withRandomTable jobPool action = do
  runInIO <- askRunInIO
  (tname :: Job.TableName) <- liftIO (fromString . ("jobs_" <>) <$> replicateM 10 (R.randomRIO ('a', 'z')))
  finally
    (liftIO $ Pool.withResource jobPool (\conn -> liftIO $ Migrations.createJobTable conn tname) >> runInIO (action tname))
    (liftIO $ Pool.withResource jobPool $ \conn -> liftIO $ void $ PGS.execute conn "drop table if exists ?" (Only tname))

runSingleJobFromQueueBlock :: Job.Config -> IO (Maybe (Either SomeException ()))
runSingleJobFromQueueBlock cfg =
  traverse waitCatch =<< runSingleJobFromQueue cfg

-- | waits untill the job has started
runSingleJobFromQueue :: Job.Config -> IO (Maybe (Async ()))
runSingleJobFromQueue config' = do
  r <- liftIO $ newIORef mempty
  waitTillJobStart <- newEmptyMVar
  let config =  config' 
                { Job.cfgPollingInterval = 0
                , Job.cfgJobRunner = \job -> do
                    putMVar waitTillJobStart ()
                    jobRunner job
                }
  let monitorEnv = Job.RunnerEnv
                   { Job.envConfig = config
                   , Job.envJobThreadsRef = r
                   }
  result <- runReaderT (Job.pollRunJob "runSingleJobFromQueue" $ readResourceConfig config) monitorEnv
  forM_ result $ const $ readMVar waitTillJobStart
  pure result

readResourceConfig :: Job.Config -> Maybe Job.ResourceCfg
readResourceConfig cfg = case Job.cfgConcurrencyControl cfg of
  Job.ResourceLimits res -> Just res
  _ -> Nothing

withRandomResourceTables :: (MonadBaseControl IO m, MonadUnliftIO m) => Int -> Pool Connection -> Job.TableName -> (Job.ResourceCfg -> m a) -> m a
withRandomResourceTables defaultLimit jobPool tname action = do
  resCfgResourceTable <- liftIO $ fromString . ("resources_" <>) <$> replicateM 10 (R.randomRIO ('a', 'z'))
  resCfgUsageTable <- liftIO $ fromString . ("usage_" <>) <$> replicateM 10 (R.randomRIO ('a', 'z'))
  resCfgCheckResourceFunction <- liftIO $ fromString . ("check_resource_fn_" <>) <$> replicateM 10 (R.randomRIO ('a', 'z'))
  let resCfg = Job.ResourceCfg
        { Job.resCfgDefaultLimit = defaultLimit
        , ..
        }

  finally
    (do
      liftIO $ Pool.withResource jobPool $ \conn -> liftIO $ Migrations.createResourceTables conn tname resCfg
      action resCfg)
    (liftIO $ Pool.withResource jobPool $ \conn -> liftIO $ void $ PGS.execute conn
        "drop function if exists ?; drop table if exists ?; drop table if exists ?"
        (resCfgCheckResourceFunction, resCfgUsageTable, resCfgResourceTable))

testMaxAttempts :: Int
testMaxAttempts = 3

-- withNewJobMonitor :: (Pool Connection) -> (TableName -> Assertion) -> Assertion
withNewJobMonitor jobPool actualTest = do
  withRandomTable jobPool $ \tname -> do
    withNamedJobMonitor tname jobPool Prelude.id (actualTest tname)

withNamedJobMonitor :: Job.TableName -> Pool Connection -> (Job.Config -> Job.Config) -> (IORef [Job.LogEvent] -> IO a) -> IO a
withNamedJobMonitor tname jobPool cfgFn actualTest =
  withConfig tname jobPool cfgFn $ \logRef cfg -> do
    withAsync (Job.startJobRunner cfg) (const $ actualTest logRef)

withConfig :: Job.TableName -> Pool Connection -> (Job.Config -> Job.Config) -> (IORef [Job.LogEvent] -> Job.Config -> IO a) -> IO a
withConfig tname jobPool cfgFn configConsumer = do
  logRef :: IORef [Job.LogEvent] <- newIORef []
  tcache <- newTimeCache simpleTimeFormat'
  withTimedFastLogger tcache LogNone $ \tlogger -> do
    let flogger logLevel logEvent = do
          tlogger $ \t -> toLogStr t <> " | " <> Job.defaultLogStr Job.defaultJobType logLevel logEvent
          atomicModifyIORef' logRef (\logs -> (logEvent:logs, ()))
        cfg = Job.mkConfig flogger tname jobPool Job.UnlimitedConcurrentJobs jobRunner (\cfg' -> cfgFn $ cfg'{Job.cfgDefaultMaxAttempts=testMaxAttempts})
    configConsumer logRef cfg

payloadGen :: MonadGen m => m JobPayload
payloadGen = Gen.recursive  Gen.choice nonRecursive recursive
  where
    nonRecursive = [ PayloadAlwaysFail <$> Gen.element [1, 2, 3]
                   , PayloadSucceed <$> Gen.element [1, 2, 3] <*> Gen.maybe (Gen.string (Range.singleton 10) Gen.alphaNum)
                   ]
    recursive = [ PayloadFail <$> Gen.element [1, 2, 3] <*> payloadGen ]

testJobCreation appPool jobPool = testCase "job creation" $ do
  withNewJobMonitor jobPool $ \tname logRef -> do
    Pool.withResource appPool $ \conn -> do
      Job{jobId} <- Job.createJob conn tname (PayloadSucceed 0 Nothing)
      delaySeconds $ Seconds 6
      assertJobIdStatus conn tname logRef "Expecting job to be successful by now" Job.Success jobId

testEnsureShutdown appPool jobPool = testCase "ensure shutdown" $ do
  withRandomTable jobPool $ \tname -> do
    (jid, logRef) <- scheduleJob tname
    delaySeconds (2 * Job.defaultPollingInterval)
    Pool.withResource appPool $ \conn -> do
      assertJobIdStatus conn tname logRef "Job should still be in queued state if job-monitor is no longer running" Job.Queued jid
  where
    scheduleJob tname = withNamedJobMonitor tname jobPool Prelude.id $ \logRef -> do
      t <- getCurrentTime
      Pool.withResource appPool $ \conn -> do
        Job{jobId} <- Job.scheduleJob conn tname (PayloadSucceed 0 Nothing) (addUTCTime (fromIntegral (2 * unSeconds Job.defaultPollingInterval)) t)
        assertJobIdStatus conn tname logRef "Job is scheduled in future, should still be queueud" Job.Queued jobId
        pure (jobId, logRef)

testGracefulShutdown ::  Pool Connection -> Pool Connection -> TestTree
testGracefulShutdown appPool jobPool =
  testCase "ensure wait for all jobs is correct" $
    withRandomTable jobPool $ \tname -> do
        waitTillJobStart <- newEmptyMVar
        Pool.withResource appPool $ \conn -> do
          j1 <- Job.createJob conn tname (PayloadSucceed (4 * Job.defaultPollingInterval) Nothing)
          time<- getCurrentTime
          j2 <- Job.scheduleJob conn tname (PayloadSucceed 0 Nothing) (addUTCTime (fromIntegral $ unSeconds $ Job.defaultPollingInterval * 2) time)
          logRef <- withNamedJobMonitor tname jobPool (\cfg -> cfg { Job.cfgOnJobStart = \ _ -> do
                putMVar waitTillJobStart ()
                } )  $ \logRef -> do
            readMVar waitTillJobStart -- as soon as the job has started we quit the monitor:w
            pure logRef -- eg closing the withNamedJobMonitor closure shutsdwon the monitor
          assertJobIdStatus conn tname logRef "Expecting the first job to be completed successfully if graceful shutdown is implemented correctly" Job.Success (jobId j1)
          assertJobIdStatus conn tname logRef "Expecting the second job to be queued because no new job should be picked up during graceful shutdown" Job.Queued (jobId j2)

testJobScheduling appPool jobPool = testCase "job scheduling" $ do
  withNewJobMonitor jobPool $ \tname logRef -> do
    Pool.withResource appPool $ \conn -> do
      t <- getCurrentTime
      job@Job{jobId} <- Job.scheduleJob conn tname (PayloadSucceed 0 Nothing) (addUTCTime (fromIntegral (3600 :: Integer)) t)
      delaySeconds $ Seconds 2
      assertJobIdStatus conn tname logRef "Job is scheduled in the future. It should NOT have been successful by now" Job.Queued jobId
      _ <- Job.saveJobIO conn tname job{jobRunAt = addUTCTime (fromIntegral (-1 :: Integer)) t}
      delaySeconds (Job.defaultPollingInterval + Seconds 2)
      assertJobIdStatus conn tname logRef "Job had a runAt date in the past. It should have been successful by now" Job.Success jobId

testJobDeletion appPool jobPool = testCase "job immediae deletion" $ do
  withRandomTable jobPool $ \tname -> do
    withNamedJobMonitor tname jobPool (\cfg -> cfg { Job.cfgDelayedJobDeletion = Just $ Job.defaultDelayedJobDeletion tname pinterval, Job.cfgDefaultMaxAttempts = 2 }) $ \_logRef -> do
      Pool.withResource appPool $ \conn -> do

        let  assertDeletedJob msg jid =
              Job.findJobByIdIO conn tname jid >>= \case
                Nothing -> pure ()
                Just _ -> assertFailure msg

        successJob <- Job.createJob conn tname (PayloadSucceed 0 Nothing)
        failJob <- Job.createJob conn tname (PayloadAlwaysFail 0)
        delaySeconds (Job.defaultPollingInterval * 3)

        assertDeletedJob "Expecting successful job to be immediately deleted" (jobId successJob)

        j <- ensureJobId conn tname (jobId failJob)
        assertEqual "Exepcting job to be in Failed status" Job.Failed (jobStatus j)

        delaySeconds (Job.defaultPollingInterval * 4)
        assertDeletedJob "Expecting failed job to be deleted after adequate delay" (jobId failJob)
  where
    pinterval = show (Job.unSeconds Job.defaultPollingInterval) <> " seconds"

testJobResults appPool jobPool = testCase "job results" $ do
  withRandomTable jobPool $ \tname -> do
    withNamedJobMonitor tname jobPool (\cfg -> cfg{Job.cfgImmediateJobDeletion=const $ pure False}) $ \logRef -> do
      Pool.withResource appPool $ \conn -> do
        Job{jobId} <- Job.createJob conn tname (PayloadSucceed 0 (Just "abcdef"))
        delaySeconds Job.defaultPollingInterval
        delaySeconds $ calculateRetryDelay testMaxAttempts + Seconds 3
        Job{jobAttempts, jobStatus, jobResult} <- ensureJobId conn tname jobId
        assertEqual "Exepcting job to be in Success status" Job.Success jobStatus
        assertEqual "Expecting job to have the correct result" (Just (Aeson.String "abcdef")) jobResult

testJobFailure appPool jobPool = testCase "job failure" $ do
  withNewJobMonitor jobPool $ \tname _logRef -> do
    Pool.withResource appPool $ \conn -> do
      Job{jobId} <- Job.createJob conn tname (PayloadAlwaysFail 0)
      delaySeconds $ calculateRetryDelay testMaxAttempts + Seconds 3
      Job{jobAttempts, jobStatus} <- ensureJobId conn tname jobId
      assertEqual "Exepcting job to be in Failed status" Job.Failed jobStatus
      assertEqual ("Expecting job attempts to be 3. Found " <> show jobAttempts)  3 jobAttempts

testPushFailedJobEndQueue ::  Pool Connection -> TestTree
testPushFailedJobEndQueue jobPool = testCase "testPushFailedJobEndQueue" $ do
  withRandomTable jobPool $ \tname -> do
    Pool.withResource jobPool $ \conn -> do
      job1 <- Job.createJob conn tname (PayloadAlwaysFail 0)
      job2 <- Job.createJob conn tname (PayloadAlwaysFail 0)
      _ <- Job.saveJobIO conn tname (job1 {jobAttempts = 1})
      [Only resId] <- Job.jobPollingIO conn "testPushFailedJobEndQueue" tname 5
      assertEqual
        "Expecting the current job to be 2 since job 1 has been modified"
        (jobId job2) resId

testOnJobFailed appPool jobPool = testCase "job error handler" $ do
  withRandomTable jobPool $ \tname -> do
    Pool.withResource appPool $ \conn -> do
      _ <- Job.createJob conn tname (PayloadThrowStringException "forced exception")
      _ <- Job.createJob conn tname PayloadThrowDivideByZero
      mvar1 <- newMVar False
      mvar2 <- newMVar False
      let addErrHandlers cfg = cfg { Job.cfgOnJobFailed = [ Job.JobErrHandler errHandler2
                                                          , Job.JobErrHandler errHandler1
                                                          ]
                                   }
          errHandler1 (_ :: StringException) _ _ = swapMVar mvar1 True
          errHandler2 (_ :: ArithException) _ _ = swapMVar mvar2 True
      withNamedJobMonitor tname jobPool addErrHandlers $ \_logRef -> do
        delaySeconds (Job.defaultPollingInterval * 3)
      readMVar mvar1 >>= assertEqual "Error Handler 1 doesn't seem to have run" True
      readMVar mvar2 >>= assertEqual "Error Handler 2 doesn't seem to have run" True

testOnJobStart appPool jobPool = testCase "onJobStart" $ do
  withRandomTable jobPool $ \tname -> do
    callbackRef <- newIORef False
    let callback = const $ modifyIORef' callbackRef (const True)
    withNamedJobMonitor tname jobPool (\cfg -> cfg{Job.cfgOnJobStart=callback}) $ \_logRef -> do
      Pool.withResource appPool $ \conn -> do
        _ <- Job.createJob conn tname (PayloadSucceed 0 Nothing)
        delaySeconds (2 * Job.defaultPollingInterval)
        readIORef callbackRef >>= assertBool "It seems that onJobStart callback has not been called"

testOnJobSuccess appPool jobPool = testCase "onJobSuccess" $ do
  withRandomTable jobPool $ \tname -> do
    callbackRef <- newIORef False
    let callback = const $ modifyIORef' callbackRef (const True)
    withNamedJobMonitor tname jobPool (\cfg -> cfg{Job.cfgOnJobSuccess=callback}) $ \_logRef -> do
      Pool.withResource appPool $ \conn -> do
        _ <- Job.createJob conn tname (PayloadSucceed 0 Nothing)
        delaySeconds (2 * Job.defaultPollingInterval)
        readIORef callbackRef >>= assertBool "It seems that onJobSuccess callback has not been called"

testOnJobTimeout appPool jobPool = testCase "onJobTimeout" $ do
  withRandomTable jobPool $ \tname -> do
    callbackRef <- newIORef False
    let callback = const $ modifyIORef' callbackRef (const True)
    withNamedJobMonitor tname jobPool (\cfg -> cfg{Job.cfgOnJobTimeout=callback, Job.cfgDefaultJobTimeout=Seconds 2}) $ \_logRef -> do
      Pool.withResource appPool $ \conn -> do
        _ <- Job.createJob conn tname (PayloadSucceed 10 Nothing)
        delaySeconds (2 * Job.defaultPollingInterval)
        readIORef callbackRef >>= assertBool "It seems that onJobTimeout callback has not been called"

testRetryBackoff appPool jobPool = testCase "retry backoff" $ do
  withRandomTable jobPool $ \tname -> do
    withNamedJobMonitor tname jobPool modifyRetryBackoff $ \_logRef -> do
      Pool.withResource appPool $ \conn -> do
        jobQueueTime <- getCurrentTime
        Job{jobId} <- Job.createJob conn tname (PayloadAlwaysFail 0)
        delaySeconds (2 * Job.defaultPollingInterval)

        let assertBackedOff = do
              Job{jobAttempts, jobStatus, jobRunAt} <- ensureJobId conn tname jobId
              assertEqual "Exepcting job to be in Retry status" Job.Retry jobStatus
              assertBool "Expecting job runAt to be in the future"
                (jobRunAt >= addUTCTime (fromIntegral $ unSeconds $ backoff jobAttempts) jobQueueTime)

        assertBackedOff

        -- Run it for another attempt and check the backoff scales
        job <- ensureJobId conn tname jobId
        _ <- Job.saveJobIO conn tname (job {jobRunAt = jobQueueTime})
        delaySeconds (2 * Job.defaultPollingInterval)

        assertBackedOff
  where

    backoff attempts = Seconds $ 300 * attempts

    modifyRetryBackoff cfg
      = cfg { Job.cfgDefaultRetryBackoff = pure . backoff }

testResourceLimitedScheduling _appPool jobPool = testGroup "concurrency control by resources"
  [ testCase "resources control how jobs run" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]
          secondResource = [(Job.ResourceId "second", 1)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource
      Job{jobId = thirdJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) secondResource

      _jone <- runSingleJobFromQueue cfg
      _jtwo <- runSingleJobFromQueue cfg
      _jthree <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "Job was created first - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Job uses same resources as first - it shouldn't be running" Job.Queued secondJob
      assertJobIdStatus conn tname logRef "Job uses different resources - it should be running" Job.Locked thirdJob

  , testCase "resource-less jobs run normally" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) []
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) []

      _jone <- runSingleJobFromQueue cfg
      _jtwo <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "First job uses no resources - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Second job uses no resources - it should be running" Job.Locked secondJob

  , testCase "jobs can hold multiple resources" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]
          secondResource = [(Job.ResourceId "second", 1)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) (firstResource <> secondResource)
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource
      Job{jobId = thirdJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) secondResource

      jone <- runSingleJobFromQueue cfg
      jtwo <- runSingleJobFromQueue cfg
      jthree <- runSingleJobFromQueue cfg
      assertEqual "first job claimed the work" (void jone) (Just ())
      assertEqual "second thread was blocked" (void jtwo) Nothing
      assertEqual "third thread was blocked " (void jthree) Nothing

      assertJobIdStatus conn tname logRef "Job was created first - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Job uses same resources as first - it shouldn't be running" Job.Queued secondJob
      assertJobIdStatus conn tname logRef "Job uses same resources as first - it shouldn't be running" Job.Queued thirdJob

  , testCase "resources are freed when jobs succeed" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource

      jone <- runSingleJobFromQueue cfg
      _jtwo <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "Job was created first - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Job uses same resources as first - it shouldn't be running" Job.Queued secondJob

      forM_ jone wait

      _three <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "First job should have succeeded by now" Job.Success firstJob
      assertJobIdStatus conn tname logRef "Second job should be running after first job suceeded" Job.Locked secondJob

  , testCase "resources are freed when jobs fail" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadAlwaysFail 10) firstResource
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource

      jone <- runSingleJobFromQueue cfg
      _jtwo <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "Job was created first - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Job uses same resources as first - it shouldn't be running" Job.Queued secondJob

      jobRes <- forM jone waitCatch
      assertEqual "should've thrown an exception" (Just (Left ())) (first (const ()) <$> jobRes)

      _three <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "First job should have failed by now" Job.Failed firstJob
      assertJobIdStatus conn tname logRef "Second job should be running after first job failed" Job.Locked secondJob

  , testCase "resources allow for multiple usage" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]
          resCfg' = resCfg { Job.resCfgDefaultLimit = 2 }

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg' (PayloadSucceed 10 Nothing) firstResource
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg' (PayloadSucceed 10 Nothing) firstResource

      _jone <- runSingleJobFromQueue cfg
      _jtwo <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "First job uses some resources - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Second job uses some resources - it should be running" Job.Locked secondJob

  , testCase "resource usage can differ by job" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource n = [(Job.ResourceId "first", n)]
          resCfg' = resCfg { Job.resCfgDefaultLimit = 5 }

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg' (PayloadSucceed 10 Nothing) (firstResource 4)
      Job{jobId = secondJob} <- Job.createJobWithResources conn tname resCfg' (PayloadSucceed 10 Nothing) (firstResource 3)
      Job{jobId = thirdJob} <- Job.createJobWithResources conn tname resCfg' (PayloadSucceed 10 Nothing) (firstResource 2)

      jone <- runSingleJobFromQueue cfg
      _jtwo <- runSingleJobFromQueue cfg
      _jthree <- runSingleJobFromQueue cfg

      -- TODO, so it's not locked but already succeeded, which is werid, but the rest is valid?
      assertJobIdStatus conn tname logRef "Job was created first - it should be running" Job.Locked firstJob
      assertJobIdStatus conn tname logRef "Job doesn't fit alongside first job - it shouldn't be running" Job.Queued secondJob
      assertJobIdStatus conn tname logRef "Job doesn't fit alongside first job - it shouldn't be running" Job.Queued thirdJob

      forM_ jone wait

      _jfour <- runSingleJobFromQueue cfg
      _jfive <- runSingleJobFromQueue cfg

      assertJobIdStatus conn tname logRef "First job should have succeeded by now" Job.Success firstJob
      assertJobIdStatus conn tname logRef "Second job should be running after first job suceeded" Job.Locked secondJob
      assertJobIdStatus conn tname logRef "Third job fits alongside second job - it should be running after first job suceeded" Job.Locked thirdJob

  , testCase "resource limits permanently block too-big jobs" $ setup jobPool $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 2)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource

      void $ runSingleJobFromQueueBlock cfg

      assertJobIdStatus conn tname logRef "Job requires too many resources - shouldn't be running" Job.Queued firstJob

  , testCase "resources with 0 limit block jobs" $ setup' jobPool 0 $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource

      void $ runSingleJobFromQueueBlock cfg

      assertJobIdStatus conn tname logRef "Job can't access 0-limited resources - shouldn't be running" Job.Queued firstJob

  , testCase "resources with negative limit block jobs" $ setup' jobPool (-5) $ \tname resCfg logRef conn cfg -> do
      let firstResource = [(Job.ResourceId "first", 1)]

      Job{jobId = firstJob} <- Job.createJobWithResources conn tname resCfg (PayloadSucceed 10 Nothing) firstResource

      void $ runSingleJobFromQueueBlock cfg

      assertJobIdStatus conn tname logRef "Job can't access negative-limited resources - shouldn't be running" Job.Queued firstJob
  ]

setup jobPool = setup' jobPool  1

setup' jobPool defaultLimit action =
  withRandomTable jobPool $ \tname ->
    withRandomResourceTables defaultLimit jobPool tname $ \resCfg ->
      withConfig tname jobPool (cfgFn resCfg) $ \logRef cfg -> do
        Pool.withResource jobPool $ \conn -> action tname resCfg logRef conn cfg

  where
    cfgFn resCfg cfg = cfg
        { Job.cfgDefaultMaxAttempts = 1 -- Simplifies some tests where we have a failing job
        , Job.cfgConcurrencyControl = Job.ResourceLimits resCfg
        , Job.cfgImmediateJobDeletion = const $ pure False
        }

testKillJob appPool jobPool = testCase "killing a ongoing job" $ do
  withRandomTable jobPool $ \tname -> withNamedJobMonitor tname jobPool Prelude.id $ \logRef -> do
    Pool.withResource appPool $ \conn -> do
      Job{jobId = jid} <- Job.createJob conn tname (PayloadSucceed 60 Nothing)
      delaySeconds $ Job.defaultPollingInterval + Seconds 2

      assertJobIdStatus conn tname logRef "Job should be running" Job.Locked jid

      _ <- Job.killJobIO conn tname jid
      delaySeconds $ Job.defaultPollingInterval + Seconds 2

      assertJobIdStatus conn tname logRef "Job is cancelled and the job thread should be killed" Job.Cancelled jid

data JobEvent = JobStart
              | JobRetry
              | JobSuccess
              | JobFailed
              deriving (Eq, Show)

-- testEverything appPool jobPool = testProperty "test everything" $ property $ do

--   jobPayloads <- forAll $ Gen.list (Range.linear 300 1000) payloadGen
--   jobsMVar <- liftIO $ newMVar (Map.empty :: Map.IntMap [(JobEvent, Job.Job)])

--   let maxDelay = sum $ map (payloadDelay jobPollingInterval) jobPayloads
--       completeRun = ((unSeconds maxDelay) `div` concurrencyFactor) + (2 * (unSeconds testPollingInterval))

--   shutdownAfter <- forAll $ Gen.choice
--     [ pure $ Seconds completeRun -- Either we allow all jobs to be completed properly
--     , (Seconds <$> (Gen.int $ Range.linear 1 completeRun)) -- Or, we shutdown early after a random number of seconds
--     ]

--   test $ withRandomTable jobPool $ \tname -> do
--     (defaults, cleanup) <- liftIO $ Test.defaultJobMonitor tname jobPool
--     let jobMonitorSettings = defaults { Job.monitorJobRunner = jobRunner
--                                       , Job.monitorTableName = tname
--                                       , Job.monitorOnJobStart = onJobEvent JobStart jobsMVar
--                                       , Job.monitorOnJobFailed = onJobEvent JobRetry jobsMVar
--                                       , Job.monitorOnJobPermanentlyFailed = onJobEvent JobFailed jobsMVar
--                                       , Job.monitorOnJobSuccess = onJobEvent JobSuccess jobsMVar
--                                       , Job.monitorDefaultMaxAttempts = 3
--                                       , Job.monitorPollingInterval = jobPollingInterval
--                                       }

--     (jobs :: [Job]) <- withAsync
--             (liftIO $ Job.runJobMonitor jobMonitorSettings)
--             (const $ finally
--               (liftIO $ actualTest shutdownAfter jobPayloads tname jobsMVar)
--               (liftIO cleanup))

--     jobAudit <- takeMVar jobsMVar

--     [(Only (lockedJobCount :: Int))] <- liftIO $ Pool.withResource appPool $ \conn ->
--       PGS.query conn ("SELECT coalesce(count(id), 0) FROM " <>  tname <> " where status=?") (Only Job.Locked)

--     -- ALL jobs should show up in the audit, which means they should have
--     -- been attempted /at least/ once
--     (DL.sort $ map jobId jobs) === (DL.sort $ Map.keys jobAudit)

--     -- No job should be in a locked state
--     0 === lockedJobCount

--     -- No job should've been simultaneously picked-up by more than one
--     -- worker
--     True === (Map.foldl (\m js -> m && noRaceCondition js) True jobAudit)

--     liftIO $ print $ "Test passed with job-count = " <> show (length jobPayloads)

--   where

--     testPollingInterval = 5
--     concurrencyFactor = 5

--     noRaceCondition js = DL.foldl (&&) True $ DL.zipWith (\(x,_) (y,_) -> not $ x==JobStart && y==JobStart) js (tail js)

--     jobPollingInterval = Seconds 2

--     onJobEvent evt jobsMVar job@Job{jobId} = void $ modifyMVar_ jobsMVar $ \jobMap -> do
--       pure $ Map.insertWith (++) jobId [(evt, job)] jobMap

--     actualTest :: Seconds -> [JobPayload] -> Job.TableName -> MVar (Map.IntMap [(JobEvent, Job.Job)]) -> IO [Job]
--     actualTest shutdownAfter jobPayloads tname jobsMVar = do
--       jobs <- forConcurrently jobPayloads $ \pload ->
--         Pool.withResource appPool $ \conn ->
--         liftIO $ Job.createJob conn tname pload
--       let poller nextAction = case nextAction of
--             Left s -> pure $ Left s
--             Right remaining ->
--               if remaining == Seconds 0
--               then pure (Right $ Seconds 0)
--               else do delaySeconds testPollingInterval
--                       print $ "------- Polling (remaining = " <> show (unSeconds remaining) <> " sec)------"
--                       x <- withMVar jobsMVar $ \jobMap ->
--                         if (Map.foldl (\m js -> m && (isJobTerminalState js)) True jobMap)
--                         then pure (Right $ Seconds 0)
--                         else if remaining < testPollingInterval
--                              then pure (Left $ "Timeout. Job count=" <> show (length jobPayloads) <> " shutdownAfter=" <> show shutdownAfter)
--                              else pure $ Right (remaining - testPollingInterval)
--                       poller x

--       poller (Right shutdownAfter) >>= \case
--         Left s -> pure jobs -- Prelude.error s
--         Right _ -> pure jobs

--     isJobTerminalState js = case js of
--       [] -> False
--       (_, j):_ -> case (Job.jobStatus j) of
--         Job.Failed -> True
--         Job.Success -> True
--         _ -> False



payloadDelay :: Seconds -> JobPayload -> Seconds
payloadDelay jobPollingInterval = payloadDelay_ (Seconds 0)
  where
    payloadDelay_ total p =
      let defaultDelay x = total + x + jobPollingInterval
      in case p of
        PayloadAlwaysFail x -> defaultDelay x
        PayloadSucceed x _ -> defaultDelay x
        PayloadFail x ip -> payloadDelay_ (defaultDelay x) ip
        PayloadThrowStringException _ -> defaultDelay 0
        PayloadThrowDivideByZero -> defaultDelay 0

deriving instance Enum Time.Unit
deriving instance Enum Time.Direction

timeGen :: MonadGen m => UTCTime -> Time.Direction -> m UTCTime
timeGen t d = do
  u <- Gen.enum Time.Seconds Time.Fortnights
  -- d <- Gen.element $ enumFrom Time.Ago
  i <- Gen.integral (Range.constant 0 10)
  pure $ Time.timeSince t (fromInteger i) u d

anyTimeGen :: MonadGen m => UTCTime -> m UTCTime
anyTimeGen t = Gen.element (enumFrom Time.Ago) >>= timeGen t

futureTimeGen :: MonadGen m => UTCTime -> m UTCTime
futureTimeGen t = timeGen t Time.FromThat

pastTimeGen :: MonadGen m => UTCTime -> m UTCTime
pastTimeGen t = timeGen t Time.Ago

jobGen :: (MonadGen m) => UTCTime -> m (UTCTime, UTCTime, UTCTime, Job.Status, Aeson.Value, Int, Maybe UTCTime, Maybe T.Text)
jobGen t = do
  createdAt <- anyTimeGen t
  updatedAt <- futureTimeGen createdAt
  runAt <- futureTimeGen createdAt
  status <- Gen.element $ enumFrom Job.Success
  pload <- payloadGen
  attempts <- Gen.int (Range.constant 0 10)
  (lockedAt, lockedBy) <- case status of
    Job.Locked -> do
      x <- futureTimeGen createdAt
      y <- Gen.text (Range.constant 1 100) Gen.ascii
      pure (Just x, Just y)
    _ -> pure (Nothing, Nothing)
  pure (createdAt, updatedAt, runAt, status, toJSON pload, attempts, lockedAt, lockedBy)

-- propFilterJobs appPool jobPool = testProperty "filter jobs" $ property $ do
--   t <- liftIO getCurrentTime
--   jobs <- forAll $ Gen.list (Range.linear 1 1000) (jobGen t)
--   f <- forAll $ genFilter t
--   test $ withRandomTable jobPool $ \tname -> do
--     (savedJobs, dbJobs) <- liftIO $ do
--       savedJobs <- Pool.withResource appPool $ \conn -> forM jobs $ \j -> (PGS.query conn (qry tname) j) >>= (pure . Prelude.head)
--       (jm, cleanup) <- liftIO $ Test.defaultJobMonitor tname jobPool
--       dbJobs <- (flip finally) cleanup $ (flip runReaderT) jm $ Web.filterJobs f
--       pure (savedJobs, dbJobs)
--     let testJobs = Test.filterJobs f savedJobs
--     -- (sortBy (comparing jobId) dbJobs) === (sortBy (comparing jobId) testJobs)
--     dbJobs === testJobs
--   where
--     qry tname = "INSERT INTO " <> tname <> " (created_at, updated_at, run_at, status, payload, attempts, locked_at, locked_by) values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING " <> Job.concatJobDbColumns


genFilter :: MonadGen m => UTCTime -> m Web.Filter
genFilter t = do
  statuses <- Gen.list (Range.constant 0 2) (Gen.element $ enumFrom Job.Success)
  createdAfter <- Gen.maybe (anyTimeGen t)
  createdBefore <- case createdAfter of
    Nothing -> Gen.maybe (anyTimeGen t)
    Just x -> Gen.maybe (futureTimeGen x)
  updatedAfter  <- Gen.maybe (anyTimeGen t)
  updatedBefore  <- case updatedAfter of
    Nothing -> Gen.maybe (anyTimeGen t)
    Just x -> Gen.maybe (futureTimeGen x)
  orderClause <- Gen.maybe ((,) <$> Gen.element (enumFrom Web.OrdCreatedAt) <*> Gen.element (enumFrom Web.Asc))
  limitOffset <- Gen.maybe ((,) <$> Gen.int (Range.constant 5 10) <*> Gen.int (Range.constant 0 30))
  runAfter <- Gen.maybe (futureTimeGen t)
  pure Web.blankFilter
    { filterStatuses = statuses
    , filterCreatedAfter = createdAfter
    , filterCreatedBefore = createdBefore
    , filterUpdatedAfter = updatedAfter
    , filterUpdatedBefore = updatedBefore
    , filterOrder = orderClause
    , filterPage = limitOffset
    , filterRunAfter = runAfter
    }


filterJobs :: Filter -> [Job] -> [Job]
filterJobs Web.Filter{filterStatuses, filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore, filterOrder, filterPage, filterRunAfter} js =
  applyLimitOffset $
  applyOrdering (fromMaybe (Web.OrdUpdatedAt, Web.Desc) filterOrder) $
  flip DL.filter js $ \j -> filterByStatus j &&
                              filterByCreatedAfter j &&
                              filterByCreatedBefore j &&
                              filterByUpdatedAfter j &&
                              filterByUpdatedBefore j &&
                              filterByRunAfter j
  where
    applyLimitOffset = maybe Prelude.id (\(l, o) -> Prelude.take l . Prelude.drop o) filterPage

    applyOrdering (fld, dir) lst =
      let comparer = resultOrder $ case fld of
            Web.OrdCreatedAt -> comparing jobCreatedAt
            Web.OrdUpdatedAt -> comparing jobUpdatedAt
            Web.OrdLockedAt -> comparing jobLockedAt
            Web.OrdStatus -> comparing jobStatus
            Web.OrdJobType -> comparing Job.defaultJobType
          resultOrder fn x y = case fn x y of
            EQ -> compare (Down $ jobId x) (Down $ jobId y)
            LT -> case dir of
              Web.Asc -> LT
              Web.Desc -> GT
            GT -> case dir of
              Web.Asc -> GT
              Web.Desc -> LT
      in sortBy comparer lst

    filterByStatus Job.Job{jobStatus} = Prelude.null filterStatuses
                                        || (jobStatus `elem` filterStatuses)
    filterByCreatedAfter Job.Job{jobCreatedAt} = maybe True (<= jobCreatedAt) filterCreatedAfter
    filterByCreatedBefore Job.Job{jobCreatedAt} = maybe True (> jobCreatedAt) filterCreatedBefore
    filterByUpdatedAfter Job.Job{jobUpdatedAt} = maybe True (<= jobUpdatedAt) filterUpdatedAfter
    filterByUpdatedBefore Job.Job{jobUpdatedAt} = maybe True (> jobUpdatedAt) filterUpdatedBefore
    filterByRunAfter Job.Job{jobRunAt} = maybe True (< jobRunAt) filterRunAfter

-- defaultJobMonitor :: Job.TableName
--                   -> Pool Connection
--                   -> (Job -> IO ())
--                   -> IO (Job.JobMonitor, IO ())
-- defaultJobMonitor tname pool = do
--   tcache <- newTimeCache simpleTimeFormat'
--   (tlogger, cleanup) <- newTimedFastLogger tcache LogNone
--   let flogger loc lsource llevel lstr = tlogger $ \t -> toLogStr t <> " | " <> defaultLogStr loc lsource llevel lstr
--   Job.mkConfig _loggingFn tname pool Job.UnlimitedConcurrentJobs (pure ())
--   pure ( Job.defaultJobMonitor flogger tname pool
--        , cleanup
--        )

calculateRetryDelay :: Int -> Seconds
calculateRetryDelay n = Seconds $ foldl' (\s x -> s + (2 ^ x)) 0 [1..n]
