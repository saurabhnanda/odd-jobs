{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, NamedFieldPuns, DeriveGeneric, FlexibleContexts, TypeFamilies, StandaloneDeriving #-}
module Test where

import           Control.Monad.Logger
import           Control.Monad.Reader
import           Data.Aeson as Aeson
import           Data.Aeson.TH as Aeson
import           Data.Functor (void)
import qualified Data.IntMap.Strict as Map
import           Data.List as DL
import           Data.Maybe (fromMaybe)
import           Data.Ord (comparing, Down(..))
import           Data.Pool as Pool
import           Data.String (fromString)
import           Data.String.Conv (toS)
import qualified Data.Text as T
import           Data.Time
import qualified Data.Time.Convenience as Time
import           Database.PostgreSQL.Simple as PGS
import           GHC.Generics
import           System.Log.FastLogger ( fromLogStr, withFastLogger, LogType'(..)
                   , defaultBufSize, FastLogger, FileLogSpec(..), newTimedFastLogger
                   , withTimedFastLogger)
import           System.Log.FastLogger.Date (newTimeCache, simpleTimeFormat')
import qualified System.Random as R
import           UnliftIO

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import           Test.Tasty as Tasty
import           Test.Tasty.Hedgehog
import           Test.Tasty.HUnit

import qualified OddJobs.ConfigBuilder as Job
import           OddJobs.Job (Job(..), JobId, delaySeconds, Seconds(..))
import qualified OddJobs.Job as Job
import qualified OddJobs.Migrations as Migrations
import qualified OddJobs.Types as Job
import           OddJobs.Web as Web

$(Aeson.deriveJSON Aeson.defaultOptions ''Seconds)

main :: IO ()
main = do
  bracket createAppPool destroyAllResources $ \appPool -> do
    bracket createJobPool destroyAllResources $ \jobPool -> do
      defaultMain $ tests appPool jobPool
  where
    connInfo = ConnectInfo
                 { connectHost = "localhost"
                 , connectPort = fromIntegral (5432 :: Int)
                 , connectUser = "jobs_test"
                 , connectPassword = "jobs_test"
                 , connectDatabase = "jobs_test"
                 }

    createAppPool = createPool
      (PGS.connect connInfo)  -- cretea a new resource
      (PGS.close)             -- destroy resource
      1                       -- stripes
      (fromRational 10)       -- number of seconds unused resources are kept around
      45
    createJobPool = createPool
      (PGS.connect connInfo)  -- cretea a new resource
      (PGS.close)             -- destroy resource
      1                       -- stripes
      (fromRational 10)       -- number of seconds unused resources are kept around
      45

tests appPool jobPool = testGroup "All tests"
  [
    testGroup "simple tests" [ testJobCreation appPool jobPool
                             , testJobScheduling appPool jobPool
                             , testJobFailure appPool jobPool
                             , testEnsureShutdown appPool jobPool
                             , testGracefulShutdown appPool jobPool
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

jobRunner :: Job.Job -> IO ()
jobRunner Job{jobPayload, jobAttempts} = case (fromJSON jobPayload) of
  Aeson.Error e -> error e
  Aeson.Success (j :: JobPayload) ->
    let recur pload idx = case pload of
          PayloadAlwaysFail delay -> (delaySeconds delay) >> (error $ "Forced error after " <> show delay <> " seconds")
          PayloadSucceed delay -> (delaySeconds delay) >> pure ()
          PayloadFail delay innerpload -> if idx<jobAttempts
                                          then recur innerpload (idx + 1)
                                          else (delaySeconds delay) >> (error $ "Forced error after " <> show delay <> " seconds. step=" <> show idx)
    in recur j 0

data JobPayload = PayloadSucceed Seconds
                | PayloadFail Seconds JobPayload
                | PayloadAlwaysFail Seconds
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
  Job.LogPoll -> Nothing
  Job.LogWebUIRequest -> Nothing
  Job.LogText _ -> Nothing

assertJobIdStatus :: (HasCallStack)
                  => Connection
                  -> Job.TableNames
                  -> IORef [Job.LogEvent]
                  -> String
                  -> Job.Status
                  -> JobId
                  -> Assertion
assertJobIdStatus conn tnames logRef msg st jid = do
  logs <- readIORef logRef
  let mjid = Just jid
  case st of
    Job.Success ->
      assertBool (msg <> ": Success event not found in job-logs for JobId=" <> show jid) $
      (flip DL.any) logs $ \logEvent -> case logEvent of
                                          Job.LogJobSuccess j _ -> jid == Job.jobId j
                                          _ -> False
    Job.Queued ->
      assertBool (msg <> ": Not expecting to find a queued job in the the job-logs JobId=" <> show jid) $
      not $ (flip DL.any) logs $ \logEvent -> case logEvent of
                                           Job.LogJobStart j -> jid == Job.jobId j
                                           Job.LogJobSuccess j _ -> jid == Job.jobId j
                                           Job.LogJobFailed j _ _ _ -> jid == Job.jobId j
                                           Job.LogJobTimeout j -> jid == Job.jobId j
                                           Job.LogWebUIRequest -> False
                                           Job.LogText _ -> False
    Job.Failed ->
      assertBool (msg <> ": Failed event not found in job-logs for JobId=" <> show jid) $
      (flip DL.any) logs $ \logEvent -> case logEvent of
                                          Job.LogJobFailed j _ _ _ -> jid == Job.jobId j
                                          _ -> False

    Job.Retry ->
      assertBool (msg <> ": Failed event not found in job-logs for JobId=" <> show jid) $
      (flip DL.any) logs $ \logEvent -> case logEvent of
                                          Job.LogJobFailed j _ _ _ -> jid == Job.jobId j
                                          _ -> False

    Job.Locked ->
      assertBool (msg <> ": Start event should be present in the log for a locked job JobId=" <> show jid) $
      (flip DL.any) logs $ \logEvent -> case logEvent of
                                          Job.LogJobStart j -> jid == Job.jobId j
                                          _ -> False

  when (st /= Job.Success) $ do
    Job.findJobByIdIO conn tnames jid >>= \case
      Nothing -> assertFailure $ "Not expecting job to be deleted. JobId=" <> show jid
      Just (Job{jobStatus}) -> assertEqual msg st jobStatus

ensureJobId :: (HasCallStack) => Connection -> Job.TableNames -> JobId -> IO Job
ensureJobId conn tnames jid = Job.findJobByIdIO conn tnames jid >>= \case
  Nothing -> error $ "Not expecting job to be deleted. JobId=" <> show jid
  Just j -> pure j

withRandomTable :: Pool Connection -> (Job.TableNames -> IO a) -> IO a
withRandomTable jobPool action = do
  tnames <- Job.simpleTableNames . ("jobs_" <>) . fromString <$> liftIO (replicateM 10 $ R.randomRIO ('a', 'z'))
  finally
    ((Pool.withResource jobPool $ \conn -> (liftIO $ Migrations.createJobTables conn tnames)) >> (action tnames))
    (Pool.withResource jobPool $ \conn -> liftIO $ void $ Migrations.dropJobTables conn tnames)

withNewJobMonitor :: Pool Connection -> (Job.TableNames -> IORef [Job.LogEvent] -> IO a) -> IO a
withNewJobMonitor jobPool actualTest = do
  withRandomTable jobPool $ \tnames -> do
    withNamedJobMonitor tnames jobPool (actualTest tnames)

withNamedJobMonitor :: Job.TableNames -> Pool Connection -> (IORef [Job.LogEvent] -> IO a) -> IO a
withNamedJobMonitor tnames jobPool actualTest = do
  logRef :: IORef [Job.LogEvent] <- newIORef []
  tcache <- newTimeCache simpleTimeFormat'
  withTimedFastLogger tcache LogNone $ \tlogger -> do
    let flogger logLevel logEvent = do
          tlogger $ \t -> toLogStr t <> " | " <> (Job.defaultLogStr Job.defaultJobType logLevel logEvent)
          atomicModifyIORef' logRef (\logs -> (logEvent:logs, ()))
        cfg = Job.mkResourceConfig flogger tnames jobPool Job.UnlimitedConcurrentJobs jobRunner (\cfg -> cfg{Job.cfgDefaultMaxAttempts=3})
    withAsync (Job.startJobRunner cfg) (const $ actualTest logRef)

payloadGen :: MonadGen m => m JobPayload
payloadGen = Gen.recursive  Gen.choice nonRecursive recursive
  where
    nonRecursive = [ PayloadAlwaysFail <$> Gen.element [1, 2, 3]
                   , PayloadSucceed <$> Gen.element [1, 2, 3]]
    recursive = [ PayloadFail <$> (Gen.element [1, 2, 3]) <*> payloadGen ]

testJobCreation appPool jobPool = testCase "job creation" $ do
  withNewJobMonitor jobPool $ \tnames logRef -> do
    Pool.withResource appPool $ \conn -> do
      Job{jobId} <- Job.createJob conn tnames (PayloadSucceed 0)
      delaySeconds $ Seconds 6
      assertJobIdStatus conn tnames logRef "Expecting job to be successful by now" Job.Success jobId

testEnsureShutdown appPool jobPool = testCase "ensure shutdown" $ do
  withRandomTable jobPool $ \tnames -> do
    (jid, logRef) <- scheduleJob tnames
    delaySeconds (2 * Job.defaultPollingInterval)
    Pool.withResource appPool $ \conn -> do
      assertJobIdStatus conn tnames logRef "Job should still be in queued state if job-monitor is no longer running" Job.Queued jid
  where
    scheduleJob tnames = withNamedJobMonitor tnames jobPool $ \logRef -> do
      t <- getCurrentTime
      Pool.withResource appPool $ \conn -> do
        Job{jobId} <- Job.scheduleJob conn tnames (PayloadSucceed 0) (addUTCTime (fromIntegral (2 * (unSeconds Job.defaultPollingInterval))) t)
        assertJobIdStatus conn tnames logRef "Job is scheduled in future, should still be queueud" Job.Queued jobId
        pure (jobId, logRef)

testGracefulShutdown appPool jobPool = testCase "ensure graceful shutdown" $ do
  withRandomTable jobPool $ \tnames -> do
    (j1, j2, logRef) <- withNamedJobMonitor tnames jobPool $ \logRef -> do
      Pool.withResource appPool $ \conn -> do
        t <- getCurrentTime
        j1 <- Job.createJob conn tnames (PayloadSucceed $ 2 * Job.defaultPollingInterval)
        j2 <- Job.scheduleJob conn tnames (PayloadSucceed 0) (addUTCTime (fromIntegral $ unSeconds $ Job.defaultPollingInterval) t)
        pure (j1, j2, logRef)
    Pool.withResource appPool $ \conn -> do
      delaySeconds 1
      assertJobIdStatus conn tnames logRef "Expecting the first job to be in locked state because it should be running" Job.Locked (jobId j1)
      assertJobIdStatus conn tnames logRef "Expecting the second job to be queued because no new job should be picked up during graceful shutdown" Job.Queued (jobId j2)
      delaySeconds $ 3 * Job.defaultPollingInterval
      assertJobIdStatus conn tnames logRef "Expecting the first job to be completed successfully if graceful shutdown is implemented correctly" Job.Success (jobId j1)
      assertJobIdStatus conn tnames logRef "Expecting the second job to be queued because no new job should be picked up during graceful shutdown" Job.Queued (jobId j2)

    pure ()

testJobScheduling appPool jobPool = testCase "job scheduling" $ do
  withNewJobMonitor jobPool $ \tnames logRef -> do
    Pool.withResource appPool $ \conn -> do
      t <- getCurrentTime
      job@Job{jobId} <- Job.scheduleJob conn tnames (PayloadSucceed 0) (addUTCTime (fromIntegral 3600) t)
      delaySeconds $ Seconds 2
      assertJobIdStatus conn tnames logRef "Job is scheduled in the future. It should NOT have been successful by now" Job.Queued jobId
      j <- Job.saveJobIO conn tnames job{jobRunAt = (addUTCTime (fromIntegral (-1)) t)}
      delaySeconds (Job.defaultPollingInterval + (Seconds 2))
      assertJobIdStatus conn tnames logRef "Job had a runAt date in the past. It should have been successful by now" Job.Success jobId

testJobFailure appPool jobPool = testCase "job retry" $ do
  withNewJobMonitor jobPool $ \tnames logRef -> do
    Pool.withResource appPool $ \conn -> do
      Job{jobId} <- Job.createJob conn tnames (PayloadAlwaysFail 0)
      delaySeconds $ Seconds 15
      Job{jobAttempts, jobStatus} <- ensureJobId conn tnames jobId
      assertEqual "Exepcting job to be in Failed status" Job.Failed jobStatus
      assertEqual ("Expecting job attempts to be 3. Found " <> show jobAttempts)  3 jobAttempts

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
payloadDelay jobPollingInterval pload = payloadDelay_ (Seconds 0) pload
  where
    payloadDelay_ total p = case p of
      PayloadAlwaysFail x -> total + x + jobPollingInterval
      PayloadSucceed x -> total + x + jobPollingInterval
      PayloadFail x ip -> payloadDelay_ (total + x + jobPollingInterval) ip



-- TODO: test to ensure that errors in callback do not affect the running of jobs

deriving instance Enum Time.Unit
deriving instance Enum Time.Direction

timeGen :: MonadGen m => UTCTime -> Time.Direction -> m UTCTime
timeGen t d = do
  u <- Gen.enum Time.Seconds Time.Fortnights
  -- d <- Gen.element $ enumFrom Time.Ago
  i <- Gen.integral (Range.constant 0 10)
  pure $ Time.timeSince t (fromInteger i) u d

anyTimeGen :: MonadGen m => UTCTime -> m UTCTime
anyTimeGen t = (Gen.element (enumFrom Time.Ago)) >>= (timeGen t)

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
  orderClause <- Gen.maybe ((,) <$> (Gen.element $ enumFrom Web.OrdCreatedAt) <*> (Gen.element $ enumFrom Web.Asc))
  limitOffset <- Gen.maybe ((,) <$> (Gen.int (Range.constant 5 10)) <*> (Gen.int (Range.constant 0 30)))
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
  (flip DL.filter) js $ \j -> (filterByStatus j) &&
                              (filterByCreatedAfter j) &&
                              (filterByCreatedBefore j) &&
                              (filterByUpdatedAfter j) &&
                              (filterByUpdatedBefore j) &&
                              (filterByRunAfter j)
  where
    applyLimitOffset = maybe Prelude.id (\(l, o) -> (Prelude.take l). (Prelude.drop o)) filterPage

    applyOrdering (fld, dir) lst =
      let comparer = resultOrder $ case fld of
            Web.OrdCreatedAt -> (comparing jobCreatedAt)
            Web.OrdUpdatedAt -> (comparing jobUpdatedAt)
            Web.OrdLockedAt -> (comparing jobLockedAt)
            Web.OrdStatus -> (comparing jobStatus)
            Web.OrdJobType -> comparing Job.defaultJobType
          resultOrder fn = \x y -> case fn x y of
            EQ -> compare (Down $ jobId x) (Down $ jobId y)
            LT -> case dir of
              Web.Asc -> LT
              Web.Desc -> GT
            GT -> case dir of
              Web.Asc -> GT
              Web.Desc -> LT
      in sortBy comparer lst

    filterByStatus Job.Job{jobStatus} = if Prelude.null filterStatuses
                                        then True
                                        else jobStatus `elem` filterStatuses
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
