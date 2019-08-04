{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, NamedFieldPuns, DeriveGeneric, FlexibleContexts, TypeFamilies, StandaloneDeriving #-}
module Test where

import Test.Tasty as Tasty
import qualified PGQueue.Migrations as Migrations
import qualified PGQueue.Job as Job
import Database.PostgreSQL.Simple as PGS
import Data.Functor (void)
import Data.Pool as Pool
import Test.Tasty.HUnit
import Debug.Trace
import Control.Exception.Lifted (finally, catch)
import Control.Monad.Logger
import Control.Monad.Reader
import Data.Aeson as Aeson
import Control.Concurrent.Lifted
import Control.Concurrent.Async.Lifted
import PGQueue.Job (Job(..), JobId)
import System.Log.FastLogger (fromLogStr, withFastLogger, LogType(..), defaultBufSize, FastLogger, FileLogSpec(..))
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
import Control.Monad.Trans.Control (liftWith, restoreT)
import Control.Monad.Morph (hoist)
import Data.List as DL
import PGQueue.Web as Web
import Data.Time.Convenience as Time
import qualified Data.Text as T
import Data.Ord (comparing, Down(..))
import Data.Maybe (fromMaybe)
import qualified Data.HashMap.Strict as HM

main :: IO ()
main = do
  let connInfo = ConnectInfo
                 { connectHost = "localhost"
                 , connectPort = fromIntegral 5432
                 , connectUser = "jobs_test"
                 , connectPassword = "jobs_test"
                 , connectDatabase = "jobs_test"
                 }
  appPool <- createPool
    (PGS.connect connInfo)  -- cretea a new resource
    (PGS.close)             -- destroy resource
    1                       -- stripes
    (fromRational 10)       -- number of seconds unused resources are kept around
    45                     -- maximum open connections

  jobPool <- createPool
    (PGS.connect connInfo)  -- cretea a new resource
    (PGS.close)             -- destroy resource
    1                       -- stripes
    (fromRational 10)       -- number of seconds unused resources are kept around
    45                     -- maximum open connections

  defaultMain $ tests appPool jobPool

tests appPool jobPool = testGroup "All tests"
  [
    testGroup "simple tests" [testJobCreation appPool jobPool, testJobScheduling appPool jobPool, testJobFailure appPool jobPool, testEnsureShutdown appPool jobPool]
  , testGroup "property tests" [
      -- testEverything appPool jobPool,
        propFilterJobs appPool jobPool
      ]
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

instance Job.HasJobMonitor TestM where
  getDbPool = envDbPool <$> ask

  onJobRetry Job{jobId} = logInfoN $ "Retry JobId=" <> toS (show jobId)
  onJobSuccess Job{jobId} = logInfoN $ "Success JobId=" <> toS (show jobId)

instance {-# OVERLAPPING #-} MonadLogger TestM where
  monadLoggerLog _ _ _ msg = do
    flogger <- envLogger <$> ask
    liftIO $ flogger $ toLogStr msg <> toLogStr ("\n" :: String)

testPayload :: Value
testPayload = toJSON (10 :: Int)

jobRunner :: Job.Job -> IO ()
jobRunner Job{jobPayload, jobAttempts} = case (fromJSON jobPayload) of
  Aeson.Error e -> error e
  Success (j :: JobPayload) ->
    let recur pload idx = case pload of
          PayloadAlwaysFail delay -> (threadDelay delay) >> (error $ "Forced error after " <> show delay <> " seconds")
          PayloadSucceed delay -> (threadDelay delay) >> pure ()
          PayloadFail delay innerpload -> if idx<jobAttempts
                                          then recur innerpload (idx + 1)
                                          else (threadDelay delay) >> (error $ "Forced error after " <> show delay <> " seconds. step=" <> show idx)
    in recur j 0

data JobPayload = PayloadSucceed Int
                | PayloadFail Int JobPayload
                | PayloadAlwaysFail Int
                deriving (Eq, Show, Generic)

instance ToJSON JobPayload where
  toJSON = genericToJSON Aeson.defaultOptions

instance FromJSON JobPayload where
  parseJSON = genericParseJSON Aeson.defaultOptions


oneSec :: Int
oneSec = 1000000

assertJobIdStatus :: (HasCallStack) => Connection -> Job.TableName -> String -> Job.Status -> JobId -> Assertion
assertJobIdStatus conn tname msg st jid = Job.findJobById conn tname jid >>= \case
  Nothing -> assertFailure $ "Not expecting job to be deleted. JobId=" <> show jid
  Just (Job{jobStatus}) -> assertEqual msg st jobStatus

ensureJobId :: (HasCallStack) => Connection -> Job.TableName -> JobId -> IO Job
ensureJobId conn tname jid = Job.findJobById conn tname jid >>= \case
  Nothing -> error $ "Not expecting job to be deleted. JobId=" <> show jid
  Just j -> pure j

-- withRandomTable :: (MonadIO m) => Pool Connection -> (Job.TableName -> m a) -> m a
withRandomTable jobPool action = do
  (tname :: Job.TableName) <- liftIO ((("jobs_" <>) . fromString) <$> (replicateM 10 (R.randomRIO ('a', 'z'))))
  finally
    ((Pool.withResource jobPool $ \conn -> (liftIO $ Migrations.createJobTable conn tname)) >> (action tname))
    (Pool.withResource jobPool $ \conn -> liftIO $ void $ PGS.execute_ conn ("drop table if exists " <> tname <> ";"))

-- withNewJobMonitor :: (Pool Connection) -> (TableName -> Assertion) -> Assertion
withNewJobMonitor jobPool actualTest = withRandomTable jobPool $ \tname -> withNamedJobMonitor tname jobPool (actualTest tname)

withNamedJobMonitor tname jobPool actualTest = do
  (defaults, cleanup) <- (Job.defaultJobMonitor tname jobPool)
  let jobMonitorSettings = defaults{ Job.monitorJobRunner = jobRunner
                                   , Job.monitorTableName = tname
                                   , Job.monitorDefaultMaxAttempts = 3
                                   }
  finally
    (withAsync (Job.runJobMonitor jobMonitorSettings) (const actualTest))
    (cleanup)



payloadGen :: MonadGen m => m JobPayload
payloadGen = Gen.recursive  Gen.choice nonRecursive recursive
  where
    nonRecursive = [ PayloadAlwaysFail <$> Gen.element [1, 2, 3]
                   , PayloadSucceed <$> Gen.element [1, 2, 3]]
    recursive = [ PayloadFail <$> (Gen.element [1, 2, 3]) <*> payloadGen ]

testJobCreation appPool jobPool = testCase "job creation" $ withNewJobMonitor jobPool $ \tname -> Pool.withResource appPool $ \conn -> do
  Job{jobId} <- Job.createJob conn tname (PayloadSucceed 0)
  threadDelay (oneSec * 6)
  assertJobIdStatus conn tname "Expecting job to tbe successful by now" Job.Success jobId

testEnsureShutdown appPool jobPool = testCase "ensure shutdown" $ withRandomTable jobPool $ \tname -> do
  jid <- scheduleJob tname
  threadDelay (2 * Job.defaultPollingInterval)
  Pool.withResource appPool $ \conn ->
    assertJobIdStatus conn tname "Job should still be in queued state if job-monitor is no longer runner" Job.Queued jid
  where
    scheduleJob tname = withNamedJobMonitor tname jobPool $ do
      t <- getCurrentTime
      Pool.withResource appPool $ \conn -> do
        Job{jobId} <- Job.scheduleJob conn tname (PayloadSucceed 0) (addUTCTime (fromIntegral (2 * (Job.defaultPollingInterval `div` oneSec))) t)
        assertJobIdStatus conn tname "Job is scheduled in future, should still be queueud" Job.Queued jobId
        pure jobId


testJobScheduling appPool jobPool = testCase "job scheduling" $ withNewJobMonitor jobPool $ \tname -> Pool.withResource appPool $ \conn -> do
  t <- getCurrentTime
  job@Job{jobId} <- Job.scheduleJob conn tname (PayloadSucceed 0) (addUTCTime (fromIntegral 3600) t)
  threadDelay (oneSec * 2)
  assertJobIdStatus conn tname "Job is scheduled in the future. It should NOT have been successful by now" Job.Queued jobId
  j <- Job.saveJob conn tname job{jobRunAt = (addUTCTime (fromIntegral (-1)) t)}
  threadDelay (Job.defaultPollingInterval + (oneSec * 2))
  assertJobIdStatus conn tname "Job had a runAt date in the past. It should have been successful by now" Job.Success jobId

testJobFailure appPool jobPool = testCase "job retry" $ withNewJobMonitor jobPool $ \tname -> Pool.withResource appPool $ \conn -> do
  Job{jobId} <- Job.createJob conn tname (PayloadAlwaysFail 0)
  threadDelay (oneSec * 15)
  Job{jobAttempts, jobStatus} <- ensureJobId conn tname jobId
  assertEqual "Exepcting job to be in Failed status" Job.Failed jobStatus
  assertEqual ("Expecting job attempts to be 3. Found " <> show jobAttempts)  3 jobAttempts

data JobEvent = JobStart
              | JobRetry
              | JobSuccess
              | JobFailed
              deriving (Eq, Show)

testEverything appPool jobPool = testProperty "test everything" $ property $ do
  jobPayloads <- forAll $ Gen.list (Range.linear 1 1000) payloadGen
  jobsMVar <- liftIO $ newMVar (Map.empty :: Map.IntMap [(JobEvent, Job.Job)])

  test $ withRandomTable jobPool $ \tname -> do
    (defaults, cleanup) <- liftIO $ Job.defaultJobMonitor tname jobPool
    let jobMonitorSettings = defaults { Job.monitorJobRunner = jobRunner
                                      , Job.monitorTableName = tname
                                      , Job.monitorOnJobStart = onJobEvent JobStart jobsMVar
                                      , Job.monitorOnJobRetry = onJobEvent JobRetry jobsMVar
                                      , Job.monitorOnJobPermanentlyFailed = onJobEvent JobFailed jobsMVar
                                      , Job.monitorOnJobSuccess = onJobEvent JobSuccess jobsMVar
                                      , Job.monitorDefaultMaxAttempts = 3
                                      , Job.monitorPollingInterval = oneSec * jobPollingInterval
                                      }

    (jobs :: [Job]) <- withAsync
            (liftIO $ Job.runJobMonitor jobMonitorSettings)
            (const $ finally
              (liftIO $ actualTest jobPayloads tname jobsMVar)
              (liftIO cleanup))

    jobAudit <- takeMVar jobsMVar

    -- All jobs should show up in the audit, which means they should have
    -- been attempted /at least/ once
    (DL.sort $ map jobId jobs) === (DL.sort $ Map.keys jobAudit)

    -- No job should've been simultaneously picked-up by more than one
    -- worker
    True === (Map.foldl (\m js -> m && noRaceCondition js) True jobAudit)

    liftIO $ print $ "Test passed with job-count = " <> show (length jobPayloads)

  where

    noRaceCondition js = DL.foldl (&&) True $ DL.zipWith (\(x,_) (y,_) -> not $ x==JobStart && y==JobStart) js (tail js)

    jobPollingInterval = 2

    onJobEvent evt jobsMVar job@Job{jobId} = void $ modifyMVar_ jobsMVar $ \jobMap -> do
      pure $ Map.insertWith (++) jobId [(evt, job)] jobMap

    actualTest :: [JobPayload] -> Job.TableName -> MVar (Map.IntMap [(JobEvent, Job.Job)]) -> IO [Job]
    actualTest jobPayloads tname jobsMVar = do
      jobs <- forConcurrently jobPayloads $ \pload ->
        Pool.withResource appPool $ \conn ->
        liftIO $ Job.createJob conn tname pload
      let concurrencyFactor = 5
          maxDelay = sum $ map (payloadDelay jobPollingInterval) jobPayloads
          timeout = (maxDelay `div` concurrencyFactor) + (2 * testPollingInterval)
          testPollingInterval = 5
          poller nextAction = case nextAction of
            Left s -> pure $ Left s
            Right remaining ->
              if remaining == 0
              then pure (Right 0)
              else do threadDelay (oneSec * testPollingInterval)
                      print "------- Polling ------"
                      x <- withMVar jobsMVar $ \jobMap ->
                        if (Map.foldl (\m js -> m && (isJobTerminalState js)) True jobMap)
                        then pure (Right 0)
                        else if remaining < testPollingInterval
                             then pure (Left $ "Timeout. Job count=" <> show (length jobPayloads) <> " timeout=" <> show timeout <> " payload delay=" <> show maxDelay)
                             else pure $ Right (remaining - testPollingInterval)
                      poller x

      poller (Right timeout) >>= \case
        Left s -> Prelude.error s
        Right _ -> pure jobs

    isJobTerminalState js = case js of
      [] -> False
      (_, j):_ -> case (Job.jobStatus j) of
        Job.Failed -> True
        Job.Success -> True
        _ -> False



payloadDelay :: Int -> JobPayload -> Int
payloadDelay jobPollingInterval pload = payloadDelay_ 0 pload
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
  pure $ timeSince t (fromInteger i) u d

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
  (lockedAt, lockedBy) <- Gen.maybe ((,) <$> (futureTimeGen createdAt) <*> (Gen.text (Range.constant 1 100) Gen.ascii)) >>= \case
    Nothing -> pure (Nothing, Nothing)
    Just (x, y) -> pure (Just x, Just y)
  pure (createdAt, updatedAt, runAt, status, toJSON pload, attempts, lockedAt, lockedBy)

propFilterJobs appPool jobPool = testProperty "filter jobs" $ property $ do
  t <- liftIO getCurrentTime
  jobs <- forAll $ Gen.list (Range.linear 1 1000) (jobGen t)
  f <- forAll $ genFilter t
  test $ withRandomTable jobPool $ \tname -> do
    (savedJobs, dbJobs) <- liftIO $ do
      savedJobs <- Pool.withResource appPool $ \conn -> forM jobs $ \j -> (PGS.query conn (qry tname) j) >>= (pure . Prelude.head)
      (jm, cleanup) <- liftIO $ Job.defaultJobMonitor tname jobPool
      dbJobs <- (flip finally) cleanup $ (flip runReaderT) jm $ Web.filterJobs f
      pure (savedJobs, dbJobs)
    let testJobs = Test.filterJobs f savedJobs
    -- (sortBy (comparing jobId) dbJobs) === (sortBy (comparing jobId) testJobs)
    dbJobs === testJobs
  where
    qry tname = "INSERT INTO " <> tname <> " (created_at, updated_at, run_at, status, payload, attempts, locked_at, locked_by) values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING " <> Job.concatJobDbColumns


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
  pure Web.blankFilter
    { filterStatuses = statuses
    , filterCreatedAfter = createdAfter
    , filterCreatedBefore = createdBefore
    , filterUpdatedAfter = updatedAfter
    , filterUpdatedBefore = updatedBefore
    , filterOrder = orderClause
    }

jobType :: Job -> T.Text
jobType Job{jobPayload} = case jobPayload of
  Aeson.Object hm -> case HM.lookup "tag" hm of
    Just (Aeson.String t) -> t
    _ -> ""
  _ -> ""

filterJobs :: Filter -> [Job] -> [Job]
filterJobs Web.Filter{filterStatuses, filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore, filterOrder} js =
  applyOrdering (fromMaybe (Web.OrdUpdatedAt, Web.Desc) filterOrder) $
  (flip DL.filter) js $ \j -> (filterByStatus j) &&
                              (filterByCreatedAfter j) &&
                              (filterByCreatedBefore j) &&
                              (filterByUpdatedAfter j) &&
                              (filterByUpdatedBefore j)
  where
    applyOrdering (fld, dir) lst =
      let comparer = resultOrder $ case fld of
            Web.OrdCreatedAt -> (comparing jobCreatedAt)
            Web.OrdUpdatedAt -> (comparing jobUpdatedAt)
            Web.OrdLockedAt -> (comparing jobLockedAt)
            Web.OrdStatus -> (comparing jobStatus)
            Web.OrdJobType -> comparing jobType
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

