{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, NamedFieldPuns, DeriveGeneric #-}
module Test where

import Test.Tasty as Tasty
import qualified Migrations
import qualified Job
import Database.PostgreSQL.Simple as PGS
import Data.Functor (void)
import Data.Pool as Pool
import Test.Tasty.HUnit
import Debug.Trace
import Control.Exception (finally)
import Control.Monad.Logger
import Control.Monad.Reader
import Data.Aeson as Aeson
import UnliftIO.Async
import UnliftIO.Concurrent
import Job (Job(..), JobId)
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


setupDatabase :: ConnectInfo -> IO ()
setupDatabase cinfo = pure ()
  -- do
  -- conn1 <- connect cinfo{connectDatabase="postgres"}
  -- finally (void $ PGS.execute_ conn1 "drop database if exists jobs_test; create database jobs_test") (close conn1)

main :: IO ()
main = do
  let connInfo = ConnectInfo
                 { connectHost = "localhost"
                 , connectPort = fromIntegral 5432
                 , connectUser = "jobs_test"
                 , connectPassword = "jobs_test"
                 , connectDatabase = "jobs_test"
                 }
  setupDatabase connInfo
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

  -- let foo i conn = do
  --       traceM $ " === executing query in thread " <> show i <> "\n\n"
  --       void $ PGS.execute_ conn "update jobs set id=id+1 where id < 0"
  --       threadDelay (oneSec * 5)

  -- void $ forConcurrently [1..50] (\i -> Pool.withResource dbpool (foo i))

  -- defaults <- (Job.defaultJobMonitor jobPool)
  -- let jobMonitorSettings = defaults{ Job.monitorJobRunner = jobRunner }

  -- withFastLogger (LogFile FileLogSpec{log_file="test.log", log_file_size=1024*1024*10, log_backup_number=5} defaultBufSize) $ \ flogger -> do
  --   let env = Env { envDbPool = jobPool, envLogger = flogger }
    -- finally
    --   (withAsync (Job.runJobMonitor jobMonitorSettings)  (const $ defaultMain $ tests appPool jobPool))
    --   ((Pool.destroyAllResources appPool) >> (Pool.destroyAllResources jobPool))

  defaultMain $ tests appPool jobPool

-- tests dbpool = testGroup "All tests" $ (\t -> t dbpool) <$> [simpleJobCreation]
-- tests appPool jobPool = testGroup "All tests"
--   [ testGroup "simple tests" $ (\t -> t appPool) <$> [testJobFailure, testJobCreation, testJobScheduling]
--   , testGroup "property tests" [testPayloadGenerator appPool jobPool] -- [testEverything appPool jobPool]
--   ]

tests appPool jobPool = testGroup "All tests"
  [ testGroup "simple tests" [testJobCreation appPool jobPool, testJobScheduling appPool jobPool, testJobFailure appPool jobPool] -- [testJobFailure appPool jobPool, testJobCreation appPool jobPool, testJobScheduling appPool jobPool]
  -- , testGroup "property tests" [testPayloadGenerator appPool jobPool] -- [testEverything appPool jobPool]
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
  -- monadLoggerLog _ _ _ _ = pure ()

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

-- simpleJobCreation appPool jobPool = testCase "simple job creation" $ do
--   jobs <- (forConcurrently [1..10000] (const $ Pool.withResource appPool $ \conn -> Job.createJob conn testPayload))
--   threadDelay (oneSec * 280)
--   forConcurrently_ jobs $ \Job{jobId} -> Pool.withResource appPool $ \conn -> assertJobIdStatus conn "Expecting job to be completed by now" Job.Success jid

withRandomTable jobPool action = do
  (tname :: Job.TableName) <- ((("jobs_" <>) . fromString) <$> (replicateM 10 (R.randomRIO ('a', 'z'))))
  finally
    ((Pool.withResource jobPool $ \conn -> Migrations.createJobTable conn tname) >> (action tname))
    (Pool.withResource jobPool $ \conn -> void $ PGS.execute_ conn ("drop table if exists " <> tname <> ";"))

-- withNewJobMonitor :: (Pool Connection) -> (TableName -> Assertion) -> Assertion
withNewJobMonitor jobPool actualTest = withRandomTable jobPool $ \tname -> do
  (defaults, cleanup) <- (Job.defaultJobMonitor jobPool)
  let jobMonitorSettings = defaults{ Job.monitorJobRunner = jobRunner, Job.monitorTableName = tname }
  finally
    (withAsync (Job.runJobMonitor jobMonitorSettings) (const $ actualTest tname))
    (cleanup)

payloadGen :: MonadGen m => m JobPayload
payloadGen = Gen.recursive  Gen.choice nonRecursive recursive
  where
    nonRecursive = [ PayloadAlwaysFail <$> Gen.element [1, 2, 3]
                   , PayloadSucceed <$> Gen.element [1, 2, 3]]
    recursive = [ PayloadFail <$> (Gen.element [1, 2, 3]) <*> payloadGen ]

testJobCreation appPool jobPool = testCase "job creation" $ withNewJobMonitor jobPool $ \tname -> Pool.withResource appPool $ \conn -> do
  Job{jobId} <- Job.createJob conn tname (PayloadSucceed 0)
  threadDelay (oneSec * 4)
  assertJobIdStatus conn tname "Expecting job to tbe successful by now" Job.Success jobId

testJobScheduling appPool jobPool = testCase "job scheduling" $ withNewJobMonitor jobPool $ \tname -> Pool.withResource appPool $ \conn -> do
  t <- getCurrentTime
  job@Job{jobId} <- Job.scheduleJob conn tname (PayloadSucceed 0) (addUTCTime (fromIntegral 3600) t)
  threadDelay (oneSec * 2)
  assertJobIdStatus conn tname "Job is scheduled in the future. It should NOT have been successful by now" Job.Queued jobId
  j <- Job.saveJob conn tname job{jobRunAt = (addUTCTime (fromIntegral (-1)) t)}
  threadDelay (Job.defaultPollingInterval + (oneSec * 2))
  assertJobIdStatus conn tname "Job had a runAt date in the past. It should have been successful by now" Job.Success jobId

testJobFailure appPool jobPool = testCase "job failure" $ withNewJobMonitor jobPool $ \tname -> Pool.withResource appPool $ \conn -> do
  Job{jobId} <- Job.createJob conn tname (PayloadAlwaysFail 0)
  threadDelay (oneSec * 12)
  Job{jobAttempts, jobStatus} <- ensureJobId conn tname jobId
  assertEqual "Exepcting job to be in Retry status" Job.Retry jobStatus
  assertBool ("Expecting job attempts to be 3 or 4. Found " <> show jobAttempts) (jobAttempts == 3 || jobAttempts == 4)


-- testEverything appPool dbPool = testProperty "test everything" $ property $ do
--   failurePattern <- forAll $ Gen.list (Range.linear 0 20) (Gen.element [1, 2, 3])
--   liftIO $ print failurePattern
--   True === True
--   -- xs <- forAll $ Gen.list (Range.linear 0 100) Gen.alpha
--   -- reverse (reverse xs) === xs

-- testPayloadGenerator appPool dbPool = testProperty "test payload generator" $ property $ do
--   pload <- forAll payloadGen
--   liftIO $ print pload
--   True === True

