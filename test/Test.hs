{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, NamedFieldPuns #-}
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

setupDatabase :: ConnectInfo -> IO ()
setupDatabase cinfo = do
  conn1 <- connect cinfo{connectDatabase="postgres"}
  finally (dropAndCreate conn1) (close conn1)
  conn2 <- connect cinfo
  finally (Migrations.createJobTable conn2) (close conn2)
  where
    dropAndCreate conn1 = do
      void $ PGS.execute_ conn1 "drop database if exists jobs_test";
      void $ PGS.execute_ conn1 "create database jobs_test";


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

  withFastLogger (LogFile FileLogSpec{log_file="test.log", log_file_size=1024*1024*10, log_backup_number=5} defaultBufSize) $ \ flogger -> do
    let env = Env { envDbPool = jobPool, envLogger = flogger }
    finally
      (withAsync (runReaderT (Job.jobMonitor jobRunner) env)  (const $ defaultMain $ tests appPool jobPool))
      ((Pool.destroyAllResources appPool) >> (Pool.destroyAllResources jobPool))

-- tests dbpool = testGroup "All tests" $ (\t -> t dbpool) <$> [simpleJobCreation]
tests appPool jobPool = testGroup "All tests" $ (\t -> t appPool) <$> [testJobCreation, testJobScheduling]

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
jobRunner _ = pure ()

oneSec :: Int
oneSec = 1000000

assertJobIdStatus :: (HasCallStack) => Connection -> String -> Job.Status -> JobId -> Assertion
assertJobIdStatus conn msg st jid = Job.findJobById conn jid >>= \case
  Nothing -> assertFailure $ "Not expecting job to be deleted. JobId=" <> show jid
  Just (Job{jobStatus}) -> assertEqual msg st jobStatus

-- simpleJobCreation appPool jobPool = testCase "simple job creation" $ do
--   jobs <- (forConcurrently [1..10000] (const $ Pool.withResource appPool $ \conn -> Job.createJob conn testPayload))
--   threadDelay (oneSec * 280)
--   forConcurrently_ jobs $ \Job{jobId} -> Pool.withResource appPool $ \conn -> assertJobIdStatus conn "Expecting job to be completed by now" Job.Success jid

testJobCreation = myTestCase "job creation" $ \ conn -> do
  Job{jobId} <- Job.createJob conn testPayload
  threadDelay (oneSec * 2)
  assertJobIdStatus conn "Expecting job to tbe successful by now" Job.Success jobId

testJobScheduling = myTestCase "job scheduling" $ \conn -> do
  t <- getCurrentTime
  job@Job{jobId} <- Job.scheduleJob conn testPayload (addUTCTime (fromIntegral 3600) t)
  threadDelay (oneSec * 2)
  assertJobIdStatus conn "Job is scheduled in the future. It should NOT have been successful by now" Job.Queued jobId
  j <- Job.saveJob conn job{jobRunAt = (addUTCTime (fromIntegral (-1)) t)}
  threadDelay (Job.defaultPollingInterval + (oneSec * 2))
  assertJobIdStatus conn "Job had a runAt date in the past. It should have been successful by now" Job.Success jobId

