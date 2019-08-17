module Try2 where

-- import Control.Concurrent.Lifted
-- import Control.Concurrent.Async.Lifted
-- import Control.Exception.Lifted
import UnliftIO.Concurrent
import UnliftIO.Async
import UnliftIO.Exception
import Control.Concurrent.Async (AsyncCancelled(..))
import Debug.Trace
import Control.Monad
import Data.Functor
import Control.Monad.Reader
import Data.Maybe

-- oneSec :: Int
-- oneSec = 1000000


-- main :: IO ()
-- main = finally
--   (withAsync
--    (catch runMonitoringThread (\AsyncCancelled -> print "---- delivered in main"))
--    (const $ threadDelay $ oneSec * 5))
--   (print "finally executed")


-- myPrint = liftIO . print

-- runMonitoringThread :: IO ()
-- runMonitoringThread = do
--   print "something random"
--   runReaderT runMonitoringThread2 10

-- runMonitoringThread2 :: ReaderT Int IO ()
-- runMonitoringThread2 = do
--   x <- ask
--   myPrint $ "==== got the number " <> show x
--   runMonitoringThread_ Nothing Nothing
--   where
--     runMonitoringThread_ mThread1 mThread2= do
--       thread1 <- maybe (async task1) pure mThread1
--       thread2 <- maybe (async task2) pure mThread2
--       -- catch (myWait thread1 thread2) (\AsyncCancelled -> myPrint "signal delivered in myWait" >> cancel thread1 >> cancel thread2)
--       finally
--         (myWait thread1 thread2)
--         (myPrint "FINALLY" >> cancel thread1 >> cancel thread2)

--     task1 = forever $ do
--       myPrint "thread a is running"
--       threadDelay oneSec

--     task2 = forever $ do
--       myPrint "thread 2 is running"
--       threadDelay (oneSec * 2)

--     myWait :: Async () -> Async () -> ReaderT Int IO ()
--     myWait thread1 thread2 = do
--       waitEitherCatch thread1 thread2 >>= \case
--         Left _ -> runMonitoringThread_ Nothing (Just thread2)
--         Right _ -> runMonitoringThread_ (Just thread1) Nothing
--       myPrint "this should never get executed"
