{-# LANGUAGE FlexibleContexts #-}
module Try where

import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Control.Exception.Lifted
import Control.Concurrent.Async.Lifted
import Control.Monad
import Data.Pool as Pool
import Debug.Trace
import 	Control.Monad.IO.Unlift (liftIO)
import qualified System.Random as R
import Data.String (fromString)

withRandomTable pool action = do
  tname <- liftIO ((("jobs_" <>) . fromString) <$> (replicateM 10 (R.randomRIO ('a', 'z'))))
  finally
    (Pool.withResource pool $ \conn -> (liftIO $ traceM "I will create the random table here") >> (action tname))
    (Pool.withResource pool $ \conn -> liftIO $ traceM "I will drop the random table here")

myTest pool = property $ do
  randomData <- forAll $ Gen.list (Range.linear 1 100) (Gen.element [1, 2, 3])
  test $ withRandomTable pool $ \tname -> do
    withAsync
      (traceM "I will be a long-running background thread")
      (const $ traceM $ "hooray... I got the random table name " <> tname)
    True === True
