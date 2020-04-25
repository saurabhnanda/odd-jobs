{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Main where

import OddJobs.Job (Job(..), defaultConfig, ConcurrencyControl(..), withConnectionPool, Config(..), throwParsePayload)
import OddJobs.Cli (defaultMain)
import Control.Monad.Logger(defaultLogStr, LogLevel(..))
import System.Log.FastLogger(withFastLogger, LogType'(..), defaultBufSize)

import Data.Text (Text)
import Data.Aeson as Aeson
import GHC.Generics
import Debug.Trace
import OddJobs.Types (delaySeconds, Seconds(..))

data MyJob = SendWelcomeEmail Int
           | SendPasswordResetEmail Text
           | SetupSampleData Int
           deriving (Eq, Show, Generic, ToJSON, FromJSON)


myJobRunner :: Job -> IO ()
myJobRunner job = do
  (throwParsePayload job) >>= \case
    SendWelcomeEmail userId -> do
      putStrLn "This should call the function that actually sends the welcome email. " <>
        "\nWe are purposely waiting 60 seconds before completing this job so that graceful shutdown can be demonstrated."
      delaySeconds (Seconds 60)
      putStrLn "60 second wait is now over..."
    SendPasswordResetEmail tkn ->
      putStrLn "This should call the function that actually sends the password-reset email"
    SetupSampleData userId ->
      putStrLn "This should call the function that actually sets up samply data in a newly registered user's account"

main :: IO ()
main = do
  defaultMain startJobMonitor
  where
    startJobMonitor callback = do
      withConnectionPool (Left "dbname=jobs_test user=jobs_test password=jobs_test host=localhost")$ \dbPool -> do
        withFastLogger (LogFileNoRotate "oddjobs.log" defaultBufSize) $ \logger -> do
          let loggerIO loc logSource logLevel logStr = if logLevel==LevelDebug
                                                       then pure ()
                                                       else logger $ defaultLogStr loc logSource logLevel logStr
              jm = defaultConfig loggerIO "jobs_aqgrqtaowi" dbPool (MaxConcurrentJobs 50)
          callback jm{cfgJobRunner=myJobRunner}
