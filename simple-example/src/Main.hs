{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import OddJobs.Job (Job(..), defaultJobMonitor, ConcurrencyControl(..), withConnectionPool, JobMonitor(..))
import OddJobs.Cli (defaultMain)
import Control.Monad.Logger(defaultLogStr, LogLevel(..))
import System.Log.FastLogger(withFastLogger, LogType'(..), defaultBufSize)

import Data.Text (Text)
import Data.Aeson as Aeson
import GHC.Generics
import Data.Aeson.Internal (ifromJSON, IResult(..), formatError)
import Debug.Trace

data MyJob = SendWelcomeEmail Int
           | SendPasswordResetEmail Text
           | SetupSampleData Int
           deriving (Eq, Show, Generic, ToJSON, FromJSON)


myJobRunner :: Job -> IO ()
myJobRunner Job{jobPayload} = do
  traceM $ show jobPayload
  case (ifromJSON jobPayload :: IResult MyJob) of
    IError jpath e ->
      Prelude.error $ formatError jpath e
    ISuccess r ->
      case r of
        SendWelcomeEmail userId ->
          putStrLn "This should call the function that actually sends the welcome email"
        SendPasswordResetEmail tkn ->
          putStrLn "This should call the function that actually sends the password-reset email"
        SetupSampleData userId ->
          putStrLn "This should call the function that actually sets up samply data in a newly registered user's account"

main :: IO ()
main = do
  withConnectionPool (Left "dbname=jobs_test user=jobs_test password=jobs_test host=localhost")$ \dbPool -> do
    withFastLogger (LogStdout defaultBufSize) $ \logger -> do
      let loggerIO loc logSource logLevel logStr = if logLevel==LevelDebug
                                                   then pure ()
                                                   else logger $ defaultLogStr loc logSource logLevel logStr
          jm = defaultJobMonitor loggerIO "jobs_aqgrqtaowi" dbPool (MaxConcurrentJobs 50)
      defaultMain jm{monitorJobRunner=myJobRunner}

