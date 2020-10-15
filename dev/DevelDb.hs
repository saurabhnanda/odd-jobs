{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Provides tools to facilitate database interactions during interactive
-- development.
module DevelDb where

import           Control.Exception (bracket)
import           Data.Aeson(FromJSON, ToJSON)
import           Data.ByteString.Char8 (ByteString, pack)
import           Data.Text (Text)
import qualified Database.PostgreSQL.Simple as PGS
import           System.Environment (lookupEnv)

import GHC.Generics

import OddJobs.Migrations
import OddJobs.Job
import OddJobs.Types

data TestJob = TestJob Text
  deriving (Eq, Show, Generic, FromJSON, ToJSON)

devTableNames :: TableNames
devTableNames = TableNames
  { tnJob = "job", tnResource = "resource" }

devConnectionString :: IO ByteString
devConnectionString = do
  maybe (error "devConnectionString: Expected environment variable \"ODD_JOBS_DEV_DB_CONNECT\" to provide a connection string")
        pack
    <$> lookupEnv "ODD_JOBS_DEV_DB_CONNECT"

createDevDatabase :: IO ()
createDevDatabase = do
  connStr <- devConnectionString
  conn <- PGS.connectPostgreSQL connStr
  PGS.withTransaction conn $ createJobTables conn devTableNames

openDevConnection :: IO PGS.Connection
openDevConnection = devConnectionString >>= PGS.connectPostgreSQL

withDevConnection :: (PGS.Connection -> IO a) -> IO a
withDevConnection = bracket openDevConnection PGS.close

withDevTransaction :: (PGS.Connection -> IO a) -> IO a
withDevTransaction act =
  withDevConnection $ \conn -> PGS.withTransaction conn (act conn)
