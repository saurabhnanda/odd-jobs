{-# LANGUAGE RecordWildCards #-}
module OddJobs.Cli where

import Options.Applicative as Opts
import Data.Text
import OddJobs.Job (startJobRunner, Config(..), lockTimeout)
import System.Daemonize (DaemonOptions(..), daemonize)
import System.FilePath (FilePath)
import System.Posix.Process (getProcessID)
import qualified System.Directory as Dir
import qualified System.Exit as Exit
import System.Environment (getProgName)
import OddJobs.Types (Seconds(..), delaySeconds)
import qualified System.Posix.Signals as Sig
import qualified UnliftIO.Async as Async

data StartArgs = StartArgs
  { startWebUiEnable :: !Bool
  , startDaemonize :: !Bool
  , startPidFile :: !FilePath
  } deriving (Eq, Show)

data StopArgs = StopArgs
  { shutTimeout :: !Seconds
  , shutPidFile :: !FilePath
  } deriving (Eq, Show)

data Command
  = Start StartArgs
  | Stop StopArgs
  | Status
  deriving (Eq, Show)

data Args = Args
  { argsCommand :: !Command
  } deriving (Eq, Show)

-- data CliActions = CliActions
--   { actionStart :: StartArgs -> (Config -> IO ()) -> IO ()
--   , actionStop :: StopArgs -> IO ()
--   , actionStatus :: IO ()
--   }

argParser :: Seconds -> Parser Args
argParser defaultTimeout = Args
  <$> (commandParser defaultTimeout)

commandParser :: Seconds -> Parser Command
commandParser defaultTimeout = hsubparser
   ( command "start" (info startParser (progDesc "start the odd-jobs runner")) <>
     command "stop" (info (stopParser defaultTimeout) (progDesc "stop the odd-jobs runner")) <>
     command "status" (info statusParser (progDesc "print status of all active jobs"))
   )

pidFileParser :: Parser FilePath
pidFileParser =
  strOption ( long "pid-file" <>
              metavar "PIDFILE" <>
              value "./odd-jobs.pid" <>
              showDefault <>
              help "Path of the PID file for the daemon. Takes effect only during stop or only when using the --daemonize option at startup"
            )

statusParser :: Parser Command
statusParser = pure Status

startParser :: Parser Command
startParser = fmap Start $ StartArgs
  <$> switch ( long "web-ui-enable" <>
               help "Please look at other web-ui-* options to configure the Web UI"
             )
  <*> switch ( long "daemonize" <>
               help "Fork the job-runner as a background daemon. If omitted, the job-runner remains in the foreground."
             )
  <*> pidFileParser

stopParser :: Seconds -> Parser Command
stopParser defaultTimeout = fmap Stop $ StopArgs
  <$> option (Seconds <$> auto) ( long "timeout" <>
                                  metavar "TIMEOUT" <>
                                  help "Maximum seconds to wait before force-killing the background daemon." <>
                                  value defaultTimeout <>
                                  showDefaultWith (show . unSeconds)
                                )
  <*> pidFileParser

-- parseCliArgs :: IO Args
-- parseCliArgs = execParser $ Opts.info (argParser <**> helper) $
--   Opts.progDesc "Start/stop the odd-jobs runner (standalone binary)" <>
--   Opts.fullDesc

defaultCliParserPrefs :: ParserPrefs
defaultCliParserPrefs = prefs $
  showHelpOnError <>
  showHelpOnEmpty

defaultCliInfo :: Seconds -> ParserInfo Args
defaultCliInfo defaultTimeout =
  info ((argParser defaultTimeout)  <**> helper) fullDesc

defaultDaemonOptions :: DaemonOptions
defaultDaemonOptions = DaemonOptions
  { daemonShouldChangeDirectory = False
  , daemonShouldCloseStandardStreams = False
  , daemonShouldIgnoreSignals = True
  , daemonUserToChangeTo = Nothing
  , daemonGroupToChangeTo = Nothing
  }

defaultStartCommand :: StartArgs
                    -> ((Config -> IO ()) -> IO ())
                    -> IO ()
defaultStartCommand StartArgs{..} startFn = do
  progName <- getProgName
  case startDaemonize of
    False -> do
      startFn startJobRunner
    True -> do
      (Dir.doesPathExist startPidFile) >>= \case
        True -> do
          putStrLn $ "PID file already exists. Please check if " <> progName <> " is still running in the background." <>
            " If not, you can safely delete this file and start " <> progName <> " again: " <> startPidFile
          Exit.exitWith (Exit.ExitFailure 1)
        False -> do
          daemonize defaultDaemonOptions (pure ()) $ const $ do
            pid <- getProcessID
            writeFile startPidFile (show pid)
            putStrLn $ "Started " <> progName <> " in background with PID=" <> show pid <> ". PID written to " <> startPidFile
            startFn $ \jm -> startJobRunner jm{cfgPidFile = Just startPidFile}

defaultStopCommand :: StopArgs
                   -> IO ()
defaultStopCommand StopArgs{..} = do
  progName <- getProgName
  pid <- read <$> (readFile shutPidFile)
  if (shutTimeout == Seconds 0)
    then forceKill pid
    else do putStrLn $ "Sending SIGINT to pid=" <> show pid <>
              " and waiting " <> (show $ unSeconds shutTimeout) <> " seconds for graceful stop"
            Sig.signalProcess Sig.sigINT pid
            (Async.race (delaySeconds shutTimeout) checkProcessStatus) >>= \case
              Right _ -> do
                putStrLn $ progName <> " seems to have exited gracefully."
                Exit.exitSuccess
              Left _ -> do
                putStrLn $ progName <> " has still not exited."
                forceKill pid
  where
    forceKill pid = do
      putStrLn $ "Sending SIGKILL to pid=" <> show pid
      Sig.signalProcess Sig.sigKILL pid

    checkProcessStatus = do
      Dir.doesPathExist shutPidFile >>= \case
        True -> do
          delaySeconds (Seconds 1)
          checkProcessStatus
        False -> do
          pure ()


defaultMain :: ((Config -> IO ()) -> IO ()) -> IO ()
defaultMain startFn = do
  Args{argsCommand} <- customExecParser defaultCliParserPrefs (defaultCliInfo lockTimeout)
  case argsCommand of
    Start cmdArgs -> do
      defaultStartCommand cmdArgs startFn
    Stop cmdArgs -> do
      defaultStopCommand cmdArgs
    Status ->
      Prelude.error "not implemented yet"

