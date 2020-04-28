{-# LANGUAGE RecordWildCards #-}
module OddJobs.Cli where

import Options.Applicative as Opts
import Data.Text
import OddJobs.Job (startJobRunner, Config(..), defaultLockTimeout)
import System.Daemonize (DaemonOptions(..), daemonize)
import System.FilePath (FilePath)
import System.Posix.Process (getProcessID)
import qualified System.Directory as Dir
import qualified System.Exit as Exit
import System.Environment (getProgName)
import OddJobs.Types (Seconds(..), delaySeconds)
import qualified System.Posix.Signals as Sig
import qualified UnliftIO.Async as Async


-- * Introduction
--
-- $intro
--
-- This module has a bunch of functions (that use the 'optparse-applicative'
-- library) to help you rapidly build a standalone job-runner deamon based on
-- odd-jobs. You should probably start-off by using the pre-packaged [default
-- behaviour](#defaultBehaviour), notably the 'defaultMain' function. If the
-- default behaviour of the resultant CLI doesn't suit your needs, consider
-- reusing\/extending the [individual argument parsers](#parsers). If you find
-- you cannot reuse those, then it would be best to write your CLI from scratch
-- instead of force-fitting whatever has been provided here.
--
-- It is __highly recommended__ that you read the \"Deployment\"
-- section in the tutorial before you put odd-jobs into production.
--
--   * TODO: link-off to tutorial
--
--   * TODO: link-off to @simple-example@

-- * Default behaviour
--
-- $defaultBehaviour
--
-- #defaultBehaviour#
--

{-|
Please do not get scared by the type-signature of the first argument.
Conceptually, it's a callback, within another callback.

The callback function that you pass to 'defaultMain' will be executed once the
job-runner has forked as a background dameon. Your callback function will be
given another callback function, i.e. the @Config -> IO ()@ part that you need
to call once you've setup @Config@ and whatever environment is required to run
your application code.

This complication is necessary because immediately after forking a new daemon
process, a bunch of resources will need to be allocated for the job-runner to
start functioning. At the minimum, a DB connection pool and logger. However,
realistically, a bunch of additional resources will also be required for
setting up the environment needed for running jobs in your application's
monad.

All of these resource allocations need to be bracketed so that when the
job-runner exits, they may be cleaned-up gracefully.

Please take a look at @simple-example@ for how to use this function in
practice. (TODO: link-off to the example).
-}
defaultMain :: ((Config -> IO ()) -> IO ())
            -- ^ A callback function that will be executed once the dameon has
            -- forked into the background.
            -> IO ()
defaultMain startFn = do
  Args{argsCommand} <- customExecParser defaultCliParserPrefs (defaultCliInfo defaultLockTimeout)
  case argsCommand of
    Start cmdArgs -> do
      defaultStartCommand cmdArgs startFn
    Stop cmdArgs -> do
      defaultStopCommand cmdArgs
    Status ->
      Prelude.error "not implemented yet"

{-| Used by 'defaultMain' if the 'Start' command is issued via the CLI. If
@--daemonize@ switch is also passed, it checks for 'startPidFile':

* If it doesn't exist, it forks a background daemon, writes the PID file, and
  exits.

* If it exists, it refuses to start, to prevent multiple invocations of the same
  background daemon.
-}
defaultStartCommand :: StartArgs
                    -> ((Config -> IO ()) -> IO ())
                    -- ^ the same callback-within-callback function described in
                    -- 'defaultMain'
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

{-| Used by 'defaultMain' if 'Stop' command is issued via the CLI. Sends a
@SIGINT@ signal to the process indicated by 'shutPidFile'. Waits for a maximum
of 'shutTimeout' seconds (controller by @--timeout@) for the daemon to shutdown
gracefully, after which a @SIGKILL@ is issued
-}
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

-- * Default CLI parsers
--
-- $parsers$
--
-- #parsers#
--
-- If the [default behaviour](#defaultBehaviour) doesn't suit your needs, you
-- can write a @main@ function yourself, and consider using\/extending the CLI
-- parsers documented in this section.


-- | The command-line is parsed into this data-structure using 'argParser'
data Args = Args
  { argsCommand :: !Command
  } deriving (Eq, Show)


-- | The top-level command-line parser
argParser :: Seconds
          -- ^ the default value for 'shutTimeout'
          -> Parser Args
argParser defaultTimeout = Args
  <$> (commandParser defaultTimeout)

-- ** Top-level command parser

-- | CLI commands are parsed into this data-structure by 'commandParser'
data Command
  = Start StartArgs
  | Stop StopArgs
  | Status
  deriving (Eq, Show)

-- Parser for 'argsCommand'
commandParser :: Seconds          -- ^ default value for 'shutTimeout'
              -> Parser Command
commandParser defaultTimeout = hsubparser
   ( command "start" (info startParser (progDesc "start the odd-jobs runner")) <>
     command "stop" (info (stopParser defaultTimeout) (progDesc "stop the odd-jobs runner")) <>
     command "status" (info statusParser (progDesc "print status of all active jobs"))
   )

-- ** Start command

-- | @start@ command is parsed into this data-structure by 'startParser'
data StartArgs = StartArgs
  {
    -- | Switch to enable/disable the web UI (the web UI is still WIP)
    startWebUiEnable :: !Bool
    -- | You'll need to pass the @--daemonize@ switch to fork the job-runner as
    -- a background daemon, else it will keep running as a foreground process.
  , startDaemonize :: !Bool
    -- | PID file for the background dameon. Ref: 'pidFileParser'
  , startPidFile :: !FilePath
  } deriving (Eq, Show)

startParser :: Parser Command
startParser = fmap Start $ StartArgs
  <$> switch ( long "web-ui-enable" <>
               help "Please look at other web-ui-* options to configure the Web UI"
             )
  <*> switch ( long "daemonize" <>
               help "Fork the job-runner as a background daemon. If omitted, the job-runner remains in the foreground."
             )
  <*> pidFileParser


-- ** Stop command

-- | @stop@ command is parsed into this data-structure by 'stopParser'. Please
-- note, that this command first sends a @SIGINT@ to the daemon and waits for
-- 'shutTimeout' seconds (which defaults to 'defaultLockTimeout'). If the daemon
-- doesn't shut down cleanly within that time, it sends a @SIGKILL@ to kill
-- immediately.
data StopArgs = StopArgs
  { -- | After sending a @SIGINT@, how many seconds to wait before sending a
    -- @SIGKILL@
    shutTimeout :: !Seconds
    -- | PID file of the deamon. Ref: 'pidFileParser'
  , shutPidFile :: !FilePath
  } deriving (Eq, Show)

stopParser :: Seconds -> Parser Command
stopParser defaultTimeout = fmap Stop $ StopArgs
  <$> option (Seconds <$> auto) ( long "timeout" <>
                                  metavar "TIMEOUT" <>
                                  help "Maximum seconds to wait before force-killing the background daemon." <>
                                  value defaultTimeout <>
                                  showDefaultWith (show . unSeconds)
                                )
  <*> pidFileParser


-- ** Status command

-- | The @status@ command has not been implemented yet. PRs welcome :-)
statusParser :: Parser Command
statusParser = pure Status

-- ** Other parsing utilities

-- | If @--pid-file@ is not given as a command-line argument, this defaults to
-- @./odd-jobs.pid@
pidFileParser :: Parser FilePath
pidFileParser =
  strOption ( long "pid-file" <>
              metavar "PIDFILE" <>
              value "./odd-jobs.pid" <>
              showDefault <>
              help "Path of the PID file for the daemon. Takes effect only during stop or only when using the --daemonize option at startup"
            )

defaultCliParserPrefs :: ParserPrefs
defaultCliParserPrefs = prefs $
  showHelpOnError <>
  showHelpOnEmpty

defaultCliInfo :: Seconds
               -- ^ default value for 'shutTimeout'
               -> ParserInfo Args
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


