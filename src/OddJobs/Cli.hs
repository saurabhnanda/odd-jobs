{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
module OddJobs.Cli where

import Options.Applicative as Opts
import Data.Text
import OddJobs.Job (startJobRunner, Config(..), LogLevel(..), LogEvent(..))
import OddJobs.Types (UIConfig(..), Seconds(..), delaySeconds)
-- import System.Daemonize (DaemonOptions(..), daemonize)
import qualified System.Posix.Daemon as Daemon
import System.FilePath (FilePath)
import System.Posix.Process (getProcessID)
import qualified System.Directory as Dir
import qualified System.Exit as Exit
import System.Environment (getProgName)
import qualified System.Posix.Signals as Sig
import qualified UnliftIO.Async as Async
import UnliftIO (bracket_)
import Safe (fromJustNote)
import qualified OddJobs.Endpoints as UI
import Servant.Server as Servant
import Servant.API
import Data.Proxy
import Data.Text.Encoding (decodeUtf8)
import Network.Wai.Handler.Warp as Warp
import Debug.Trace
import Data.String.Conv (toS)

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
-- It is __highly recommended__ that you read the following links before putting
-- odd-jobs into production.
--
--   * The [getting started
--     guide](https://www.haskelltutorials.com/odd-jobs/guide.html#getting-started)
--     which will walk you through a bare-bones example of using the
--     'defaultMain' function. It should make the callback-within-callback
--     clearer.
--
--   * The [section on
--     deployment](https://www.haskelltutorials.com/odd-jobs/guide.html#deployment)
--     in the guide.

-- * Default behaviour
--
-- $defaultBehaviour
--
-- #defaultBehaviour#
--


data CliType = CliOnlyJobRunner { cliStartJobRunner :: (Config -> Config) -> IO () }
             | CliOnlyWebUi { cliStartWebUI :: UIStartArgs -> (UIConfig -> UIConfig) -> IO () }
             | CliBoth { cliStartJobRunner :: (Config -> Config) -> IO (), cliStartWebUI :: UIStartArgs -> (UIConfig -> UIConfig) -> IO () }

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

Please take a look at the [getting started
guide](https://www.haskelltutorials.com/odd-jobs/guide.html#getting-started) for
an example of how to use this function.
-}
runCli :: CliType -> IO ()
runCli cliType = do
  Args{argsCommand} <- customExecParser defaultCliParserPrefs (defaultCliInfo cliType)
  case argsCommand of
    Start commonArgs mUIArgs ->
      defaultStartCommand commonArgs mUIArgs cliType
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
defaultStartCommand :: CommonStartArgs
                    -> Maybe UIStartArgs
                    -> CliType
                    -> IO ()
defaultStartCommand CommonStartArgs{..} mUIArgs cliType = do
  if startDaemonize then do
    Daemon.runDetached (Just startPidFile) (Daemon.ToFile "/tmp/oddjobs.out") coreStartupFn
  else
    coreStartupFn
  where
    uiArgs = fromJustNote "Please specify Web UI Startup Args" $ traceShowId mUIArgs
    coreStartupFn =
      case cliType of
        CliOnlyJobRunner{..} -> do
          cliStartJobRunner Prelude.id
        CliOnlyWebUi{..} -> do
          traceM "CliOnlyWebUi before"
          cliStartWebUI uiArgs Prelude.id
        CliBoth{..} -> do
          traceM "CliBoth before"
          Async.withAsync (cliStartWebUI uiArgs Prelude.id) $ \_ -> do
            traceM "CliBoth inside withAsync"
            cliStartJobRunner Prelude.id
            traceM "CliBoth end"

defaultWebUI :: UIStartArgs
             -> UIConfig
             -> IO ()
defaultWebUI UIStartArgs{..} uicfg@UIConfig{..} = do
  env <- UI.mkEnv uicfg ("/" <>)
  case uistartAuth of
    AuthNone -> do
      let app = UI.server uicfg env Prelude.id
      uicfgLogger LevelInfo $ LogText $ "Starting admin UI on port " <> (toS $ show uistartPort)
      Warp.run uistartPort $ Servant.serve (Proxy :: Proxy UI.FinalAPI) app
    (AuthBasic u p) -> do
      let api = Proxy :: Proxy (BasicAuth "OddJobs Admin UI" OddJobsUser :> UI.FinalAPI)
          ctx = defaultBasicAuth (u, p) :. EmptyContext
          -- Now the app will receive an extra argument for OddJobsUser,
          -- which we aren't really interested in.
          app _ = UI.server uicfg env Prelude.id
      uicfgLogger LevelInfo $ LogText $ "Starting admin UI on port " <> (toS $ show uistartPort)
      Warp.run uistartPort $ Servant.serveWithContext api ctx app

{-| Used by 'defaultMain' if 'Stop' command is issued via the CLI. Sends a
@SIGINT@ signal to the process indicated by 'shutPidFile'. Waits for a maximum
of 'shutTimeout' seconds (controller by @--timeout@) for the daemon to shutdown
gracefully, after which a @SIGKILL@ is issued
-}
defaultStopCommand :: StopArgs
                   -> IO ()
defaultStopCommand StopArgs{..} = do
  progName <- getProgName
  -- pid <- read <$> (readFile shutPidFile)
  if (shutTimeout == Seconds 0)
    then Daemon.brutalKill shutPidFile
    else do putStrLn $ "Sending sigINT to " <> show progName <>
              " and waiting " <> (show $ unSeconds shutTimeout) <> " seconds for graceful stop"
            readFile shutPidFile >>= Sig.signalProcess Sig.sigINT . read
            (Async.race (delaySeconds shutTimeout) checkProcessStatus) >>= \case
              Right _ -> do
                putStrLn $ progName <> " seems to have exited gracefully."
                Exit.exitSuccess
              Left _ -> do
                putStrLn $ progName <> " has still not exited. Sending sigKILL for forced exit."
                Daemon.brutalKill shutPidFile
  where
    checkProcessStatus = do
      Daemon.isRunning shutPidFile >>= \case
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
argParser :: CliType -> Parser Args
argParser cliType = Args <$> (commandParser cliType)

-- ** Top-level command parser

-- | CLI commands are parsed into this data-structure by 'commandParser'
data Command
  = Start CommonStartArgs (Maybe UIStartArgs)
  | Stop StopArgs
  | Status
  deriving (Eq, Show)

-- Parser for 'argsCommand'
commandParser :: CliType -> Parser Command
commandParser cliType = hsubparser
   ( command "start" (info (startCmdParser cliType) (progDesc "start the odd-jobs runner and/or admin UI")) <>
     command "stop" (info stopParser (progDesc "stop the odd-jobs runner and/or admin UI")) <>
     command "status" (info statusParser (progDesc "print status of all active jobs"))
   )


data UIStartArgs = UIStartArgs
  { uistartAuth :: !WebUiAuth
  , uistartPort :: !Int
  } deriving (Eq, Show)

-- ** Start command

-- | @start@ command is parsed into this data-structure by 'startParser'
data CommonStartArgs = CommonStartArgs
  { startDaemonize :: !Bool
    -- | PID file for the background dameon. Ref: 'pidFileParser'
  , startPidFile :: !FilePath
  } deriving (Eq, Show)

uiStartArgsParser :: Parser UIStartArgs
uiStartArgsParser = UIStartArgs
  <$> webUiAuthParser
  <*> option auto ( long "web-ui-port" <>
                    metavar "PORT" <>
                    value 7777 <>
                    showDefault <>
                    help "The port on which the Web UI listens. Please note, to actually enable the Web UI you need to pick one of the available auth schemes"
                  )

commonStartArgsParser :: Parser CommonStartArgs
commonStartArgsParser = CommonStartArgs
  <$> switch ( long "daemonize" <>
               help "Fork the job-runner as a background daemon. If omitted, the job-runner remains in the foreground."
             )
  <*> pidFileParser

startCmdParser :: CliType -> Parser Command
startCmdParser cliType = Start
  <$> commonStartArgsParser
  <*> (case cliType of
         CliOnlyJobRunner _ -> pure Nothing
         CliOnlyWebUi _     -> Just <$> uiStartArgsParser
         CliBoth _ _        -> (Just <$> uiStartArgsParser) <|> (pure Nothing)
      )

data WebUiAuth
  = AuthNone
  | AuthBasic !Text !Text
  deriving (Eq, Show)

-- | Pick one of the following auth mechanisms for the web UI:
--
--   * No auth - @--web-ui-no-auth@  __NOT RECOMMENDED__
--   * Basic auth - @--web-ui-basic-auth-user <USER>@ and
--     @--web-ui-basic-auth-password <PASS>@
webUiAuthParser :: Parser WebUiAuth
webUiAuthParser = basicAuthParser <|> noAuthParser
  where
    basicAuthParser = AuthBasic
      <$> strOption ( long "web-ui-basic-auth-user" <>
                      metavar "USER" <>
                      help "Username for basic auth"
                    )
      <*> strOption ( long "web-ui-basic-auth-password" <>
                      metavar "PASS" <>
                      help "Password for basic auth"
                    )
    noAuthParser = flag' AuthNone
      ( long "web-ui-no-auth" <>
        help "Start the web UI with any authentication. NOT RECOMMENDED."
      )

-- ** Stop command

-- | @stop@ command is parsed into this data-structure by 'stopParser'. Please
-- note, that this command first sends a @SIGINT@ to the daemon and waits for
-- 'shutTimeout' seconds. If the daemon doesn't shut down cleanly within that
-- time, it sends a @SIGKILL@ to kill immediately.
data StopArgs = StopArgs
  { -- | After sending a @SIGINT@, how many seconds to wait before sending a
    -- @SIGKILL@
    shutTimeout :: !Seconds
    -- | PID file of the deamon. Ref: 'pidFileParser'
  , shutPidFile :: !FilePath
  } deriving (Eq, Show)

stopParser :: Parser Command
stopParser = fmap Stop $ StopArgs
  <$> option (Seconds <$> auto) ( long "timeout" <>
                                  metavar "TIMEOUT" <>
                                  help "Maximum seconds to wait before force-killing the background daemon."
                                  -- value defaultTimeout <>
                                  -- showDefaultWith (show . unSeconds)
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

defaultCliInfo :: CliType -> ParserInfo Args
defaultCliInfo cliType =
  info ((argParser cliType)  <**> helper) fullDesc

-- defaultDaemonOptions :: DaemonOptions
-- defaultDaemonOptions = DaemonOptions
--   { daemonShouldChangeDirectory = False
--   , daemonShouldCloseStandardStreams = False
--   , daemonShouldIgnoreSignals = True
--   , daemonUserToChangeTo = Nothing
--   , daemonGroupToChangeTo = Nothing
--   }


-- ** Auth implementations for the default Web UI

-- *** Basic Auth

data OddJobsUser = OddJobsUser !Text !Text deriving (Eq, Show)

defaultBasicAuth :: (Text, Text) -> BasicAuthCheck OddJobsUser
defaultBasicAuth (user, pass) = BasicAuthCheck $ \b ->
  let u = decodeUtf8 (basicAuthUsername b)
      p = decodeUtf8 (basicAuthPassword b)
  in if u==user && p==pass
     then pure (Authorized $ OddJobsUser u p)
     else pure BadPassword
