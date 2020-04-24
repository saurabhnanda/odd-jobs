module OddJobs.Cli where

import Options.Applicative as Opts
import Data.Text
import OddJobs.Job (runJobMonitor, JobMonitor)

data StartArgs = StartArgs
  { startWebUiEnable :: Bool
  } deriving (Eq, Show)

data ShutdownArgs = ShutdownArgs
  { shutImmediate :: Bool
  } deriving (Eq, Show)

data Command
  = Start StartArgs
  | Shutdown ShutdownArgs
  deriving (Eq, Show)

data Args = Args
  { argsCommand :: Command
  } deriving (Eq, Show)


argParser :: Parser Args
argParser = Args
  <$> commandParser

commandParser :: Parser Command
commandParser = hsubparser
   ( command "start" (info startParser ( progDesc "start the odd-jobs runner")) <>
     command "shutdown" (info shutdownParser (progDesc "shutdown the odd-jobs runner"))
   )

startParser :: Parser Command
startParser = (Start . StartArgs)
  <$> flag False True ( long "web-ui-enable" <>
                        help "Please look at other web-ui-* options to configure the Web UI"
                      )

shutdownParser :: Parser Command
shutdownParser = (Shutdown . ShutdownArgs)
  <$> switch ( long "immediate" <>
               help "Don't wait for currently running job-threads to complete. WARNING: Might leave a bunch of jobs in the 'locked' state"
             )

parseCliArgs :: IO Args
parseCliArgs = execParser $ Opts.info (argParser <**> helper) $
  Opts.progDesc "Start/stop the odd-jobs runner (standalone binary)" <>
  Opts.fullDesc

defaultCliParserPrefs :: ParserPrefs
defaultCliParserPrefs = prefs $
  showHelpOnError <>
  showHelpOnEmpty

defaultCliInfo :: ParserInfo Args
defaultCliInfo =
  info (argParser  <**> helper) fullDesc

defaultMain :: JobMonitor -> IO ()
defaultMain jm = do
  Args{argsCommand} <- customExecParser defaultCliParserPrefs defaultCliInfo
  case argsCommand of
    Start _ ->
      runJobMonitor jm
    Shutdown _ ->
      Prelude.error "not implemented yet"
