{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

module OddJobs.ConfigBuilder where

import OddJobs.Types
import Database.PostgreSQL.Simple as PGS
import Data.Pool
import Control.Monad.Logger (LogLevel(..), LogStr, toLogStr)
import Data.Text (Text)
import Lucid (Html, toHtml, class_, div_, span_, br_, button_, a_, href_, onclick_)
import Data.Maybe (fromMaybe)
import Data.List as DL
import Data.Aeson as Aeson hiding (Success)
import qualified Data.Text as T
import qualified Data.HashMap.Lazy as HM
import GHC.Generics
import Data.Proxy (Proxy(..))
import Generics.Deriving.ConNames
import Control.Monad
import Data.String.Conv
import GHC.Exts (toList)
import qualified Data.ByteString as BS
import UnliftIO (MonadUnliftIO, withRunInIO, bracket, liftIO)
import qualified System.Log.FastLogger as FLogger


-- | This function gives you a 'Config' with a bunch of sensible defaults
-- already applied. It requires the bare minimum configuration parameters that
-- this library cannot assume on your behalf.
--
-- It makes a few __important assumptions__ about your 'jobPayload 'JSON, which
-- are documented in 'defaultJobType'.
mkConfig :: (LogLevel -> LogEvent -> IO ())
         -- ^ "Structured logging" function. Ref: 'cfgLogger'
         -> TableName
         -- ^ DB table which holds your jobs. Ref: 'cfgTableName'
         -> Pool Connection
         -- ^ DB connection-pool to be used by job-runner. Ref: 'cfgDbPool'
         -> ConcurrencyControl
         -- ^ Concurrency configuration. Ref: 'cfgConcurrencyControl'
         -> (Job -> IO ())
         -- ^ The actual "job runner" which contains your application code. Ref: 'cfgJobRunner'
         -> (Config -> Config)
         -- ^ A function that allows you to modify the \"interim config\". The
         -- \"interim config\" will cotain a bunch of in-built default config
         -- params, along with the config params that you\'ve just provided
         -- (i.e. logging function, table name, DB pool, etc). You can use this
         -- function to override values in the \"interim config\". If you do not
         -- wish to modify the \"interim config\" just pass 'Prelude.id' as an
         -- argument to this parameter. __Note:__ it is strongly recommended
         -- that you __do not__ modify the generated 'Config' outside of this
         -- function, unless you know what you're doing.
         -> Config
         -- ^ The final 'Config' that can be used to start various job-runners
mkConfig logger tname dbpool ccControl jrunner configOverridesFn =
  let cfg = configOverridesFn $ Config
            { cfgPollingInterval = defaultPollingInterval
            , cfgOnJobSuccess = (const $ pure ())
            , cfgOnJobFailed = []
            , cfgJobRunner = jrunner
            , cfgLogger = logger
            , cfgDbPool = dbpool
            , cfgOnJobStart = (const $ pure ())
            , cfgDefaultMaxAttempts = 10
            , cfgTableName = tname
            , cfgOnJobTimeout = (const $ pure ())
            , cfgConcurrencyControl = ccControl
            , cfgJobType = defaultJobType
            , cfgDefaultJobTimeout = Seconds 600
            }
  in cfg


mkUIConfig :: (LogLevel -> LogEvent -> IO ())
           -- ^ "Structured logging" function. Ref: 'uicfgLogger'
           -> TableName
           -- ^ DB table which holds your jobs. Ref: 'uicfgTableName'
           -> Pool Connection
           -- ^ DB connection-pool to be used by web UI. Ref: 'uicfgDbPool'
           -> (UIConfig -> UIConfig)
           -- ^ A function that allows you to modify the \"interim UI config\".
           -- The \"interim config\" will cotain a bunch of in-built default
           -- config params, along with the config params that you\'ve just
           -- provided (i.e. logging function, table name, DB pool, etc). You
           -- can use this function to override values in the \"interim UI
           -- config\". If you do not wish to modify the \"interim UI config\"
           -- just pass 'Prelude.id' as an argument to this parameter. __Note:__
           -- it is strongly recommended that you __do not__ modify the
           -- generated 'Config' outside of this function, unless you know what
           -- you're doing.
           -> UIConfig
           -- ^ The final 'UIConfig' that needs to be passed to
           -- 'OddJobs.Endpoint.mkEnv' and 'OddJobs.Cli.defaultWebUI'
mkUIConfig logger tname dbpool configOverridesFn =
  let cfg = configOverridesFn $ UIConfig
            { uicfgLogger = logger
            , uicfgDbPool = dbpool
            , uicfgTableName = tname
            , uicfgJobType = defaultJobType
            , uicfgJobToHtml = defaultJobsToHtml (uicfgJobType cfg)
            , uicfgAllJobTypes = (defaultDynamicJobTypes (uicfgTableName cfg) (uicfgJobTypeSql cfg))
            , uicfgJobTypeSql = defaultJobTypeSql
            }
  in cfg




-- | If you aren't interested in structured logging, you can use this function
-- to emit plain-text logs (or define your own).
defaultLogStr :: (Job -> Text)
              -> LogLevel
              -> LogEvent
              -> LogStr
defaultLogStr jobTypeFn logLevel logEvent =
  (toLogStr $ show logLevel) <> " | " <> str
  where
    jobToLogStr job@Job{jobId} =
      "JobId=" <> (toLogStr $ show jobId) <> " JobType=" <> (toLogStr $ jobTypeFn job)

    str = case logEvent of
      LogJobStart j ->
        "Started | " <> jobToLogStr j
      LogJobFailed j e fm t ->
        let tag = case fm of
                    FailWithRetry -> "Failed (retry)"
                    FailPermanent -> "Failed (permanent)"
        in tag <> " | " <> jobToLogStr j <> " | runtime=" <> (toLogStr $ show t) <> " | error=" <> (toLogStr $ show e)
      LogJobSuccess j t ->
        "Success | " <> (jobToLogStr j) <> " | runtime=" <> (toLogStr $ show t)
      LogJobTimeout j@Job{jobLockedAt, jobLockedBy} ->
        "Timeout | " <> jobToLogStr j <> " | lockedBy=" <> (toLogStr $ maybe  "unknown" unJobRunnerName jobLockedBy) <>
        " lockedAt=" <> (toLogStr $ maybe "unknown" show jobLockedAt)
      LogPoll ->
        "Polling jobs table"
      LogWebUIRequest ->
        "WebUIRequest (TODO: Log the actual request)"
      LogText t ->
        toLogStr t

defaultJobsToHtml :: (Job -> Text)
                  -> [Job]
                  -> IO [Html ()]
defaultJobsToHtml jobType js = pure $ DL.map (defaultJobToHtml jobType) js


defaultJobToHtml :: (Job -> Text)
                 -> Job
                 -> Html ()
defaultJobToHtml jobType j =
  div_ [ class_ "job" ] $ do
    div_ [ class_ "job-type" ] $ do
      toHtml $ jobType j
    div_ [ class_ "job-payload" ] $ do
      defaultPayloadToHtml $ defaultJobContent $ jobPayload j
    case jobLastError j of
      Nothing -> mempty
      Just e -> do
        div_ [ class_ "job-error collapsed" ] $ do
          a_ [ href_ "javascript: void(0);", onclick_ "toggleError(this)" ] $ do
            span_ [ class_ "badge badge-secondary error-expand" ] "+ Last error"
            span_ [ class_ "badge badge-secondary error-collapse d-none" ] "- Last error"
          " "
          defaultErrorToHtml e


defaultErrorToHtml :: Value -> Html ()
defaultErrorToHtml e =
  case e of
    Aeson.String s -> handleLineBreaks s
    Aeson.Bool b -> toHtml $ show b
    Aeson.Number n -> toHtml $ show n
    Aeson.Null -> toHtml ("(null)" :: Text)
    Aeson.Object o -> toHtml $ show o -- TODO: handle this properly
    Aeson.Array a -> toHtml $ show a -- TODO: handle this properly
  where
    handleLineBreaks s = do
      forM_ (T.splitOn "\n" s) $ \x -> do
        toHtml x
        br_ []

defaultJobContent :: Value -> Value
defaultJobContent v = case v of
  Aeson.Object o -> case HM.lookup "contents" o of
    Nothing -> v
    Just c -> c
  _ -> v

defaultPayloadToHtml :: Value -> Html ()
defaultPayloadToHtml v = case v of
  Aeson.Object o -> do
    toHtml ("{ " :: Text)
    forM_ (HM.toList o) $ \(k, v2) -> do
      span_ [ class_ " key-value-pair " ] $ do
        span_ [ class_ "key" ] $ toHtml $ k <> ":"
        span_ [ class_ "value" ] $ defaultPayloadToHtml v2
    toHtml (" }" :: Text)
  Aeson.Array a -> do
    toHtml ("[" :: Text)
    forM_ (toList a) $ \x -> do
      defaultPayloadToHtml x
      toHtml (", " :: Text)
    toHtml ("]" :: Text)
  Aeson.String t -> toHtml t
  Aeson.Number n -> toHtml $ show n
  Aeson.Bool b -> toHtml $ show b
  Aeson.Null -> toHtml ("null" :: Text)

defaultJobTypeSql :: PGS.Query
defaultJobTypeSql = "payload->>'tag'"

defaultConstantJobTypes :: forall a . (Generic a, ConNames (Rep a))
                         => Proxy a
                         -> AllJobTypes
defaultConstantJobTypes _ =
  AJTFixed $ DL.map toS $ conNames (undefined :: a)

defaultDynamicJobTypes :: TableName
                       -> PGS.Query
                       -> AllJobTypes
defaultDynamicJobTypes tname jobTypeSql = AJTSql $ \conn -> do
  fmap (DL.map ((fromMaybe "(unknown)") . fromOnly)) $ PGS.query conn ("select distinct(" <> jobTypeSql <> ") from ? order by 1 nulls last") (Only tname)

-- | This makes __two important assumptions__. First, this /assumes/ that jobs
-- in your app are represented by a sum-type. For example:
--
-- @
-- data MyJob = SendWelcomeEmail Int
--            | SendPasswordResetEmail Text
--            | SetupSampleData Int
-- @
--
-- Second, it /assumes/ that the JSON representatin of this sum-type is
-- "tagged". For example, the following...
--
-- > let pload = SendWelcomeEmail 10
--
-- ...when converted to JSON, would look like...
--
-- > {"tag":"SendWelcomeEmail", "contents":10}
--
-- It uses this assumption to extract the "job type" from a 'Data.Aeson.Value'
-- (which would be @SendWelcomeEmail@ in the example given above). This is used
-- in logging and the admin UI.
--
-- Even if tihs assumption is violated, the job-runner /should/ continue to
-- function. It's just that you won't get very useful log messages.
--
-- __Note:__ If your job payload does not conform to the structure described
-- above, please read the section on [customising the job payload's
-- structure](https://www.haskelltutorials.com/odd-jobs/guide.html#custom-payload-structure)
-- in the implementation guide.
defaultJobType :: Job -> Text
defaultJobType Job{jobPayload} =
  case jobPayload of
    Aeson.Object hm -> case HM.lookup "tag" hm of
      Just (Aeson.String t) -> t
      _ -> "unknown"
    _ -> "unknown"


-- | As the name says. Ref: 'cfgPollingInterval'
defaultPollingInterval :: Seconds
defaultPollingInterval = Seconds 5

-- | Convenience function to create a DB connection-pool with some sensible
-- defaults. Please see the source-code of this function to understand what it's
-- doing.
withConnectionPool :: (MonadUnliftIO m)
                   => Either BS.ByteString PGS.ConnectInfo
                   -> (Pool PGS.Connection -> m a)
                   -> m a
withConnectionPool connConfig action = withRunInIO $ \runInIO -> do
  bracket poolCreator destroyAllResources (runInIO . action)
  where
    poolCreator = liftIO $
      case connConfig of
        Left connString ->
          createPool (PGS.connectPostgreSQL connString) PGS.close 1 (fromIntegral $ 2 * (unSeconds defaultPollingInterval)) 8
        Right connInfo ->
          createPool (PGS.connect connInfo) PGS.close 1 (fromIntegral $ 2 * (unSeconds defaultPollingInterval)) 8

-- | A convenience function to help you define a timed-logger with some sensible
-- defaults.
defaultTimedLogger :: FLogger.TimedFastLogger
                   -> (LogLevel -> LogEvent -> LogStr)
                   -> LogLevel
                   -> LogEvent
                   -> IO ()
defaultTimedLogger logger logStrFn logLevel logEvent =
  if logLevel == LevelDebug
  then pure ()
  else logger $ \t -> (toLogStr t) <> " | " <>
                      (logStrFn logLevel logEvent) <>
                      "\n"


defaultJsonLogEvent :: LogEvent -> Aeson.Value
defaultJsonLogEvent logEvent =
  case logEvent of
    LogJobStart job ->
      Aeson.object [ "tag" Aeson..= ("LogJobStart" :: Text)
                   , "contents" Aeson..= (defaultJsonJob job) ]
    LogJobSuccess job runTime ->
      Aeson.object [ "tag" Aeson..= ("LogJobSuccess" :: Text)
                   , "contents" Aeson..= (defaultJsonJob job, runTime) ]
    LogJobFailed job e fm runTime ->
      Aeson.object [ "tag" Aeson..= ("LogJobFailed" :: Text)
                   , "contents" Aeson..= (defaultJsonJob job, show e, defaultJsonFailureMode fm, runTime) ]
    LogJobTimeout job ->
      Aeson.object [ "tag" Aeson..= ("LogJobTimeout" :: Text)
                   , "contents" Aeson..= (defaultJsonJob job) ]
    LogPoll ->
      Aeson.object [ "tag" Aeson..= ("LogJobPoll" :: Text)]
    LogWebUIRequest ->
      Aeson.object [ "tag" Aeson..= ("LogWebUIRequest" :: Text)]
    LogText t ->
      Aeson.object [ "tag" Aeson..= ("LogText" :: Text)
                   , "contents" Aeson..= t ]

defaultJsonJob :: Job -> Aeson.Value
defaultJsonJob job = genericToJSON Aeson.defaultOptions job

defaultJsonFailureMode :: FailureMode -> Aeson.Value
defaultJsonFailureMode fm = genericToJSON Aeson.defaultOptions fm
