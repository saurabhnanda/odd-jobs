{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

module OddJobs.ConfigBuilder where

import OddJobs.Types hiding (Config(..))
import qualified OddJobs.Types as Types (Config(..))
import Database.PostgreSQL.Simple as PGS
import Data.Pool
import Control.Monad.Logger (LogLevel(..), LogStr(..), toLogStr)
import Data.Text (Text)
import Lucid (Html(..), toHtml, class_, div_, span_, br_, button_, a_, href_, onclick_)
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


-- | While odd-jobs is highly configurable and the 'Config' data-type might seem
-- daunting at first, it is not necessary to tweak every single configuration
-- parameter by hand. Please start-off by using the sensible defaults provided
-- by the [configuration helpers](#configHelpers), and tweaking
-- config parameters on a case-by-case basis.
data ConfigBuilder = ConfigBuilder
  { -- | The DB table which holds your jobs. Please note, this should have been
    -- created by the 'OddJobs.Migrations.createJobTable' function.
    cfgTableName :: TableName

    -- | The actualy "job-runner" that __you__ need to provide. Please look at
    -- the examples/tutorials if your applicaton's code is not in the @IO@
    -- monad.
  , cfgJobRunner :: Job -> IO ()

    -- | The number of times a failing job is retried before it is considered is
    -- "permanently failed" and ignored by the job-runner. This config parameter
    -- is called "/default/ max attempts" because, in the future, it would be
    -- possible to specify the number of retry-attemps on a per-job basis
    -- (__Note:__ per-job retry-attempts has not been implemented yet)
  , cfgDefaultMaxAttempts :: Maybe Int

    -- | Controls how many jobs can be run concurrently by /this instance/ of
    -- the job-runner. __Please note,__ this is NOT the global concurrency of
    -- entire job-queue. It is possible to have job-runners running on multiple
    -- machines, and each will apply the concurrency control independnt of other
    -- job-runners. TODO: Link-off to relevant section in the tutorial.
  , cfgConcurrencyControl :: ConcurrencyControl

    -- | The DB connection-pool to use for the job-runner. __Note:__ in case
    -- your jobs require a DB connection, please create a separate
    -- connection-pool for them. This pool will be used ONLY for monitoring jobs
    -- and changing their status. We need to have __at least 4 connections__ in
    -- this connection-pool for the job-runner to work as expected. (TODO:
    -- Link-off to tutorial)
  , cfgDbPool :: Pool Connection

    -- | How frequently should the 'jobPoller' check for jobs where the Job's
    -- 'jobRunAt' field indicates that it's time for the job to be executed.
    -- TODO: link-off to the tutorial.
  , cfgPollingInterval :: Maybe Seconds

  -- | User-defined callback function that is called whenever a job succeeds.
  , cfgOnJobSuccess :: Maybe (Job -> IO ())

  -- | User-defined callback function that is called whenever a job fails. This
  -- does not indicate permanent failure and means the job will be retried. It
  -- is a good idea to log the failures to Airbrake, NewRelic, Sentry, or some
  -- other error monitoring tool.
  , cfgOnJobFailed :: forall a . [JobErrHandler a]

  -- | User-defined callback function that is called whenever a job starts
  -- execution.
  , cfgOnJobStart :: Maybe (Job -> IO ())

  -- | User-defined callback function that is called whenever a job times-out.
  -- Also check 'cfgDefaultJobTimeout'
  , cfgOnJobTimeout :: Maybe (Job -> IO ())

  -- | File to store the PID of the job-runner process. This is used only when
  -- invoking the job-runner as an independent background deemon (the usual mode
  -- of deployment). (TODO: Link-off to tutorial).
  , cfgPidFile :: Maybe FilePath

  -- | A "structured logging" function that __you__ need to provide. The
  -- @odd-jobs@ library does NOT use the standard logging interface provided by
  -- 'monad-logger' on purpose. TODO: link-off to tutorial. Also look at
  -- 'cffJobType' and 'defaultLogStr'
  , cfgLogger :: (LogLevel -> LogEvent -> IO ())

  -- | How to extract the "job type" from a 'Job'. Related: 'defaultJobType'
  , cfgJobType :: Maybe (Job -> Text)

    -- | How long can a job run after which it is considered to be "crashed" and
    -- picked up for execution again
  , cfgDefaultJobTimeout :: Maybe Seconds

    -- | How to convert a list of 'Job's to a list of HTML fragments. This is
    -- used in the Web\/Admin UI. This function accepts a /list/ of jobs and
    -- returns a /list/ of 'Html' fragments is because, in case, you need to
    -- query another table to fetch some metadata (eg. convert a primary-key to
    -- a human-readable name), you can do it efficiently instead of resulting in
    -- an N+1 SQL bug. Ref: defaultJobToHtml
  , cfgJobToHtml :: Maybe ([Job] -> IO [Html ()])

    -- | How to get a list of all known job-types?
  , cfgAllJobTypes :: Maybe AllJobTypes

    -- | TODO
  , cfgJobTypeSql :: Maybe PGS.Query
  }

-- | This function gives you a 'Config' with a bunch of sensible defaults
-- already applied. It requies the bare minimum arguments that this library
-- cannot assume on your behalf.
--
-- It makes a few __important assumptions__ about your 'jobPayload 'JSON, which
-- are documented in 'defaultJobType'.
defaultConfigBuilder :: (LogLevel -> LogEvent -> IO ())
                     -- ^ "Structured logging" function. Ref: 'cfgLogger'
                     -> TableName
                     -- ^ DB table which holds your jobs. Ref: 'cfgTableName'
                     -> Pool Connection
                     -- ^ DB connection-pool to be used by job-runner. Ref: 'cfgDbPool'
                     -> ConcurrencyControl
                     -- ^ Concurrency configuration. Ref: 'cfgConcurrencyControl'
                     -> (Job -> IO ())
                     -- ^ The actual "job runner" which contains your application code. Ref: 'cfgJobRunner'
                     -> ConfigBuilder
defaultConfigBuilder logger tname dbpool ccControl jrunner = ConfigBuilder
  { cfgPollingInterval = Nothing
  , cfgOnJobSuccess = Nothing
  , cfgOnJobFailed = []
  , cfgJobRunner = jrunner
  , cfgLogger = logger
  , cfgDbPool = dbpool
  , cfgOnJobStart = Nothing
  , cfgDefaultMaxAttempts = Nothing
  , cfgTableName = tname
  , cfgOnJobTimeout = Nothing
  , cfgConcurrencyControl = ccControl
  , cfgPidFile = Nothing
  , cfgJobType = Nothing
  , cfgDefaultJobTimeout = Nothing
  , cfgJobToHtml = Nothing
  , cfgAllJobTypes = Nothing
  , cfgJobTypeSql = Nothing
  }

finalizeConfig :: ConfigBuilder
               -> Types.Config
finalizeConfig cb =
  let cfg = Types.Config
            { Types.cfgPollingInterval = fromMaybe defaultPollingInterval $ cfgPollingInterval cb
            , Types.cfgOnJobSuccess = fromMaybe (const $ pure ()) $ cfgOnJobSuccess cb
            , Types.cfgOnJobFailed = cfgOnJobFailed cb
            , Types.cfgJobRunner = cfgJobRunner cb
            , Types.cfgLogger = cfgLogger cb
            , Types.cfgDbPool = cfgDbPool cb
            , Types.cfgOnJobStart = fromMaybe (const $ pure ()) $ cfgOnJobStart cb
            , Types.cfgDefaultMaxAttempts = fromMaybe 10 $ cfgDefaultMaxAttempts cb
            , Types.cfgTableName = cfgTableName cb
            , Types.cfgOnJobTimeout = fromMaybe (const $ pure ()) $ cfgOnJobTimeout cb
            , Types.cfgConcurrencyControl = cfgConcurrencyControl cb
            , Types.cfgPidFile = cfgPidFile cb
            , Types.cfgJobType = fromMaybe defaultJobType $ cfgJobType cb
            , Types.cfgDefaultJobTimeout = fromMaybe (Seconds 600) $ cfgDefaultJobTimeout cb
            , Types.cfgJobToHtml = fromMaybe (defaultJobToHtml (Types.cfgJobType cfg)) $ cfgJobToHtml cb
            , Types.cfgAllJobTypes = fromMaybe (defaultDynamicJobTypes (Types.cfgTableName cfg) (Types.cfgJobTypeSql cfg)) $ cfgAllJobTypes cb
            , Types.cfgJobTypeSql = fromMaybe defaultJobTypeSql $ cfgJobTypeSql cb
            }
  in cfg

  -- TODO; Should the library be doing this?
defaultLogStr :: (Job -> Text)
              -> LogLevel
              -> LogEvent
              -> LogStr
defaultLogStr jobToText logLevel logEvent =
  (toLogStr $ show logLevel) <> " | " <> str
  where
    jobToLogStr j = toLogStr $ jobToText j
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
      LogText t ->
        toLogStr t

defaultJobToHtml :: (Job -> Text)
                 -> [Job]
                 -> IO [Html ()]
defaultJobToHtml jobType js =
  pure $ DL.map jobToHtml js
  where
    jobToHtml :: Job -> Html ()
    jobToHtml j = do
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
    forM_ (HM.toList o) $ \(k, v) -> do
      span_ [ class_ " key-value-pair " ] $ do
        span_ [ class_ "key" ] $ toHtml $ k <> ":"
        span_ [ class_ "value" ] $ defaultPayloadToHtml v
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
  fmap (DL.map ((fromMaybe "(unknown)") . fromOnly)) $ PGS.query_ conn $ "select distinct(" <> jobTypeSql <> ") from " <> tname <> " order by 1 nulls last"

-- | Used only by 'defaultLogStr' now. TODO: Is this even required anymore?
-- Should this be removed?
defaultJobToText :: (Job -> Text) -> Job -> Text
defaultJobToText jobTypeFn job@Job{jobId} =
  "JobId=" <> (toS $ show jobId) <> " JobType=" <> jobTypeFn job


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
-- doing. TODO: link-off to tutorial.
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
          createPool (PGS.connectPostgreSQL connString) PGS.close 1 (fromIntegral $ 2 * (unSeconds defaultPollingInterval)) 5
        Right connInfo ->
          createPool (PGS.connect connInfo) PGS.close 1 (fromIntegral $ 2 * (unSeconds defaultPollingInterval)) 5

-- | TODO: Should the library be doing this?
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

