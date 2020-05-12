{-# LANGUAGE TypeOperators, DeriveGeneric, NamedFieldPuns, DataKinds, StandaloneDeriving, FlexibleContexts, RecordWildCards, RankNTypes #-}


-- | TODO: Rename this to OddJobs.Servant

module OddJobs.Endpoints where

import OddJobs.Web as Web hiding (Routes(..))
import qualified OddJobs.Web as Web
import OddJobs.Job as Job
import OddJobs.Types
import GHC.Generics

import Servant
import Servant.API.Generic
import Servant.Server.Generic

import Servant.HTML.Lucid
import Lucid
import Lucid.Html5
import Lucid.Base
import Data.Text as T
import Network.Wai.Handler.Warp   (run)
import Servant.Server.StaticFiles (serveDirectoryFileServer)
import UnliftIO hiding (Handler)
import Database.PostgreSQL.Simple as PGS
import Data.Pool as Pool
import Control.Monad.Reader
import Data.String.Conv (toS)
import Control.Monad.Except
import Data.Time as Time
import Data.Aeson as Aeson
import qualified Data.HashMap.Strict as HM
import GHC.Exts (toList)
import Data.Maybe (fromMaybe, mapMaybe)
import Control.Applicative ((<|>))
-- import qualified OddJobs.Links as Links
import Data.List ((\\))
import qualified System.Log.FastLogger as FLogger
import qualified System.Log.FastLogger.Date as FLogger
import Control.Monad.Logger as MLogger
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as DL
import UnliftIO.IORef
import Debug.Trace
import qualified OddJobs.ConfigBuilder as Builder
import Servant.Static.TH (createApiAndServerDecs)

-- startApp :: IO ()
-- startApp = undefined

-- stopApp :: IO ()
-- stopApp = undefined

$(createApiAndServerDecs "StaticAssetRoutes" "staticAssetServer" "assets")

data Routes route = Routes
  { rFilterResults :: route :- QueryParam "filters" Web.Filter :> Get '[HTML] (Html ())
  , rEnqueue :: route :- "enqueue" :> Capture "jobId" JobId :> Post '[HTML] NoContent
  , rRunNow :: route :- "run" :> Capture "jobId" JobId :> Post '[HTML] NoContent
  , rCancel :: route :- "cancel" :> Capture "jobId" JobId :> Post '[HTML] NoContent
  , rRefreshJobTypes :: route :- "refresh-job-types" :> Post '[HTML] NoContent
  , rRefreshJobRunners :: route :- "refresh-job-runners" :> Post '[HTML] NoContent
  } deriving (Generic)


type FinalAPI =
  (ToServant Routes AsApi) :<|>
  "assets" :> StaticAssetRoutes

data Env = Env
  { envRoutes :: Web.Routes
  , envJobTypesRef :: IORef [Text]
  , envJobRunnersRef :: IORef [JobRunnerName]
  }

mkEnv :: (MonadIO m) => Config -> (Text -> Text) -> m Env
mkEnv cfg@Config{..} linksFn = do
  allJobTypes <- fetchAllJobTypes cfg
  allJobRunners <- fetchAllJobRunners cfg
  envJobTypesRef <- newIORef allJobTypes
  envJobRunnersRef <- newIORef allJobRunners
  let envRoutes = routes linksFn

  pure Env{..}
  -- TODO: remove hard-coded port
  -- run 8080 $ genericServe (server cfg Env{..})

  -- let nt :: ReaderT Job.Config IO a -> Servant.Handler a
  --     nt action = (liftIO $ try $ runReaderT action jm) >>= \case
  --       Left (e :: SomeException) -> Servant.Handler  $ ExceptT $ pure $ Left $ err500 { errBody = toS $ show e }
  --       Right a -> Servant.Handler $ ExceptT $ pure $ Right a
  --     appProxy = (Proxy :: Proxy (ToServant Routes AsApi))

  -- finally
  --   (run 8080 $ genericServe (server jm dbPool jobTypesRef jobRunnerRef))
  --   (cleanup >> (Pool.destroyAllResources dbPool))

stopApp :: IO ()
stopApp = pure ()


server :: forall m . (MonadIO m)
       => Config
       -> Env
       -> (forall a . Handler a -> m a)
       -> ServerT FinalAPI m
server cfg env nt =
  (toServant routeServer) :<|> staticAssetServer
  where
    routeServer :: Routes (AsServerT m)
    routeServer = Routes
      { rFilterResults = nt . (filterResults cfg env)
      , rEnqueue = nt . (enqueueJob cfg env)
      , rCancel = nt . (cancelJob cfg env)
      , rRunNow = nt . (runJobNow cfg env)
      , rRefreshJobTypes = nt $ refreshJobTypes cfg env
      , rRefreshJobRunners = nt $ refreshJobRunners cfg env
      }

server2 :: Config
        -> Env
        -> Routes AsServer
server2 cfg env = Routes
  { rFilterResults = (filterResults cfg env)
  , rEnqueue = (enqueueJob cfg env)
  , rCancel = (cancelJob cfg env)
  , rRunNow = (runJobNow cfg env)
  , rRefreshJobTypes = refreshJobTypes cfg env
  , rRefreshJobRunners = refreshJobRunners cfg env
  }


refreshJobRunners :: Config
                  -> Env
                  -> Handler NoContent
refreshJobRunners cfg@Config{..} Env{envRoutes=Web.Routes{..}, envJobRunnersRef} = do
  allJobRunners <- fetchAllJobRunners cfg
  atomicModifyIORef' envJobRunnersRef (\_ -> (allJobRunners, ()))
  throwError $ err302{errHeaders=[("Location", toS $ rFilterResults Nothing)]}

refreshJobTypes :: Config
                -> Env
                -> Handler NoContent
refreshJobTypes cfg Env{envRoutes=Web.Routes{..}, envJobTypesRef} = do
  allJobTypes <- fetchAllJobTypes cfg
  atomicModifyIORef' envJobTypesRef (\_ -> (allJobTypes, ()))
  throwError $ err302{errHeaders=[("Location", toS $ rFilterResults Nothing)]}

cancelJob :: Config
          -> Env
          -> JobId
          -> Handler NoContent
cancelJob Config{..} env jid = do
  liftIO $ withResource cfgDbPool $ \conn -> void $ cancelJobIO conn cfgTableName jid
  redirectToHome env

runJobNow :: Config
          -> Env
          -> JobId
          -> Handler NoContent
runJobNow Config{..} env jid = do
  liftIO $ withResource cfgDbPool $ \conn -> void $ runJobNowIO conn cfgTableName jid
  redirectToHome env

enqueueJob :: Config
           -> Env
           -> JobId
           -> Handler NoContent
enqueueJob Config{..} env jid = do
  liftIO $ withResource cfgDbPool $ \conn -> do
    void $ unlockJobIO conn cfgTableName jid
    void $ runJobNowIO conn cfgTableName jid
  redirectToHome env

redirectToHome :: Env -> Handler NoContent
redirectToHome Env{envRoutes=Web.Routes{..}} = do
  throwError $ err301{errHeaders=[("Location", toS $ rFilterResults Nothing)]}


filterResults :: Config
              -> Env
              -> Maybe Filter
              -> Handler (Html ())
filterResults cfg@Config{cfgJobToHtml, cfgDbPool} Env{..}  mFilter = do
  let filters = fromMaybe mempty mFilter
  (jobs, runningCount) <- liftIO $ Pool.withResource cfgDbPool $ \conn -> (,)
    <$> (filterJobs cfg conn filters)
    <*> (countJobs cfg conn filters{ filterStatuses = [Job.Locked] })
  t <- liftIO getCurrentTime
  js <- liftIO $ fmap (DL.zip jobs) $ cfgJobToHtml jobs
  allJobTypes <- readIORef envJobTypesRef
  let navHtml = Web.sideNav envRoutes allJobTypes [] t filters
      bodyHtml = Web.resultsPanel envRoutes t filters js runningCount
  pure $ Web.pageLayout envRoutes navHtml bodyHtml

routes :: (Text -> Text) -> Web.Routes
routes linkFn = Web.Routes
  { Web.rFilterResults = rFilterResults
  , Web.rEnqueue = rEnqueue
  , Web.rRunNow = rRunNow
  , Web.rCancel = rCancel
  , Web.rRefreshJobTypes = rRefreshJobTypes
  , Web.rRefreshJobRunners = rRefreshJobRunners
  , Web.rStaticAsset = linkFn
  }
  where
    OddJobs.Endpoints.Routes{..} = allFieldLinks' (linkFn . toS . show . linkURI) :: OddJobs.Endpoints.Routes (AsLink Text)

-- absText :: Link -> Text
-- absText l = "/" <> (toS $ show $ linkURI l)
