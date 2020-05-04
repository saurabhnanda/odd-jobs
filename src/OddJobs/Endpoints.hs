{-# LANGUAGE TypeOperators, DeriveGeneric, NamedFieldPuns, DataKinds, StandaloneDeriving, FlexibleContexts, RecordWildCards #-}

module OddJobs.Endpoints where

import OddJobs.Web as Web
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
import Data.Time.Format.Human (humanReadableTime')
import Data.Aeson as Aeson
import qualified Data.HashMap.Strict as HM
import GHC.Exts (toList)
import Data.Maybe (fromMaybe)
import Data.Text.Conversions (fromText, toText)
import Control.Applicative ((<|>))
import Data.Time.Convenience (timeSince, Unit(..), Direction(..))
import qualified OddJobs.Links as Links
import Data.List ((\\))
import qualified System.Log.FastLogger as FLogger
import qualified System.Log.FastLogger.Date as FLogger
import Control.Monad.Logger as MLogger
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as DL
import UnliftIO.IORef
import Debug.Trace

-- startApp :: IO ()
-- startApp = undefined

-- stopApp :: IO ()
-- stopApp = undefined

tname :: TableName
tname = "jobs_aqgrqtaowi"

startApp :: IO ()
startApp = do
  let connInfo = ConnectInfo
                 { connectHost = "localhost"
                 , connectPort = fromIntegral 5432
                 , connectUser = "jobs_test"
                 , connectPassword = "jobs_test"
                 , connectDatabase = "jobs_test"
                 }
  dbPool <- createPool
    (PGS.connect connInfo)  -- cretea a new resource
    (PGS.close)             -- destroy resource
    1                       -- stripes
    (fromRational 10)       -- number of seconds unused resources are kept around
    5                     -- maximum open connections

  tcache <- FLogger.newTimeCache FLogger.simpleTimeFormat'
  (tlogger, cleanup) <- FLogger.newTimedFastLogger tcache (FLogger.LogStdout FLogger.defaultBufSize)
  let flogger = Job.defaultTimedLogger tlogger (Job.defaultLogStr (Job.defaultJobToText Job.defaultJobType))
      jm = Job.defaultConfig flogger tname dbPool Job.UnlimitedConcurrentJobs (const $ pure ())

  allJobTypes <- fetchAllJobTypes jm dbPool
  jobTypesRef <- newIORef allJobTypes

  -- let nt :: ReaderT Job.Config IO a -> Servant.Handler a
  --     nt action = (liftIO $ try $ runReaderT action jm) >>= \case
  --       Left (e :: SomeException) -> Servant.Handler  $ ExceptT $ pure $ Left $ err500 { errBody = toS $ show e }
  --       Right a -> Servant.Handler $ ExceptT $ pure $ Right a
  --     appProxy = (Proxy :: Proxy (ToServant Routes AsApi))

  finally
    (run 8080 $ genericServe (server jm dbPool jobTypesRef))
    (cleanup >> (Pool.destroyAllResources dbPool))

stopApp :: IO ()
stopApp = pure ()

server :: Config
       -> Pool Connection
       -> IORef [Text]
       -> Routes AsServer
server config dbPool jobTypesRef = Routes
  { rFilterResults = (\mFilter -> filterResults config dbPool jobTypesRef mFilter)
  , rStaticAssets = serveDirectoryFileServer "assets"
  , rEnqueue = enqueueJob config dbPool
  , rCancel = cancelJob config dbPool
  , rRunNow = runJobNow config dbPool
  , rRefreshJobTypes = refreshJobTypes config dbPool jobTypesRef
  }

refreshJobTypes :: Config
                -> Pool Connection
                -> IORef [Text]
                -> Handler NoContent
refreshJobTypes cfg dbPool jobTypesRef = do
  allJobTypes <- fetchAllJobTypes cfg dbPool
  atomicModifyIORef' jobTypesRef (\_ -> (allJobTypes, ()))
  throwError $ err301{errHeaders=[("Location", toS $ Links.rFilterResults Nothing)]}

cancelJob :: Config
          -> Pool Connection
          -> JobId
          -> Handler NoContent
cancelJob cfg dbPool jid = do
  updateHelper cfg dbPool (Failed, jid, In [Queued, Retry])

runJobNow :: Config
          -> Pool Connection
          -> JobId
          -> Handler NoContent
runJobNow cfg dbPool jid = do
  updateHelper cfg dbPool (Queued, jid, In [Queued, Retry])


enqueueJob :: Config
           -> Pool Connection
           -> JobId
           -> Handler NoContent
enqueueJob cfg dbPool jid = do
  updateHelper cfg dbPool (Queued, jid, In [Locked, Failed])

updateHelper :: Config
             -> Pool Connection
             -> (Status, JobId, In [Status])
             -> Handler NoContent
updateHelper Config{cfgTableName} dbPool  args = do
  liftIO $ void $ withResource dbPool $ \conn -> do
    PGS.execute conn (updateQuery tname) args
  throwError $ err301{errHeaders=[("Location", toS $ Links.rFilterResults Nothing)]}


fetchAllJobTypes :: (MonadIO m)
                 => Config
                 -> Pool Connection
                 -> m [Text]
fetchAllJobTypes Config{cfgAllJobTypes} dbPool = liftIO $ do
  case cfgAllJobTypes of
    AJTFixed jts -> pure jts
    AJTSql fn -> withResource dbPool fn
    AJTCustom fn -> fn

updateQuery :: TableName -> PGS.Query
updateQuery tname = "update " <> tname <> " set locked_at=null, locked_by=null, attempts=0, status=?, run_at=now() where id=? and status in ?"

filterResults :: Config
              -> Pool Connection
              -> IORef [Text]
              -> Maybe Filter
              -> Handler (Html ())
filterResults cfg@Config{cfgJobToHtml} dbPool jobTypesRef mFilter = do
  let filters = fromMaybe mempty mFilter
  (jobs, runningCount) <- liftIO $ Pool.withResource dbPool $ \conn -> (,)
    <$> (filterJobs cfg conn filters)
    <*> (countJobs cfg conn filters{ filterStatuses = [Job.Locked] })
  t <- liftIO getCurrentTime
  js <- liftIO $ fmap (DL.zip jobs) $ cfgJobToHtml jobs
  allJobTypes <- readIORef jobTypesRef
  let navHtml = sideNav allJobTypes t filters
      bodyHtml = resultsPanel t filters js runningCount
  pure $ pageLayout navHtml bodyHtml


pageNav :: Html ()
pageNav = do
  div_ $ nav_ [ class_ "navbar navbar-default navigation-clean" ] $ div_ [ class_ "container-fluid" ] $ do
    div_ [ class_ "navbar-header" ] $ do
      a_ [ class_ "navbar-brand navbar-link", href_ "#", style_ "margin-left: 2px; padding: 0px;" ] $ img_ [ src_ "/assets/odd-jobs-color-logo.png", title_ "Odd Jobs Logo" ]
      button_ [ class_ "navbar-toggle collapsed", data_ "toggle" "collapse", data_ "target" "#navcol-1" ] $ do
        span_ [ class_ "sr-only" ] $ "Toggle navigation"
        span_ [ class_ "icon-bar" ] $ ""
        span_ [ class_ "icon-bar" ] $ ""
        span_ [ class_ "icon-bar" ] $ ""
    -- div_ [ class_ "collapse navbar-collapse", id_ "navcol-1" ] $ ul_ [ class_ "nav navbar-nav navbar-right" ] $ do
    --   li_ [ class_ "active", role_ "presentation" ] $ a_ [ href_ "#" ] $ "First Item"
    --   li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Second Item"
    --   li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Third Item"
    --   li_ [ class_ "dropdown" ] $ do
    --     a_ [ class_ "dropdown-toggle", data_ "toggle" "dropdown", ariaExpanded_ "false", href_ "#" ] $ do
    --       "Dropdown"
    --       span_ [ class_ "caret" ] $ ""
    --     ul_ [ class_ "dropdown-menu", role_ "menu" ] $ do
    --       li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "First Item"
    --       li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Second Item"
    --       li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Third Item"

pageLayout :: Html() -> Html () -> Html ()
pageLayout navHtml bodyHtml = do
  doctype_
  html_ $ do
    head_ $ do
      meta_ [ charset_ "utf-8" ]
      meta_ [ name_ "viewport", content_ "width=device-width, initial-scale=1.0" ]
      title_ "haskell-pg-queue"
      link_ [ rel_ "stylesheet", href_ "assets/bootstrap/css/bootstrap.min.css" ]
      link_ [ rel_ "stylesheet", href_ "https://fonts.googleapis.com/css?family=Lato:100i,300,300i,400,700,900" ]
      -- link_ [ rel_ "stylesheet", href_ "assets/css/logo-slider.css" ]
      -- link_ [ rel_ "stylesheet", href_ "assets/css/Navigation-Clean1.css" ]
      link_ [ rel_ "stylesheet", href_ "assets/css/styles.css" ]
    body_ $ do
      pageNav
      div_ $ div_ [ class_ "container-fluid", style_ "/*background-color:#f2f2f2;*/" ] $ div_ [ class_ "row" ] $ do
        div_ [ class_ "d-none d-md-block col-md-2" ] navHtml
        div_ [ class_ "col-12 col-md-10" ] bodyHtml
      script_ [ src_ "assets/js/jquery.min.js" ] $ ("" :: Text)
      script_ [ src_ "assets/bootstrap/js/bootstrap.min.js" ] $ ("" :: Text)
      -- script_ [ src_ "https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.6.0/slick.js" ] $ ("" :: Text)
      -- script_ [ src_ "assets/js/logo-slider.js" ] $ ("" :: Text)

sideNav :: [Text] -> UTCTime -> Filter -> Html ()
sideNav jobTypes t filter@Filter{..} = do
  div_ [ class_ "filters mt-3" ] $ do
    jobStatusFilters
    jobTypeFilters
    jobRunnerFilters
  where
    jobStatusFilters = do
      h6_ "Filter by job status"
      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ ("list-group-item " <> if filterStatuses == [] then "active-nav" else "") ] $ do
            let lnk = (Links.rFilterResults $ Just filter{filterStatuses = [], filterPage = (Web.filterPage blankFilter)})
            a_ [ href_ lnk ] $ do
              "all"
              span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"
          forM_ ((\\) (enumFrom minBound) [Job.Success]) $ \st -> do
            li_ [ class_ ("list-group-item " <> if (st `elem` filterStatuses) then "active-nav" else "") ] $ do
              let lnk = (Links.rFilterResults $ Just filter{filterStatuses = [st], filterPage = Nothing})
              a_ [ href_ lnk ] $ do
                toHtml $ toText st
                span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"

    jobRunnerFilters = do
      h6_ [ class_ "mt-3" ] "Filter by job runner"
      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ "list-group-item active-nav" ] $ do
            "Link 1 "
            span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"
          li_ [ class_ "list-group-item" ] $ toHtml $ ("Link 1 " :: Text)
          li_ [ class_ "list-group-item" ] $ toHtml $ ("Link 1 " :: Text)

    jobTypeFilters = do
      h6_ [ class_ "mt-3" ] $ do
        "Filter by job-type"
        form_ [ method_ "post", action_ Links.rRefreshJobTypes, class_ "d-inline"] $ do
          button_ [ type_ "submit", class_ "btn btn-link m-0 p-0 ml-1 float-right"] $ do
            small_ "refresh"

      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ ("list-group-item " <> if filterJobTypes == [] then "active-nav" else "") ] $ do
            let lnk = (Links.rFilterResults $ Just filter{filterJobTypes = [], filterPage = (Web.filterPage blankFilter)})
            a_ [ href_ lnk ] "all"
          forM_ jobTypes $ \jt -> do
            li_ [ class_ ("list-group-item" <> if (jt `elem` filterJobTypes) then " active-nav" else "")] $ do
              a_ [ href_ (Links.rFilterResults $ Just filter{filterJobTypes=[jt]}) ] $ toHtml jt

searchBar :: UTCTime -> Filter -> Html ()
searchBar t filter@Filter{filterStatuses, filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore, filterJobTypes, filterRunAfter} = do
  form_ [ style_ "padding-top: 2em;" ] $ do
    div_ [ class_ "form-group" ] $ do
      div_ [ class_ "search-container" ] $ do
        ul_ [ class_ "list-inline search-bar" ] $ do
          forM_ filterStatuses $ \s -> renderFilter "Status" (toText s) (Links.rFilterResults $ Just filter{filterStatuses = filterStatuses \\ [s]})
          maybe mempty (\x -> renderFilter "Created after" (showText x) (Links.rFilterResults $ Just filter{filterCreatedAfter = Nothing})) filterCreatedAfter
          maybe mempty (\x -> renderFilter "Created before" (showText x) (Links.rFilterResults $ Just filter{filterCreatedBefore = Nothing})) filterCreatedBefore
          maybe mempty (\x -> renderFilter "Updated after" (showText x) (Links.rFilterResults $ Just filter{filterUpdatedAfter = Nothing})) filterUpdatedAfter
          maybe mempty (\x -> renderFilter "Updated before" (showText x) (Links.rFilterResults $ Just filter{filterUpdatedBefore = Nothing})) filterUpdatedBefore
          maybe mempty (\x -> renderFilter "Run after" (showText x) (Links.rFilterResults $ Just filter{filterRunAfter = Nothing})) filterRunAfter
          forM_ filterJobTypes $ \x -> renderFilter "Job type" x (Links.rFilterResults $ Just filter{filterJobTypes = filterJobTypes \\ [x]})

        button_ [ class_ "btn btn-default search-button", type_ "button" ] $ "Search"
      -- ul_ [ class_ "list-inline" ] $ do
      --   li_ $ span_ $ strong_ "Common searches:"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just mempty) ] $ "All jobs"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterStatuses = [Job.Locked] }) ] $ "Currently running"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterStatuses = [Job.Success] }) ] $ "Successful"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterStatuses = [Job.Failed] }) ] $ "Failed"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterRunAfter = Just t }) ] $ "Future"
      --   -- li_ $ a_ [ href_ "#" ] $ "Retried"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterStatuses = [Job.Queued] }) ] $ "Queued"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterUpdatedAfter = Just $ timeSince t 10 Minutes Ago }) ] $ "Last 10 mins"
      --   li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter{ filterCreatedAfter = Just $ timeSince t 10 Minutes Ago }) ] $ "Recently created"
  where
    renderFilter :: Text -> Text -> Text -> Html ()
    renderFilter k v u = do
      li_ [ class_ "search-filter" ] $ do
        span_ [ class_ "filter-name" ] $ toHtml k
        span_ [ class_ "filter-value" ] $ do
          toHtml v
          a_ [ href_ u, class_ "text-danger" ] $ i_ [ class_ "glyphicon glyphicon-remove" ] $ ""


timeDuration :: UTCTime -> UTCTime -> (Int, String)
timeDuration from to = (diff, str)
  where
    str = if diff <= 0
          then "under 1s"
          else (if d>0 then (show d) <> "d" else "") <>
               (if m>0 then (show m) <> "m" else "") <>
               (if s>0 then (show s) <> "s" else "")
    diff = (abs $ round $ diffUTCTime from to)
    (m', s) = diff `divMod` 60
    (h', m) = m' `divMod` 60
    (d, h) = h' `divMod` 24

showText :: (Show a) => a -> Text
showText a = toS $ show a

jobContent :: Value -> Value
jobContent v = case v of
  Aeson.Object o -> case HM.lookup "contents" o of
    Nothing -> v
    Just c -> c
  _ -> v

jobRow :: UTCTime -> (Job, Html ()) -> Html ()
jobRow t (job@Job{..}, jobHtml) = do
  tr_ $ do
    td_ [ class_ "job-type" ] $ do
      let statusFn = case jobStatus of
            Job.Success -> statusSuccess
            Job.Failed -> statusFailed
            Job.Queued -> if jobRunAt > t
                          then statusFuture
                          else statusWaiting
            Job.Retry -> statusRetry
            Job.Locked -> statusLocked
      statusFn t job

    td_ jobHtml
    td_ $ do
      let actionsFn = case jobStatus of
            Job.Success -> (const mempty)
            Job.Failed -> actionsFailed
            Job.Queued -> if jobRunAt > t
                          then actionsFuture
                          else actionsWaiting
            Job.Retry -> actionsRetry
            Job.Locked -> (const mempty)
      actionsFn job


actionsFailed :: Job -> Html ()
actionsFailed Job{..} = do
  form_ [ action_ (Links.rEnqueue jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-secondary", type_ "submit" ] $ "Enqueue again"

actionsRetry :: Job -> Html ()
actionsRetry Job{..} = do
  form_ [ action_ (Links.rRunNow jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-secondary", type_ "submit" ] $ "Run now"

actionsFuture :: Job -> Html ()
actionsFuture Job{..} = do
  form_ [ action_ (Links.rRunNow jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-seconday", type_ "submit" ] $ "Run now"

actionsWaiting :: Job -> Html ()
actionsWaiting Job{..} = do
  form_ [ action_ (Links.rCancel jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-danger", type_ "submit" ] $ "Cancel"

statusSuccess :: UTCTime -> Job -> Html ()
statusSuccess t Job{..} = do
  span_ [ class_ "badge badge-success" ] $ "Success"
  span_ [ class_ "job-run-time" ] $ do
    let (d, s) = timeDuration jobCreatedAt jobUpdatedAt
    abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Completed " <> humanReadableTime' t jobUpdatedAt <> ". "
    abbr_ [ title_ (showText d <> " seconds")] $ toHtml $ "Took " <> s

statusFailed :: UTCTime -> Job -> Html ()
statusFailed t Job{..} = do
  span_ [ class_ "badge badge-danger" ] $ "Failed"
  span_ [ class_ "job-run-time" ] $ do
    abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Failed " <> humanReadableTime' t jobUpdatedAt <> " after " <> show jobAttempts <> " attempts"

statusFuture :: UTCTime -> Job -> Html ()
statusFuture t Job{..} = do
  span_ [ class_ "badge badge-secondary" ] $ "Future"
  span_ [ class_ "job-run-time" ] $ do
    abbr_ [ title_ (showText jobRunAt) ] $ toHtml $ humanReadableTime' t jobRunAt

statusWaiting :: UTCTime -> Job -> Html ()
statusWaiting t Job{..} = do
      span_ [ class_ "badge badge-warning" ] $ "Waiting"
      -- span_ [ class_ "job-run-time" ] ("Waiting to be picked up" :: Text)

statusRetry :: UTCTime -> Job -> Html ()
statusRetry t Job{..} = do
  span_ [ class_ "badge badge-warning" ] $ toHtml $ "Retries (" <> show jobAttempts <> ")"
  span_ [ class_ "job-run-time" ] $ do
    abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Retried " <> humanReadableTime' t jobUpdatedAt <> ". "
    abbr_ [ title_ (showText jobRunAt)] $ toHtml $ "Next retry in " <> humanReadableTime' t jobRunAt

statusLocked :: UTCTime -> Job -> Html ()
statusLocked t Job{..} = do
  span_ [ class_ "badge badge-info" ] $ toHtml ("Locked"  :: Text)
  -- span_ [ class_ "job-run-time" ] $ do
  --   abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Retried " <> humanReadableTime' t jobUpdatedAt <> ". "
  --   abbr_ [ title_ (showText jobRunAt)] $ toHtml $ "Next retry in " <> humanReadableTime' t jobRunAt

resultsPanel :: UTCTime -> Filter -> [(Job, Html ())] -> Int -> Html ()
resultsPanel t filter@Filter{filterPage} js runningCount = do
  div_ [ class_ "card mt-3" ] $ do
    div_ [ class_ "card-header bg-secondary text-white" ] $ do
      "Currently running "
      span_ [ class_ "badge badge-primary badge-primary" ] $ toHtml (show runningCount)
    div_ [ class_ "currently-running" ] $ div_ [ class_ "" ] $ table_ [ class_ "table table-striped table-hover" ] $ do
      thead_ [ class_ "thead-dark"] $ do
        tr_ $ do
          th_ "Job status"
          th_ "Job"
          th_ [ style_ "min-width: 12em;" ] "Actions"
      tbody_ $ do
        forM_ js (jobRow t)
    div_ [ class_ "card-footer" ] $ do
      nav_ $ do
        ul_ [ class_ "pagination" ] $ do
          prevLink
          nextLink
  where
    prevLink = do
      let (extraClass, lnk) = case filterPage of
            Nothing -> ("disabled", "")
            Just (l, 0) -> ("disabled", "")
            Just (l, o) -> ("", Links.rFilterResults $ Just $ filter {filterPage = Just (l, max 0 $ o - l)})
      li_ [ class_ ("page-item previous " <> extraClass) ] $ do
        a_ [ class_ "page-link", href_ lnk ] $ "Prev"

    nextLink = do
      let (extraClass, lnk) = case filterPage of
            Nothing ->
              if (DL.length js) == 0
              then ("disabled", "")
              else ("", (Links.rFilterResults $ Just $ filter {filterPage = Just (10, 10)}))
            Just (l, o) ->
              if (DL.length js) < l
              then ("disabled", "")
              else ("", (Links.rFilterResults $ Just $ filter {filterPage = Just (l, o + l)}))
      li_ [ class_ ("page-item next " <> extraClass) ] $ do
        a_ [ class_ "page-link", href_ lnk ] $ "Next"

ariaExpanded_ :: Text -> Attribute
ariaExpanded_ v = makeAttribute "aria-expanded" v

