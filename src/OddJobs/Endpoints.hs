{-# LANGUAGE TypeOperators, DeriveGeneric, NamedFieldPuns, DataKinds, StandaloneDeriving, FlexibleContexts #-}

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

  let nt :: ReaderT Job.Config IO a -> Servant.Handler a
      nt action = (liftIO $ try $ runReaderT action jm) >>= \case
        Left (e :: SomeException) -> Servant.Handler  $ ExceptT $ pure $ Left $ err500 { errBody = toS $ show e }
        Right a -> Servant.Handler $ ExceptT $ pure $ Right a
      appProxy = (Proxy :: Proxy (ToServant Routes AsApi))

  finally
    (run 8080 $ genericServe (server dbPool))
    (cleanup >> (Pool.destroyAllResources dbPool))

stopApp :: IO ()
stopApp = pure ()

server :: Pool Connection
       -> Routes AsServer
server dbPool = Routes
  { rFilterResults = (\mFilter -> filterResults dbPool mFilter)
  , rStaticAssets = serveDirectoryFileServer "assets"
  }


-- withDbConnection :: HasJobMonitor m
--                  => (Connection -> m a)
--                  -> m a
-- withDbConnection fn = getDbPool >>= \pool -> Pool.withResource pool fn

filterResults :: Pool Connection
              -> Maybe Filter
              -> Handler (Html ())
filterResults dbPool mFilter = do
  let filters = fromMaybe mempty mFilter
  (jobs, runningCount) <- liftIO $ Pool.withResource dbPool $ \conn -> (,)
    <$> (filterJobs conn tname filters)
    <*> (countJobs conn tname filters{ filterStatuses = [Job.Locked] })
  t <- liftIO getCurrentTime
  pure $ pageLayout $ do
    searchBar t filters
    resultsPanel t filters jobs runningCount


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

pageLayout :: Html () -> Html ()
pageLayout inner = do
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
        div_ [ class_ "d-none d-md-block col-md-2" ] sideNav
        div_ [ class_ "col-12 col-md-10" ] inner
      script_ [ src_ "assets/js/jquery.min.js" ] $ ("" :: Text)
      script_ [ src_ "assets/bootstrap/js/bootstrap.min.js" ] $ ("" :: Text)
      -- script_ [ src_ "https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.6.0/slick.js" ] $ ("" :: Text)
      -- script_ [ src_ "assets/js/logo-slider.js" ] $ ("" :: Text)

sideNav :: Html ()
sideNav = do
  div_ [ style_ "padding-top: 2em;", class_ "filters" ] $ do
    jobStatusFilters
    jobRunnerFilters
    jobTypeFilters
  where
    jobStatusFilters = do
      h6_ "Filter by job status"
      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ "list-group-item active-nav" ] $ do
            "Link 1 "
            span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"
          li_ [ class_ "list-group-item" ] $ toHtml $ ("Link 1 " :: Text)
          li_ [ class_ "list-group-item" ] $ toHtml $ ("Link 1 " :: Text)

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
      h6_ [ class_ "mt-3" ] "Filter by job-type"
      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ "list-group-item active-nav" ] $ do
            "Link 1 "
            span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"
          li_ [ class_ "list-group-item" ] $ toHtml $ ("Link 1 " :: Text)
          li_ [ class_ "list-group-item" ] $ toHtml $ ("Link 1 " :: Text)

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

rowSuccess :: UTCTime -> Job -> Html ()
rowSuccess t job@Job{jobStatus, jobCreatedAt, jobUpdatedAt, jobPayload, jobAttempts, jobRunAt} = do
  tr_ $ do
    td_ [ class_ "job-type" ] $ case jobStatus of
      Job.Success -> statusSuccess
      Job.Failed -> statusFailed
      Job.Queued -> if jobRunAt > t
                    then statusFuture
                    else statusWaiting
      Job.Retry -> statusRetry
      Job.Locked -> statusLocked


      -- span_ [ class_ "label label-success" ] $ "Success"
      -- span_ [ class_ "job-run-time" ] $ "Completed 23h ago. Took 3 sec."

    td_ $ toHtml $ Job.defaultJobType job -- TODO: this needs to be changed
    td_ $ div_ [ class_ "job-payload" ] $ payloadToHtml $ jobContent jobPayload
      -- span_ [ class_ "key-value-pair" ] $ do
      --   span_ [ class_ "key" ] $ "args"
      --   span_ [ class_ "value" ] $ do
      --     "[\"flexi_payment_reminder\", 3432423,"
      --     a_ [ href_ "#", class_ "json-ellipsis" ] $ "\8230"
      --     "]"
    td_ "Text"
    td_ $ case jobStatus of
      Job.Success -> mempty
      Job.Failed -> actionsFailed
      Job.Queued -> if jobRunAt > t
                    then actionsFuture
                    else mempty
      Job.Retry -> actionsRetry
      Job.Locked -> mempty
  where

    actionsFailed = do
      button_ [ class_ "btn btn-default", type_ "button" ] $ "Retry again"

    actionsRetry = do
      button_ [ class_ "btn btn-default", type_ "button" ] $ "Retry now"

    actionsFuture = do
      button_ [ class_ "btn btn-default", type_ "button" ] $ "Run now"

    payloadToHtml :: Value -> Html ()
    payloadToHtml v = case v of
      Aeson.Object o -> do
        toHtml ("{ " :: Text)
        forM_ (HM.toList o) $ \(k, v) -> do
          span_ [ class_ " key-value-pair " ] $ do
            span_ [ class_ "key" ] $ toHtml $ k <> ":"
            span_ [ class_ "value" ] $ payloadToHtml v
        toHtml (" }" :: Text)
      Aeson.Array a -> do
        toHtml ("[" :: Text)
        forM_ (toList a) $ \x -> do
          payloadToHtml x
          toHtml (", " :: Text)
        toHtml ("]" :: Text)
      Aeson.String t -> toHtml t
      Aeson.Number n -> toHtml $ show n
      Aeson.Bool b -> toHtml $ show b
      Aeson.Null -> toHtml ("null" :: Text)

    statusSuccess = do
      span_ [ class_ "label label-success" ] $ "Success"
      span_ [ class_ "job-run-time" ] $ do
        let (d, s) = timeDuration jobCreatedAt jobUpdatedAt
        abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Completed " <> humanReadableTime' t jobUpdatedAt <> ". "
        abbr_ [ title_ (showText d <> " seconds")] $ toHtml $ "Took " <> s

    statusFailed = do
      span_ [ class_ "label label-danger" ] $ "Failed"
      span_ [ class_ "job-run-time" ] $ do
        abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Failed " <> humanReadableTime' t jobUpdatedAt <> " after " <> show jobAttempts <> " attempts"

    statusFuture = do
      span_ [ class_ "label" ] $ "Future"
      span_ [ class_ "job-run-time" ] $ do
        abbr_ [ title_ (showText jobRunAt) ] $ toHtml $ humanReadableTime' t jobRunAt

    statusWaiting = do
      span_ [ class_ "label label-warning" ] $ "Waiting"
      -- span_ [ class_ "job-run-time" ] ("Waiting to be picked up" :: Text)

    statusRetry = do
      span_ [ class_ "label label-info" ] $ toHtml $ "Retries (" <> show jobAttempts <> ")"
      span_ [ class_ "job-run-time" ] $ do
        abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Retried " <> humanReadableTime' t jobUpdatedAt <> ". "
        abbr_ [ title_ (showText jobRunAt)] $ toHtml $ "Next retry in " <> humanReadableTime' t jobRunAt

    statusLocked = do
      span_ [ class_ "label label-warning" ] $ toHtml ("Locked"  :: Text)
      -- span_ [ class_ "job-run-time" ] $ do
      --   abbr_ [ title_ (showText jobUpdatedAt) ] $ toHtml $ "Retried " <> humanReadableTime' t jobUpdatedAt <> ". "
      --   abbr_ [ title_ (showText jobRunAt)] $ toHtml $ "Next retry in " <> humanReadableTime' t jobRunAt


rowRetry :: Html ()
rowRetry = do
  tr_ $ do
    td_ [ class_ "job-type" ] $ do
      span_ [ class_ "label label-info" ] $ "Retried (5)"
      span_ [ class_ "job-run-time" ] $ "23 mins ago. Next retry in 90 min."
    td_ "Queued Mail"
    td_ $ div_ [ class_ "job-payload" ] $ do
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "client_id"
        span_ [ class_ "value" ] $ "456"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "user_id"
        span_ [ class_ "value" ] $ "123"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "args"
        span_ [ class_ "value" ] $ do
          "[\"flexi_payment_reminder\", 3432423,"
          a_ [ href_ "#", class_ "json-ellipsis" ] $ "\8230"
          "]"
    td_ "Text"
    td_ $ div_ [ class_ "btn-group" ] $ do
      button_ [ class_ "btn btn-default", type_ "button" ] $ "Retry now"
      button_ [ class_ "btn btn-default dropdown-toggle", data_ "toggle" "dropdown", ariaExpanded_ "false", type_ "button" ] $ span_ [ class_ "caret" ] $ ""
      ul_ [ class_ "dropdown-menu", role_ "menu" ] $ do
        li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "First Item"
        li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Second Item"
        li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Third Item"


rowFailed :: Html ()
rowFailed = do
  tr_ $ do
    td_ [ class_ "job-type" ] $ do
      span_ [ class_ "label label-danger" ] $ "Failed"
      span_ [ class_ "job-run-time" ] $ "23 mins ago. After 25 attempts."
    td_ "Queued Mail"
    td_ $ div_ [ class_ "job-payload" ] $ do
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "client_id"
        span_ [ class_ "value" ] $ "456"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "user_id"
        span_ [ class_ "value" ] $ "123"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "args"
        span_ [ class_ "value" ] $ do
          "[\"flexi_payment_reminder\", 3432423,"
          a_ [ href_ "#", class_ "json-ellipsis" ] $ "\8230"
          "]"
    td_ "Text"
    td_ $ button_ [ class_ "btn btn-default", type_ "button" ] $ "Retry again"

rowFuture :: Html ()
rowFuture = do
  tr_ $ do
    td_ [ class_ "job-type" ] $ do
      span_ [ class_ "label label-default" ] $ "Future"
      span_ [ class_ "job-run-time" ] $ "37 mins from now"
    td_ "Queued Mail"
    td_ $ div_ [ class_ "job-payload" ] $ do
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "client_id"
        span_ [ class_ "value" ] $ "456"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "user_id"
        span_ [ class_ "value" ] $ "123"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "args"
        span_ [ class_ "value" ] $ do
          "[\"flexi_payment_reminder\", 3432423,"
          a_ [ href_ "#", class_ "json-ellipsis" ] $ "\8230"
          "]"
    td_ "Text"
    td_ $ div_ [ class_ "btn-group" ] $ do
      button_ [ class_ "btn btn-default", type_ "button" ] $ "Run now"
      button_ [ class_ "btn btn-default dropdown-toggle", data_ "toggle" "dropdown", ariaExpanded_ "false", type_ "button" ] $ span_ [ class_ "caret" ] $ ""
      ul_ [ class_ "dropdown-menu", role_ "menu" ] $ do
        li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "First Item"
        li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Second Item"
        li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Third Item"


rowLocked :: Html ()
rowLocked = do
  tr_ $ do
    td_ [ class_ "job-type" ] $ do
      span_ [ class_ "label label-warning" ] $ "Locked"
      span_ [ class_ "job-run-time" ] $ "Since 2min by"
      span_ [ class_ "job-runner-name" ] $ "hostname:3242"
    td_ "Queued Mail"
    td_ $ div_ [ class_ "job-payload" ] $ do
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "client_id"
        span_ [ class_ "value" ] $ "456"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "user_id"
        span_ [ class_ "value" ] $ "123"
      span_ [ class_ "key-value-pair" ] $ do
        span_ [ class_ "key" ] $ "args"
        span_ [ class_ "value" ] $ do
          "[\"flexi_payment_reminder\", 3432423,"
          a_ [ href_ "#", class_ "json-ellipsis" ] $ "\8230"
          "]"
    td_ "Text"
    td_ $ button_ [ class_ "btn btn-default", type_ "button" ] $ "Unlock"

resultsPanel :: UTCTime -> Filter -> [Job] -> Int -> Html ()
resultsPanel t filter@Filter{filterPage} jobs runningCount = do
  div_ [ class_ "card" ] $ do
    div_ [ class_ "card-header bg-secondary text-white" ] $ do
      "Currently running "
      span_ [ class_ "badge badge-primary badge-primary" ] $ toHtml (show runningCount)
    div_ [ class_ "currently-running" ] $ div_ [ class_ "" ] $ table_ [ class_ "table table-responsive table-striped table-hover" ] $ do
      thead_ [ class_ "thead-dark"] $ do
        tr_ $ do
          th_ "Job status"
          th_ "Job type"
          th_ "Job payload"
          th_ "Last error"
          th_ "Actions"
      tbody_ $ do
        forM_ jobs $ \j -> case jobStatus j of
          Job.Success -> rowSuccess t j
          _ -> rowSuccess t j
        -- rowLocked
        -- rowSuccess
        -- rowFuture
        -- rowRetry
        -- rowFailed
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
            Nothing -> ("", (Links.rFilterResults $ Just $ filter {filterPage = Just (10, 10)}))
            Just (l, o) -> ("", (Links.rFilterResults $ Just $ filter {filterPage = Just (l, o + l)}))
      li_ [ class_ ("page-item next" <> extraClass) ] $ do
        a_ [ class_ "page-link", href_ lnk ] $ "Next"

ariaExpanded_ :: Text -> Attribute
ariaExpanded_ v = makeAttribute "aria-expanded" v
