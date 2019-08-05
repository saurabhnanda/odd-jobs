{-# LANGUAGE TypeOperators, DeriveGeneric, NamedFieldPuns, DataKinds, StandaloneDeriving, FlexibleContexts #-}

module PGQueue.Endpoints where

import PGQueue.Web as Web
import PGQueue.Job as Job
import PGQueue.Types
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
import UnliftIO
import Database.PostgreSQL.Simple as PGS
import Data.Pool as Pool
import Control.Monad.Reader
import Data.String.Conv (toS)
import Control.Monad.Except
import Data.Time as Time
import Data.Time.Format.Human as Time
import Data.Aeson as Aeson
import qualified Data.HashMap.Strict as HM
import GHC.Exts (toList)
import Data.Maybe (fromMaybe)
import Data.Text.Conversions (fromText, toText)
import Control.Applicative ((<|>))
import Data.Time.Convenience as Time
import qualified PGQueue.Links as Links

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

  (jm, cleanup) <- Job.defaultJobMonitor "jobs_ugqsdyceqf" dbPool

  let nt :: ReaderT Job.JobMonitor IO a -> Servant.Handler a
      nt action = (liftIO $ try $ runReaderT action jm) >>= \case
        Left (e :: SomeException) -> Servant.Handler  $ ExceptT $ pure $ Left $ err500 { errBody = toS $ show e }
        Right a -> Servant.Handler $ ExceptT $ pure $ Right a
      appProxy = (Proxy :: Proxy (ToServant Routes AsApi))

  finally
    (run 8080 $ serve appProxy $ hoistServer appProxy nt (toServant server))
    (cleanup >> (Pool.destroyAllResources dbPool))

stopApp :: IO ()
stopApp = pure ()

server :: HasJobMonitor m
       => Routes (AsServerT m)
server = Routes
  { rFilterResults = filterResults
  , rStaticAssets = serveDirectoryFileServer "assets"
  }


withDbConnection :: HasJobMonitor m
                 => (Connection -> m a)
                 -> m a
withDbConnection fn = getDbPool >>= \pool -> Pool.withResource pool fn

filterResults :: HasJobMonitor m
              => Maybe Filter
              -> m (Html ())
filterResults mFilter = do
  let filters = fromMaybe mempty mFilter
  jobs <- withDbConnection $ \conn -> filterJobs filters
  t <- liftIO getCurrentTime
  pure $ pageLayout $ do
    searchBar t filters
    resultsPanel t filters jobs


pageNav :: Html ()
pageNav = do
  div_ $ nav_ [ class_ "navbar navbar-default navigation-clean" ] $ div_ [ class_ "container" ] $ do
    div_ [ class_ "navbar-header" ] $ do
      a_ [ class_ "navbar-brand navbar-link", href_ "#" ] $ "Company Name"
      button_ [ class_ "navbar-toggle collapsed", data_ "toggle" "collapse", data_ "target" "#navcol-1" ] $ do
        span_ [ class_ "sr-only" ] $ "Toggle navigation"
        span_ [ class_ "icon-bar" ] $ ""
        span_ [ class_ "icon-bar" ] $ ""
        span_ [ class_ "icon-bar" ] $ ""
    div_ [ class_ "collapse navbar-collapse", id_ "navcol-1" ] $ ul_ [ class_ "nav navbar-nav navbar-right" ] $ do
      li_ [ class_ "active", role_ "presentation" ] $ a_ [ href_ "#" ] $ "First Item"
      li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Second Item"
      li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Third Item"
      li_ [ class_ "dropdown" ] $ do
        a_ [ class_ "dropdown-toggle", data_ "toggle" "dropdown", ariaExpanded_ "false", href_ "#" ] $ do
          "Dropdown"
          span_ [ class_ "caret" ] $ ""
        ul_ [ class_ "dropdown-menu", role_ "menu" ] $ do
          li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "First Item"
          li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Second Item"
          li_ [ role_ "presentation" ] $ a_ [ href_ "#" ] $ "Third Item"

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
      link_ [ rel_ "stylesheet", href_ "assets/css/logo-slider.css" ]
      link_ [ rel_ "stylesheet", href_ "assets/css/Navigation-Clean1.css" ]
      link_ [ rel_ "stylesheet", href_ "assets/css/styles.css" ]
    body_ $ do
      pageNav
      div_ $ div_ [ class_ "container", style_ "/*background-color:#f2f2f2;*/" ] $ div_ [ class_ "row" ] $ div_ [ class_ "col-md-12" ] $ do
        inner
      script_ [ src_ "assets/js/jquery.min.js" ] $ ("" :: Text)
      script_ [ src_ "assets/bootstrap/js/bootstrap.min.js" ] $ ("" :: Text)
      script_ [ src_ "https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.6.0/slick.js" ] $ ("" :: Text)
      script_ [ src_ "assets/js/logo-slider.js" ] $ ("" :: Text)

searchBar :: UTCTime -> Filter -> Html ()
searchBar t filter@Filter{filterStatuses, filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore, filterJobTypes} = do
  form_ [ style_ "padding-top: 2em;" ] $ do
    div_ [ class_ "form-group" ] $ do
      div_ [ class_ "search-container" ] $ do
        ul_ [ class_ "list-inline search-bar" ] $ do
          forM_ filterStatuses $ \s -> renderFilter "Status" (toText s) "#"
          maybe mempty (\x -> renderFilter "Created after" (showText x) "#") filterCreatedAfter
          maybe mempty (\x -> renderFilter "Created before" (showText x) "#") filterCreatedBefore
          maybe mempty (\x -> renderFilter "Updated after" (showText x) "#") filterUpdatedAfter
          maybe mempty (\x -> renderFilter "Updated before" (showText x) "#") filterUpdatedBefore
          forM_ filterJobTypes $ \x -> renderFilter "Job type" x "#"

        button_ [ class_ "btn btn-default search-button", type_ "button" ] $ "Search"
      ul_ [ class_ "list-inline" ] $ do
        li_ $ span_ $ strong_ "Common searches:"
        li_ $ a_ [ href_ (Links.rFilterResults $ Just mempty) ] $ "All jobs"
        li_ $ a_ [ href_ "#" ] $ "Currently running"
        li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter <> mempty{ filterStatuses = [Job.Success] }) ] $ "Successful"
        li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter <> mempty{ filterStatuses = [Job.Failed] }) ] $ "Failed"
        li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter <> mempty{ filterStatuses = [Job.Queued] }) ] $ "Future"
        li_ $ a_ [ href_ "#" ] $ "Retried"
        li_ $ a_ [ href_ (Links.rFilterResults $ Just $ filter <> mempty{ filterUpdatedAfter = Just $ timeSince t 10 Minutes Ago }) ] $ "Last 10 mins"
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

      -- span_ [ class_ "label label-success" ] $ "Success"
      -- span_ [ class_ "job-run-time" ] $ "Completed 23h ago. Took 3 sec."
    td_ $ toHtml $ Job.jobType job
    td_ $ div_ [ class_ "job-payload" ] $ do
      payloadToHtml $ jobContent jobPayload
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

resultsPanel :: UTCTime -> Filter -> [Job] -> Html ()
resultsPanel t filter@Filter{filterPage} jobs = do
  div_ [ class_ "panel panel-default" ] $ do
    div_ [ class_ "panel-heading" ] $ h3_ [ class_ "panel-title" ] $ do
      "Currently running"
      span_ [ class_ "badge" ] $ "3"
    div_ [ class_ "panel-body" ] $ div_ [ class_ "currently-running" ] $ div_ [ class_ "table-responsive" ] $ table_ [ class_ "table" ] $ do
      thead_ $ tr_ $ do
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
    div_ [ class_ "panel-footer" ] $ nav_ $ ul_ [ class_ "pager", style_ "margin:0px;" ] $ do
      li_ [ class_ "previous" ] $ case filterPage of
        Nothing -> a_ [ disabled_ "disabled" ] $ "Prev"
        Just (l, 0) -> a_ [ disabled_ "disabled" ] $ "Prev"
        Just (l, o) -> a_ [ href_ (Links.rFilterResults $ Just $ filter {filterPage = Just (l, max 0 $ o - l)}) ] $ "Prev"
      li_ [ class_ "next" ] $ case filterPage of
        Nothing -> a_ [ href_ (Links.rFilterResults $ Just $ filter {filterPage = Just (10, 10)}) ] $ "Next"
        Just (l, o) -> a_ [ href_ (Links.rFilterResults $ Just $ filter {filterPage = Just (l, o + l)}) ] $ "Next"


ariaExpanded_ :: Text -> Attribute
ariaExpanded_ v = makeAttribute "aria-expanded" v
