{-# LANGUAGE DeriveGeneric, NamedFieldPuns, TypeOperators, DataKinds, RecordWildCards, DeriveAnyClass #-}
module OddJobs.Web where

import OddJobs.Types
import OddJobs.Job as Job
import Data.Time
import Data.Aeson as Aeson
import qualified Data.Text as T
import Data.Text (Text)
import GHC.Generics hiding (from, to)
import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.ToRow as PGS
import Database.PostgreSQL.Simple.ToField as PGS
import Data.Pool as Pool
import UnliftIO
import Data.Maybe
import Data.String (fromString)
import Control.Applicative ((<|>))
import Data.List (nub)
import Servant
import Servant.API.Generic
import Servant.HTML.Lucid
import Lucid
import Lucid.Html5
import Lucid.Base
import Data.String.Conv
import qualified Data.HashMap.Strict as HM
import Data.List as DL hiding (filter, and)
import Control.Monad
import Data.Time.Format.Human (humanReadableTime')
import Data.Time.Convenience (timeSince, Unit(..), Direction(..))
import Data.Text.Conversions (fromText, toText)
import Prelude hiding (filter, and)

data OrderDirection = Asc | Desc deriving (Eq, Show, Generic, Enum)

data OrderByField = OrdCreatedAt
                  | OrdUpdatedAt
                  | OrdLockedAt
                  | OrdStatus
                  | OrdJobType
                  deriving (Eq, Show, Generic, Enum)

data Filter = Filter
  { filterStatuses :: [Status]
  , filterCreatedAfter :: Maybe UTCTime
  , filterCreatedBefore :: Maybe UTCTime
  , filterUpdatedAfter :: Maybe UTCTime
  , filterUpdatedBefore :: Maybe UTCTime
  , filterJobTypes :: [Text]
  , filterOrder :: Maybe (OrderByField, OrderDirection)
  , filterPage :: Maybe (Int, Int)
  , filterRunAfter :: Maybe UTCTime
  , filterJobRunner :: [JobRunnerName]
  } deriving (Eq, Show, Generic)

instance Semigroup Filter where
  (<>) a b = Filter
    { filterStatuses = nub (filterStatuses b <> filterStatuses a)
    , filterCreatedAfter = filterCreatedAfter b <|> filterCreatedAfter a
    , filterCreatedBefore = filterCreatedBefore b <|> filterCreatedBefore a
    , filterUpdatedAfter = filterUpdatedAfter b <|> filterUpdatedBefore a
    , filterUpdatedBefore = filterUpdatedBefore b <|> filterUpdatedBefore a
    , filterJobTypes = nub (filterJobTypes b <> filterJobTypes a)
    , filterOrder = filterOrder b <|> filterOrder a
    , filterPage = filterPage b <|> filterPage a
    , filterRunAfter = filterRunAfter b <|> filterRunAfter a
    , filterJobRunner = nub (filterJobRunner b <> filterJobRunner a)
    }

instance Monoid Filter where
  mempty = blankFilter

blankFilter :: Filter
blankFilter = Filter
  { filterStatuses = []
  , filterCreatedAfter = Nothing
  , filterCreatedBefore = Nothing
  , filterUpdatedAfter = Nothing
  , filterUpdatedBefore = Nothing
  , filterJobTypes = []
  , filterOrder = Nothing
  , filterPage = Just (10, 0)
  , filterRunAfter = Nothing
  , filterJobRunner = []
  }


instance ToJSON Status
instance FromJSON Status

instance ToJSON OrderDirection
instance FromJSON OrderDirection
instance ToJSON OrderByField
instance FromJSON OrderByField

instance ToJSON Filter where
  toJSON = Aeson.genericToJSON  Aeson.defaultOptions{omitNothingFields = True}
instance FromJSON Filter where
  parseJSON = Aeson.genericParseJSON Aeson.defaultOptions{omitNothingFields = True}


instance FromHttpApiData Filter where
  parseQueryParam x = case eitherDecode (toS x) of
    Left e -> Left $ toS e
    Right r -> Right r

instance ToHttpApiData Filter where
  toQueryParam x = toS $ Aeson.encode x

-- data Routes route = Routes
--   { rFilterResults :: route :- QueryParam "filters" Filter :> Get '[HTML] (Html ())
--   , rStaticAssets :: route :- "assets" :> Raw
--   , rEnqueue :: route :- "enqueue" :> Capture "jobId" JobId :> Post '[HTML] NoContent
--   , rRunNow :: route :- "run" :> Capture "jobId" JobId :> Post '[HTML] NoContent
--   , rCancel :: route :- "cancel" :> Capture "jobId" JobId :> Post '[HTML] NoContent
--   , rRefreshJobTypes :: route :- "refresh-job-types" :> Post '[HTML] NoContent
--   , rRefreshJobRunners :: route :- "refresh-job-runners" :> Post '[HTML] NoContent
--   } deriving (Generic)


data Routes = Routes
  { rFilterResults :: Maybe Filter -> Text
  , rStaticAssets :: Text -> Text
  , rEnqueue :: JobId -> Text
  , rRunNow :: JobId -> Text
  , rCancel :: JobId -> Text
  , rRefreshJobTypes :: Text
  , rRefreshJobRunners :: Text
  }


filterJobsQuery :: Config -> Filter -> (PGS.Query, [Action])
filterJobsQuery Config{cfgTableName, cfgJobTypeSql} Filter{..} =
  ( "SELECT " <> Job.concatJobDbColumns <> " FROM " <> cfgTableName <> whereClause <> " " <> (orderClause $ fromMaybe (OrdUpdatedAt, Desc) filterOrder) <> " " <> limitOffsetClause
  , whereActions
  )
  where
    orderClause (flt, dir) =
      let fname = case flt of
            OrdCreatedAt -> "created_at"
            OrdUpdatedAt -> "updated_at"
            OrdLockedAt -> "locked_at"
            OrdStatus -> "status"
            OrdJobType -> "payload->>'tag'"
          dname = case dir of
            Asc -> "asc nulls first"
            Desc -> "desc nulls last"
      in "ORDER BY " <> fname <> " " <> dname <> ", id desc"

    limitOffsetClause :: Query
    limitOffsetClause = case filterPage of
      Nothing -> mempty
      Just (l, o) -> "LIMIT " <> fromString (show l) <> " OFFSET " <> fromString (show o)

    (whereClause, whereActions) =
      let finalClause = statusClause `and` createdAfterClause `and`
                       createdBeforeClause `and` updatedBeforeClause `and`
                       updatedAfterClause `and` jobTypeClause `and`
                       runAfterClause `and` jobRunnerClause
      in case finalClause  of
        Nothing -> (mempty, toRow ())
        Just (q, as) -> (" WHERE " <> q, as)

    statusClause = if Prelude.null filterStatuses
                   then Nothing
                   else Just ("status IN ?", toRow $ (Only (In filterStatuses)))

    createdAfterClause = Prelude.fmap (\x -> ("created_at >= ?", toRow $ Only x)) filterCreatedAfter
    createdBeforeClause = Prelude.fmap (\x -> ("created_at < ?", toRow $ Only x)) filterCreatedBefore
    updatedAfterClause = Prelude.fmap (\x -> ("updated_at >= ?", toRow $ Only x)) filterUpdatedAfter
    updatedBeforeClause = Prelude.fmap (\x -> ("updated_at < ?", toRow $ Only x)) filterUpdatedBefore
    runAfterClause = Prelude.fmap (\x -> ("run_at > ?", toRow $ Only x)) filterRunAfter

    jobTypeClause :: Maybe (Query, [Action])
    jobTypeClause = case filterJobTypes of
      [] -> Nothing
      xs ->
        let qFragment = "(" <> cfgJobTypeSql <> ")=?"
            build ys (q, vs) = case ys of
              [] -> (q, vs)
              (y:[]) -> (qFragment <> q, (toField y):vs)
              (y:ys_) -> build ys_ (" OR " <> qFragment <> q, (toField y):vs)
        in Just $ build xs (mempty, [])

    jobRunnerClause :: Maybe (Query, [Action])
    jobRunnerClause = case filterJobRunner of
      [] -> Nothing
      xs -> Just ("locked_by in ?", toRow $ Only $ In xs)

    and :: Maybe (Query, [PGS.Action]) -> Maybe (Query, [PGS.Action]) -> Maybe (Query, [PGS.Action])
    and Nothing Nothing = Nothing
    and Nothing (Just (q, as)) = Just (q, as)
    and (Just (q, as)) Nothing = Just (q, as)
    and (Just (qa, as)) (Just (qb, bs)) = Just ("(" <> qa <> ") AND (" <> qb <> ")", as <> bs)

    -- orderClause = _

filterJobs :: Config -> Connection -> Filter -> IO [Job]
filterJobs cfg conn f = do
  let (q, queryArgs) = filterJobsQuery cfg f
  PGS.query conn q queryArgs

countJobs :: Config -> Connection -> Filter -> IO Int
countJobs cfg conn f = do
  let (q, queryArgs) = filterJobsQuery cfg f
      finalqry = "SELECT count(*) FROM (" <> q <> ") a"
  [Only r] <- PGS.query conn finalqry queryArgs
  pure r


-- f = encode [FTStatuses [Job.Success, Queued], FTJobType "QueuedMail"]

-- f = blankFilter
--   { filterStatuses = [Job.Success, Queued]
--   , filterJobTypes = ["QueuedMail", "ConfirmBooking"]
--   }



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
      script_ [ src_ "assets/js/custom.js" ] $ ("" :: Text)
      -- script_ [ src_ "https://cdnjs.cloudflare.com/ajax/libs/slick-carousel/1.6.0/slick.js" ] $ ("" :: Text)
      -- script_ [ src_ "assets/js/logo-slider.js" ] $ ("" :: Text)

sideNav :: Routes -> [Text] -> [JobRunnerName] -> UTCTime -> Filter -> Html ()
sideNav Routes{..} jobTypes jobRunnerNames t filter@Filter{..} = do
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
            let lnk = (rFilterResults $ Just filter{filterStatuses = [], filterPage = (OddJobs.Web.filterPage blankFilter)})
            a_ [ href_ lnk ] $ do
              "all"
              span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"
          forM_ ((\\) (enumFrom minBound) [Job.Success]) $ \st -> do
            li_ [ class_ ("list-group-item " <> if (st `elem` filterStatuses) then "active-nav" else "") ] $ do
              let lnk = (rFilterResults $ Just filter{filterStatuses = [st], filterPage = Nothing})
              a_ [ href_ lnk ] $ do
                toHtml $ toText st
                span_ [ class_ "badge badge-pill badge-secondary float-right" ] "12"

    jobRunnerFilters = do
      h6_ [ class_ "mt-3" ] $ do
        "Filter by job-runner"
        form_ [ method_ "post", action_ rRefreshJobRunners, class_ "d-inline"] $ do
          button_ [ type_ "submit", class_ "btn btn-link m-0 p-0 ml-1 float-right"] $ do
            small_ "refresh"

      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ ("list-group-item " <> if filterJobRunner == [] then "active-nav" else "") ] $ do
            let lnk = (rFilterResults $ Just filter{filterJobRunner = [], filterPage = (OddJobs.Web.filterPage blankFilter)})
            a_ [ href_ lnk ] "all"
          forM_ jobRunnerNames $ \jr -> do
            li_ [ class_ ("list-group-item" <> if (jr `elem` filterJobRunner) then " active-nav" else "")] $ do
              a_ [ href_ "#" ] $ toHtml $ unJobRunnerName jr

    jobTypeFilters = do
      h6_ [ class_ "mt-3" ] $ do
        "Filter by job-type"
        form_ [ method_ "post", action_ rRefreshJobTypes, class_ "d-inline"] $ do
          button_ [ type_ "submit", class_ "btn btn-link m-0 p-0 ml-1 float-right"] $ do
            small_ "refresh"

      div_ [ class_ "card" ] $ do
        ul_ [ class_ "list-group list-group-flush" ] $ do
          li_ [ class_ ("list-group-item " <> if filterJobTypes == [] then "active-nav" else "") ] $ do
            let lnk = (rFilterResults $ Just filter{filterJobTypes = [], filterPage = (OddJobs.Web.filterPage blankFilter)})
            a_ [ href_ lnk ] "all"
          forM_ jobTypes $ \jt -> do
            li_ [ class_ ("list-group-item" <> if (jt `elem` filterJobTypes) then " active-nav" else "")] $ do
              a_ [ href_ (rFilterResults $ Just filter{filterJobTypes=[jt]}) ] $ toHtml jt

searchBar :: Routes -> UTCTime -> Filter -> Html ()
searchBar Routes{..} t filter@Filter{filterStatuses, filterCreatedAfter, filterCreatedBefore, filterUpdatedAfter, filterUpdatedBefore, filterJobTypes, filterRunAfter} = do
  form_ [ style_ "padding-top: 2em;" ] $ do
    div_ [ class_ "form-group" ] $ do
      div_ [ class_ "search-container" ] $ do
        ul_ [ class_ "list-inline search-bar" ] $ do
          forM_ filterStatuses $ \s -> renderFilter "Status" (toText s) (rFilterResults $ Just filter{filterStatuses = filterStatuses \\ [s]})
          maybe mempty (\x -> renderFilter "Created after" (showText x) (rFilterResults $ Just filter{filterCreatedAfter = Nothing})) filterCreatedAfter
          maybe mempty (\x -> renderFilter "Created before" (showText x) (rFilterResults $ Just filter{filterCreatedBefore = Nothing})) filterCreatedBefore
          maybe mempty (\x -> renderFilter "Updated after" (showText x) (rFilterResults $ Just filter{filterUpdatedAfter = Nothing})) filterUpdatedAfter
          maybe mempty (\x -> renderFilter "Updated before" (showText x) (rFilterResults $ Just filter{filterUpdatedBefore = Nothing})) filterUpdatedBefore
          maybe mempty (\x -> renderFilter "Run after" (showText x) (rFilterResults $ Just filter{filterRunAfter = Nothing})) filterRunAfter
          forM_ filterJobTypes $ \x -> renderFilter "Job type" x (rFilterResults $ Just filter{filterJobTypes = filterJobTypes \\ [x]})

        button_ [ class_ "btn btn-default search-button", type_ "button" ] $ "Search"
      -- ul_ [ class_ "list-inline" ] $ do
      --   li_ $ span_ $ strong_ "Common searches:"
      --   li_ $ a_ [ href_ (rFilterResults $ Just mempty) ] $ "All jobs"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterStatuses = [Job.Locked] }) ] $ "Currently running"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterStatuses = [Job.Success] }) ] $ "Successful"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterStatuses = [Job.Failed] }) ] $ "Failed"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterRunAfter = Just t }) ] $ "Future"
      --   -- li_ $ a_ [ href_ "#" ] $ "Retried"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterStatuses = [Job.Queued] }) ] $ "Queued"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterUpdatedAfter = Just $ timeSince t 10 Minutes Ago }) ] $ "Last 10 mins"
      --   li_ $ a_ [ href_ (rFilterResults $ Just $ filter{ filterCreatedAfter = Just $ timeSince t 10 Minutes Ago }) ] $ "Recently created"
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

jobRow :: Routes -> UTCTime -> (Job, Html ()) -> Html ()
jobRow routes t (job@Job{..}, jobHtml) = do
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
      actionsFn routes job


actionsFailed :: Routes -> Job -> Html ()
actionsFailed Routes{..} Job{..} = do
  form_ [ action_ (rEnqueue jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-secondary", type_ "submit" ] $ "Enqueue again"

actionsRetry :: Routes -> Job -> Html ()
actionsRetry Routes{..} Job{..} = do
  form_ [ action_ (rRunNow jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-secondary", type_ "submit" ] $ "Run now"

actionsFuture :: Routes -> Job -> Html ()
actionsFuture Routes{..} Job{..} = do
  form_ [ action_ (rRunNow jobId), method_ "post" ] $ do
    button_ [ class_ "btn btn-secondary", type_ "submit" ] $ "Run now"

actionsWaiting :: Routes -> Job -> Html ()
actionsWaiting Routes{..} Job{..} = do
  form_ [ action_ (rCancel jobId), method_ "post" ] $ do
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

resultsPanel :: Routes -> UTCTime -> Filter -> [(Job, Html ())] -> Int -> Html ()
resultsPanel routes@Routes{..} t filter@Filter{filterPage} js runningCount = do
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
        forM_ js (jobRow routes t)
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
            Just (l, o) -> ("", rFilterResults $ Just $ filter {filterPage = Just (l, max 0 $ o - l)})
      li_ [ class_ ("page-item previous " <> extraClass) ] $ do
        a_ [ class_ "page-link", href_ lnk ] $ "Prev"

    nextLink = do
      let (extraClass, lnk) = case filterPage of
            Nothing ->
              if (DL.length js) < 10
              then ("disabled", "")
              else ("", (rFilterResults $ Just $ filter {filterPage = Just (10, 10)}))
            Just (l, o) ->
              if (DL.length js) < l
              then ("disabled", "")
              else ("", (rFilterResults $ Just $ filter {filterPage = Just (l, o + l)}))
      li_ [ class_ ("page-item next " <> extraClass) ] $ do
        a_ [ class_ "page-link", href_ lnk ] $ "Next"

ariaExpanded_ :: Text -> Attribute
ariaExpanded_ v = makeAttribute "aria-expanded" v

