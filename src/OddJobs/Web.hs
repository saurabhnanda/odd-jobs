{-# LANGUAGE DeriveGeneric, NamedFieldPuns, TypeOperators, DataKinds #-}
module OddJobs.Web where

import OddJobs.Types
import OddJobs.Job as Job
import Data.Time
import Data.Aeson as Aeson
import Data.Text as T
import GHC.Generics
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
import Data.String.Conv
import qualified Data.HashMap.Strict as HM

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

data Routes route = Routes
  { rFilterResults :: route :- QueryParam "filters" Filter :> Get '[HTML] (Html ())
  , rStaticAssets :: route :- "assets" :> Raw
  , rEnqueue :: route :- "enqueue" :> Capture "jobId" JobId :> Post '[HTML] NoContent
  , rRunNow :: route :- "run" :> Capture "jobId" JobId :> Post '[HTML] NoContent
  } deriving (Generic)


filterJobsQuery :: TableName -> Filter -> (PGS.Query, [Action])
filterJobsQuery tname Filter{filterStatuses, filterCreatedBefore, filterCreatedAfter, filterUpdatedBefore, filterUpdatedAfter, filterJobTypes, filterOrder, filterPage, filterRunAfter} =
  ( "SELECT " <> Job.concatJobDbColumns <> " FROM " <> tname <> whereClause <> " " <> (orderClause $ fromMaybe (OrdUpdatedAt, Desc) filterOrder) <> " " <> limitOffsetClause
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

    (whereClause, whereActions) = case statusClause `and` createdAfterClause `and` createdBeforeClause `and` updatedBeforeClause `and` updatedAfterClause `and` jobTypeClause `and` runAfterClause of
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
        let qFragment = "payload @> ?"
            valBuilder v = toField $ Aeson.object ["tag" .= v]
            build ys (q, vs) = case ys of
              [] -> (q, vs)
              (y:[]) -> (qFragment <> q, (valBuilder y):vs)
              (y:ys_) -> build ys_ (" OR " <> qFragment <> q, (valBuilder y):vs)
        in Just $ build xs (mempty, [])

    and :: Maybe (Query, [PGS.Action]) -> Maybe (Query, [PGS.Action]) -> Maybe (Query, [PGS.Action])
    and Nothing Nothing = Nothing
    and Nothing (Just (q, as)) = Just (q, as)
    and (Just (q, as)) Nothing = Just (q, as)
    and (Just (qa, as)) (Just (qb, bs)) = Just ("(" <> qa <> ") AND (" <> qb <> ")", as <> bs)

    -- orderClause = _

filterJobs :: Connection -> TableName -> Filter -> IO [Job]
filterJobs conn tname f = do
  let (q, queryArgs) = filterJobsQuery tname f
  PGS.query conn q queryArgs

countJobs :: Connection -> TableName -> Filter -> IO Int
countJobs conn tname f = do
  let (q, queryArgs) = filterJobsQuery tname f
      finalqry = "SELECT count(*) FROM (" <> q <> ") a"
  [Only r] <- PGS.query conn finalqry queryArgs
  pure r


-- f = encode [FTStatuses [Job.Success, Queued], FTJobType "QueuedMail"]

-- f = blankFilter
--   { filterStatuses = [Job.Success, Queued]
--   , filterJobTypes = ["QueuedMail", "ConfirmBooking"]
--   }


