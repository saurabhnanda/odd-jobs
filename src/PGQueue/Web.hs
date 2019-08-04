{-# LANGUAGE DeriveGeneric, NamedFieldPuns, RankNTypes, ExistentialQuantification #-}
module PGQueue.Web where

import PGQueue.Types
import PGQueue.Job as Job
import Data.Time
import Data.Aeson as Aeson
import Data.Text as T
import GHC.Generics
import Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.ToRow as PGS
import Database.PostgreSQL.Simple.ToField as PGS
import Data.Pool as Pool
import UnliftIO

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
  } deriving (Eq, Show, Generic)

blankFilter :: Filter
blankFilter = Filter
  { filterStatuses = []
  , filterCreatedAfter = Nothing
  , filterCreatedBefore = Nothing
  , filterUpdatedAfter = Nothing
  , filterUpdatedBefore = Nothing
  , filterJobTypes = []
  , filterOrder = Nothing
  }


-- data FilterTerm = FTStatuses [Status]
--                 | FTCreatedAfter UTCTime
--                 | FTCreatedBefore UTCTime
--                 | FTUpdatedAfter UTCTime
--                 | FTUpdatedBefore UTCTime
--                 | FTJobType Text
--                 | FTOrderBy OrderByField OrderDirection
--                 deriving (Eq, Show, Generic)

-- type Filter = [FilterTerm]

instance ToJSON Status
instance FromJSON Status

instance ToJSON OrderDirection
instance FromJSON OrderDirection
instance ToJSON OrderByField
instance FromJSON OrderByField
-- instance ToJSON FilterTerm
-- instance FromJSON FilterTerm
instance ToJSON Filter where
  toJSON = Aeson.genericToJSON  Aeson.defaultOptions{omitNothingFields = True}
instance FromJSON Filter where
  parseJSON = Aeson.genericParseJSON Aeson.defaultOptions{omitNothingFields = True}


data WrappedToRow = forall a . ToRow a => WrappedToRow a

filterJobsQuery :: TableName -> Filter -> (PGS.Query, [Action])
filterJobsQuery tname Filter{filterStatuses, filterCreatedBefore, filterCreatedAfter, filterUpdatedBefore, filterUpdatedAfter, filterJobTypes, filterOrder} =
  ( "SELECT " <> Job.concatJobDbColumns <> " FROM " <> tname <> whereClause -- <> " ORDER BY " <> orderClause
  , whereActions
  )
  where

    (whereClause, whereActions) = case statusClause `and` createdAfterClause `and` createdBeforeClause `and` updatedBeforeClause `and` updatedAfterClause `and` jobTypeClause of
      Nothing -> (mempty, toRow ())
      Just (q, as) -> (" WHERE " <> q, as)

    statusClause = if Prelude.null filterStatuses
                   then Nothing
                   else Just ("status IN ?", toRow $ (Only (In filterStatuses)))

    createdAfterClause = Prelude.fmap (\x -> ("created_at >= ?", toRow $ Only x)) filterCreatedAfter
    createdBeforeClause = Prelude.fmap (\x -> ("created_at < ?", toRow $ Only x)) filterCreatedBefore
    updatedAfterClause = Prelude.fmap (\x -> ("updated_at >= ?", toRow $ Only x)) filterUpdatedAfter
    updatedBeforeClause = Prelude.fmap (\x -> ("updated_at < ?", toRow $ Only x)) filterUpdatedBefore

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

filterJobs :: (HasJobMonitor m) => Filter -> m [Job]
filterJobs f = do
  tname <- getTableName
  let (q, queryArgs) = filterJobsQuery tname f
  pool <- getDbPool
  Pool.withResource pool $ \conn -> liftIO $ PGS.query conn q queryArgs

-- f = encode [FTStatuses [Job.Success, Queued], FTJobType "QueuedMail"]

f = blankFilter
  { filterStatuses = [Job.Success, Queued]
  , filterJobTypes = ["QueuedMail", "ConfirmBooking"]
  }
