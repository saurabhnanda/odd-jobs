{-# LANGUAGE RecordWildCards, OverloadedStrings #-}

module PGQueue.Links where

import Servant
import Servant.API.Generic
import PGQueue.Web (Routes(..))
import Data.String.Conv
import Data.Text

Routes{..} = allFieldLinks' absText :: Routes (AsLink Text)

absText :: Link -> Text
absText l = "/" <> (toS $ show $ linkURI l)
