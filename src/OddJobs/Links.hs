{-# LANGUAGE RecordWildCards, OverloadedStrings #-}

module OddJobs.Links where

import Servant
import Servant.API.Generic
import OddJobs.Web (Routes(..))
import Data.String.Conv
import Data.Text

Routes{..} = allFieldLinks' absText :: Routes (AsLink Text)

absText :: Link -> Text
absText l = "/" <> (toS $ show $ linkURI l)
