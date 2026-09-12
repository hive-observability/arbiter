-- | A fresh env over the suite's shared pool, with the queue table emptied.
module Test.Arbiter.Worker.SharedPool (withPool) where

import Arbiter.Simple (SimpleEnv, createSimpleEnvWithPool)
import Arbiter.Test.Setup (cleanupData)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy)
import Data.Text (Text)
import Database.PostgreSQL.Simple (Connection)

withPool :: Proxy registry -> Text -> Text -> Pool Connection -> (SimpleEnv registry -> IO a) -> IO a
withPool registry schema table sharedPool action = do
  env <- createSimpleEnvWithPool registry sharedPool schema
  withResource sharedPool (cleanupData schema table)
  action env
