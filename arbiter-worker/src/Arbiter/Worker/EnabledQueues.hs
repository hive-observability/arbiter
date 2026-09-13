{-# LANGUAGE OverloadedStrings #-}

-- | Resolving which queues a worker process should run from
-- @ARBITER_ENABLED_QUEUES@.
module Arbiter.Worker.EnabledQueues
  ( getEnabledQueues
  , requestedQueues
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy)
import Data.Text (Text)
import Data.Text qualified as T
import System.Environment (lookupEnv)

-- | The environment variable naming the queues a worker process should run.
enabledQueuesEnvVar :: String
enabledQueuesEnvVar = "ARBITER_ENABLED_QUEUES"

-- | The comma-separated queue names @ARBITER_ENABLED_QUEUES@ asks for, every one of them
-- checked against the registry. Unset or blank gives the registry's whole queue set, and
-- a name outside it throws.
getEnabledQueues :: (RegistryTables registry) => Proxy registry -> IO [Text]
getEnabledQueues registry =
  fromMaybe (registryTableNames registry) <$> requestedQueues registry

-- | The queue names @ARBITER_ENABLED_QUEUES@ asks for, or 'Nothing' when it is
-- unset or blank. Names are validated against the registry.
requestedQueues :: (RegistryTables registry) => Proxy registry -> IO (Maybe [Text])
requestedQueues registry = do
  rawValue <- lookupEnv enabledQueuesEnvVar
  case T.strip . T.pack <$> rawValue of
    Just trimmed | not (T.null trimmed) -> Just <$> validate trimmed
    _ -> pure Nothing
  where
    validate trimmed =
      let allQueues = registryTableNames registry
          requested = filter (not . T.null) . map T.strip $ T.splitOn "," trimmed
          invalid = filter (`notElem` allQueues) requested
       in case (requested, invalid) of
            ([], _) -> throwInternal $ T.pack enabledQueuesEnvVar <> " is set but names no queues"
            (_, []) -> pure requested
            _ -> throwInternal $ "Unknown queue names: " <> T.intercalate ", " invalid
