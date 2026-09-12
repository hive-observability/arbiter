{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.TestHelpers
  ( createHasqlPool
  ) where

import Data.ByteString (ByteString)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes)
import Hasql.Connection qualified as Hasql

import Arbiter.Hasql.HasqlDb (hasqlSettings)

createHasqlPool :: Int -> ByteString -> IO (Pool Hasql.Connection)
createHasqlPool numConnections connStr =
  newPool
    $ setNumStripes (Just 1)
    $ defaultPoolConfig
      ( do
          result <- Hasql.acquire (hasqlSettings connStr)
          case result of
            Right conn -> pure conn
            Left err -> error $ "hasql test: connection failed: " <> show err
      )
      Hasql.release
      60
      numConnections
