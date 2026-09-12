{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.TestHelpers
  ( createHasqlPool
  , createHasqlTestEnv
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Data.ByteString (ByteString)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes)
import Data.Proxy (Proxy)
import Hasql.Connection qualified as Hasql

import Arbiter.Hasql.Compat (hasqlAcquire, hasqlSettings)
import Arbiter.Hasql.HasqlDb (HasqlEnv, createHasqlEnv)

#if MIN_VERSION_hasql(2,0,0)
import Pqi.Ffi qualified as Ffi
#endif

-- | 'createHasqlEnv' over the libpq adapter on hasql 2.
createHasqlTestEnv :: Proxy registry -> ByteString -> SchemaName -> IO (HasqlEnv registry)
#if MIN_VERSION_hasql(2,0,0)
createHasqlTestEnv proxy = createHasqlEnv proxy Ffi.adapter
#else
createHasqlTestEnv = createHasqlEnv
#endif

createHasqlPool :: Int -> ByteString -> IO (Pool Hasql.Connection)
createHasqlPool numConnections connStr =
  newPool
    $ setNumStripes (Just 1)
    $ defaultPoolConfig
      ( do
          result <- acquire (hasqlSettings connStr)
          case result of
            Right conn -> pure conn
            Left err -> error $ "hasql test: connection failed: " <> err
      )
      Hasql.release
      60
      numConnections
  where
#if MIN_VERSION_hasql(2,0,0)
    acquire = hasqlAcquire Ffi.adapter
#else
    acquire = hasqlAcquire
#endif
