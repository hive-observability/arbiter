{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.TestHelpers
  ( createHasqlPool
  , createHasqlTestEnv
  , runHasqlCommand
  , useDedicatedTestListener
  ) where

import Arbiter.Core.Backend (withConn)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Test.Setup (createPoolWith)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import Data.Proxy (Proxy)
import Data.Text (Text)
import Data.Text.Encoding qualified as TE
import Hasql.Connection qualified as Hasql

import Arbiter.Hasql.Compat (hasqlAcquire, hasqlSettings, runSQL)
import Arbiter.Hasql.HasqlDb (HasqlDb, HasqlEnv, createHasqlEnv, useDedicatedListener)

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

-- | 'useDedicatedListener' over the libpq adapter on hasql 2.
useDedicatedTestListener :: ByteString -> HasqlEnv registry -> IO (HasqlEnv registry)
#if MIN_VERSION_hasql(2,0,0)
useDedicatedTestListener = useDedicatedListener Ffi.adapter
#else
useDedicatedTestListener = useDedicatedListener
#endif

createHasqlPool :: Int -> ByteString -> IO (Pool Hasql.Connection)
createHasqlPool numConnections connStr =
  createPoolWith numConnections connect Hasql.release
  where
    connect = acquire (hasqlSettings connStr) >>= either (fail . ("hasql test: connection failed: " <>)) pure
#if MIN_VERSION_hasql(2,0,0)
    acquire = hasqlAcquire Ffi.adapter
#else
    acquire = hasqlAcquire
#endif

-- | Run a command as a bare script on the monad's current connection.
runHasqlCommand :: Text -> HasqlDb registry IO ()
runHasqlCommand sql = withConn $ \conn -> liftIO (runSQL conn (TE.encodeUtf8 sql))
