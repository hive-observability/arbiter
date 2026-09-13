{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Hasql.TestHelpers
  ( createHasqlPool
  , runHasqlCommand
  , testConnect
  ) where

import Arbiter.Core.Backend (withConn)
import Arbiter.Test.Setup (createPoolWith)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import Data.Text (Text)
import Data.Text.Encoding qualified as TE
import Hasql.Connection qualified as Hasql

import Arbiter.Hasql.Compat (acquireConnect, runSQL)
import Arbiter.Hasql.HasqlDb (HasqlConnect, HasqlDb, toHasqlConnect)

#if MIN_VERSION_hasql(2,0,0)
import Pqi.Ffi qualified as Ffi
#endif

-- | The test connect. On hasql 2 it uses the libpq adapter.
testConnect :: ByteString -> HasqlConnect
#if MIN_VERSION_hasql(2,0,0)
testConnect = toHasqlConnect Ffi.adapter
#else
testConnect = toHasqlConnect
#endif

createHasqlPool :: Int -> ByteString -> IO (Pool Hasql.Connection)
createHasqlPool numConnections connStr =
  createPoolWith numConnections connect Hasql.release
  where
    connect = acquireConnect (testConnect connStr) >>= either (fail . ("hasql test: connection failed: " <>)) pure

-- | Run a command as a bare script on the monad's current connection.
runHasqlCommand :: Text -> HasqlDb registry IO ()
runHasqlCommand sql = withConn $ \conn -> liftIO (runSQL conn (TE.encodeUtf8 sql))
