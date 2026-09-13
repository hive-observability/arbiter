{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Every hasql version difference that arbiter-hasql depends on.
module Arbiter.Hasql.Compat
  ( runSQL
  , connectionInTransaction
  , withHasqlListenConn
  , hasqlAcquire
  , hasqlSettings
  , HasqlSettings
  , WithConnect
  , mapConnect
  , hasqlConnect
  , withDedicatedListenConn
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn)
import Data.ByteString (ByteString)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error qualified as TE
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session

#if MIN_VERSION_hasql(2,0,0)
import Arbiter.Core.Listen (Notification (..))
import Arbiter.Core.Listen.Driver (ConnectDriver (..), ListenDriver (..), driverListenConn, withDriverListenConn)
import Hasql.Connection.Settings qualified as Settings
import Pqi qualified as PQ
#elif MIN_VERSION_hasql(1,10,0)
import Arbiter.LibPQ (libpqListenConn, withLibPQListenConn)
import Database.PostgreSQL.LibPQ qualified as PQ
import Hasql.Connection.Settings qualified as Settings
#else
import Arbiter.LibPQ (libpqListenConn, withLibPQListenConn)
import Database.PostgreSQL.LibPQ qualified as PQ
import Hasql.Connection.Setting qualified as Setting
import Hasql.Connection.Setting.Connection qualified as ConnSetting
#endif

-- | Run a bare SQL command, such as @BEGIN@ or @COMMIT@.
runSQL :: Hasql.Connection -> ByteString -> IO ()
runSQL conn sql =
  Hasql.use conn (runScript (TE.decodeUtf8With TE.lenientDecode sql))
    >>= either (\err -> throwInternal $ "hasql runSQL error: " <> T.pack (show err)) pure

#if MIN_VERSION_hasql(1,10,0)
runScript :: T.Text -> Session.Session ()
runScript = Session.script
#else
runScript :: T.Text -> Session.Session ()
runScript = Session.sql
#endif

-- | Open a connection, describing any failure. hasql 2 takes the transport adapter first.
#if MIN_VERSION_hasql(2,0,0)
hasqlAcquire :: PQ.Adapter -> HasqlSettings -> IO (Either String Hasql.Connection)
hasqlAcquire adapter settings = either (Left . show) Right <$> Hasql.acquire adapter settings
#else
hasqlAcquire :: HasqlSettings -> IO (Either String Hasql.Connection)
hasqlAcquire settings = either (Left . show) Right <$> Hasql.acquire settings
#endif

#if MIN_VERSION_hasql(2,0,0)
-- | A function of the connect arguments: a transport adapter, such as @Pqi.Ffi.adapter@, then a connection string.
type WithConnect r = PQ.Adapter -> ByteString -> r

mapConnect :: (a -> b) -> WithConnect a -> WithConnect b
mapConnect = fmap . fmap

-- | 'hasqlAcquire' from the connect arguments.
hasqlConnect :: WithConnect (IO (Either String Hasql.Connection))
hasqlConnect adapter connStr = hasqlAcquire adapter (hasqlSettings connStr)

-- | Run the listener loop on a driver connection of its own.
withDedicatedListenConn :: WithConnect ((ListenConn -> IO a) -> IO a)
withDedicatedListenConn adapter = withDriverListenConn (pqiConnectDriver adapter) pqiListenDriver
#else
-- | A function of the connect arguments: a connection string.
type WithConnect r = ByteString -> r

mapConnect :: (a -> b) -> WithConnect a -> WithConnect b
mapConnect = fmap

-- | 'hasqlAcquire' from the connect arguments.
hasqlConnect :: WithConnect (IO (Either String Hasql.Connection))
hasqlConnect connStr = hasqlAcquire (hasqlSettings connStr)

-- | Run the listener loop on a driver connection of its own.
withDedicatedListenConn :: WithConnect ((ListenConn -> IO a) -> IO a)
withDedicatedListenConn = withLibPQListenConn
#endif

-- | Whether the connection is in a transaction block, valid or aborted.
connectionInTransaction :: Hasql.Connection -> IO Bool
#if MIN_VERSION_hasql(1,10,0)
connectionInTransaction conn = do
  result <- Hasql.use conn $ Session.onLibpqConnection $ \libpq -> do
    status <- PQ.transactionStatus libpq
    pure (Right (txStatusNeedsRollback status), libpq)
  case result of
    Right inTx -> pure inTx
    Left _ -> pure False
#else
connectionInTransaction conn =
  Hasql.withLibPQConnection conn $ \libpq -> do
    status <- PQ.transactionStatus libpq
    pure (txStatusNeedsRollback status)
#endif

-- | Run the listener loop on the connection's driver handle.
withHasqlListenConn :: Hasql.Connection -> (ListenConn -> IO a) -> IO a
#if MIN_VERSION_hasql(1,10,0)
withHasqlListenConn conn action = do
  result <- Hasql.use conn $ Session.onLibpqConnection $ \libpq -> do
    actionResult <- action (toListenConn libpq)
    pure (Right actionResult, libpq)
  either (const (throwInternal "connection lost")) pure result
#else
withHasqlListenConn conn action = Hasql.withLibPQConnection conn (action . toListenConn)
#endif

#if MIN_VERSION_hasql(2,0,0)
toListenConn :: PQ.Connection -> ListenConn
toListenConn = driverListenConn pqiListenDriver

pqiListenDriver :: ListenDriver PQ.Connection PQ.Notify PQ.Result PQ.ExecStatus
pqiListenDriver =
  ListenDriver
    { notifies = PQ.notifies
    , socket = PQ.socket
    , consumeInput = PQ.consumeInput
    , exec = PQ.exec
    , resultStatus = PQ.resultStatus
    , escapeIdentifier = PQ.escapeIdentifier
    , commandOk = PQ.CommandOk
    , notification = \notify -> Notification (PQ.notifyRelname notify) (PQ.notifyExtra notify)
    }

pqiConnectDriver :: PQ.Adapter -> ConnectDriver PQ.Connection PQ.ConnStatus PQ.PollingStatus
pqiConnectDriver adapter =
  ConnectDriver
    { connectStart = PQ.connectStart adapter
    , connectPoll = PQ.connectPoll
    , status = PQ.status
    , finish = PQ.finish
    , errorMessage = PQ.errorMessage
    , connectionOk = PQ.ConnectionOk
    , connectionBad = PQ.ConnectionBad
    , pollingReading = PQ.PollingReading
    , pollingWriting = PQ.PollingWriting
    }
#else
toListenConn :: PQ.Connection -> ListenConn
toListenConn = libpqListenConn
#endif

-- | @TransInTrans@ and @TransInError@ accept a @ROLLBACK@ without warning.
txStatusNeedsRollback :: PQ.TransactionStatus -> Bool
txStatusNeedsRollback PQ.TransInTrans = True
txStatusNeedsRollback PQ.TransInError = True
txStatusNeedsRollback _ = False

#if MIN_VERSION_hasql(1,10,0)
-- | Connection settings, whose representation follows the hasql version.
type HasqlSettings = Settings.Settings

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings = Settings.connectionString . TE.decodeUtf8With TE.lenientDecode
#else
-- | Connection settings, whose representation follows the hasql version.
type HasqlSettings = [Setting.Setting]

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings connStr = [Setting.connection (ConnSetting.string (TE.decodeUtf8With TE.lenientDecode connStr))]
#endif
