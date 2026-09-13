{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Every hasql version difference that arbiter-hasql depends on.
module Arbiter.Hasql.Compat
  ( runSQL
  , connectionInTransaction
  , withHasqlListenConn
  , hasqlSettings
  , HasqlSettings
  , HasqlConnect
  , toHasqlConnect
  , acquireConnect
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
import Arbiter.Core.Listen.Driver
  ( ConnStatus (..)
  , ConnectDriver (..)
  , ListenDriver (..)
  , Polling (..)
  , driverListenConn
  , execOutcome
  , withDriverListenConn
  )
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
  Hasql.use conn (runScript sql)
    >>= either (\err -> throwInternal $ "hasql runSQL error: " <> T.pack (show err)) pure

#if MIN_VERSION_hasql(1,10,0)
runScript :: ByteString -> Session.Session ()
runScript = Session.script . TE.decodeUtf8With TE.lenientDecode
#else
runScript :: ByteString -> Session.Session ()
runScript = Session.sql
#endif

#if MIN_VERSION_hasql(2,0,0)
-- | How to open a connection. On hasql 2 this is a transport adapter, such as @Pqi.Ffi.adapter@, and a connection string.
data HasqlConnect = HasqlConnect PQ.Adapter ByteString

toHasqlConnect :: PQ.Adapter -> ByteString -> HasqlConnect
toHasqlConnect = HasqlConnect

-- | Open a connection. A failure comes back as its description.
acquireConnect :: HasqlConnect -> IO (Either String Hasql.Connection)
acquireConnect (HasqlConnect adapter connStr) = either (Left . show) Right <$> Hasql.acquire adapter (hasqlSettings connStr)

-- | Run the listener loop on a driver connection of its own.
withDedicatedListenConn :: HasqlConnect -> (ListenConn -> IO a) -> IO a
withDedicatedListenConn (HasqlConnect adapter connStr) = withDriverListenConn (pqiConnectDriver adapter) pqiListenDriver connStr
#else
-- | How to open a connection. On hasql 1.x this is a connection string.
newtype HasqlConnect = HasqlConnect ByteString

toHasqlConnect :: ByteString -> HasqlConnect
toHasqlConnect = HasqlConnect

-- | Open a connection. A failure comes back as its description.
acquireConnect :: HasqlConnect -> IO (Either String Hasql.Connection)
acquireConnect (HasqlConnect connStr) = either (Left . show) Right <$> Hasql.acquire (hasqlSettings connStr)

-- | Run the listener loop on a driver connection of its own.
withDedicatedListenConn :: HasqlConnect -> (ListenConn -> IO a) -> IO a
withDedicatedListenConn (HasqlConnect connStr) = withLibPQListenConn connStr
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

pqiListenDriver :: ListenDriver PQ.Connection
pqiListenDriver =
  ListenDriver
    { notifies = fmap (fmap notification) . PQ.notifies
    , socket = PQ.socket
    , consumeInput = PQ.consumeInput
    , exec = \conn sql -> PQ.exec conn sql >>= execOutcome PQ.CommandOk PQ.resultStatus
    , escapeIdentifier = PQ.escapeIdentifier
    }
  where
    notification notify = Notification (PQ.notifyRelname notify) (PQ.notifyExtra notify)

pqiConnectDriver :: PQ.Adapter -> ConnectDriver PQ.Connection
pqiConnectDriver adapter =
  ConnectDriver
    { connectStart = PQ.connectStart adapter
    , connectPoll = fmap polling . PQ.connectPoll
    , status = fmap connStatus . PQ.status
    , finish = PQ.finish
    , errorMessage = PQ.errorMessage
    }
  where
    polling PQ.PollingReading = PollReading
    polling PQ.PollingWriting = PollWriting
    polling _ = PollDone
    connStatus PQ.ConnectionOk = ConnOk
    connStatus PQ.ConnectionBad = ConnBad
    connStatus _ = ConnPending
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
