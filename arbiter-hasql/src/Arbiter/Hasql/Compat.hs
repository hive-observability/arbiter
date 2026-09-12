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
  , noRowCount
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn (..))
import Data.ByteString (ByteString)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error qualified as TE
import Hasql.Connection qualified as Hasql
import Hasql.Errors qualified as Errors
import Hasql.Session qualified as Session

#if MIN_VERSION_hasql(2,0,0)
import Arbiter.Core.Listen (Notification (..))
import Control.Monad ((>=>))
import Hasql.Connection.Settings qualified as Settings
import Pqi qualified as PQ
import Pqi.Ffi qualified as Ffi
#elif MIN_VERSION_hasql(1,10,0)
import Arbiter.Core.Listen (libpqListenConn)
import Database.PostgreSQL.LibPQ qualified as PQ
import Hasql.Connection.Settings qualified as Settings
#else
import Arbiter.Core.Listen (libpqListenConn)
import Database.PostgreSQL.LibPQ qualified as PQ
import Hasql.Connection.Setting qualified as Setting
import Hasql.Connection.Setting.Connection qualified as ConnSetting
#endif

-- | Whether a statement failed only because its command tag carries no row count.
noRowCount :: Errors.SessionError -> Bool
#if MIN_VERSION_hasql(1,10,0)
noRowCount (Errors.StatementSessionError _ _ _ _ _ (Errors.UnexpectedResultStatementError "Empty bytes")) = True
#else
noRowCount (Errors.QueryError _ _ (Errors.ResultError (Errors.UnexpectedResult "Empty bytes"))) = True
#endif
noRowCount _ = False

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

-- | Open a connection, describing any failure.
hasqlAcquire :: HasqlSettings -> IO (Either String Hasql.Connection)
#if MIN_VERSION_hasql(2,0,0)
hasqlAcquire settings = either (Left . show) Right <$> Hasql.acquire Ffi.adapter settings
#else
hasqlAcquire settings = either (Left . show) Right <$> Hasql.acquire settings
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
toListenConn conn =
  ListenConn
    { listenNotifies = fmap toNotification <$> PQ.notifies conn
    , listenSocket = PQ.socket conn
    , listenConsumeInput = PQ.consumeInput conn
    , listenExec = PQ.exec conn >=> maybe (pure (Left "returned no result")) commandOk
    , listenEscapeIdentifier = PQ.escapeIdentifier conn
    }
  where
    commandOk res = do
      status <- PQ.resultStatus res
      pure $ if status == PQ.CommandOk then Right () else Left ("failed with " <> T.pack (show status))
    toNotification notify =
      Notification
        { notificationChannel = PQ.notifyRelname notify
        , notificationData = PQ.notifyExtra notify
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
