{-# LANGUAGE OverloadedStrings #-}

-- | The LISTEN/NOTIFY hub over a libpq connection.
module Arbiter.LibPQ
  ( libpqListenConn
  , withLibPQListenConn
  , newLibPQListener
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn (..), Listener, Notification (..), newListener)
import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Exception (bracket, onException)
import Control.Monad ((>=>))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Text qualified as T
import Database.PostgreSQL.LibPQ qualified as PQ

-- | A 'ListenConn' over a libpq connection.
libpqListenConn :: PQ.Connection -> ListenConn
libpqListenConn conn =
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

-- | Run an action on a libpq connection of its own, opened from a connection string.
withLibPQListenConn :: ByteString -> (ListenConn -> IO a) -> IO a
withLibPQListenConn connStr action =
  bracket (interruptibleConnectDb connStr) PQ.finish $ \conn -> do
    status <- PQ.status conn
    case status of
      PQ.ConnectionOk -> action (libpqListenConn conn)
      _ -> do
        merr <- PQ.errorMessage conn
        throwInternal $ "connect failed" <> foldMap ((": " <>) . T.pack . BSC.unpack) merr

-- | A 'Listener' over its own libpq connection, opened from a connection string.
newLibPQListener :: (MonadIO m) => ByteString -> m Listener
newLibPQListener connStr = liftIO (newListener (withLibPQListenConn connStr))

-- | Open a libpq connection asynchronously. A teardown cancel interrupts the connect.
interruptibleConnectDb :: ByteString -> IO PQ.Connection
interruptibleConnectDb connStr = do
  conn <- PQ.connectStart connStr
  status <- PQ.status conn
  case status of
    PQ.ConnectionBad -> pure conn
    _ -> (poll conn >> pure conn) `onException` PQ.finish conn
  where
    poll conn = do
      status <- PQ.connectPoll conn
      case status of
        PQ.PollingReading -> waitSocket conn threadWaitRead >> poll conn
        PQ.PollingWriting -> waitSocket conn threadWaitWrite >> poll conn
        _ -> pure ()
    waitSocket conn wait =
      PQ.socket conn >>= \case
        Just socketFd -> wait socketFd
        Nothing -> throwInternal "connection has no socket during connect"
