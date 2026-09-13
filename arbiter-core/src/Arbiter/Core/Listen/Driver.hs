{-# LANGUAGE OverloadedStrings #-}

-- | A 'ListenConn' and an interruptible connect over any libpq-shaped driver.
module Arbiter.Core.Listen.Driver
  ( ListenDriver (..)
  , ConnectDriver (..)
  , driverListenConn
  , withDriverListenConn
  ) where

import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Exception (bracket, onException)
import Control.Monad ((>=>))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (traverse_)
import Data.Text qualified as T
import System.Posix.Types (Fd)

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn (..), Notification (..))

-- | The driver calls the hub loop runs on an open connection.
data ListenDriver conn notify result status = ListenDriver
  { notifies :: conn -> IO (Maybe notify)
  , socket :: conn -> IO (Maybe Fd)
  , consumeInput :: conn -> IO Bool
  , exec :: conn -> ByteString -> IO (Maybe result)
  , resultStatus :: result -> IO status
  , escapeIdentifier :: conn -> ByteString -> IO (Maybe ByteString)
  , commandOk :: status
  , notification :: notify -> Notification
  }

-- | The driver calls that open a connection asynchronously.
data ConnectDriver conn status poll = ConnectDriver
  { connectStart :: ByteString -> IO conn
  , connectPoll :: conn -> IO poll
  , status :: conn -> IO status
  , finish :: conn -> IO ()
  , errorMessage :: conn -> IO (Maybe ByteString)
  , connectionOk :: status
  , connectionBad :: status
  , pollingReading :: poll
  , pollingWriting :: poll
  }

-- | A 'ListenConn' over a driver connection.
driverListenConn :: (Eq status, Show status) => ListenDriver conn notify result status -> conn -> ListenConn
driverListenConn driver conn =
  ListenConn
    { listenNotifies = fmap (notification driver) <$> notifies driver conn
    , listenSocket = socket driver conn
    , listenConsumeInput = consumeInput driver conn
    , listenExec = exec driver conn >=> maybe (pure (Left "returned no result")) ok
    , listenEscapeIdentifier = escapeIdentifier driver conn
    }
  where
    ok res = do
      st <- resultStatus driver res
      pure $ if st == commandOk driver then Right () else Left ("failed with " <> T.pack (show st))

-- | Run an action on a connection of its own, opened from a connection string.
withDriverListenConn
  :: (Eq cstatus, Eq poll, Eq status, Show status)
  => ConnectDriver conn cstatus poll
  -> ListenDriver conn notify result status
  -> ByteString
  -> (ListenConn -> IO a)
  -> IO a
withDriverListenConn connector driver connStr action =
  bracket (interruptibleConnect connector (socket driver) connStr) (finish connector) $ \conn -> do
    st <- status connector conn
    if st == connectionOk connector
      then action (driverListenConn driver conn)
      else do
        merr <- errorMessage connector conn
        throwInternal $ "connect failed" <> foldMap ((": " <>) . T.pack . BSC.unpack) merr

-- | Open a connection asynchronously. A teardown cancel interrupts the connect.
interruptibleConnect
  :: (Eq cstatus, Eq poll) => ConnectDriver conn cstatus poll -> (conn -> IO (Maybe Fd)) -> ByteString -> IO conn
interruptibleConnect connector socketOf connStr = do
  conn <- connectStart connector connStr
  st <- status connector conn
  if st == connectionBad connector
    then pure conn
    else (poll conn >> pure conn) `onException` finish connector conn
  where
    poll conn = connectPoll connector conn >>= traverse_ (\wait -> waitSocket conn wait >> poll conn) . waitFor
    waitFor st
      | st == pollingReading connector = Just threadWaitRead
      | st == pollingWriting connector = Just threadWaitWrite
      | otherwise = Nothing
    waitSocket conn wait =
      socketOf conn >>= \case
        Just socketFd -> wait socketFd
        Nothing -> throwInternal "connection has no socket during connect"
