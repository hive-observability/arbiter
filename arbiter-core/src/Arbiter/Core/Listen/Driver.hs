{-# LANGUAGE OverloadedStrings #-}

-- | A 'ListenConn' and an interruptible connect over any libpq-shaped driver.
module Arbiter.Core.Listen.Driver
  ( ListenDriver (..)
  , ConnectDriver (..)
  , ConnStatus (..)
  , Polling (..)
  , execOutcome
  , driverListenConn
  , withDriverListenConn
  ) where

import Control.Concurrent (threadWaitRead, threadWaitWrite)
import Control.Exception (bracket, onException)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (traverse_)
import Data.Text (Text)
import Data.Text qualified as T
import System.Posix.Types (Fd)

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Listen (ListenConn (..), Notification (..))

-- | The driver calls the hub loop runs on an open connection.
data ListenDriver conn = ListenDriver
  { notifies :: conn -> IO (Maybe Notification)
  , socket :: conn -> IO (Maybe Fd)
  , consumeInput :: conn -> IO Bool
  , exec :: conn -> ByteString -> IO (Either Text ())
  -- ^ Run a command, reporting why it failed.
  , escapeIdentifier :: conn -> ByteString -> IO (Maybe ByteString)
  }

-- | Where an asynchronous connect stands.
data ConnStatus = ConnOk | ConnBad | ConnPending
  deriving stock (Eq, Show)

-- | What a connect poll asks the caller to wait for.
data Polling = PollReading | PollWriting | PollDone
  deriving stock (Eq, Show)

-- | The driver calls that open a connection asynchronously.
data ConnectDriver conn = ConnectDriver
  { connectStart :: ByteString -> IO conn
  , connectPoll :: conn -> IO Polling
  , status :: conn -> IO ConnStatus
  , finish :: conn -> IO ()
  , errorMessage :: conn -> IO (Maybe ByteString)
  }

-- | Judge a command's result by its status against the driver's success status.
execOutcome :: (Eq status, Show status) => status -> (result -> IO status) -> Maybe result -> IO (Either Text ())
execOutcome okStatus resultStatus = maybe (pure (Left "returned no result")) (fmap judge . resultStatus)
  where
    judge st
      | st == okStatus = Right ()
      | otherwise = Left ("failed with " <> T.pack (show st))

-- | A 'ListenConn' over a driver connection.
driverListenConn :: ListenDriver conn -> conn -> ListenConn
driverListenConn driver conn =
  ListenConn
    { listenNotifies = notifies driver conn
    , listenSocket = socket driver conn
    , listenConsumeInput = consumeInput driver conn
    , listenExec = exec driver conn
    , listenEscapeIdentifier = escapeIdentifier driver conn
    }

-- | Run an action on a connection of its own, opened from a connection string.
withDriverListenConn
  :: ConnectDriver conn
  -> ListenDriver conn
  -> ByteString
  -> (ListenConn -> IO a)
  -> IO a
withDriverListenConn connector driver connStr action =
  bracket (interruptibleConnect connector (socket driver) connStr) (finish connector) $ \conn -> do
    st <- status connector conn
    if st == ConnOk
      then action (driverListenConn driver conn)
      else do
        merr <- errorMessage connector conn
        throwInternal $ "connect failed" <> foldMap ((": " <>) . T.pack . BSC.unpack) merr

-- | Open a connection asynchronously. A teardown cancel interrupts the connect.
interruptibleConnect :: ConnectDriver conn -> (conn -> IO (Maybe Fd)) -> ByteString -> IO conn
interruptibleConnect connector socketOf connStr = do
  conn <- connectStart connector connStr
  st <- status connector conn
  if st == ConnBad
    then pure conn
    else (poll conn >> pure conn) `onException` finish connector conn
  where
    poll conn = connectPoll connector conn >>= traverse_ (\wait -> waitSocket conn wait >> poll conn) . waitFor
    waitFor PollReading = Just threadWaitRead
    waitFor PollWriting = Just threadWaitWrite
    waitFor PollDone = Nothing
    waitSocket conn wait =
      socketOf conn >>= \case
        Just socketFd -> wait socketFd
        Nothing -> throwInternal "connection has no socket during connect"
