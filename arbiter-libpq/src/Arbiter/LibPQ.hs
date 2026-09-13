-- | The LISTEN/NOTIFY hub over a libpq connection.
module Arbiter.LibPQ
  ( libpqListenConn
  , withLibPQListenConn
  , newLibPQListener
  ) where

import Arbiter.Core.Listen (ListenConn, Listener, Notification (..), newListener)
import Arbiter.Core.Listen.Driver
  ( ConnStatus (..)
  , ConnectDriver (..)
  , ListenDriver (..)
  , Polling (..)
  , driverListenConn
  , execOutcome
  , withDriverListenConn
  )
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Database.PostgreSQL.LibPQ qualified as PQ

listenDriver :: ListenDriver PQ.Connection
listenDriver =
  ListenDriver
    { notifies = fmap (fmap notification) . PQ.notifies
    , socket = PQ.socket
    , consumeInput = PQ.consumeInput
    , exec = \conn sql -> PQ.exec conn sql >>= execOutcome PQ.CommandOk PQ.resultStatus
    , escapeIdentifier = PQ.escapeIdentifier
    }
  where
    notification notify = Notification (PQ.notifyRelname notify) (PQ.notifyExtra notify)

connectDriver :: ConnectDriver PQ.Connection
connectDriver =
  ConnectDriver
    { connectStart = PQ.connectStart
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

-- | A 'ListenConn' over a libpq connection.
libpqListenConn :: PQ.Connection -> ListenConn
libpqListenConn = driverListenConn listenDriver

-- | Run an action on a libpq connection of its own, opened from a connection string.
withLibPQListenConn :: ByteString -> (ListenConn -> IO a) -> IO a
withLibPQListenConn = withDriverListenConn connectDriver listenDriver

-- | A 'Listener' over its own libpq connection, opened from a connection string.
newLibPQListener :: (MonadIO m) => ByteString -> m Listener
newLibPQListener connStr = liftIO (newListener (withLibPQListenConn connStr))
