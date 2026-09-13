-- | The LISTEN/NOTIFY hub over a libpq connection.
module Arbiter.LibPQ
  ( libpqListenConn
  , withLibPQListenConn
  , newLibPQListener
  ) where

import Arbiter.Core.Listen (ListenConn, Listener, Notification (..), newListener)
import Arbiter.Core.Listen.Driver (ConnectDriver (..), ListenDriver (..), driverListenConn, withDriverListenConn)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Database.PostgreSQL.LibPQ qualified as PQ

listenDriver :: ListenDriver PQ.Connection PQ.Notify PQ.Result PQ.ExecStatus
listenDriver =
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

connectDriver :: ConnectDriver PQ.Connection PQ.ConnStatus PQ.PollingStatus
connectDriver =
  ConnectDriver
    { connectStart = PQ.connectStart
    , connectPoll = PQ.connectPoll
    , status = PQ.status
    , finish = PQ.finish
    , errorMessage = PQ.errorMessage
    , connectionOk = PQ.ConnectionOk
    , connectionBad = PQ.ConnectionBad
    , pollingReading = PQ.PollingReading
    , pollingWriting = PQ.PollingWriting
    }

-- | A 'ListenConn' over a libpq connection.
libpqListenConn :: PQ.Connection -> ListenConn
libpqListenConn = driverListenConn listenDriver

-- | Run an action on a libpq connection of its own, opened from a connection string.
withLibPQListenConn :: ByteString -> (ListenConn -> IO a) -> IO a
withLibPQListenConn = withDriverListenConn connectDriver listenDriver

-- | A 'Listener' over its own libpq connection, opened from a connection string.
newLibPQListener :: (MonadIO m) => ByteString -> m Listener
newLibPQListener connStr = liftIO (newListener (withLibPQListenConn connStr))
