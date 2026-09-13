{-# LANGUAGE RankNTypes #-}

-- | One backend's answers to what the generic worker suites need.
module Arbiter.Worker.TestKit.Backend (TestBackend (..)) where

import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.MonadArbiter (JobHandler, ResultOf)
import Data.ByteString (ByteString)
import Data.Text (Text)

data TestBackend payload m env = TestBackend
  { schema :: Text
  -- ^ Schema name, also the LISTEN channel prefix
  , table :: Text
  , connStr :: ByteString
  -- ^ For raw side connections
  , mkSimple :: Text -> payload
  , mkFailing :: Int -> payload
  , mkEnv :: IO env
  -- ^ A fresh env over an emptied queue table
  , mkEnvPollOnly :: IO env
  , destroyEnv :: env -> IO ()
  , mkHandler :: (JobRead payload -> m (ResultOf m payload)) -> JobHandler m payload (ResultOf m payload)
  , runCommand :: Text -> m ()
  -- ^ Run one SQL command on the monad's current connection. It may report no row count.
  , runM :: forall a. env -> m a -> IO a
  }
