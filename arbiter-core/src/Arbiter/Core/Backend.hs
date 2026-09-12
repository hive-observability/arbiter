{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The env, monad, pool state, and savepoint ladder shared by the pooled backends,
-- parameterized over the connection type.
module Arbiter.Core.Backend
  ( -- * Database Monad
    Db (..)
  , Env (..)
  , runDb
  , inTransaction

    -- * Environment Creation
  , WithListenConn
  , createEnvWithConfig
  , createEnvWithPool
  , destroyEnv
  , disableListener
  , useDedicatedListener
  , poolListener

    -- * Pool state
  , PoolState (..)
  , HasPoolState (..)
  , withConn
  , pinConnection
  , withSavepointTransaction
  ) where

import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT (..), asks, local, runReaderT)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (traverse_)
import Data.Pool (Pool, defaultPoolConfig, destroyAllResources, newPool, setNumStripes, withResource)
import Data.Text (Text)
import UnliftIO (MonadUnliftIO, mask, onException, withRunInIO)

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Listen (ListenConn, Listener, dedicatedListener, newDedicatedListen, newPoolListener)
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)

-- | Pool, pinned connection, and savepoint depth.
data PoolState conn = PoolState
  { connectionPool :: Maybe (Pool conn)
  , activeConn :: Maybe conn
  , transactionDepth :: Int
  }

-- | Ambient access to the pool state.
class (Monad m) => HasPoolState conn m | m -> conn where
  getPoolState :: m (PoolState conn)
  localPoolState :: (PoolState conn -> PoolState conn) -> m a -> m a

-- | Schema name, pool state, listener, and the prepared-statement flag.
data Env conn (registry :: JobPayloadRegistry) = Env
  { schema :: SchemaName
  , poolState :: PoolState conn
  , listener :: Maybe Listener
  -- ^ Resolved LISTEN source. 'Nothing' runs poll-only.
  , preparedStatements :: Bool
  -- ^ Whether a backend prepares its hot statements once per connection.
  }

-- | A pooled backend's database monad.
newtype Db conn (registry :: JobPayloadRegistry) m a = Db {unDb :: ReaderT (Env conn registry) m a}
  deriving newtype
    ( Applicative
    , Functor
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadReader (Env conn registry)
    , MonadThrow
    , MonadUnliftIO
    )

instance (Monad m) => HasPoolState conn (Db conn registry m) where
  getPoolState = asks poolState
  localPoolState adjust = local (\env -> env {poolState = adjust (poolState env)})

-- | Run a 'Db' action in its env.
runDb :: Env conn registry -> Db conn registry m a -> m a
runDb env action = runReaderT (unDb action) env

-- | Run a 'Db' action on one connection without a pool. The connection is pinned as
-- an open transaction. 'Arbiter.Core.MonadArbiter.withDbTransaction' nests through
-- savepoints. The caller owns the transaction.
inTransaction :: conn -> SchemaName -> Db conn registry m a -> m a
inTransaction conn schemaName =
  runDb
    Env
      { schema = schemaName
      , poolState = PoolState {connectionPool = Nothing, activeConn = Just conn, transactionDepth = 1}
      , listener = Nothing
      , preparedStatements = True
      }

-- | Release the env's connection pool, closing its open connections.
destroyEnv :: (MonadIO m) => Env conn registry -> m ()
destroyEnv env = liftIO $ traverse_ destroyAllResources (connectionPool (poolState env))

-- | Turn off the shared LISTEN listener for an env, running poll-only.
disableListener :: Env conn registry -> Env conn registry
disableListener env = env {listener = Nothing}

-- | Give the env a dedicated LISTEN connection opened from a connection string.
-- The listener takes no pool slot.
useDedicatedListener :: (MonadIO m) => ByteString -> Env conn registry -> m (Env conn registry)
useDedicatedListener connStr env = do
  dedicated <- newDedicatedListen connStr
  pure env {listener = Just (dedicatedListener dedicated)}

-- | Run the listener loop on a connection's driver handle.
type WithListenConn conn = conn -> (ListenConn -> IO ()) -> IO ()

-- | A listener that borrows one pool connection for the hub's lifetime.
poolListener :: WithListenConn conn -> Pool conn -> IO Listener
poolListener withListenConn pool = newPoolListener (\action -> withResource pool (`withListenConn` action))

-- | Create an env over a new pool opened with the connect and release actions.
createEnvWithConfig
  :: (MonadIO m)
  => WithListenConn conn
  -> IO conn
  -> (conn -> IO ())
  -> SchemaName
  -> PoolConfig
  -> m (Env conn registry)
createEnvWithConfig withListenConn connect release schemaName config = liftIO $ do
  connPool <-
    newPool
      $ setNumStripes (poolStripes config)
      $ defaultPoolConfig connect release (fromIntegral $ poolIdleTimeout config) (poolSize config)
  createEnvWithPool withListenConn connPool schemaName

-- | Create an env over a caller's own connection pool. The shared listener holds one
-- pool connection for the env's lifetime. Size the pool for the worker load plus
-- one. 'disableListener' runs poll-only and frees that slot. 'useDedicatedListener'
-- gives the listener its own connection.
createEnvWithPool :: (MonadIO m) => WithListenConn conn -> Pool conn -> SchemaName -> m (Env conn registry)
createEnvWithPool withListenConn connPool schemaName = liftIO $ do
  lstn <- poolListener withListenConn connPool
  pure
    Env
      { schema = schemaName
      , poolState = PoolState {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
      , listener = Just lstn
      , preparedStatements = True
      }

-- | The pinned connection, or one checked out of the pool.
withConn :: (HasPoolState conn m, MonadUnliftIO m) => (conn -> m a) -> m a
withConn action = do
  pool <- getPoolState
  case (activeConn pool, connectionPool pool) of
    (Just conn, _) -> action conn
    (Nothing, Just connPool) -> withRunInIO $ \run -> withResource connPool (run . action)
    (Nothing, Nothing) -> throwInternal noConnection

-- | Pin one pooled connection for the action.
pinConnection :: (HasPoolState conn m, MonadUnliftIO m) => m a -> m a
pinConnection action = do
  pool <- getPoolState
  case (activeConn pool, connectionPool pool) of
    (Just _, _) -> action
    (Nothing, Just connPool) -> withRunInIO $ \run ->
      withResource connPool $ \conn ->
        run $ localPoolState (\st -> st {activeConn = Just conn}) action
    (Nothing, Nothing) -> throwInternal noConnection

-- | Transaction bracket. Nests via savepoints. The outer bracket and the statement
-- runner are the backend's.
withSavepointTransaction
  :: (HasPoolState conn m, MonadUnliftIO m)
  => (conn -> ByteString -> IO ())
  -> (conn -> IO a -> IO a)
  -> m a
  -> m a
withSavepointTransaction runSql transaction action = do
  pool <- getPoolState
  let depth = transactionDepth pool
  case (activeConn pool, depth) of
    (Nothing, _) -> case connectionPool pool of
      Nothing -> throwInternal noConnection
      Just connPool -> withRunInIO $ \run ->
        withResource connPool $ \conn ->
          transaction conn
            $ run
            $ localPoolState (\st -> st {activeConn = Just conn, transactionDepth = 1}) action
    (Just conn, 0) -> withRunInIO $ \run ->
      transaction conn
        $ run
        $ localPoolState (\st -> st {transactionDepth = 1}) action
    (Just conn, _) -> mask $ \restore -> do
      let spName = "arbiter_sp_" <> BSC.pack (show depth)
      liftIO $ runSql conn ("SAVEPOINT " <> spName)
      result <-
        restore (localPoolState (\st -> st {transactionDepth = depth + 1}) action)
          `onException` liftIO (runSql conn ("ROLLBACK TO SAVEPOINT " <> spName))
      liftIO $ runSql conn ("RELEASE SAVEPOINT " <> spName)
      pure result

noConnection :: Text
noConnection = "No active connection and no connection pool available"
