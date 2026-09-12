{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TypeFamilies #-}

-- | The hasql database monad with a built-in 'MonadArbiter' instance:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Hasql
--
-- myFunction :: HasqlDb MyRegistry IO ()
-- myFunction = insertJob (defaultJob myPayload)
-- @
module Arbiter.Hasql.HasqlDb
  ( -- * Database Monad
    HasqlDb
  , HasqlEnv
  , Prepared (..)
  , Db (..)
  , Env (..)
  , PoolState (..)
  , HasPoolState (..)
  , runHasqlDb
  , inTransaction

    -- * Environment Creation
  , createHasqlEnv
  , createHasqlEnvWithConfig
  , createHasqlEnvWithPool
  , destroyHasqlEnv
  , disableListener
  , useDedicatedListener
  , setPreparedStatements

    -- * Hasql Settings
  , hasqlSettings

    -- * Exceptions
  , HasqlConnectionError (..)
  ) where

import Arbiter.Core.Backend
  ( Db (..)
  , Env (..)
  , HasPoolState (..)
  , PoolState (..)
  , createEnvWithConfig
  , createEnvWithPool
  , destroyEnv
  , disableListener
  , runDb
  , useDedicatedListener
  )
import Arbiter.Core.Backend qualified as Backend
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig)
import Arbiter.Core.PoolConfig qualified as PC
import Control.Exception (Exception, throwIO)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (asks)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Hasql.Connection qualified as Hasql
import UnliftIO (MonadUnliftIO)

import Arbiter.Hasql.Compat (hasqlSettings, withHasqlLibPQConnection)
import Arbiter.Hasql.MonadArbiter
  ( hasqlExecuteQuery
  , hasqlExecuteQueryPrepared
  , hasqlExecuteStatement
  , hasqlRunHandlerWithConnection
  , hasqlWithDbTransaction
  )

-- | Thrown when a hasql connection cannot be acquired from the pool.
newtype HasqlConnectionError = HasqlConnectionError String
  deriving stock (Show)
  deriving anyclass (Exception)

-- | Whether the hot statements are prepared, see 'setPreparedStatements'.
newtype Prepared = Prepared Bool

-- | Schema name and connection pool for 'HasqlDb'.
type HasqlEnv = Env Hasql.Connection Prepared

-- | The hasql database monad.
type HasqlDb = Db Hasql.Connection Prepared

instance (MonadUnliftIO m) => MonadArbiter (Db Hasql.Connection Prepared registry m) where
  type RegistryOf (Db Hasql.Connection Prepared registry m) = registry
  type
    Handler (Db Hasql.Connection Prepared registry m) job result =
      Hasql.Connection -> job -> Db Hasql.Connection Prepared registry m result
  getSchema = asks schema
  executeQuery = hasqlExecuteQuery
  executeQueryPrepared query = asks extra >>= \(Prepared on) -> hasqlExecuteQueryPrepared on query
  executeStatement = hasqlExecuteStatement
  withDbTransaction = hasqlWithDbTransaction
  runHandlerWithConnection = hasqlRunHandlerWithConnection
  getListener = asks listener

-- | Release the env's connection pool, closing its open connections.
destroyHasqlEnv :: (MonadIO m) => HasqlEnv registry -> m ()
destroyHasqlEnv = destroyEnv

-- | Run a 'HasqlDb' action in its env.
runHasqlDb :: HasqlEnv registry -> HasqlDb registry m a -> m a
runHasqlDb = runDb

-- | Run a 'HasqlDb' action on one connection without a pool. The connection is pinned
-- as an open transaction. 'Arbiter.Core.MonadArbiter.withDbTransaction' nests through
-- savepoints. The caller owns the transaction.
--
-- @
-- _ <- Hasql.use conn (Session.script "BEGIN")
-- inTransaction conn "arbiter" $ do
--   Arb.insertJob (Arb.defaultJob myPayload)
-- _ <- Hasql.use conn (Session.script "COMMIT")
-- @
inTransaction
  :: forall registry m a
   . Hasql.Connection
  -> SchemaName
  -- ^ Schema name
  -> HasqlDb registry m a
  -> m a
inTransaction = Backend.inTransaction (Prepared True)

-- | Create a 'HasqlEnv' with conservative pool defaults. Size worker pools with
-- 'createHasqlEnvWithConfig' and @poolConfigForWorkers@.
createHasqlEnv
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> m (HasqlEnv registry)
createHasqlEnv proxy connStr schemaName =
  createHasqlEnvWithConfig proxy connStr schemaName PC.defaultPoolConfig

-- | Create a 'HasqlEnv' with custom pool settings.
createHasqlEnvWithConfig
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> PoolConfig
  -> m (HasqlEnv registry)
createHasqlEnvWithConfig _proxy connStr =
  createEnvWithConfig withHasqlLibPQConnection (Prepared True) acquire Hasql.release
  where
    acquire =
      Hasql.acquire (hasqlSettings connStr)
        >>= either (throwIO . HasqlConnectionError . show) pure

-- | Create a 'HasqlEnv' over a caller's own connection pool. The shared listener
-- holds one pool connection for the env's lifetime. Size the pool for the worker
-- load plus one. 'disableListener' runs poll-only and frees that slot.
-- 'useDedicatedListener' gives the listener its own connection.
createHasqlEnvWithPool
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -> Pool Hasql.Connection
  -> SchemaName
  -- ^ Schema name
  -> m (HasqlEnv registry)
createHasqlEnvWithPool _proxy = createEnvWithPool withHasqlLibPQConnection (Prepared True)

-- | Enable or disable prepared hot statements (the claim). Each pooled connection
-- prepares once and reuses the plan. Requires direct connections or a pooler that
-- supports server-side prepared statements.
setPreparedStatements :: Bool -> HasqlEnv registry -> HasqlEnv registry
setPreparedStatements flag env = env {extra = Prepared flag}
