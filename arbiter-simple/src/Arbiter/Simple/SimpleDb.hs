{-# LANGUAGE TypeFamilies #-}

-- | The postgresql-simple database monad with a built-in 'MonadArbiter' instance:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Simple
--
-- myFunction :: SimpleDb MyRegistry IO ()
-- myFunction = insertJob (defaultJob myPayload)
-- @
module Arbiter.Simple.SimpleDb
  ( -- * Database Monad
    SimpleDb
  , SimpleEnv
  , Simple (..)
  , Db (..)
  , Env (..)
  , PoolState (..)
  , HasPoolState (..)
  , runSimpleDb
  , inTransaction

    -- * Environment Creation
  , createSimpleEnv
  , createSimpleEnvWithConfig
  , createSimpleEnvWithPool
  , destroySimpleEnv
  , disableListener
  , useDedicatedListener
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
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (asks)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)
import Database.PostgreSQL.Simple.Internal (withConnection)
import UnliftIO (MonadUnliftIO)

import Arbiter.Simple.MonadArbiter
  ( simpleExecuteQuery
  , simpleExecuteStatement
  , simpleRunHandlerWithConnection
  , simpleWithDbTransaction
  )

-- | The postgresql-simple backend tag.
data Simple = Simple

-- | Schema name and connection pool for 'SimpleDb'.
type SimpleEnv = Env Connection Simple

-- | The postgresql-simple database monad.
type SimpleDb = Db Connection Simple

instance (MonadUnliftIO m) => MonadArbiter (Db Connection Simple registry m) where
  type RegistryOf (Db Connection Simple registry m) = registry
  type Handler (Db Connection Simple registry m) job result = Connection -> job -> Db Connection Simple registry m result
  getSchema = asks schema
  executeQuery = simpleExecuteQuery
  executeStatement = simpleExecuteStatement
  withDbTransaction = simpleWithDbTransaction
  runHandlerWithConnection = simpleRunHandlerWithConnection
  getListener = asks listener

-- | Release the env's connection pool, closing its open connections.
destroySimpleEnv :: (MonadIO m) => SimpleEnv registry -> m ()
destroySimpleEnv = destroyEnv

-- | Run a 'SimpleDb' action in its env.
runSimpleDb :: SimpleEnv registry -> SimpleDb registry m a -> m a
runSimpleDb = runDb

-- | Run a 'SimpleDb' action on one connection without a pool or env. The connection is
-- pinned as an open transaction. 'Arbiter.Core.MonadArbiter.withDbTransaction' nests
-- through savepoints. The caller owns the transaction.
--
-- @
-- PG.withTransaction conn $ do
--   PG.execute conn "INSERT INTO orders ..." params
--   inTransaction conn "arbiter" $
--     Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
-- @
inTransaction
  :: forall registry m a
   . Connection
  -> SchemaName
  -- ^ Schema name
  -> SimpleDb registry m a
  -> m a
inTransaction = Backend.inTransaction Simple

-- | Create a 'SimpleEnv' with default pool settings. Size worker pools with
-- 'createSimpleEnvWithConfig' and @poolConfigForWorkers@.
createSimpleEnv
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> m (SimpleEnv registry)
createSimpleEnv proxy connStr schemaName =
  createSimpleEnvWithConfig proxy connStr schemaName PC.defaultPoolConfig

-- | Create a 'SimpleEnv' with custom pool settings.
--
-- @
-- let config = PoolConfig
--       { poolSize = 50
--       , poolIdleTimeout = 120
--       , poolStripes = Just 4
--       }
-- env <- createSimpleEnvWithConfig (Proxy @MyRegistry) "host=localhost dbname=mydb" "arbiter" config
-- @
createSimpleEnvWithConfig
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> PoolConfig
  -- ^ Pool configuration
  -> m (SimpleEnv registry)
createSimpleEnvWithConfig _proxy connStr =
  createEnvWithConfig withConnection Simple (connectPostgreSQL connStr) close

-- | Create a 'SimpleEnv' over a caller's own connection pool. The shared listener
-- holds one pool connection for the env's lifetime. Size the pool for the worker
-- load plus one. 'disableListener' runs poll-only and frees that slot.
-- 'useDedicatedListener' gives the listener its own connection.
createSimpleEnvWithPool
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> Pool Connection
  -- ^ User-provided connection pool
  -> SchemaName
  -- ^ Schema name
  -> m (SimpleEnv registry)
createSimpleEnvWithPool _proxy = createEnvWithPool withConnection Simple
