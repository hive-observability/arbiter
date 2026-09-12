{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Connection failure recovery tests, instantiated for each backend.
module Arbiter.Worker.TestKit.ConnectionRecovery (connectionRecoverySpec) where

import Arbiter.Core.Codec (Col (CInt4), col)
import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( JobRead
  , defaultJob
  , payload
  , setGroupKey
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (JobHandler, RegistryOf, ResultOf, withDbTransaction)
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (execQuery)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int32)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple (Only (..), close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec
import UnliftIO (bracket)
import UnliftIO.Async (withAsync)

-- | The worker pool survives connection termination and keeps processing
-- after it reconnects.
connectionRecoverySpec
  :: forall payload m env
   . ( Eq payload
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ ()
     )
  => Text
  -- ^ Schema name, also the LISTEN channel prefix
  -> ByteString
  -- ^ Connection string, for raw side connections
  -> (Text -> payload)
  -- ^ Construct a simple task payload
  -> IO env
  -- ^ Create a fresh env over an emptied queue table
  -> (env -> IO ())
  -- ^ Release an env built by the action above
  -> ((JobRead payload -> m (ResultOf m payload)) -> JobHandler m payload (ResultOf m payload))
  -- ^ Adapt a job action into the backend's handler shape
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function
  -> Spec
connectionRecoverySpec schema connStr mkSimple mkEnv destroyEnv mkHandler runM =
  around (bracket ((,) <$> mkEnv <*> mkEnv) (\(env, spare) -> destroyEnv env >> destroyEnv spare)) $
    describe "Connection Recovery" $ do
      it "processes jobs inserted before and after a connection kill" $ \(env, spare) -> do
        completedRef <- newIORef (0 :: Int)

        let handler _job = liftIO $ atomicModifyIORef' completedRef $ \count -> (count + 1, ())

        traverse_
          ( \jobIndex ->
              runM env
                $ void
                $ HL.insertJob
                $ setGroupKey (Just "g1")
                $ defaultJob (mkSimple (T.pack $ "Pre-kill " <> show jobIndex))
          )
          [1 :: Int .. 3]

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler handler)
        let workerConfig =
              config
                { workerCount = 1
                , pollInterval = 0.2
                , jitter = NoJitter
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 3) <$> readIORef completedRef

          preKillCompleted <- readIORef completedRef
          preKillCompleted `shouldBe` 3

          killSchemaConnections connStr schema

          threadDelay 7_000_000

          insertJobsDirect
            spare
            [ mkSimple "Post-kill 1"
            , mkSimple "Post-kill 2"
            , mkSimple "Post-kill 3"
            ]

          waitUntil 10_000 $ (== 6) <$> readIORef completedRef

          completed <- readIORef completedRef
          completed `shouldBe` 6

      it "retries job after worker ack fails due to terminated connection" $ \(env, _) -> do
        completedRef <- newIORef (0 :: Int)
        killOnceRef <- newIORef True

        let handler job = do
              when (payload job == mkSimple "slow") $ do
                shouldKill <- liftIO $ atomicModifyIORef' killOnceRef $ \armed -> (False, armed)
                when shouldKill $ do
                  myPid <- execQuery "SELECT pg_backend_pid() AS pid" [] (col "pid" CInt4)
                  liftIO $ traverse_ (terminatePid connStr) myPid
              liftIO $ atomicModifyIORef' completedRef $ \count -> (count + 1, ())

        runM env
          $ void
          $ HL.insertJob
          $ setMaxAttempts (Just 3)
          $ setGroupKey (Just "g1")
          $ defaultJob (mkSimple "slow")

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler handler)
        let workerConfig =
              config
                { workerCount = 1
                , pollInterval = 0.2
                , jitter = NoJitter
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 15_000 $ (== 2) <$> readIORef completedRef

          completed <- readIORef completedRef
          completed `shouldBe` 2

          let remainingCount =
                length <$> runM env (HL.listJobs @payload 10 0)
          waitUntil 10_000 $ (== 0) <$> remainingCount

          remaining <- remainingCount
          remaining `shouldBe` 0
  where
    insertJobsDirect :: env -> [payload] -> IO ()
    insertJobsDirect spare payloads =
      runM spare
        $ withDbTransaction
        $ traverse_ (void . HL.insertJob . setGroupKey (Just "g1") . defaultJob) payloads

-- | Kill active connections that reference the schema.
killSchemaConnections :: ByteString -> Text -> IO ()
killSchemaConnections connStr schemaName =
  bracket (connectPostgreSQL connStr) close $ \conn ->
    void $
      PG.query @_ @(Only Bool)
        conn
        "SELECT pg_terminate_backend(pid) \
        \FROM pg_stat_activity \
        \WHERE pid <> pg_backend_pid() \
        \  AND datname = current_database() \
        \  AND query LIKE ?"
        (Only ("%" <> schemaName <> "%" :: Text))

-- | Terminate one backend by pid.
terminatePid :: ByteString -> Int32 -> IO ()
terminatePid connStr pid =
  bracket (connectPostgreSQL connStr) close $ \conn ->
    void $ PG.query @_ @(Only Bool) conn "SELECT pg_terminate_backend(?)" (Only pid)
