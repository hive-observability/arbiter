{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Reclaimed-job, stolen-job, and handler-exception tests, instantiated for each backend.
module Arbiter.Worker.TestKit.Reclaim (reclaimSpec) where

import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( JobRead
  , ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  , payload
  , primaryKey
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (JobHandler, RegistryOf, ResultOf)
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Text (Text)
import Database.PostgreSQL.Simple (close, connectPostgreSQL, execute)
import Database.PostgreSQL.Simple.Types (QualifiedIdentifier (..))
import Test.Hspec
import UnliftIO (bracket)

-- | A failing payload's remaining failure count, high enough to fail every attempt.
alwaysFailing :: Int
alwaysFailing = 999

-- | Reclaim-during-processing, heartbeat theft detection, and worker-loop
-- exception safety, instantiated for each backend.
reclaimSpec
  :: forall payload m env
   . ( Eq payload
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ ()
     )
  => Text
  -- ^ Schema name, also the LISTEN channel prefix
  -> Text
  -- ^ Queue table name
  -> ByteString
  -- ^ Connection string, for raw side connections
  -> (Text -> payload)
  -- ^ Construct a simple task payload
  -> (Int -> payload)
  -- ^ Construct a failing task payload
  -> IO env
  -- ^ Create a fresh env over an emptied queue table
  -> (env -> IO ())
  -- ^ Release an env built by the action above
  -> ((JobRead payload -> m (ResultOf m payload)) -> JobHandler m payload (ResultOf m payload))
  -- ^ Adapt a job action into the backend's handler shape
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function
  -> Spec
reclaimSpec schema table connStr mkSimple mkFailing mkEnv destroyEnv mkHandler runM =
  around (bracket mkEnv destroyEnv) $ do
    describe "Job Reclaim During Processing" $ do
      it "gracefully skips retry when job is reclaimed by another worker" $ \env -> do
        failureCalls <- newIORef (0 :: Int)
        successCalls <- newIORef (0 :: Int)
        unavailableCalls <- newIORef (0 :: Int)
        handlerCompleted <- newIORef False

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successCalls (\count -> (count + 1, ()))
                , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' failureCalls (\count -> (count + 1, ()))
                , onJobUnavailable = \_ _ -> liftIO $ atomicModifyIORef' unavailableCalls (\count -> (count + 1, ()))
                }

        Just inserted <- runM env $ HL.insertJob (defaultJob (mkSimple "slow"))
        let jobId = primaryKey inserted

        let jobHandler _job = liftIO $ do
              simulateAnotherWorkerClaim connStr schema table jobId
              atomicModifyIORef' handlerCompleted (\_ -> (True, ()))

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , pollInterval = 0.1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ -> do
            waitUntil 10_000 $ readIORef handlerCompleted
            threadDelay 500_000

        failureCount <- readIORef failureCalls
        successCount <- readIORef successCalls
        failureCount `shouldBe` 0
        successCount `shouldBe` 0

        unavailableCount <- readIORef unavailableCalls
        unavailableCount `shouldBe` 1

        allJobs <- runM env $ HL.listJobs @payload 10 0
        map primaryKey allJobs `shouldContain` [jobId]

      it "onJobFailure fires when handler throws a retryable exception" $ \env -> do
        failureCalls <- newIORef (0 :: Int)
        successCalls <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successCalls (\count -> (count + 1, ()))
                , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' failureCalls (\count -> (count + 1, ()))
                }

        void
          $ runM env
          $ HL.insertJob (setMaxAttempts (Just 1) $ defaultJob (mkSimple "will-fail"))

        let jobHandler _job = error "intentional failure"

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , pollInterval = 0.1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ ->
            waitUntil 10_000 $ (== 1) <$> readIORef failureCalls

        failureCount <- readIORef failureCalls
        successCount <- readIORef successCalls
        failureCount `shouldBe` 1
        successCount `shouldBe` 0

    describe "Heartbeat Stolen Job Detection" $ do
      it "heartbeat cancels handler via race when job is stolen mid-processing" $ \env -> do
        handlerStarted <- newIORef False
        handlerCompleted <- newIORef False

        Just inserted <- runM env $ HL.insertJob (defaultJob (mkSimple "slow"))
        let jobId = primaryKey inserted

        let jobHandler _job = liftIO $ do
              atomicModifyIORef' handlerStarted (\_ -> (True, ()))
              threadDelay 200_000
              simulateAnotherWorkerClaim connStr schema table jobId
              threadDelay 5_000_000
              atomicModifyIORef' handlerCompleted (\_ -> (True, ()))

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { pollInterval = 0.1
                , visibilityTimeout = 2
                , jobHeartbeatInterval = 1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ ->
            waitUntil 10_000 $ readIORef handlerStarted

        started <- readIORef handlerStarted
        started `shouldBe` True

        completed <- readIORef handlerCompleted
        completed `shouldBe` False

        allJobs <- runM env $ HL.listJobs @payload 10 0
        map primaryKey allJobs `shouldContain` [jobId]

    describe "Worker Loop Exception Safety" $ do
      it "continues processing after handler exceptions" $ \env -> do
        processedCount <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' processedCount (\count -> (count + 1, ()))
                , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' processedCount (\count -> (count + 1, ()))
                }

        runM env $
          traverse_
            (void . HL.insertJob . setMaxAttempts (Just 1) . defaultJob . mkFailing)
            [alwaysFailing, alwaysFailing, alwaysFailing]

        let jobHandler job = when (payload job == mkFailing alwaysFailing) $ error "Failing task"

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , workerCount = 1
                , pollInterval = 0.1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ ->
            waitUntil 10_000 $ (== 3) <$> readIORef processedCount

        processed <- readIORef processedCount
        processed `shouldBe` 3

-- | Bump the attempts and claim counters from a side connection, so the worker's ack fails.
simulateAnotherWorkerClaim :: ByteString -> Text -> Text -> Int64 -> IO ()
simulateAnotherWorkerClaim connStr schema table jobId =
  bracket (connectPostgreSQL connStr) close $ \conn ->
    void $
      execute
        conn
        "UPDATE ? SET attempts = attempts + 1, claim_seq = claim_seq + 1 WHERE id = ?"
        (QualifiedIdentifier (Just schema) table, jobId)
