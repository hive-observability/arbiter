{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Simple.Worker
  ( spec
  , listenerSpec
  , multiQueueSpec
  , deadlineSpec
  , cronSpec
  , reclaimSpec
  , connectionRecoverySpec
  , lifecycleSpec
  ) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (defaultJob, setGroupKey)
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (addQueueTable, cleanupData, cleanupOnce, createPoolOf, createSharedPool, setupOnce, withConn)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Arbiter.Worker.TestKit qualified as TestKit
import Control.Concurrent (threadDelay)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)
import Test.Hspec (Spec, afterAll_, around, beforeAll, describe, it, runIO, shouldBe)

import Arbiter.Simple
  ( SimpleDb
  , SimpleEnv
  , createSimpleEnv
  , createSimpleEnvWithPool
  , destroySimpleEnv
  , disableListener
  , runSimpleDb
  , useDedicatedListener
  )

simpleHandler :: (job -> m r) -> conn -> job -> m r
simpleHandler handler _conn job = handler job

fresh :: Proxy registry -> ByteString -> Text -> IO (SimpleEnv registry)
fresh proxy connStr schema = cleanupOnce connStr schema schema >> createSimpleEnv proxy connStr schema

testSchema :: Text
testSchema = "arbiter_worker_test"

type WorkerTestRegistry = '[QueueWithResult "arbiter_worker_test" WorkerTestPayload (Maybe [Text])]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testSchema True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  sharedEnv <- runIO (createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema)
  afterAll_ (destroySimpleEnv sharedEnv)
    $ around (\action -> cleanupOnce connStr testSchema testSchema >> action sharedEnv)
    $ TestKit.workerSpec @WorkerTestPayload SimpleTask FailingTask simpleHandler runSimpleDb

listenSchema :: Text
listenSchema = "arbiter_worker_listen_test"

type ListenTestRegistry = '[Queue "arbiter_worker_listen_test" WorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr =
  beforeAll (setupOnce connStr listenSchema listenSchema True) $ do
    TestKit.listenerSpec @WorkerTestPayload
      listenSchema
      connStr
      SimpleTask
      (fresh (Proxy @ListenTestRegistry) connStr listenSchema)
      (disableListener <$> fresh (Proxy @ListenTestRegistry) connStr listenSchema)
      destroySimpleEnv
      simpleHandler
      runSimpleDb
    dedicatedListenerSpec connStr

dedicatedListenerSpec :: ByteString -> Spec
dedicatedListenerSpec connStr =
  describe "dedicated listener" $
    it "wakes the dispatcher on a size-1 pool the pool listener would starve" $ do
      cleanupOnce connStr listenSchema listenSchema
      pool <- createPoolOf 1 connStr
      env <- useDedicatedListener connStr =<< createSimpleEnvWithPool (Proxy @ListenTestRegistry) pool listenSchema
      ref <- newIORef (0 :: Int)
      let handler :: JobHandler (SimpleDb ListenTestRegistry IO) WorkerTestPayload ()
          handler _conn _job = liftIO $ atomicModifyIORef' ref $ \count -> (count + 1, ())
      config <- transactionalWorkerConfig 1 handler
      let workerConfig = config {workerCount = 1, pollInterval = 300, jitter = NoJitter}
      withLinkedAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
        threadDelay 1_000_000
        runSimpleDb env
          $ void
          $ HL.insertJob
          $ setGroupKey (Just "g1")
          $ defaultJob (SimpleTask "dedicated")
        waitUntil 5_000 $ (== 1) <$> readIORef ref
        readIORef ref >>= (`shouldBe` 1)
      destroySimpleEnv env

newtype QueueAPayload = QueueAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype QueueBPayload = QueueBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type MultiQRegistry =
  '[ Queue "mq_listen_a" QueueAPayload
   , Queue "mq_listen_b" QueueBPayload
   ]

mqSchema :: Text
mqSchema = "mq_listen_test"

mqTableA :: Text
mqTableA = "mq_listen_a"

mqTableB :: Text
mqTableB = "mq_listen_b"

multiQueueSpec :: ByteString -> Spec
multiQueueSpec connStr =
  beforeAll (setupOnce connStr mqSchema mqTableA True >> addQueueTable connStr mqSchema mqTableB True) $
    TestKit.multiQueueListenerSpec @QueueAPayload @QueueBPayload
      mqTableA
      mqTableB
      connStr
      QueueAPayload
      QueueBPayload
      mkEnv
      destroySimpleEnv
      simpleHandler
      runSimpleDb
  where
    mkEnv = do
      withConn connStr $ \conn -> cleanupData mqSchema mqTableA conn *> cleanupData mqSchema mqTableB conn
      createSimpleEnv (Proxy @MultiQRegistry) connStr mqSchema

deadlineSchema :: Text
deadlineSchema = "arbiter_worker_deadline_test"

type DeadlineRegistry = '[Queue "arbiter_worker_deadline_test" WorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr =
  beforeAll (setupOnce connStr deadlineSchema deadlineSchema True) $
    TestKit.deadlineSpec @WorkerTestPayload
      deadlineSchema
      deadlineSchema
      connStr
      SimpleTask
      (fresh (Proxy @DeadlineRegistry) connStr deadlineSchema)
      destroySimpleEnv
      simpleHandler
      runSimpleDb

cronSchema :: Text
cronSchema = "arbiter_cron_test"

type CronRegistry = '[Queue "arbiter_cron_test" WorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr =
  beforeAll (setupOnce connStr cronSchema cronSchema True) $
    TestKit.cronSpec @WorkerTestPayload
      cronSchema
      cronSchema
      connStr
      SimpleTask
      (fresh (Proxy @CronRegistry) connStr cronSchema)
      destroySimpleEnv
      runSimpleDb

reclaimSchema :: Text
reclaimSchema = "arbiter_worker_concurrency_test"

type ReclaimRegistry = '[Queue "arbiter_worker_concurrency_test" WorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr =
  beforeAll (setupOnce connStr reclaimSchema reclaimSchema True) $
    TestKit.reclaimSpec @WorkerTestPayload
      reclaimSchema
      reclaimSchema
      connStr
      SimpleTask
      FailingTask
      (fresh (Proxy @ReclaimRegistry) connStr reclaimSchema)
      destroySimpleEnv
      simpleHandler
      runSimpleDb

recoverySchema :: Text
recoverySchema = "arbiter_worker_recovery_test"

type RecoveryRegistry = '[Queue "arbiter_worker_recovery_test" WorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  beforeAll (setupOnce connStr recoverySchema recoverySchema True) $
    TestKit.connectionRecoverySpec @WorkerTestPayload
      recoverySchema
      connStr
      SimpleTask
      (fresh (Proxy @RecoveryRegistry) connStr recoverySchema)
      destroySimpleEnv
      simpleHandler
      runSimpleDb

lifecycleSchema :: Text
lifecycleSchema = "arbiter_worker_lifecycle_test"

type LifecycleRegistry = '[QueueWithResult "arbiter_worker_lifecycle_test" WorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr =
  beforeAll (setupOnce connStr lifecycleSchema lifecycleSchema True) $
    TestKit.lifecycleSpec @WorkerTestPayload
      lifecycleSchema
      lifecycleSchema
      connStr
      SimpleTask
      (fresh (Proxy @LifecycleRegistry) connStr lifecycleSchema)
      (disableListener <$> fresh (Proxy @LifecycleRegistry) connStr lifecycleSchema)
      destroySimpleEnv
      simpleHandler
      runSimpleDb
