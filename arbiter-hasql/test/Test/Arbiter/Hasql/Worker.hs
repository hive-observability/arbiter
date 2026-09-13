{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Hasql.Worker
  ( spec
  , listenerSpec
  , multiQueueSpec
  , deadlineSpec
  , cronSpec
  , reclaimSpec
  , connectionRecoverySpec
  , lifecycleSpec
  ) where

import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Setup (addQueueTable, cleanupOnce, setupOnce)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (transactionalWorkerConfig)
import Arbiter.Worker.TestKit (workerSpec)
import Arbiter.Worker.TestKit qualified as TestKit
import Control.Concurrent (threadDelay)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Maybe (isJust)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)
import System.Timeout (timeout)
import Test.Hspec
import UnliftIO.Async (async, cancel)

import Arbiter.Hasql.HasqlDb
  ( HasqlDb
  , HasqlEnv
  , createHasqlEnv
  , createHasqlEnvWithPool
  , destroyHasqlEnv
  , disableListener
  , runHasqlDb
  , useDedicatedListener
  )
import Test.Arbiter.Hasql.TestHelpers (createHasqlPool, runHasqlCommand, testConnect)

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_hasql_worker_test"

data HasqlWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlWorkerTestRegistry = '[QueueWithResult "arbiter_hasql_worker_test" HasqlWorkerTestPayload (Maybe [Text])]

testTable :: Text
testTable = "arbiter_hasql_worker_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr workerTestSchemaName testTable False) $ do
    sharedPool <- runIO (createHasqlPool 10 connStr)
    sharedEnv <- runIO (createHasqlEnvWithPool (Proxy @HasqlWorkerTestRegistry) sharedPool workerTestSchemaName)
    around (\action -> cleanupOnce connStr workerTestSchemaName testTable >> action sharedEnv) $
      workerSpec @HasqlWorkerTestPayload
        SimpleTask
        FailingTask
        TestKit.plainHandler
        runHasqlDb

listenSchema :: Text
listenSchema = "arbiter_hasql_listen_test"

type HasqlListenRegistry = '[Queue "arbiter_hasql_listen_test" HasqlWorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr =
  beforeAll (setupOnce connStr listenSchema listenSchema True) $ do
    TestKit.listenerSpec (hasqlBackend (Proxy @HasqlListenRegistry) connStr listenSchema)
    dedicatedListenerSpec connStr

-- | An address that never completes the TCP handshake.
blackHoleConnStr :: ByteString
blackHoleConnStr = "host=10.255.255.1 port=5432 dbname=arbiter user=arbiter"

dedicatedListenerSpec :: ByteString -> Spec
dedicatedListenerSpec connStr =
  describe "dedicated listener" $
    it "lets pool shutdown interrupt a connect in progress" $ do
      cleanupOnce connStr listenSchema listenSchema
      pool <- createHasqlPool 1 connStr
      env <-
        useDedicatedListener (testConnect blackHoleConnStr)
          =<< createHasqlEnvWithPool (Proxy @HasqlListenRegistry) pool listenSchema
      let handler :: JobHandler (HasqlDb HasqlListenRegistry IO) HasqlWorkerTestPayload ()
          handler _conn _job = pure ()
      config <- transactionalWorkerConfig 1 handler
      worker <- async (runHasqlDb env (runWorkerPool config))
      threadDelay 1_000_000
      stopped <- timeout 5_000_000 (cancel worker)
      stopped `shouldSatisfy` isJust
      destroyHasqlEnv env

mqSchema :: Text
mqSchema = "arbiter_hasql_mq_test"

mqTableA :: Text
mqTableA = "mqh_listen_a"

mqTableB :: Text
mqTableB = "mqh_listen_b"

newtype MqAPayload = MqAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype MqBPayload = MqBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlMultiQRegistry =
  '[ Queue "mqh_listen_a" MqAPayload
   , Queue "mqh_listen_b" MqBPayload
   ]

multiQueueSpec :: ByteString -> Spec
multiQueueSpec connStr =
  beforeAll (setupOnce connStr mqSchema mqTableA True >> addQueueTable connStr mqSchema mqTableB True) $
    TestKit.multiQueueListenerSpec @MqAPayload @MqBPayload
      mqTableA
      mqTableB
      connStr
      MqAPayload
      MqBPayload
      mkEnv
      destroyHasqlEnv
      TestKit.plainHandler
      runHasqlDb
  where
    mkEnv = do
      cleanupOnce connStr mqSchema mqTableA
      cleanupOnce connStr mqSchema mqTableB
      createHasqlEnv (Proxy @HasqlMultiQRegistry) (testConnect connStr) mqSchema

fresh :: Proxy registry -> ByteString -> Text -> IO (HasqlEnv registry)
fresh proxy connStr schema = cleanupOnce connStr schema schema >> createHasqlEnv proxy (testConnect connStr) schema

hasqlBackend
  :: Proxy registry
  -> ByteString
  -> Text
  -> TestKit.TestBackend HasqlWorkerTestPayload (HasqlDb registry IO) (HasqlEnv registry)
hasqlBackend proxy connStr schema =
  TestKit.TestBackend
    { schema
    , table = schema
    , connStr
    , mkSimple = SimpleTask
    , mkFailing = FailingTask
    , mkEnv = fresh proxy connStr schema
    , mkEnvPollOnly = disableListener <$> fresh proxy connStr schema
    , destroyEnv = destroyHasqlEnv
    , mkHandler = TestKit.plainHandler
    , runCommand = runHasqlCommand
    , runM = runHasqlDb
    }

deadlineSchema :: Text
deadlineSchema = "arbiter_hasql_deadline_test"

type HasqlDeadlineRegistry = '[Queue "arbiter_hasql_deadline_test" HasqlWorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr =
  beforeAll (setupOnce connStr deadlineSchema deadlineSchema True) $
    TestKit.deadlineSpec (hasqlBackend (Proxy @HasqlDeadlineRegistry) connStr deadlineSchema)

cronSchema :: Text
cronSchema = "arbiter_hasql_cron_test"

type HasqlCronRegistry = '[Queue "arbiter_hasql_cron_test" HasqlWorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr =
  beforeAll (setupOnce connStr cronSchema cronSchema True) $
    TestKit.cronSpec (hasqlBackend (Proxy @HasqlCronRegistry) connStr cronSchema)

reclaimSchema :: Text
reclaimSchema = "arbiter_hasql_reclaim_test"

type HasqlReclaimRegistry = '[Queue "arbiter_hasql_reclaim_test" HasqlWorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr =
  beforeAll (setupOnce connStr reclaimSchema reclaimSchema True) $
    TestKit.reclaimSpec (hasqlBackend (Proxy @HasqlReclaimRegistry) connStr reclaimSchema)

recoverySchema :: Text
recoverySchema = "arbiter_hasql_recovery_test"

type HasqlRecoveryRegistry = '[Queue "arbiter_hasql_recovery_test" HasqlWorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  beforeAll (setupOnce connStr recoverySchema recoverySchema True) $
    TestKit.connectionRecoverySpec (hasqlBackend (Proxy @HasqlRecoveryRegistry) connStr recoverySchema)

lifecycleSchema :: Text
lifecycleSchema = "arbiter_hasql_lifecycle_test"

type HasqlLifecycleRegistry = '[QueueWithResult "arbiter_hasql_lifecycle_test" HasqlWorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr =
  beforeAll (setupOnce connStr lifecycleSchema lifecycleSchema True) $
    TestKit.lifecycleSpec (hasqlBackend (Proxy @HasqlLifecycleRegistry) connStr lifecycleSchema)
