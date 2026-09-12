{-# LANGUAGE DeriveAnyClass #-}
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

import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Setup (addQueueTable, cleanupOnce, setupOnce)
import Arbiter.Worker.TestKit (workerSpec)
import Arbiter.Worker.TestKit qualified as TestKit
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)
import Test.Hspec

import Arbiter.Hasql.HasqlDb
  ( HasqlEnv
  , createHasqlEnvWithPool
  , destroyHasqlEnv
  , disableListener
  , runHasqlDb
  )
import Test.Arbiter.Hasql.TestHelpers (createHasqlPool, createHasqlTestEnv)

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
        (\handler _conn job -> handler job)
        runHasqlDb

listenSchema :: Text
listenSchema = "arbiter_hasql_listen_test"

type HasqlListenRegistry = '[Queue "arbiter_hasql_listen_test" HasqlWorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr =
  beforeAll (setupOnce connStr listenSchema listenSchema True) $
    TestKit.listenerSpec @HasqlWorkerTestPayload
      listenSchema
      connStr
      SimpleTask
      (cleanupOnce connStr listenSchema listenSchema >> createHasqlTestEnv (Proxy @HasqlListenRegistry) connStr listenSchema)
      ( cleanupOnce connStr listenSchema listenSchema
          >> (disableListener <$> createHasqlTestEnv (Proxy @HasqlListenRegistry) connStr listenSchema)
      )
      destroyHasqlEnv
      (\handler _conn job -> handler job)
      runHasqlDb

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
      (\handler _conn job -> handler job)
      runHasqlDb
  where
    mkEnv = do
      cleanupOnce connStr mqSchema mqTableA
      cleanupOnce connStr mqSchema mqTableB
      createHasqlTestEnv (Proxy @HasqlMultiQRegistry) connStr mqSchema

hasqlHandler :: (job -> m r) -> conn -> job -> m r
hasqlHandler handler _conn job = handler job

fresh :: Proxy registry -> ByteString -> Text -> IO (HasqlEnv registry)
fresh proxy connStr schema = cleanupOnce connStr schema schema >> createHasqlTestEnv proxy connStr schema

deadlineSchema :: Text
deadlineSchema = "arbiter_hasql_deadline_test"

type HasqlDeadlineRegistry = '[Queue "arbiter_hasql_deadline_test" HasqlWorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr =
  beforeAll (setupOnce connStr deadlineSchema deadlineSchema True) $
    TestKit.deadlineSpec @HasqlWorkerTestPayload
      deadlineSchema
      deadlineSchema
      connStr
      SimpleTask
      (fresh (Proxy @HasqlDeadlineRegistry) connStr deadlineSchema)
      destroyHasqlEnv
      hasqlHandler
      runHasqlDb

cronSchema :: Text
cronSchema = "arbiter_hasql_cron_test"

type HasqlCronRegistry = '[Queue "arbiter_hasql_cron_test" HasqlWorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr =
  beforeAll (setupOnce connStr cronSchema cronSchema True) $
    TestKit.cronSpec @HasqlWorkerTestPayload
      cronSchema
      cronSchema
      connStr
      SimpleTask
      (fresh (Proxy @HasqlCronRegistry) connStr cronSchema)
      destroyHasqlEnv
      runHasqlDb

reclaimSchema :: Text
reclaimSchema = "arbiter_hasql_reclaim_test"

type HasqlReclaimRegistry = '[Queue "arbiter_hasql_reclaim_test" HasqlWorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr =
  beforeAll (setupOnce connStr reclaimSchema reclaimSchema True) $
    TestKit.reclaimSpec @HasqlWorkerTestPayload
      reclaimSchema
      reclaimSchema
      connStr
      SimpleTask
      FailingTask
      (fresh (Proxy @HasqlReclaimRegistry) connStr reclaimSchema)
      destroyHasqlEnv
      hasqlHandler
      runHasqlDb

recoverySchema :: Text
recoverySchema = "arbiter_hasql_recovery_test"

type HasqlRecoveryRegistry = '[Queue "arbiter_hasql_recovery_test" HasqlWorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  beforeAll (setupOnce connStr recoverySchema recoverySchema True) $
    TestKit.connectionRecoverySpec @HasqlWorkerTestPayload
      recoverySchema
      connStr
      SimpleTask
      (fresh (Proxy @HasqlRecoveryRegistry) connStr recoverySchema)
      destroyHasqlEnv
      hasqlHandler
      runHasqlDb

lifecycleSchema :: Text
lifecycleSchema = "arbiter_hasql_lifecycle_test"

type HasqlLifecycleRegistry = '[QueueWithResult "arbiter_hasql_lifecycle_test" HasqlWorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr =
  beforeAll (setupOnce connStr lifecycleSchema lifecycleSchema True) $
    TestKit.lifecycleSpec @HasqlWorkerTestPayload
      lifecycleSchema
      lifecycleSchema
      connStr
      SimpleTask
      (fresh (Proxy @HasqlLifecycleRegistry) connStr lifecycleSchema)
      (disableListener <$> fresh (Proxy @HasqlLifecycleRegistry) connStr lifecycleSchema)
      destroyHasqlEnv
      hasqlHandler
      runHasqlDb
