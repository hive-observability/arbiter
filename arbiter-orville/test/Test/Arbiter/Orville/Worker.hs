{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Orville.Worker
  ( OrvilleWorkerTestPayload (..)
  , orvilleBackend
  , spec
  , deadlineSpec
  , cronSpec
  , reclaimSpec
  , connectionRecoverySpec
  , lifecycleSpec
  ) where

import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Setup qualified as TestSetup
import Arbiter.Worker.TestKit (workerSpec)
import Arbiter.Worker.TestKit qualified as TestKit
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Text (Text)
import GHC.Generics (Generic)
import Test.Hspec (Spec, beforeAll, beforeWith)

import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv
  , TestOrville
  , cleanupOrvilleTest
  , createOrvilleTestEnv
  , destroyOrvilleTestEnv
  , disableOrvilleListener
  , runOrvilleTest
  , setupOrvilleTest
  )

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_orville_worker_test"

data OrvilleWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type OrvilleWorkerTestRegistry =
  '[QueueWithResult "arbiter_orville_worker_test" OrvilleWorkerTestPayload (Maybe [Text])]

testTable :: Text
testTable = "arbiter_orville_worker_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr workerTestSchemaName testTable 10) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  workerSpec @OrvilleWorkerTestPayload @(TestOrville OrvilleWorkerTestRegistry) SimpleTask FailingTask id runOrvilleTest

fresh :: ByteString -> Text -> IO (OrvilleTestEnv registry)
fresh connStr schema = TestSetup.cleanupOnce connStr schema schema >> createOrvilleTestEnv connStr schema schema orvillePoolSize

orvilleBackend
  :: forall registry
   . ByteString
  -> Text
  -> TestKit.TestBackend OrvilleWorkerTestPayload (TestOrville registry) (OrvilleTestEnv registry)
orvilleBackend connStr schema =
  TestKit.TestBackend
    { schema
    , table = schema
    , connStr
    , mkSimple = SimpleTask
    , mkFailing = FailingTask
    , mkEnv = fresh connStr schema
    , mkEnvPollOnly = disableOrvilleListener <$> fresh connStr schema
    , destroyEnv = destroyOrvilleTestEnv
    , mkHandler = id
    , runCommand = TestKit.statementCommand
    , runM = runOrvilleTest
    }

orvillePoolSize :: Int
orvillePoolSize = 10

deadlineSchema :: Text
deadlineSchema = "arbiter_orville_deadline_test"

type OrvilleDeadlineRegistry = '[Queue "arbiter_orville_deadline_test" OrvilleWorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr =
  beforeAll (TestSetup.setupOnce connStr deadlineSchema deadlineSchema True) $
    TestKit.deadlineSpec (orvilleBackend @OrvilleDeadlineRegistry connStr deadlineSchema)

cronSchema :: Text
cronSchema = "arbiter_orville_cron_test"

type OrvilleCronRegistry = '[Queue "arbiter_orville_cron_test" OrvilleWorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr =
  beforeAll (TestSetup.setupOnce connStr cronSchema cronSchema True) $
    TestKit.cronSpec (orvilleBackend @OrvilleCronRegistry connStr cronSchema)

reclaimSchema :: Text
reclaimSchema = "arbiter_orville_reclaim_test"

type OrvilleReclaimRegistry = '[Queue "arbiter_orville_reclaim_test" OrvilleWorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr =
  beforeAll (TestSetup.setupOnce connStr reclaimSchema reclaimSchema True) $
    TestKit.reclaimSpec (orvilleBackend @OrvilleReclaimRegistry connStr reclaimSchema)

recoverySchema :: Text
recoverySchema = "arbiter_orville_recovery_test"

type OrvilleRecoveryRegistry = '[Queue "arbiter_orville_recovery_test" OrvilleWorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  beforeAll (TestSetup.setupOnce connStr recoverySchema recoverySchema True) $
    TestKit.connectionRecoverySpec (orvilleBackend @OrvilleRecoveryRegistry connStr recoverySchema)

lifecycleSchema :: Text
lifecycleSchema = "arbiter_orville_lifecycle_test"

type OrvilleLifecycleRegistry =
  '[QueueWithResult "arbiter_orville_lifecycle_test" OrvilleWorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr =
  beforeAll (TestSetup.setupOnce connStr lifecycleSchema lifecycleSchema True) $
    TestKit.lifecycleSpec (orvilleBackend @OrvilleLifecycleRegistry connStr lifecycleSchema)
