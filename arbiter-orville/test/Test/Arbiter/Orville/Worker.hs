{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Orville.Worker (spec) where

import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( JobRead
  , defaultJob
  , payload
  , primaryKey
  , setGroupKey
  , setMaxAttempts
  )
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Arbiter.Worker.TestKit (workerSpec)
import Control.Exception (bracket_)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.Execution.ExecutionResult qualified as ExecResult
import Orville.PostgreSQL.Raw.RawSql qualified as RawSql
import Orville.PostgreSQL.Raw.SqlValue qualified as SqlValue
import Test.Hspec
  ( Spec
  , beforeAll
  , beforeWith
  , describe
  , it
  , shouldBe
  )
import UnliftIO.Async (withAsync)

import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv
  , TestOrville
  , cleanupOrvilleTest
  , executeSql
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

-- | The table a handler records its own work in.
testOperations :: Text
testOperations = workerTestSchemaName <> ".test_operations"

-- | Run a test body with an empty test_operations table, dropped afterwards.
withTestOperations :: OrvilleTestEnv OrvilleWorkerTestRegistry -> IO a -> IO a
withTestOperations env =
  bracket_
    ( runOrvilleTest env $ do
        executeSql $ "CREATE TABLE IF NOT EXISTS " <> testOperations <> " (job_id INT, operation TEXT)"
        executeSql $ "TRUNCATE " <> testOperations
    )
    (runOrvilleTest env $ executeSql $ "DROP TABLE IF EXISTS " <> testOperations)

-- | Record a job as processed inside the handler's transaction.
recordProcessed :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
recordProcessed job =
  O.executeVoid O.InsertQuery
    $ RawSql.fromText
    $ "INSERT INTO "
      <> testOperations
      <> " (job_id, operation) VALUES ("
      <> T.pack (show (primaryKey job))
      <> ", 'processed')"

processedCount :: Text
processedCount = "SELECT COUNT(*) FROM " <> testOperations <> " WHERE operation = 'processed'"

-- | The single count a COUNT query returns.
countRows :: (O.MonadOrville m) => Text -> m Int64
countRows sql = O.withConnection $ \conn -> liftIO $ do
  rows <- ExecResult.readRows =<< RawSql.execute conn (RawSql.fromText sql)
  case rows of
    [[(_, val)]] -> either (fail . ("Failed to decode count: " <>)) pure (SqlValue.toInt64 val)
    _ -> fail "Expected one row from COUNT query"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr workerTestSchemaName testTable 10) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  workerSpec @OrvilleWorkerTestPayload SimpleTask FailingTask id runOrvilleTest

  describe "Transactional Atomicity" $ do
    it "rolls back user operations when handler fails" $ \env -> withTestOperations env $ do
      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry (Maybe [Text])
          handler job = recordProcessed job >> throwRetryable "Simulated failure"

      -- Insert a job
      let job =
            setMaxAttempts (Just 1) $ setGroupKey (Just "g1") $ defaultJob (SimpleTask "WillFail")
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start a worker pool. One attempt sends the job to the DLQ.
      config <- transactionalWorkerConfig 10 handler
      runOrvilleTest env
        $ withAsync
          ( runWorkerPool
              ( config
                  { workerCount = 1
                  , pollInterval = 0.1
                  }
              )
          )
        $ \_ ->
          do
            -- Wait for job to be processed and moved to DLQ
            liftIO $ waitUntil 10_000 $ do
              dlqJobs <- runOrvilleTest env $ HL.listDLQJobs @OrvilleWorkerTestPayload 10 0
              pure (length dlqJobs == 1)

            -- Verify the job is in the DLQ with the correct payload
            dlqJobs <- HL.listDLQJobs @OrvilleWorkerTestPayload 10 0
            liftIO $ length dlqJobs `shouldBe` 1
            liftIO $ (payload $ DLQ.jobSnapshot (head dlqJobs)) `shouldBe` SimpleTask "WillFail"

            -- Verify the user's database operation was rolled back
            countRows processedCount >>= liftIO . (`shouldBe` 0)

    it "commits user operations when handler succeeds" $ \env -> withTestOperations env $ do
      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry (Maybe [Text])
          handler job = recordProcessed job >> pure mempty

      -- Insert a job
      let job =
            setGroupKey (Just "g1") $ defaultJob (SimpleTask "WillSucceed")
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool
      config <- transactionalWorkerConfig 10 handler
      runOrvilleTest env
        $ withAsync
          ( runWorkerPool
              ( config
                  { workerCount = 1
                  , pollInterval = 0.1
                  }
              )
          )
        $ \_ -> do
          -- Wait for job to be processed
          liftIO $ waitUntil 10_000 $ do
            jobs <- runOrvilleTest env $ HL.listJobs @OrvilleWorkerTestPayload 10 0
            pure (null jobs)

          -- Verify the queue is empty
          countRows ("SELECT COUNT(*) FROM " <> Schema.jobQueueTable workerTestSchemaName testTable) >>= liftIO . (`shouldBe` 0)

          -- Verify the user's database operation was committed
          countRows processedCount >>= liftIO . (`shouldBe` 1)
