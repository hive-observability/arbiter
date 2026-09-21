{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Dispatcher
  ( runDispatcher
  ) where

import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Listen (Notification)
import Arbiter.Core.Operations qualified as Ops
import Data.Bifunctor (first)
import Data.Foldable (for_, traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config
  ( HandlerMode (..)
  , WorkerConfig (..)
  , pulseHeartbeat
  , readEffectiveState
  )
import Arbiter.Worker.Logger (LogLevel (..), newFailureGate, tryLog, tryReported)
import Arbiter.Worker.Logger.Internal (withJobContext)
import Arbiter.Worker.NotificationListener (runNotificationConsumer)
import Arbiter.Worker.WorkQueue (WorkQueue, awaitFinished, inFlight, pushWork)

-- | Wake on NOTIFY, poll timer, or worker-finished, then claim up to capacity.
-- @notifVar@ is filled from the shared hub in "Arbiter.Core.Listen".
runDispatcher
  :: forall payload m
   . (QueueOperation m payload)
  => WorkerConfig m payload
  -> Int
  -> Ops.JobStatements
  -> WorkQueue (NonEmpty (JobRead payload))
  -> STM.TVar (Maybe Notification)
  -> m ()
runDispatcher config workerCapacity statements workQueue notifVar = do
  claimGate <- newFailureGate
  let
    getFreeWorkers :: STM.STM (Maybe Int)
    getFreeWorkers = do
      free <- (workerCapacity -) <$> inFlight workQueue
      pure $ if free > 0 then Just free else Nothing

    claimAndEnqueue :: Int -> m ()
    claimAndEnqueue freeWorkers = do
      eJobs <- tryReported (logConfig config) Error claimGate "Dispatcher claim" $
        case handlerMode config of
          SingleJobMode _ ->
            first (map (:| [])) <$> Ops.claimJobsCached statements freeWorkers
          BatchedJobsMode _ _ ->
            Ops.claimJobsBatchedCached statements freeWorkers
      for_ eJobs $ \(jobs, rejected) -> do
        traverse_
          (\(row, err) -> tryLog (withJobContext (logConfig config) (row :| [])) Error ("Job moved to the DLQ, " <> err))
          rejected
        pushWork workQueue jobs
      -- Pulse on every attempt, including a failed claim.
      STM.atomically (pulseHeartbeat config)

    claimOnWakeup :: m ()
    claimOnWakeup = STM.atomically getFreeWorkers >>= traverse_ claimAndEnqueue

  runNotificationConsumer
    (readEffectiveState config)
    (pollInterval config)
    notifVar
    (awaitFinished workQueue)
    (const claimOnWakeup)
