{-# LANGUAGE OverloadedStrings #-}

-- | The guard loop. It sleeps until the earliest lease, deadline or beat. On
-- waking it fences the batches past a lease or deadline, then issues one
-- extend over every batch due a beat, on a thread of its own so a hung
-- statement cannot stall the fence. A register wakes the loop only when it
-- precedes the published target.
--
-- The lease fence waits for an extend that carries the batch, until that
-- extend gives up, or once it has landed, until it is over. The fence and
-- the settle each take the batch's status in one transaction, so whichever
-- commits first wins and the other sees it.
module Arbiter.Worker.Heartbeat.Guard.Loop
  ( runHeartbeatGuard
  , trySync
  ) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), JobGoneException (..), displayEx)
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM
  ( MonadSTM
  , STM
  , atomically
  , check
  , modifyTVar'
  , orElse
  , readTVar
  , retry
  , stateTVar
  , writeTVar
  )
import Control.Monad (filterM, forever, unless, void, when, (>=>))
import Control.Monad.Class.MonadFork (MonadFork (..))
import Control.Monad.Class.MonadThrow (Exception (..), MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException)
import Control.Monad.Class.MonadTime.SI
  ( DiffTime
  , MonadMonotonicTime (..)
  , MonadTime (..)
  , Time
  , UTCTime
  , addTime
  , diffTime
  )
import Control.Monad.Class.MonadTimer.SI (MonadTimer (..))
import Data.Foldable (for_, toList, traverse_)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust, isNothing, mapMaybe)
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Void (Void)
import UnliftIO.Exception (isSyncException)

import Arbiter.Worker.Heartbeat.Guard.Signal (signal)
import Arbiter.Worker.Heartbeat.Guard.State
import Arbiter.Worker.Logger (LogLevel (..))

-- | The guard loop. Fences what is due, then extends what is due.
runHeartbeatGuard
  :: (MonadFork n, MonadMask n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> n Void
{-# SPECIALIZE runHeartbeatGuard :: HeartbeatGuard IO job -> IO Void #-}
runHeartbeatGuard guard = forever $ do
  (target, count) <- atomically (plan guard)
  sleepUntil guard count target
  woke <- getMonotonicTime
  (inFlight, current) <- atomically ((,) <$> readTVar (guardInFlight guard) <*> snapshot guard)
  -- A batch the fence stops gets no beat.
  leased <- filterM (fence guard woke) current
  let due = [entry | (entry, status) <- leased, beatAt status <= woke]
  unless (null due || isJust inFlight) (issue guard woke leased due)

-- ---------------------------------------------------------------------------
-- Sleeping
-- ---------------------------------------------------------------------------

-- | Publish the loop's target, the earliest due time.
plan :: (MonadSTM n) => HeartbeatGuard n job -> STM n (Maybe Time, Int)
plan guard = do
  times <- concatMap <$> (dueTimes <$> readTVar (guardInFlight guard)) <*> snapshot guard
  let target = if null times then Nothing else Just (minimum times)
  writeTVar (wakeTarget (guardWake guard)) target
  (,) target <$> readTVar (wakeCount (guardWake guard))

-- | When the guard next acts on a batch. Beats wait for the extend in flight.
dueTimes :: Maybe InFlight -> (Guarded n job, Status n) -> [Time]
dueTimes inFlight (entry, status) =
  [at | not (leaseLapsed status), Just at <- [leaseFenceAt inFlight entry status]]
    <> [beatAt status | not (leaseLapsed status), isNothing inFlight]
    <> [deadline | not (deadlineSent status), Just deadline <- [guardedDeadline entry]]

-- | When the lease fence fires. An extend carrying the batch holds it until the extend
-- gives up, or, once landed, until it is over.
leaseFenceAt :: Maybe InFlight -> Guarded n job -> Status n -> Maybe Time
leaseFenceAt inFlight entry status = case inFlight of
  Just running
    | carried running, landed running -> Nothing
    | carried running -> Just (max lease (givesUp running))
  _ -> Just lease
  where
    lease = leaseAt status
    carried running = Set.member (guardedToken entry) (carries running) && issuedAt running < lease

-- | Sleep until @at@ or the next wake.
sleepUntil :: (MonadTimer n) => HeartbeatGuard n job -> Int -> Maybe Time -> n ()
sleepUntil guard count at = do
  alarm <- traverse arm at
  atomically $
    (readTVar (wakeCount (guardWake guard)) >>= check . (/= count))
      `orElse` maybe retry (readTVar >=> check) alarm
  where
    arm target = do
      now <- getMonotonicTime
      registerDelay (max 0 (target `diffTime` now))

-- ---------------------------------------------------------------------------
-- Fencing
-- ---------------------------------------------------------------------------

-- | Signal a handler past its lease or its deadline. Whether its lease still stands.
fence :: (MonadFork n, MonadSTM n) => HeartbeatGuard n job -> Time -> (Guarded n job, Status n) -> n Bool
fence guard woke (entry, status) = do
  (lapsed, standing) <- atomically (lapse guard woke entry)
  when lapsed $ do
    live <- pendingOf entry
    unless (null live) $
      signal guard woke entry (toException (JobGoneException leaseExpiredReason (map (guardKey guard) live)))
  for_ (guardedDeadline entry) $ \deadline ->
    when (not (deadlineSent status) && woke >= deadline) $ do
      adjust entry (\current -> current {deadlineSent = True})
      signal guard woke entry (toException (JobDeadlineExceeded (durationMessage guard)))
  pure standing

-- | Mark the lease lapsed if it is, against the extend in flight. Whether it lapsed now, and whether it stands.
lapse :: (MonadSTM n) => HeartbeatGuard n job -> Time -> Guarded n job -> STM n (Bool, Bool)
lapse guard woke entry = do
  inFlight <- readTVar (guardInFlight guard)
  status <- readTVar (guardedStatus entry)
  let lapsed = not (leaseLapsed status) && maybe False (woke >=) (leaseFenceAt inFlight entry status)
  when lapsed (writeTVar (guardedStatus entry) status {leaseLapsed = True})
  pure (lapsed, not (leaseLapsed status || lapsed))

durationMessage :: HeartbeatGuard n job -> T.Text
durationMessage guard =
  "handler ran past the maximum job duration"
    <> foldMap ((" of " <>) . T.pack . show) (configMaxDuration (guardConfig guard))

-- ---------------------------------------------------------------------------
-- The extend in flight
-- ---------------------------------------------------------------------------

-- | Issue one extend over @due@, bounded by the earliest lease among @leased@.
issue
  :: (MonadFork n, MonadMask n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> Time
  -> [(Guarded n job, Status n)]
  -> [Guarded n job]
  -> n ()
issue guard woke leased due = do
  issued <- getMonotonicTime
  atomically $
    writeTVar
      (guardInFlight guard)
      (Just (InFlight issued (addTime bound issued) (Set.fromList (map guardedToken due)) False))
  void . forkIO $ extend guard issued bound due `finally` finish guard
  where
    bound =
      max
        minRetryPause
        (minimum (configTimeout (guardConfig guard) : [leaseAt status `diffTime` woke | (_, status) <- leased]))

-- | The statement returned. The fence holds its batches until it is over.
land :: (MonadSTM n) => HeartbeatGuard n job -> n ()
land guard = atomically $ modifyTVar' (guardInFlight guard) (fmap (\running -> running {landed = True}))

-- | The extend is over. The fence is due at the leases again.
finish :: (MonadSTM n) => HeartbeatGuard n job -> n ()
finish guard = atomically (writeTVar (guardInFlight guard) Nothing *> wake guard)

-- | One extend statement over every due batch, bounded by @bound@.
extend
  :: (MonadCatch n, MonadFork n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> Time
  -> DiffTime
  -> [Guarded n job]
  -> n ()
extend guard issued bound due = do
  lives <- traverse (\entry -> (,) entry <$> pendingOf entry) due
  outcome <- timeout bound (trySync (configExtend config (concatMap snd lives)))
  case outcome of
    Nothing -> traverse_ (retryLater guard) due
    Just (Left exception) -> do
      for_ due $ \entry ->
        configLog config Error (toList (batchJobs (guardedBatch entry))) ("Heartbeat error (retrying): " <> displayEx exception)
      traverse_ (retryLater guard) due
    Just (Right results) -> do
      land guard
      configExtended config
      currentTime <- getCurrentTime
      let byJob = Map.fromList [(resultId result, result) | result <- results]
      traverse_ (settle guard issued currentTime byJob) lives
  where
    config = guardConfig guard

-- | Beat again after a failed extend.
retryLater :: (MonadMonotonicTime n, MonadSTM n) => HeartbeatGuard n job -> Guarded n job -> n ()
retryLater guard entry = do
  now <- getMonotonicTime
  adjust entry $ \status ->
    status {beatAt = addTime (heartbeatWait (configInterval (guardConfig guard)) False (leaseAt status `diffTime` now)) now}

-- | 'try' for synchronous exceptions only.
trySync :: (MonadCatch n) => n a -> n (Either SomeException a)
trySync = tryJust (\exc -> if isSyncException exc then Just exc else Nothing)

-- ---------------------------------------------------------------------------
-- Settling
-- ---------------------------------------------------------------------------

-- | Act on one batch's verdicts from the extend. A stopped batch takes none.
settle
  :: (MonadFork n, MonadMonotonicTime n, MonadSTM n)
  => HeartbeatGuard n job
  -> Time
  -> UTCTime
  -> Map JobId SetVisibilityResult
  -> (Guarded n job, [job])
  -> n ()
settle guard issued currentTime byJob (entry, live) = do
  -- Rows this worker settled during the statement do not count.
  stillPending <- Set.fromList . map key <$> pendingOf entry
  let verdicts = mapMaybe ((`Map.lookup` byJob) . key) live
      cancelledJobs = [jobId | JobCancelled jobId <- verdicts]
      stolenJobs = [jobId | JobReclaimed jobId _ _ <- verdicts]
      goneJobs = [jobId | JobGone jobId <- verdicts]
      unrenewed = [jobId | verdict <- verdicts, jobId <- unextended verdict, Set.member jobId stillPending]
      extended = [job | job <- live, Just (VisibilityExtended _) <- [Map.lookup (key job) byJob]]
  applied <- atomically $ stateTVar (guardedStatus entry) $ \status ->
    let lease = if null unrenewed then addTime (configTimeout config) issued else leaseAt status
        beat = addTime (heartbeatWait (configInterval config) True (lease `diffTime` issued)) issued
     in if leaseLapsed status then (False, status) else (True, status {leaseAt = lease, beatAt = beat})
  when applied $ do
    now <- getMonotonicTime
    case (cancelledJobs, stolenJobs) of
      (_ : _, _) -> signal guard now entry (toException (JobForceCancelled cancelledJobs (stolenJobs <> goneJobs)))
      ([], _ : _) -> signal guard now entry (toException (JobGoneException reclaimedReason stolenJobs))
      ([], []) ->
        unless (null extended) . void . forkIO . batchInherit batch $
          for_ extended $
            \job -> configHeartbeat config job currentTime (batchStart batch)
  where
    config = guardConfig guard
    key = configKey config
    batch = guardedBatch entry

-- | The row the extend left un-leased: it read back untouched, or, suspended, holds none.
unextended :: SetVisibilityResult -> [JobId]
unextended result = case result of
  VisibilityUnchanged jobId -> [jobId]
  JobSuspended jobId -> [jobId]
  _ -> []

resultId :: SetVisibilityResult -> JobId
resultId result = case result of
  VisibilityExtended jobId -> jobId
  VisibilityUnchanged jobId -> jobId
  JobCancelled jobId -> jobId
  JobReclaimed jobId _ _ -> jobId
  JobGone jobId -> jobId
  JobSuspended jobId -> jobId
