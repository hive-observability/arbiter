-- | The worker pool's run state.
module Arbiter.Worker.WorkerState
  ( WorkerState (..)
  ) where

-- | A worker pool's effective state, read off the shutdown and pause flags on its
-- 'Arbiter.Worker.Config.WorkerConfig'. Shutdown wins, then pause, then running.
data WorkerState
  = Running
  | Paused
  | ShuttingDown
  deriving stock (Eq, Show)
