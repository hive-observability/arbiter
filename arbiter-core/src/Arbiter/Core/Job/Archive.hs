{-# LANGUAGE OverloadedStrings #-}

-- | Completed-job archive types. Arbiter archives an acked job when @archiveFor@
-- is positive and removes it after the retention period. Re-enqueue entries with
-- 'Arbiter.Core.HighLevel.reEnqueueFromArchive'.
module Arbiter.Core.Job.Archive
  ( ArchiveJob (..)
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), Value, object, withObject, (.:), (.:?), (.=))
import Data.Int (Int64)
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.DLQ (JobSnapshot)

-- | A completed job in the archive.
data ArchiveJob payload = ArchiveJob
  { archivePrimaryKey :: Int64
  -- ^ Archive table primary key. The snapshot keeps its own job id.
  , completedAt :: UTCTime
  -- ^ When the job was acked and archived
  , jobSnapshot :: JobSnapshot payload
  -- ^ Full job state at time of completion (payload, attempts, etc.)
  , archivedResult :: Maybe Value
  -- ^ Handler result stored for a completed root job (one with no parent).
  }
  deriving stock (Eq, Generic, Show)

instance (ToJSON payload) => ToJSON (ArchiveJob payload) where
  toJSON archived =
    object
      [ "archivePrimaryKey" .= archivePrimaryKey archived
      , "completedAt" .= completedAt archived
      , "jobSnapshot" .= jobSnapshot archived
      , "result" .= archivedResult archived
      ]

instance (FromJSON payload) => FromJSON (ArchiveJob payload) where
  parseJSON = withObject "ArchiveJob" $ \obj ->
    ArchiveJob
      <$> obj .: "archivePrimaryKey"
      <*> obj .: "completedAt"
      <*> obj .: "jobSnapshot"
      <*> obj .:? "result"
