{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Stats SQL templates.
module Arbiter.Core.Sql.Stats
  ( getQueueStatsSQL
  , allQueueStatsSQL
  , countChildrenBatchSQL
  ) where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol)
import Arbiter.Core.Codec (RowCodec)
import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTable, arbiterConcurrencyTable)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueDLQTable, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable)
import Arbiter.Core.Sql.Claim (concHeadroomPred, groupHeadBatch)
import Arbiter.Core.Sql.Jobs (claimablePred, jobStatusCaseSQL, unionAllOverQueueTables)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rawRows)
import Arbiter.Core.Sql.RateLimit (refilledBucketTokens)
import Arbiter.Core.SqlLiterals (textLiteral)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- | Per-status queue counts plus the age of the oldest @ready@ and @in_flight@ job.
-- The ready age covers ready and blocked rows and runs from when the row became visible.
-- Counts follow the 'jobStatusCaseSQL' taxonomy and sum to @total_jobs@.
getQueueStatsSQL :: RowCodec a -> SchemaName -> TableName -> [Text] -> Query a
getQueueStatsSQL codec schema tableName kinds = rawRows codec (queueStatsSelect schema tableName kinds)

-- | One queue's stats row. The classified rows are aggregated once per kind and once
-- over the whole table in one pass. @total_row@ marks the whole-table row. A ready
-- row a claim would skip counts as blocked instead.
queueStatsSelect :: SchemaName -> TableName -> [Text] -> Text
queueStatsSelect schema tableName kinds =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      kindExpr = declaredKindSQL kinds
      heads = groupHeadsSQL schema tableName
      blocked = blockedExpr schema
   in [text|
        SELECT MAX(total_jobs) FILTER (WHERE total_row = 1) AS total_jobs,
               MAX(ready_jobs) FILTER (WHERE total_row = 1) AS ready_jobs,
               MAX(in_flight_jobs) FILTER (WHERE total_row = 1) AS in_flight_jobs,
               MAX(scheduled_jobs) FILTER (WHERE total_row = 1) AS scheduled_jobs,
               MAX(backoff_jobs) FILTER (WHERE total_row = 1) AS backoff_jobs,
               MAX(throttled_jobs) FILTER (WHERE total_row = 1) AS throttled_jobs,
               MAX(suspended_jobs) FILTER (WHERE total_row = 1) AS suspended_jobs,
               MAX(cancelled_jobs) FILTER (WHERE total_row = 1) AS cancelled_jobs,
               MAX(exhausted_jobs) FILTER (WHERE total_row = 1) AS exhausted_jobs,
               MAX(blocked_jobs) FILTER (WHERE total_row = 1) AS blocked_jobs,
               MAX(oldest_ready_age_seconds) FILTER (WHERE total_row = 1) AS oldest_ready_age_seconds,
               MAX(oldest_in_flight_age_seconds) FILTER (WHERE total_row = 1) AS oldest_in_flight_age_seconds,
               (SELECT COUNT(*)::int8 FROM ${dlqTbl}) AS dlq_jobs,
               jsonb_object_agg(kind, total_jobs) FILTER (WHERE total_row = 0 AND kind IS NOT NULL) AS kind_counts
        FROM (
          SELECT kind, GROUPING(kind) AS total_row,
                 COUNT(*)::int8 AS total_jobs,
                 COUNT(*) FILTER (WHERE status = 'ready' AND NOT blocked) AS ready_jobs,
                 COUNT(*) FILTER (WHERE status = 'in_flight') AS in_flight_jobs,
                 COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled_jobs,
                 COUNT(*) FILTER (WHERE status = 'backoff') AS backoff_jobs,
                 COUNT(*) FILTER (WHERE status = 'throttled') AS throttled_jobs,
                 COUNT(*) FILTER (WHERE status = 'suspended') AS suspended_jobs,
                 COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_jobs,
                 COUNT(*) FILTER (WHERE status = 'exhausted') AS exhausted_jobs,
                 COUNT(*) FILTER (WHERE blocked) AS blocked_jobs,
                 EXTRACT(EPOCH FROM (
                   clock_timestamp() - MIN(GREATEST(inserted_at, not_visible_until)) FILTER (WHERE status = 'ready')
                 ))::float8 AS oldest_ready_age_seconds,
                 EXTRACT(EPOCH FROM (
                   clock_timestamp() - MIN(last_attempted_at) FILTER (WHERE status = 'in_flight')
                 ))::float8 AS oldest_in_flight_age_seconds
          FROM (
            SELECT inserted_at, not_visible_until, last_attempted_at, ${kindExpr} AS kind, ${jobStatusCaseSQL} AS status,
                   ${blocked} AS blocked
            FROM ${tbl} job
            LEFT JOIN (${heads}) head ON head.id = job.id
          ) classified
          GROUP BY GROUPING SETS ((), (kind))
        ) rollup
      |]

-- | Whether a row is one a claim would skip, over alias @job@ joined to its group
-- head as @head@: behind the head, or behind a full concurrency or rate-limit key.
-- The CASE keeps the key probes off every row a claim would not consider.
blockedExpr :: SchemaName -> Text
blockedExpr schema =
  let concTbl = arbiterConcurrencyTable schema
      concPolicies = arbiterConcurrencyPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      rlPolicies = arbiterRateLimitPoliciesTable schema
      claimable = claimablePred "job"
      concOk = concHeadroomPred concTbl concPolicies "job"
      rlOk = rateLimitHeadroomPred buckets rlPolicies "job"
   in [text|
        CASE WHEN ${claimable}
             THEN (job.group_key IS NOT NULL AND head.id IS NULL) OR NOT ${concOk} OR NOT ${rlOk}
             ELSE FALSE END
      |]

-- | A batch limit of one row. The head is judged as a single claim takes it.
headOnly :: Text
headOnly = "1"

-- | The id of each open group's head, the row a claim of that group takes first.
groupHeadsSQL :: SchemaName -> TableName -> Text
groupHeadsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      headBatch = groupHeadBatch tbl "summary.group_key" [] headOnly
   in [text|
        SELECT head.id
        FROM ${groupsTbl} summary
        CROSS JOIN LATERAL (
          ${headBatch}
        ) head
        WHERE summary.job_count > 0
          AND ((summary.ready_count > 0 AND summary.in_flight_until IS NULL) OR summary.next_due <= NOW())
          AND (summary.in_flight_until IS NULL OR summary.in_flight_until <= NOW())
      |]

-- | Whether a job's rate-limit policy admits its cost, over a row alias. A key without
-- a policy is admitted. The first claim seeds a full bucket for a key without one, so
-- only the policy's cap applies there. OFFSET 0 keeps the probe correlated.
rateLimitHeadroomPred :: Text -> Text -> Text -> Text
rateLimitHeadroomPred buckets rlPolicies alias =
  let effMax = effectivePolicyCol "policy" "max_tokens"
   in [text|
        (${alias}.rate_limit_key IS NULL OR NOT EXISTS (
          SELECT 1 FROM ${rlPolicies} policy
          LEFT JOIN ${buckets} bucket ON bucket.rate_limit_key = ${alias}.rate_limit_key
          WHERE policy.prefix_id = ${alias}.rate_limit_prefix
            AND NOT (${effMax} > 0
                     AND (bucket.rate_limit_key IS NULL
                          OR LEAST(GREATEST(${alias}.rate_limit_cost, 0), ${effMax}) <= ${refilledBucketTokens}))
          OFFSET 0
        ))
      |]

-- | A stored label the payload declares, and NULL for anything else.
declaredKindSQL :: [Text] -> Text
declaredKindSQL [] = "NULL::text"
declaredKindSQL kinds =
  let literals = T.intercalate ", " (map textLiteral kinds)
   in [text|CASE WHEN kind IN (${literals}) THEN kind END|]

-- | Every queue's stats in one query, tagged by name. Caller guards the empty list.
allQueueStatsSQL :: RowCodec a -> SchemaName -> [(TableName, [Text])] -> Query a
allQueueStatsSQL codec schema queueKinds =
  let queuesTbl = arbiterQueuesTable schema
      workersTbl = arbiterWorkersTable schema
   in rawRows codec $ unionAllOverQueueTables schema (map fst queueKinds) $ \tableName _ ->
        let stats = queueStatsSelect schema tableName (fromMaybe [] (lookup tableName queueKinds))
         in [text|
          SELECT '${tableName}' AS queue, stats.*,
                 COALESCE((SELECT paused FROM ${queuesTbl} WHERE queue_name = '${tableName}'), FALSE) AS queue_paused,
                 worker_counts.workers_live, worker_counts.workers_paused
          FROM (${stats}) stats
          CROSS JOIN (
            -- Live matches the worker health CASE: a fresh heartbeat and not draining.
            SELECT COUNT(*)::int8 AS workers_live, COUNT(*) FILTER (WHERE paused)::int8 AS workers_paused
            FROM ${workersTbl}
            WHERE queue_name = '${tableName}'
              AND last_heartbeat >= NOW() - stale_threshold_secs * interval '1 second'
              AND NOT shutting_down
          ) worker_counts
        |]

-- ---------------------------------------------------------------------------
-- Parent-Child Operations
-- ---------------------------------------------------------------------------

-- | Child counts as @(parent_id, total, suspended)@ per parent, over a set of job ids.
-- The caller attaches the row decoder.
countChildrenBatchSQL :: Text -> Text -> [Int64] -> Query ()
countChildrenBatchSQL schema tableName jobIds =
  let tbl = jobQueueTable schema tableName
   in [sql|
        SELECT parent_id, COUNT(*),
               COUNT(*) FILTER (WHERE suspended)
        FROM ${tbl}
        WHERE parent_id = ANY(#{jobIds :: [CInt8]})
        GROUP BY parent_id
      |]
