{-# LANGUAGE OverloadedStrings #-}

-- | 'Arbiter.Core.MonadArbiter.MonadArbiter' primitives backed by postgresql-simple.
module Arbiter.Simple.MonadArbiter
  ( -- * MonadArbiter implementation
    simpleExecuteQuery
  , simpleExecuteStatement
  , simpleWithDbTransaction
  , simpleRunHandlerWithConnection
  , simpleWithConnection
  ) where

import Arbiter.Core.Backend (HasPoolState, pinConnection, withConn, withSavepointTransaction)
import Arbiter.Core.Codec (Col (..), NullCol (..), runCodec)
import Arbiter.Core.MonadArbiter hiding (Query (..))
import Arbiter.Core.MonadArbiter qualified as MA
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Int (Int64)
import Data.Text.Encoding qualified as T
import Database.PostgreSQL.Simple (Connection)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.FromRow (RowParser, field)
import Database.PostgreSQL.Simple.ToField (Action, ToField (..), toField, toJSONField)
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))
import UnliftIO (MonadUnliftIO)

-- | Pin one pooled connection for the action.
simpleWithConnection :: (HasPoolState Connection m, MonadUnliftIO m) => m a -> m a
simpleWithConnection = pinConnection

-- | Run a query, decoding rows.
simpleExecuteQuery
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => MA.Query a
  -> m [a]
simpleExecuteQuery query = do
  let sql = Query $ T.encodeUtf8 (MA.qSql query)
      params = MA.qParams query
      parser = runCodec interpretNullCol (MA.qDecode query)
  withConn $ \conn -> liftIO $ case params of
    [] -> PG.queryWith_ parser conn sql
    _ -> PG.queryWith parser conn sql (map someParamToAction params)

-- | Run a statement, returning rows affected.
simpleExecuteStatement
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => MA.Query a
  -> m Int64
simpleExecuteStatement query = do
  let sql = Query $ T.encodeUtf8 (MA.qSql query)
      params = MA.qParams query
  withConn $ \conn -> liftIO $ case params of
    [] -> PG.execute_ conn sql
    _ -> PG.execute conn sql (map someParamToAction params)

interpretNullCol :: NullCol a -> RowParser a
interpretNullCol (NotNull _ col) = colField col
interpretNullCol (Nullable _ col) = colFieldNullable col

colField :: Col a -> RowParser a
colField CInt4 = field
colField CInt8 = field
colField CText = field
colField CBool = field
colField CTimestamptz = field
colField CJsonb = field
colField CFloat8 = field
colField CUuid = field

colFieldNullable :: Col a -> RowParser (Maybe a)
colFieldNullable CInt4 = field
colFieldNullable CInt8 = field
colFieldNullable CText = field
colFieldNullable CBool = field
colFieldNullable CTimestamptz = field
colFieldNullable CJsonb = field
colFieldNullable CFloat8 = field
colFieldNullable CUuid = field

-- | Transaction bracket. Nests via savepoints.
simpleWithDbTransaction
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => m a
  -> m a
simpleWithDbTransaction =
  withSavepointTransaction (\conn sql -> void (PG.execute_ conn (Query sql))) PG.withTransaction

-- | Run a handler on the pinned connection.
simpleRunHandlerWithConnection
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => (Connection -> job -> m result)
  -> job
  -> m result
simpleRunHandlerWithConnection handler job =
  withConn $ \conn -> handler conn job

someParamToAction :: SomeParam -> Action
someParamToAction (SomeParam (PScalar CJsonb) value) = toJSONField value
someParamToAction (SomeParam (PScalar col) value) = withColToField col (toField value)
someParamToAction (SomeParam (PNullable CJsonb) value) = maybe (toField (Nothing :: Maybe Int)) toJSONField value
someParamToAction (SomeParam (PNullable col) value) = withColToField col (toField value)
someParamToAction (SomeParam (PArray col) value) = withColToField col (toField (PGArray value))
someParamToAction (SomeParam (PNullArray col) value) = withColToField col (toField (PGArray value))

withColToField :: Col a -> ((ToField a) => r) -> r
withColToField CInt4 continuation = continuation
withColToField CInt8 continuation = continuation
withColToField CText continuation = continuation
withColToField CBool continuation = continuation
withColToField CTimestamptz continuation = continuation
withColToField CJsonb continuation = continuation
withColToField CFloat8 continuation = continuation
withColToField CUuid continuation = continuation
