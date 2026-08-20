{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Compatibility layer for hasql API differences.
--
-- All version-specific code lives here. The rest of arbiter-hasql
-- imports from this module and never uses CPP directly.
module Arbiter.Hasql.Compat
  ( runSQL
  , acquire
  , AcquireErrorOf
  , hasqlSettings
  , HasqlSettings
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error qualified as TE
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session
import UnliftIO (MonadUnliftIO)

#if MIN_VERSION_hasql(2,0,0)
import Pqi.Ffi qualified
#endif

import Hasql.Errors qualified as Errors

#if MIN_VERSION_hasql(1,10,0)
import Hasql.Connection.Settings qualified as Settings
#else
import Hasql.Connection.Setting qualified as Setting
import Hasql.Connection.Setting.Connection qualified as ConnSetting
#endif

-- | Run a simple SQL command on a hasql connection (e.g., BEGIN, COMMIT).
runSQL :: (MonadUnliftIO m) => Hasql.Connection -> ByteString -> m ()
runSQL conn sql = do
  result <- liftIO $ Hasql.use conn (runScript (TE.decodeUtf8With TE.lenientDecode sql))
  case result of
    Right () -> pure ()
    Left err -> throwInternal $ "hasql runSQL error: " <> T.pack (show err)

#if MIN_VERSION_hasql(1,10,0)
runScript :: T.Text -> Session.Session ()
runScript = Session.script
#else
runScript :: T.Text -> Session.Session ()
runScript = Session.sql
#endif

-- | Convert a connection string ByteString to hasql settings.
hasqlSettings :: ByteString -> HasqlSettings
hasqlSettings = hasqlSettingsFromConnStr

#if MIN_VERSION_hasql(1,10,0)
type HasqlSettings = Settings.Settings
hasqlSettingsFromConnStr :: ByteString -> Settings.Settings
hasqlSettingsFromConnStr = Settings.connectionString . TE.decodeUtf8With TE.lenientDecode
#else
type HasqlSettings = [Setting.Setting]
hasqlSettingsFromConnStr :: ByteString -> [Setting.Setting]
hasqlSettingsFromConnStr connStr = [Setting.connection (ConnSetting.string (TE.decodeUtf8With TE.lenientDecode connStr))]
#endif

-- | What 'Hasql.acquire' reports on failure.
--
-- hasql 2.1 replaced @ConnectionError@ with @AcquireError@, which classifies
-- the failure (networking, authentication, compatibility, other) rather than
-- carrying a bare message.
#if MIN_VERSION_hasql(2,1,0)
type AcquireErrorOf = Errors.AcquireError
#else
type AcquireErrorOf = Errors.ConnectionError
#endif

-- | Establish a connection.
--
-- From hasql 2 on, 'Hasql.acquire' takes a @pqi@ adapter that selects the
-- transport implementation; before that the settings were the only argument.
-- We pick the libpq-backed adapter, which is the one that matches how earlier
-- versions talked to PostgreSQL.
acquire :: HasqlSettings -> IO (Either AcquireErrorOf Hasql.Connection)
#if MIN_VERSION_hasql(2,0,0)
acquire = Hasql.acquire Pqi.Ffi.adapter
#else
acquire = Hasql.acquire
#endif
