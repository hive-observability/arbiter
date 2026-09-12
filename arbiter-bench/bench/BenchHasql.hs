{-# LANGUAGE CPP #-}

-- | The hasql envs the bench compares, one per transport.
module BenchHasql (hasqlTransports) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.PoolConfig (PoolConfig)
import Arbiter.Hasql (HasqlEnv, createHasqlEnvWithConfig)
import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Proxy (Proxy)

#if MIN_VERSION_hasql(2,0,0)
import Pqi.Ffi qualified as Ffi
import Pqi.Native qualified as Native
#endif

-- | Labelled env constructors. The first is the libpq transport.
hasqlTransports :: NonEmpty (String, Proxy registry -> ByteString -> SchemaName -> PoolConfig -> IO (HasqlEnv registry))
#if MIN_VERSION_hasql(2,0,0)
hasqlTransports =
  ("hasql", \proxy -> createHasqlEnvWithConfig proxy Ffi.adapter)
    :| [("hasql-native", \proxy -> createHasqlEnvWithConfig proxy Native.adapter)]
#else
hasqlTransports = ("hasql", createHasqlEnvWithConfig) :| []
#endif
