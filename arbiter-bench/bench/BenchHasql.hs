{-# LANGUAGE CPP #-}

-- | The hasql connects the bench compares, one per transport.
module BenchHasql (hasqlTransports) where

import Arbiter.Hasql (HasqlConnect, toHasqlConnect)
import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty (..))

#if MIN_VERSION_hasql(2,0,0)
import Pqi.Ffi qualified as Ffi
import Pqi.Native qualified as Native
#endif

-- | Labelled connects. The first is the libpq transport.
hasqlTransports :: NonEmpty (String, ByteString -> HasqlConnect)
#if MIN_VERSION_hasql(2,0,0)
hasqlTransports = ("hasql", toHasqlConnect Ffi.adapter) :| [("hasql-native", toHasqlConnect Native.adapter)]
#else
hasqlTransports = ("hasql", toHasqlConnect) :| []
#endif
