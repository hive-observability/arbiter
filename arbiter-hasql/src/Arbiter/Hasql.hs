{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- | Convenience re-exports for the @hasql@ backend.
--
-- @
-- import Arbiter.Hasql
-- import Data.Proxy (Proxy (..))
-- import Pqi.Ffi qualified as Ffi
--
-- main :: IO ()
-- main = do
--   env <- createHasqlEnv (Proxy \@MyRegistry) Ffi.adapter connStr "arbiter"
--   runHasqlDb env $ do
--     insertJob (defaultJob myPayload)
-- @
--
-- On hasql 1.x the constructors take no adapter.
module Arbiter.Hasql
  ( -- * Re-exports
    module Arbiter.Hasql.MonadArbiter
  , module Arbiter.Hasql.HasqlDb
  ) where

import Arbiter.Hasql.HasqlDb
import Arbiter.Hasql.MonadArbiter
