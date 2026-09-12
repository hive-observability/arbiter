{-# LANGUAGE OverloadedStrings #-}

-- | PostgreSQL array literal formatting for the text wire format.
module Arbiter.Orville.Array
  ( fmtArray
  , fmtNullableArray
  ) where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC

-- | Format a list of values as a PostgreSQL array literal.
fmtArray :: [ByteString] -> ByteString
fmtArray = braces . BSC.intercalate "," . map quoted

-- | Format a nullable list as a PostgreSQL array literal. Nothing becomes NULL.
fmtNullableArray :: [Maybe ByteString] -> ByteString
fmtNullableArray = braces . BSC.intercalate "," . map (maybe "NULL" quoted)

braces :: ByteString -> ByteString
braces body = "{" <> body <> "}"

quoted :: ByteString -> ByteString
quoted bytes = "\"" <> BSC.concatMap escape bytes <> "\""
  where
    escape '"' = "\\\""
    escape '\\' = "\\\\"
    escape c = BSC.singleton c
