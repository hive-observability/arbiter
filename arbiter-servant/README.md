# arbiter-servant

REST API for managing and monitoring Arbiter job queues, built on Servant.

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Arbiter.Servant (Queue, initArbiterServer, runArbiterAPI)
import Data.Aeson (FromJSON, ToJSON)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)

data EmailPayload = SendEmail {to :: Text, subject :: Text, body :: Text}
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

type AppRegistry = '[Queue "email_queue" EmailPayload]

main :: IO ()
main = do
  -- Run the Arbiter migrations first. connStr is a libpq connection string and
  -- "arbiter" is the migrated schema. Live SSE updates also need
  -- enableEventStreaming = True.
  config <- initArbiterServer (Proxy @AppRegistry) connStr "arbiter"
  runArbiterAPI 8080 config
```

A queue with a handler result is `QueueWithResult "email_queue" EmailPayload
Report`. Import `QueueSpec (..)` for the constructor.

See the [Arbiter guide](https://arbiterq.dev/docs/) for installation, setup, and examples.
