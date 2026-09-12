# arbiter-hasql (hasql)

This backend uses `hasql` and `resource-pool`. Handlers receive a
`Hasql.Connection` for typed queries in the worker transaction.

```haskell
env <- ArbH.createHasqlEnv (Proxy @AppRegistry) connStr "arbiter"
ArbH.runHasqlDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

Share a transaction with external hasql work:

```haskell
-- Session.script (hasql >= 1.10) or Session.sql (hasql < 1.10)
_ <- Hasql.use conn (Session.script "BEGIN")
ArbH.inTransaction @AppRegistry conn "arbiter" $
  Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
_ <- Hasql.use conn (Session.script "COMMIT")
```

Bring your own pool. On hasql 2 you pick the transport adapter when you open
connections. The env constructors use the libpq adapter from `pqi-ffi`:

```haskell
import Data.Pool (defaultPoolConfig, newPool)
import Pqi.Ffi qualified as Ffi

let acquire = Hasql.acquire Ffi.adapter (ArbH.hasqlSettings connStr) >>= either (fail . show) pure
pool <- newPool (defaultPoolConfig acquire Hasql.release 60 10)
env <- ArbH.createHasqlEnvWithPool (Proxy @AppRegistry) pool "arbiter"
```

On hasql 1.x, `Hasql.acquire` takes only the settings.

See the [arbiter-hasql haddocks](https://arbiterq.dev/arbiter-hasql/Arbiter-Hasql.html) for the env and pool constructors.
