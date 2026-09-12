# arbiter-hasql (hasql)

This backend uses `hasql` and `resource-pool`. Handlers receive a
`Hasql.Connection` for typed queries in the worker transaction.

```haskell
import Pqi.Ffi qualified as Ffi

env <- ArbH.createHasqlEnv (Proxy @AppRegistry) Ffi.adapter connStr "arbiter"
ArbH.runHasqlDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

The adapter is the transport. `pqi-ffi` wraps libpq. `pqi-native` is pure
Haskell and needs no C library. On hasql 1.x the constructors take no adapter.

Share a transaction with external hasql work:

```haskell
-- Session.script (hasql >= 1.10) or Session.sql (hasql < 1.10)
_ <- Hasql.use conn (Session.script "BEGIN")
ArbH.inTransaction @AppRegistry conn "arbiter" $
  Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
_ <- Hasql.use conn (Session.script "COMMIT")
```

Bring your own pool. The env borrows one pool connection for `LISTEN/NOTIFY`,
whichever adapter opened it:

```haskell
import Data.Pool (defaultPoolConfig, newPool)
import Pqi.Ffi qualified as Ffi

let acquire = Hasql.acquire Ffi.adapter (ArbH.hasqlSettings connStr) >>= either (fail . show) pure
pool <- newPool (defaultPoolConfig acquire Hasql.release 60 10)
env <- ArbH.createHasqlEnvWithPool (Proxy @AppRegistry) pool "arbiter"
```

See the [arbiter-hasql haddocks](https://arbiterq.dev/arbiter-hasql/Arbiter-Hasql.html) for the env and pool constructors.
