# arbiter-hasql (hasql)

`hasql` and `resource-pool`. Handlers receive a `Hasql.Connection` in the
worker transaction.

```haskell
import Pqi.Ffi qualified as Ffi

env <- ArbH.createHasqlEnv (Proxy @AppRegistry) (ArbH.toHasqlConnect Ffi.adapter connStr) "arbiter"
ArbH.runHasqlDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

`toHasqlConnect` takes the transport adapter and the connection string.
`pqi-ffi` wraps libpq. `pqi-native` is pure Haskell. On hasql 1.x,
`toHasqlConnect` takes only the connection string.

Share a transaction with external hasql work:

```haskell
-- Session.script (hasql >= 1.10) or Session.sql (hasql < 1.10)
_ <- Hasql.use conn (Session.script "BEGIN")
ArbH.inTransaction @AppRegistry conn "arbiter" $
  Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
_ <- Hasql.use conn (Session.script "COMMIT")
```

Use your own pool. The env holds one pool connection for `LISTEN/NOTIFY`. Any
adapter can open the pool:

```haskell
import Data.Pool (defaultPoolConfig, newPool)
import Pqi.Ffi qualified as Ffi

let acquire = Hasql.acquire Ffi.adapter (ArbH.hasqlSettings connStr) >>= either (fail . show) pure
pool <- newPool (defaultPoolConfig acquire Hasql.release 60 10)
env <- ArbH.createHasqlEnvWithPool (Proxy @AppRegistry) pool "arbiter"
```

See the [arbiter-hasql haddocks](https://arbiterq.dev/arbiter-hasql/Arbiter-Hasql.html) for the env and pool constructors.
