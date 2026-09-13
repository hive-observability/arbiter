# Writing a Backend

A backend is a `MonadArbiter` instance. [arbiter-orville](orville.md)
applications define one.

| Member | |
| --- | --- |
| `RegistryOf` | the queue registry |
| `Handler` | the handler type, with a connection argument if the library has one |
| `getSchema` | the Arbiter schema |
| `executeQuery`, `executeStatement` | run a `Query`: SQL with `?` placeholders, the same with `$n` for libpq, parameters, decoder |
| `withDbTransaction` | a transaction, or a savepoint when nested. See [Worker Configuration](../worker/configuration.md). |
| `runHandlerWithConnection` | check out a connection and run a handler |
| `getListener` | the shared `LISTEN/NOTIFY` listener, or `Nothing` for polling. See [Wakeups](../worker/wakeups.md). |
| `executeQueryPrepared` | optional. Defaults to `executeQuery`. Override to prepare once per connection. The claim uses it. See the numbers in [Backend Integration](index.md). |

See the [`MonadArbiter` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-MonadArbiter.html) for each method's signature.
