# Wakeups (LISTEN/NOTIFY)

`LISTEN/NOTIFY` wakes workers at once for new jobs, pause and resume,
force-cancel, and manual cron runs. `SimpleDb`, `HasqlDb`, and custom
`MonadArbiter` instances supply the listener.

Without a listener, workers poll every `pollInterval`. Control paths fall back
to:

| Path | Without a listener |
| --- | --- |
| Pause and resume | the next worker heartbeat (`workerHeartbeatInterval`) |
| Cron run-now | the scheduler's next tick |
| Force-cancel | the next job heartbeat (`jobHeartbeatInterval`) |

The listener opens with the first worker pool and holds one pool connection.
A producer-only process opens none.

`useDedicatedListener` opens a separate listener connection. On hasql 2 the
transport adapter comes first:

```haskell
env <- ArbS.useDedicatedListener connStr =<< ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
env <- ArbH.useDedicatedListener Ffi.adapter connStr =<< ArbH.createHasqlEnv (Proxy @AppRegistry) Ffi.adapter connStr "arbiter"
```

`disableListener` switches to polling:

```haskell
env <- ArbS.disableListener <$> ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```
