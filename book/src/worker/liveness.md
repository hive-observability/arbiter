# Liveness Probes

The heartbeat loop touches `livenessFile` at each worker heartbeat. The default
is `arbiter-worker-<workerId>` in the system temp directory. A probe checks the
file's age:

```yaml
livenessProbe:
  exec:
    command: ["sh", "-c", "find ${TMPDIR:-/tmp}/arbiter-worker-* -mmin -5 | grep -q ."]
  initialDelaySeconds: 30
  periodSeconds: 60
```

`grep -q .` fails the probe when no fresh file exists. `find` alone exits 0.
A normal shutdown removes the file.

The REST API adds `GET health`, a readiness check that queries the database,
and `GET health/live`, a liveness check that does not. See
[REST API and Admin UI](../rest-api.md).
