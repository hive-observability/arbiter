# Priority

Lower numbers claim first. The default is `0`.

```haskell
job = Arb.defaultJob payload & Arb.setPriority 10  -- runs behind priority 0
```

| Case | Order |
| --- | --- |
| Equal priorities | Insertion order |
| Job in flight | No preemption. A new high-priority job waits for a free worker. |
| Retrying job in a group | Stays first in its group until it succeeds or moves to the DLQ, at any priority. |
| Group rank | The lowest priority number in the group, delayed jobs included. |

A group is eligible when it has a ready job.
