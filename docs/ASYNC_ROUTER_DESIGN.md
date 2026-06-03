# Async + Derived Router Forwarding

Status: implemented.

Routers forward from a committed source log into a destination topic without
putting forwarding on the source write path. A forwarded copy is derived from the
source WAL plus the router cursor, so one source append is one WAL append no matter
how many destinations are attached.

## Core contract

- **No WAL amplification.** Forwarded destination records are not separately
  WAL-logged. Recovery replays forwarding from the source WAL and the durable
  per-router cursor.
- **Off the ack path.** A source write marks the source dirty and returns after its
  own commit. Background draining and read-path catch-up materialize destination
  records.
- **Delivery modes, per-source FIFO.** The default `at_least_once` mode advances
  the router cursor only after the destination append succeeds. A crash before
  cursor advance may re-forward, so consumers remain idempotent. Opt-in
  `exactly_once` keeps the derived/no-WAL model but stamps a stable key into
  `meta._topics_router`; if that key is already present in the destination, catch-up
  advances the cursor without appending a duplicate. The destination must still retain
  the key; a delete or eviction removes the evidence.
- **Backpressure is not loss.** A full `discard:"reject"` destination keeps the
  cursor behind the blocked record and retries later.
- **Single-source derived destination.** A second router with a different source
  into the same destination is rejected with `409 topic_exists_incompatible` and
  `error.detail.reason: "router_dest_fan_in"`.
- **Deterministic destination sequence numbers.** Each router stores a `dest_base`
  and `forwarded_total`, so re-derived destination records keep the same sequence
  numbers across restart.
- **Source retention is explicit.** If a source trims records before a router can
  materialize them, the destination records a `source_trim` tombstone rather than
  opening a silent gap.

## Recovery

The durable truth for forwarded records is:

1. source WAL frames,
2. router definitions,
3. router cursors and destination bases captured in snapshots.

After WAL and snapshot recovery, the engine drains every router from its recovered
cursor until it reaches a fixed point. Chains are handled by repeated passes; cycles
terminate through the hop cap carried on derived in-memory records.

Snapshots hold `router_snapshot_lock` exclusively while capturing topics and router
cursors. Router advancement holds the same lock shared while publishing destination
records and advancing the cursor. That makes the `(derived dest content, cursor)`
pair one checkpoint unit.

## Verification

The focused router integration tests assert:

- one source append fanning to N destinations adds exactly one WAL frame,
- a backpressured forward is retried rather than skipped,
- derived destinations re-materialize deterministically across restart,
- source trimming before forward surfaces a tombstone,
- multi-source fan-in is rejected,
- `exactly_once` routers stamp stable keys without extra WAL frames,
- snapshots racing router advancement recover without duplicate or missing records.
