# Changelog

All notable changes to this project are documented in this file.

## 8.1.0

Cron schedules can now be separated from the runs they produce. The default mode
keeps 8.0.1 semantics byte for byte, so upgrading changes no behaviour until
`cronMode` is set explicitly — with two API-level notes listed under *Behaviour
changes* below, both reachable only if you opt into `'spawn'`.

### Added

- **`cronMode: 'inline' | 'spawn'`** (manager option, default `'inline'`).
  In `'spawn'` mode a cron row never executes user code: each round inserts an
  independent `async` run row and advances the schedule inside a single
  transaction. This fixes the design flaw behind the classic cron problems — a
  row that must both persist forever and be reused every round ends up
  overwriting `result`, resetting `stage`, and freezing the entire schedule as
  soon as a handler suspends.
- **`runCronNow(scheduleId, payload?, options?)`** — spawn one run immediately
  without moving `next_run_time`. Works on any schedule status, so a paused or
  cancelled schedule is never revived by a manual trigger.
- **`getCronSchedules()[].last_spawned_task_id`** — id of the most recently
  spawned run, derived from the `task_cron_spawned` audit event (no schema
  change, so no table rebuild on upgrade).
- **New audit events**: `task_cron_spawned` (`spawned_task_id`,
  `scheduled_run_time`, `next_run_time`, `trigger`) and `task_cron_spawn_failed`
  (error plus the advanced `next_run_time`). Only the 20 most recent per-round
  schedule events are kept: a schedule row is never collected by retention, so an
  uncapped per-round trail would grow forever. The spawned run rows are the
  authoritative round history.
- **New attempt outcomes**: `spawned` and `spawn_failed`, so a spawn round is
  never mistaken for an executed one when triaging.

### Behaviour in `'spawn'` mode

- **At most one spawn per window.** The run-row insert and the schedule advance
  share one transaction; a crash in between rolls the whole round back instead of
  producing a second run on the next tick.
- **Suspension no longer freezes scheduling.** A suspending run row is an ordinary
  `async` task; the schedule keeps ticking.
- **The schedule row never accumulates attempts or claim events, and its
  per-round trail is bounded.** A spawn round is not an execution, so no attempt
  row is opened and no `task_claimed` / `task_started` events are written. Only
  the 20 most recent `task_cron_spawned` / `task_cron_spawn_failed` /
  `task_cron_missed` rows per schedule are retained. This matters because
  schedule rows are never collected by retention (`pending` / `running` /
  `suspended` are outside the retention status gate): one attempt plus two events
  per round would grow without bound.
- **Handler concurrency limits cannot starve a schedule.** `max_concurrent_tasks`
  gates runs; schedules are claimed separately so a saturated handler does not
  remove the name from the claim list.
- **An orphan schedule does not spawn.** A schedule whose handler has been
  unregistered (definition deleted but `cancelCron` never ran — a crash in
  between, or a partially failed definition load) is skipped instead of spawning.
  Its run rows would carry a name nobody registered, so they could never be
  claimed, never reach a terminal state, and never become retention candidates —
  an unbounded leak on a long-lived process. The row stays `pending` and picks up
  again as soon as a handler is registered, so this is not a one-way cancellation.
  Inline mode was never affected: `claimTask` already filters on the registered
  name list.
- **Rounds do not overlap by default.** Inline mode was mutually exclusive for
  free: its cron row was `running`, so it could not be claimed. Spawn removes that
  guarantee, so `cron(..., { maxInstances })` supplies it explicitly and defaults
  to `1`. Without a cap, a handler slower than its cron period stacks overlapping
  runs forever and the queued `pending` rows never reach a terminal state — so
  retention never collects them either. A round over the cap is skipped:
  `next_run_time` advances and `task_cron_overlap_skipped` is recorded, but no run
  row is created and `error` is left alone. `maxInstances: 0` allows unbounded
  overlap.
- **A failing round stays visible on the schedule row.** Inline mode reported
  failures on the schedule because the row was the execution; spawn moved
  execution to a run row, which left the schedule a pristine `pending` with
  `error = NULL` while every round failed — a consumer reading `last_error` would
  show "active, no errors" forever. The run's failure text is now mirrored onto the
  schedule's `error` (`task_cron_run_failed` / `task_cron_run_recovered` events),
  and only a run that actually **succeeds** clears it. Clearing at spawn time
  instead would blank the signal for the whole duration of each run: with a
  handler slower than its period, a permanently failing schedule looked healthy
  18 out of 24 samples. The status deliberately stays `pending` because the
  schedule keeps firing.
- **Run ↔ schedule links are exact.** In-flight counting and outcome reporting
  match the runs a schedule actually spawned (a `schedule_task_id` marker on the
  run's `task_created` event), not `name` + `tag`. With one handler name, a task
  enqueued by hand shares both: a long manual run would freeze the schedule for
  its whole duration (measured: one 4s manual run, zero cron rounds, six
  `overlap_skipped`), and a failing manual run would overwrite the schedule's
  `last_error`. Two schedules sharing `name` + `tag` and differing only by
  `cron_key` starved each other the same way (6 runs instead of 12).
- **The outcome signal is written once per state change.** Appending a round event
  on every failing round would put them on a row retention never collects — the
  same unbounded shape the earlier spawn-event cap fixed, just under another event
  name. `task_cron_run_failed` / `task_cron_run_recovered` are now both deduplicated
  and part of the capped round-event family.
- **Omitting `maxInstances` keeps the stored value.** `cron()` is an idempotent
  upsert — every call restates the same configuration — so an absent field means
  "leave it alone", matching how `timezone` / `catchUp` re-registration behaves.
  It used to fall back to the default, which silently wiped a non-default cap on
  every re-registration (measured: `maxInstances: 7` → a later `cron()` without the
  option → `1`), and because the value is persisted the wipe survived restarts.
  Pass `null` to reset explicitly, `0` for unlimited.

### Behaviour changes

Both are reachable only with `cronMode: 'spawn'`.

- **`runCronNow()` throws when `maxInstances` is reached.** Previously it created
  the run regardless, while that run still occupied the cap, so the scheduled
  rounds were skipped for as long as it lived. A caller that explicitly asked for a
  run now gets an error it can surface; the message names the escape hatch
  (`ignoreMaxInstances: true`) and is safe to show to a user.
- **`getCronSchedules()` gained a field** (`max_instances`). Code that asserts on
  the exact key set of a schedule object must allow it.

### Consistency fixes on top of the above

- **`runCronNow()` obeys `maxInstances`.** A manual trigger produces a run of the
  schedule, so it counted toward the cap while being exempt from it: three triggers
  created three runs at `maxInstances: 1`, and the scheduled rounds were then
  skipped for as long as those runs lived. It now throws when the cap is reached
  (`{ ignoreMaxInstances: true }` forces one), so the limit means one thing.
- **`maxInstances` is persisted.** It used to live in memory only, so a tick that
  ran before the caller re-registered the schedule silently reverted to `1`. It is
  now stored per schedule in `fib_flow_meta` (key-value, so no schema change) and
  also reported as `getCronSchedules()[].max_instances`.
- **A cancelled or paused schedule is never rewritten.** A run finishing after its
  schedule was cancelled used to write its failure onto that row and refresh
  `last_active_time`, falsifying the record and postponing retention. Outcome
  reporting now applies to `pending` / `running` schedules only.
- **The in-flight count no longer scans the global queue.** It read every
  `pending` / `running` row of every handler on every tick, for every schedule
  (25ms at 20k queued rows, growing linearly). It now reads the schedule's own
  spawn events, which is O(1) in the queue length; the exact form is used only as
  a fallback when the capped event trail may have been truncated.
- **The round payload is the schedule payload, verbatim.** fib-flow does not
  flatten or validate it.
- **Spawn failures do not stall the schedule.** `next_run_time` advances, the
  error is recorded on the row (visible as `last_error`) and in
  `task_cron_spawn_failed`. The round is not retried.

### Implementation notes

- `insertTask()`, `updateTaskStatus()` and `insertTaskEvents()` each open their own
  transaction, so nesting them is fatal (single-connection wrappers raise
  `cannot start a transaction within a transaction`; pooled SQLite blocks until
  the pool timeout). Spawn therefore uses connection-scoped private helpers
  (`_insertTaskRowWithConnection`, `spawnCronRun`, `failCronSpawn`) instead of
  composing the public API.
- `claimTask()` gained optional `skipAttempt` / `skipEvents` / `typeFilter`
  parameters. Defaults are unchanged.
- No schema change: `schema_version` stays `2`, and no column was added.

### Documentation

- `docs/core-concepts.md`: cron execution modes, and why spawn exists.
- `docs/api-reference.md`: `cronMode`, `runCronNow`, the execution-mode matrix.

### Test suite

- Verified against SQLite, PostgreSQL 16 and MySQL 8.0 (236 passing, 1 skipped on
  each; `FIB_FLOW_TEST_DB` selects the backend).
- Two pre-existing tests were made deterministic. Both timed on wall-clock
  seconds and failed intermittently on the network backends:
  - *"should use deterministic task id tie-breaking for equal workflow critical
    paths"* relied on two siblings sleeping exactly as long as each other;
    durations are whole seconds (`ended_at - started_at`), so siblings straddling
    a second boundary produced unequal paths and the tie-break never triggered.
    Attempt durations are now pinned before the assertion.
  - *"should handle cron task with async subtasks"* waited for the cron row to
    reach `pending` only. A cron row also passes through `pending` between
    "children completed" and "the round's final execution", where `result` still
    holds the child entries. It now waits for the round result.
- The spawn suite runs in `test/cron_spawn_test.js` (37 cases).

## 8.0.1

Bugfix release for `8.0.0`. Two of these are availability issues: a public API
call could take the worker process down, and the default retention configuration
could delete rows the runtime still needed.

### Fixed

- **A disappeared task row no longer kills the process.** When a `running` task's
  row was deleted (operator cleanup, `deleteTasks()`, `clearTasks()`, another
  process), the terminal-status write threw `Task <id> not found` from inside the
  worker fiber, and fibjs propagated it out of `coroutine.start` — exit code 1.
  Status writes are now best-effort: a missing row, a stale owner, or any other
  database error is logged and swallowed. See `docs/task-handler.md`.
- **Row-level retention now respects the status gate.** An expired
  `retention_expire_time` used to delete the row *regardless of status*, so a
  long-gap cron schedule, an async task waiting on `delay`, a `failed` task inside
  its `retry_interval`, or a `suspended` approval snapshot could vanish without a
  trace — and deleting a `running` row additionally stranded the executing worker.
  Row-level windows now share the global `statuses` gate: they change how long a
  row is kept, never which states may be dropped.
- **`runRetention()` no longer throws `datatype mismatch` when the global window is
  disabled.** `normalizeRetentionPolicy()` returned early without `batch_size`, so
  the cleanup query bound `undefined` into `LIMIT ?`.
- **Automatic sweeps run with row-level-only configuration.** Both guards required
  `retention.expire_time`, so `retention: { expire_time: null }` never collected
  row-level windows automatically; they only worked via an explicit
  `runRetention(<seconds>)`.
- **Child tasks inherit the parent's retention window**, and may override it. This
  was documented but not implemented.
- Retention deletions are now reported by an info-level `[retention]` log grouped
  by policy, alongside the existing `tasks_deleted_by_row_policy` /
  `tasks_deleted_by_global_policy` counters. No per-task audit event is written:
  an event keyed to a deleted task would never be collected by a later sweep and
  would grow the event table without bound.

### Documentation

- `docs/audit-retention-policy.md`: replaced "regardless of status" with the actual
  semantics, documented the shared status gate and row-level-only behaviour.
- `docs/execution-audit-events.md`: `task_resumed` *is* emitted (the doc claimed
  otherwise); `task_expired_deleted` is documented as a reserved name only.
- `docs/task-handler.md`: added the status-write failure matrix.

## 7.0.0

This release makes cron scheduling a first-class capability: registration is
idempotent, cancellation is a real lifecycle outcome, schedules carry their own
time zone and catch-up policy, and retention supports per-row windows.

### Breaking changes

- **Schema v2.** `setup()` now records `schema_version` in `fib_flow_meta` and
  refuses to run against a v1 (or unversioned, non-empty) `fib_flow_tasks` table:

  ```
  fib-flow schema v1 is not supported; expected v2.
  Drop the fib_flow_* tables and re-run setup() to rebuild them.
  ```

  There is no in-place migration: recreating the tables is the supported path.

- **`cron()` is idempotent.** Calling it repeatedly with the same `(name, cronKey)`
  updates the existing row instead of inserting a new one. Code that relied on
  repeated `cron()` calls producing multiple independent triggers must pass
  distinct `cronKey` values.

- **`cancelTask()` cancels instead of failing.** It now moves the row to
  `cancelled` rather than `permanently_failed`, and rejects `running` targets.

- **`deleteTasks({})` throws.** Wiping the table requires `clearTasks()`.

### Added

- **TypeScript declarations.** `package.json#types` now points at `index.d.ts`,
  backed by `lib/types/core.d.ts` (data shapes) and `lib/types/task-manager.d.ts`
  (TaskManager, handler contract, adapter surface). See `docs/typescript.md`.
- `cron()` options: `cronKey` (schedule identity, default `'default'`),
  `timezone` (IANA name, validated at registration), `catchUp` (`'once'` | `'skip'`),
  `retention_expire_time` (per-schedule retention window).
- `cancelCron({ name, cronKey, tag })` — cancel schedules by identity.
- `getCronSchedules()` — schedule listing with an `orphan` flag.
- `getCronScheduleStats()` — `{ total, active, paused, cancelled, orphan }`.
- `pruneCronSchedules()` — cancel pending schedules whose handler is gone.
- `deleteTasks()` accepts `type`.
- `async()` accepts `retention_expire_time`.
- New status `cancelled`, excluded from failure statistics.
- New audit events: `task_cancelled`, `task_resumed` (schedule re-registration),
  `task_cron_rescheduled`, `task_cron_missed`.
- `runRetention()` reports `tasks_deleted_by_row_policy` and
  `tasks_deleted_by_global_policy`.
- `getWorkflowAuditSummary()` reports `cancelled_tasks`.

### Fixed

- `resumeTask()`, `pauseTask()` and `cancelTask()` now return the updated task
  snapshot instead of the adapter's internal update result, matching the
declared `TaskRecord | null` return type.
- A successful cron round now resets `retry_count` and clears `error`, so a past
  failure no longer consumes the retry budget of later rounds.
- Adapter construction no longer throws for native connection objects
  (`SQLite: the object can not be converted to string`).
- `updateTaskStatus()` no longer overwrites `result` with the string `'null'`
  when no result is supplied.
- A changed cron expression takes effect on the round that just finished, not one
  round later.

## 6.2.4

Baseline release; see the documentation under `docs/` for behaviour prior to 7.0.0.
