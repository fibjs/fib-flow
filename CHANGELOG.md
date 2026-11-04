# Changelog

All notable changes to this project are documented in this file.

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
