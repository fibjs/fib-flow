# Changelog

All notable changes to this project are documented in this file.

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
