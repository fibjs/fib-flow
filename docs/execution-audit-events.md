# Execution Audit Events

This document describes the execution audit events currently exposed by fib-flow and how to interpret them.

## Principles

- Task rows are the latest snapshot for scheduling and lightweight status queries.
- Audit events and attempts are the source of truth for historical replay and diagnosis.
- Event payloads are structured around `event_type`, status transition fields, workflow relation fields, and `metadata`.
- Some event types are declared in the shared type definitions for forward compatibility, but are not emitted by the current runtime.

## Common Event Fields

Each event row may contain these fields:

- `task_id`: the task that emitted the event.
- `root_id`: the workflow root task id. For a root task, this is the root task itself.
- `parent_id`: direct parent task id when the task belongs to a workflow.
- `event_type`: the event semantic identifier.
- `from_status`: previous task status when the event corresponds to a state transition.
- `to_status`: next task status when the event corresponds to a state transition.
- `stage`: task stage associated with the event.
- `worker_id`: worker associated with the event when available.
- `attempt`: current attempt number when the event belongs to an execution round.
- `event_time`: persisted Unix timestamp in seconds.
- `message`: human-readable summary.
- `metadata`: structured event-specific details.

## Currently Emitted Events

| Event type | When emitted | Status semantics | Typical metadata |
| --- | --- | --- | --- |
| `task_created` | A task row is inserted | Usually `null -> pending` | task name, creation context |
| `task_subtasks_created` | A running parent creates child tasks | Parent remains represented by a separate status event | child count, child ids, stage context |
| `task_claimed` | A pending task is claimed by a worker | `pending -> running` at claim time | task name |
| `task_started` | A claimed task starts its execution round | `pending -> running` for the first round | task name, retry count |
| `task_retry_started` | A retried task starts a later execution round | `pending -> running` for retry attempts | task name, retry count |
| `task_completed` | `updateTaskStatus(..., 'completed')` succeeds | `running -> completed` | retry count, next run time when present |
| `task_failed` | `updateTaskStatus(..., 'failed')` succeeds | `running -> failed` | error, retry count |
| `task_timed_out` | A running task times out or is explicitly marked timeout | Usually `running -> timeout` | timeout reason, retry metadata |
| `task_recovered` | A running task is reclaimed because its owner worker became unavailable | Usually `running -> pending` | recovery reason, previous worker id, recovering worker id, recovering pod id |
| `task_retry_scheduled` | Timeout handling schedules another run | Usually `failed/timeout -> pending` | retry count, next run time |
| `task_paused` | A cron task is paused after retry exhaustion or explicit pause transition | Usually `failed/timeout -> paused` or `running -> paused` | error, retry exhaustion flags |
| `task_permanently_failed` | An async task exhausts retries | Usually `failed/timeout -> permanently_failed` | error, retry exhaustion flags |
| `task_status_changed` | A status change does not map to a dedicated event type | Used for generic transitions such as `running -> suspended` or `suspended -> pending` | retry count, next run time, resume reason |
| `task_progress` | Handler code calls `task.progress()` | No authoritative status transition; snapshot cache update only | stage name, progress text, progress percent, handler metadata |
| `task_checkpoint` | Handler code calls `task.audit()` | No authoritative status transition; checkpoint only | checkpoint code, message, handler metadata |

## Generic vs Dedicated State Events

fib-flow uses dedicated event types for the most important terminal and retry-related transitions. Other transitions still persist as audit rows, but use `task_status_changed`.

Current generic transitions include:

- parent task enters `suspended` after child task creation
- parent task resumes from `suspended` to `pending` after all children complete
- manually resumed tasks move from `paused` to `pending` through `task_status_changed`
- transitions to `pending` that are not represented by a dedicated retry event

When consuming audit history, prefer `event_type` first, then inspect `from_status` and `to_status` for generic state-change rows.
The current runtime does not emit a dedicated `task_resumed` row; resume semantics are represented through `task_status_changed` plus transition fields.

## Attempt Semantics

Events and attempts complement each other:

- `task_started` and `task_retry_started` indicate the beginning of an execution round.
- Attempt rows capture round-level timing and final outcome.
- `task_progress` and `task_checkpoint` can attach to an open attempt when one exists.
- A completed or failed attempt is the authoritative source for duration and outcome analysis.
- When a running task is reclaimed because its owner worker is dead or superseded, the open attempt is closed with outcome `interrupted` and the audit stream emits `task_recovered`.

## Snapshot Cache Fields

The main task table may also keep these lightweight fields:

- `current_stage_name`
- `progress_text`
- `progress_percent`
- `last_event_time`
- `last_event_type`

These fields are cache only. They are useful for list views and lightweight dashboards, but they are not a replacement for persisted events and attempts.

## Declared but Not Currently Emitted

The shared type definitions still include some event types that are not emitted by the current runtime implementation:

- `task_heartbeat`
- `task_resumed`
- `task_expired_deleted`

Treat these as reserved names for future evolution unless and until a runtime path starts producing them.

## Precision Notes

- Event and attempt times are stored in Unix seconds.
- Very short steps may collapse to the same persisted duration.
- Workflow `critical_path` and `stage_timings` should therefore be treated as best-effort operational analysis, not precise tracing.
- Equal-duration branches are resolved deterministically, but the chosen path should still be interpreted as an estimate rather than a precise distributed trace.
