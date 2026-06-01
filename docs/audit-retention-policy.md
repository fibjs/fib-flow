# Audit Retention Policy

This document describes the current retention behavior in fib-flow and the boundaries of the minimal retention phase.

## Current Scope

fib-flow now supports a minimal explicit retention phase for expired terminal tasks.

Included in the current scope:

- explicit retention policy configuration through `TaskManager` options
- explicit retention execution through `taskManager.runRetention()`
- low-level retention execution through `adapter.cleanupExpiredTasks()`
- automatic retention execution inside the periodic maintenance loop
- consistent cleanup of task rows, event rows, and attempt rows together

Not included in the current scope:

- archive tables
- cold storage export
- separate retention windows for tasks, events, and attempts
- post-archive degraded query semantics
- summary materialization for deleted workflows

Current implementation note:

- At this stage, fib-flow only supports deletion-based retention.
- Historical data is cleaned up in place; it is not moved to another storage tier.
- Archive / dump is intentionally out of scope for the current phase.
- fib-flow only guarantees auditability within a limited retention window.
- Long-term historical retention and long-term compliance review are responsibilities of the surrounding business system, not this runtime layer.

## Backward Compatibility

The existing `expire_time` option still works.

It is now treated as a backward-compatible shortcut for the retention window. Internally, the preferred model is a structured retention policy:

```javascript
const taskManager = new TaskManager({
    dbConnection: 'sqlite::memory:',
    retention: {
        expire_time: 86400,
        statuses: ['completed', 'permanently_failed']
    }
});
```

## Policy Shape

Current minimal policy fields:

- `expire_time`: expiration window in seconds
- `statuses`: terminal statuses eligible for cleanup

Currently supported statuses:

- `completed`
- `permanently_failed`
- `paused`

Default statuses:

- `completed`
- `permanently_failed`

## Why `paused` Is Not in the Default Set

`paused` is a terminal-like operational state for cron tasks, but it often still represents something that requires manual inspection or intervention.

For that reason, the default retention policy does not automatically delete paused tasks. If a deployment wants paused tasks to expire automatically, it must opt in explicitly.

## Cleanup Semantics

Retention cleanup is task-centric and lifecycle-consistent:

1. identify expired tasks whose `last_active_time` is older than `now - expire_time`
2. restrict candidates to the configured terminal statuses
3. delete the corresponding audit events
4. delete the corresponding attempts
5. delete the task rows themselves

This preserves the invariant that retention cleanup should not leave orphaned audit rows behind.

## Automatic vs Manual Cleanup

Automatic cleanup happens inside the TaskManager maintenance loop.

The runtime now skips the startup tick for automatic cleanup and deletes expired rows in bounded internal batches. This reduces lock duration on busy databases and avoids a cold-start sweep when a worker first comes up.

Manual cleanup is also available:

```javascript
const result = taskManager.runRetention();

console.log(result);
// {
//   tasks_deleted: 10,
//   events_deleted: 84,
//   attempts_deleted: 12
// }
```

You can also override the configured policy for a one-off cleanup:

```javascript
taskManager.runRetention({
    expire_time: 7 * 24 * 3600,
    statuses: ['completed', 'permanently_failed', 'paused']
});
```

## Source of Truth Considerations

Retention cleanup currently deletes the task row and its audit history together.

This is intentional for the minimal phase:

- it avoids partial historical loss while keeping the latest task row alive
- it avoids breaking current audit view semantics in subtle ways
- it keeps workflow summary behavior simple: deleted workflows are no longer queryable
- it keeps the platform boundary clear: fib-flow provides short-term operational auditability, not long-term evidence storage

A later archive phase may relax this by introducing separate storage for historical audit data, but that is not part of the current roadmap.

## Future Evolution Boundary

If the project ever needs archive capabilities later, it would need to answer additional questions:

- Should old audit rows move to archive tables instead of being deleted?
- Should task snapshots and audit rows have different retention windows?
- What should `getTaskAudit()` or `getWorkflowAuditSummary()` return after archival?
- Should there be materialized workflow summaries that survive raw-event deletion?

Those questions remain intentionally out of scope for the current phase. The current direction is to keep retention simple and deletion-based, and to leave long-term preservation to business systems that actually need it.
