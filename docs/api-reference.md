# API Reference

The API is designed to be intuitive and developer-friendly, allowing you to quickly implement task scheduling in your applications while maintaining full control over task lifecycles and execution parameters.

## Table of Contents
- [TaskManager](#taskmanager)
  - [Constructor](#constructor)
  - [Task Registration](#task-registration)
  - [Task Creation](#task-creation)
  - [Task Control](#task-control)
  - [Task Query](#task-query)
    - [Audit Query](#audit-query)
    - [Handler Audit](#handler-audit)
  - [Task Lifecycle](#task-lifecycle)

## TaskManager
The TaskManager is the core component responsible for managing task lifecycles, scheduling, and execution.

### Constructor
```javascript
/**
 * Create a task manager instance
 * @param {Object} options Configuration options
 * @param {string|object} options.dbConnection Required database connection string or object
 * @param {string} [options.dbType] Database type ('sqlite', 'mysql', or 'postgres')
 * @param {number} [options.poll_interval=1000] Poll interval in milliseconds
 * @param {number} [options.max_retries=3] Default maximum retry attempts
 * @param {number} [options.retry_interval=0] Default retry interval in seconds
 * @param {number} [options.timeout=60] Default task timeout in seconds
 * @param {number} [options.max_concurrent_tasks=10] Maximum concurrent tasks
 * @param {number} [options.task_heartbeat_interval=5000] Running task heartbeat interval in milliseconds
 * @param {number} [options.task_heartbeat_timeout=30000] Timeout window in milliseconds before a running task is treated as stalled
 * @param {string} [options.worker_id] Unique identifier for this worker instance (auto-generated if not provided)
 * @param {string} [options.pod_id] Stable logical node identifier used for worker recovery and peer fencing
 * @param {number} [options.worker_heartbeat_interval=5000] Worker registry heartbeat interval in milliseconds
 * @param {number} [options.worker_heartbeat_timeout=30000] Worker liveness timeout window in milliseconds
 * @param {boolean} [options.recover_running_jobs=true] Whether startup and peer scans reclaim running jobs owned by dead or superseded workers
 * @param {number} [options.expire_time=86400] Time in seconds after which completed/failed tasks are deleted (1 day)
 * @param {Object} [options.retention] Explicit retention policy for expired terminal tasks
 * @param {number} [options.retention.expire_time] Expiration window in seconds
 * @param {Array<string>} [options.retention.statuses] Terminal statuses eligible for cleanup; defaults to ['completed', 'permanently_failed']
 */
new TaskManager(options)
```

Worker recovery notes:
- `worker_id` represents a single process instance, not a stable node identity.
- `pod_id` groups multiple worker instances that belong to the same logical node across restarts.
- When `pod_id` is configured, fib-flow maintains a `fib_flow_workers` registry and can reclaim `running` tasks owned by dead or superseded workers without waiting for task timeout.
- Running-task writes are ownership-fenced by `worker_id`, so stale workers cannot safely write back after a task has been recovered.

### Task Registration

Tasks must be registered with handlers before they can be executed. The TaskManager provides flexible handler registration through the `use()` method, and handlers can be updated or removed at runtime.

Runtime semantics:
- A task that is already executing keeps the handler version captured when that execution attempt started.
- A paused or suspended task that resumes later is claimed again and uses the latest registered handler.
- Child tasks created by a running parent are resolved against the live handler registry at creation time.

#### Function Form Registration
```javascript
/**
 * Register a task handler using function form
 * @param {string} taskName Task type identifier
 * @param {Function} handler Async function(task, next) to handle task execution
 */
use(taskName, handler)
```

Example:
```javascript
taskManager.use('processImage', async (task) => {
    const { path } = task.payload;
    // Process single image
    return { processed: true };
});
```

#### Object Form Registration
```javascript
/**
 * Register a task handler using object form with options
 * @param {string} taskName Task type identifier
 * @param {Object} config Handler configuration object
 * @param {Function} config.handler Async function(task, next) to handle task execution
 * @param {number} [config.timeout] Default timeout in seconds for this task type
 * @param {number} [config.max_retries] Default maximum retry attempts for this task type
 * @param {number} [config.retry_interval] Default retry interval in seconds for this task type
 * @param {number} [config.priority] Default priority level for this task type
 */
use(taskName, config)
```

Example:
```javascript
taskManager.use('processImage', {
    // Handler function implementation
    handler: async (task) => {
        const { path } = task.payload;
        // Process single image
        return { processed: true };
    },
    // Task type specific defaults
    timeout: 120,       // 2 minutes timeout
    max_retries: 2,     // Maximum 2 retries
    retry_interval: 30, // Retry every 30 seconds
    priority: 5         // Higher priority tasks
});
```

#### Bulk Task Registration 
```javascript
/**
 * Register multiple task handlers at once
 * @param {Object} handlers Object mapping task types to handlers/configs
 */
use(handlersMap)
```

Example:
```javascript
taskManager.use({
    // Function form handlers
    processText: async (task) => {
        return { processed: true };
    },
    
    // Object form handlers with options
    processImage: {
        handler: async (task) => {
            return { processed: true };
        },
        timeout: 120,
        max_retries: 2
    },
    
    processVideo: {
        handler: async (task) => {
            return { processed: true };
        },
        timeout: 300,
        priority: 3
    }
});
```

#### Handler Removal
```javascript
/**
 * Unregister one or more task handlers
 * @param {string|string[]} taskName Task type identifier or identifiers
 * @returns {number} Number of handlers removed
 */
unuse(taskName)
```

Example:
```javascript
taskManager.unuse('processImage');

taskManager.unuse([
    'processVideo',
    'processAudio'
]);
```

`unuse()` only affects future work selection. It does not interrupt a task attempt that is already executing.

#### Handler Options

When registering a task handler using the object form, you can specify the following options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| handler | Function | Required | The async function that processes the task |
| timeout | Number | 60 | Task execution timeout in seconds |
| max_retries | Number | 3 | Maximum total attempts for tasks (including initial attempt) |
| retry_interval | Number | 0 | Delay between retries in seconds |
| priority | Number | - | Default priority for all tasks of this type |
| max_concurrent_tasks | Number | - | Maximum number of concurrent tasks of this type |

Notes:
- Options specified during handler registration become the defaults for that task type
- These defaults can be overridden when creating individual tasks
- Handler options take precedence over global TaskManager options
- If a handler is registered as a function, it will use the global TaskManager options
- When max_concurrent_tasks is set, the system will ensure no more than that many tasks of this type run simultaneously
- Handler schema metadata is not supported; validate payloads inside the handler when needed

### Task Options

Task execution can be configured through three levels:

1. **Global Configuration** (TaskManager level)

> All option names are **snake_case**. `cronMode` is the one exception (it was
> introduced in that form). camelCase spellings of known options — `pollInterval`,
> `maxRetries`, … — are mapped onto their snake_case names and logged as a warning,
> because a silently ignored key makes the manager run with the default while the
> configuration looks applied. Prefer the documented spelling; the mapping exists
> to make the mistake visible, not to make it idiomatic.

```javascript
const taskManager = new TaskManager({
    poll_interval: 1000,          // Poll interval in milliseconds
    max_retries: 3,              // Maximum total attempts (including initial attempt)
    retry_interval: 0,           // No delay between retries
    timeout: 60,                // Default task timeout in seconds
    max_concurrent_tasks: 10,   // Maximum concurrent tasks
    task_heartbeat_interval: 5000, // Running task heartbeat interval
    task_heartbeat_timeout: 30000, // Running task heartbeat timeout window
    pod_id: 'scheduler-a',      // Stable logical node identity for worker recovery
    worker_heartbeat_interval: 5000, // Worker registry heartbeat interval
    worker_heartbeat_timeout: 30000, // Worker liveness timeout window in milliseconds
    recover_running_jobs: true, // Reclaim running jobs from dead or superseded workers
    cronMode: 'inline',         // 'inline' (default) or 'spawn' — see core-concepts.md
    expire_time: 86400,         // Backward-compatible retention shortcut
    retention: {
        expire_time: 86400,
        statuses: ['completed', 'permanently_failed']
    }
});
```

2. **Task Type Configuration** (Handler registration level)
```javascript
taskManager.use('processImage', {
    handler: async (task) => { /* ... */ },
    timeout: 120,           // 2 minutes timeout
    max_retries: 2,        // Maximum 2 total attempts
    retry_interval: 30,    // 30 seconds retry interval
    priority: 5,           // Higher priority tasks
    max_concurrent_tasks: 5 // Max 5 concurrent tasks of this type
});
```

3. **Task Instance Configuration** (Task creation level)
```javascript
taskManager.async('processImage', payload, {
    timeout: 180,      // Override timeout for this task
    max_retries: 5,    // Override retry attempts
    retry_interval: 60 // Override retry interval
});
```

Configuration Priority (highest to lowest):
1. Task Instance Options
2. Task Type (Handler) Options
3. Global TaskManager Options

### Task Creation
Tasks can be created in two modes: async (one-time) tasks and cron (scheduled) tasks. Each task can be configured with specific execution parameters.
```javascript
/**
 * Create an async task
 * @param {string} taskName Task type
 * @param {Object} payload Task data
 * @param {Object} options Task options
 * @param {number} [options.delay] Delay in seconds
 * @param {number} [options.priority] Priority level
 * @param {number} [options.timeout] Timeout in seconds
 * @param {number} [options.max_retries] Max retry attempts
 * @param {number} [options.retry_interval] Retry interval in seconds
 * @param {string} [options.tag] Task tag for categorization
 * @param {number} [options.retention_expire_time] Per-task retention window in seconds
 * @returns {number} Task id
 */
async(taskName, payload, options)

/**
 * Create or update a cron schedule
 * @param {string} taskName Task type
 * @param {string} cronExpr Cron expression
 * @param {Object} payload Task data
 * @param {Object} options Task options
 * @param {string} [options.cronKey='default'] Schedule identity key
 * @param {string} [options.timezone] IANA time zone used to evaluate the expression
 * @param {string} [options.catchUp='once'] 'once' runs a missed window immediately, 'skip' advances past it
 * @param {number} [options.retention_expire_time] Per-schedule retention window in seconds
 * @returns {number} Task id
 */
cron(taskName, cronExpr, payload, options)
```

#### Idempotent cron registration

`cron()` is idempotent per `(taskName, cronKey)`: calling it again from a restarted
process, another instance, or a config reload updates the existing row instead of
creating a duplicate trigger.

```javascript
// First call inserts, later calls update the same row.
taskManager.cron('nightly_report', '0 3 * * *');
taskManager.cron('nightly_report', '0 4 * * *');   // same row, expression updated

// Same handler name can host independent schedules via distinct keys.
taskManager.cron('nightly_report', '0 3 * * *', { region: 'eu' }, { cronKey: 'eu' });
taskManager.cron('nightly_report', '0 3 * * *', { region: 'us' }, { cronKey: 'us' });
```

Update semantics:

- `cron_expr` / `cron_tz` unchanged → `next_run_time` is preserved, so a restart never postpones the next run.
- `cron_expr` / `cron_tz` changed → `next_run_time` is recomputed immediately.
- `paused` / `cancelled` row → revived to `pending` with `retry_count = 0` and an audit event (`task_resumed`, `resume_reason: 'schedule_reregistered'`).
- `failed` / `timeout` row → treated as an in-flight retry chain: scheduling fields update, retry budget is untouched.

A changed expression also takes effect for the round that is currently running: the
completion path re-reads the persisted schedule rather than trusting the value
captured at claim time.

#### Time zones

```javascript
taskManager.cron('daily_digest', '0 9 * * *', {}, {
    timezone: 'Asia/Shanghai' // IANA name, validated at registration
});
```

- `timezone` is validated with `Intl.DateTimeFormat`; an invalid name throws `Invalid timezone: ...` instead of silently falling back to the process zone.
- Omitted → the expression is evaluated in the process time zone (unchanged behaviour).
- Daylight-saving gaps are handled by `cron-parser`: a non-existent local instant rolls forward. For `America/New_York` on 2027-03-14, `30 2 * * *` fires at 03:30 EDT.

#### Catch-up policy

```javascript
taskManager.cron('hourly_sync', '0 * * * *', {}, { catchUp: 'skip' });
```

- `once` (default): a missed window runs once, as soon as capacity allows.
- `skip`: a window that is at least one full period late is skipped. The round still gets an attempt row (`outcome: 'skipped'`), `next_run_time` advances past it, and a `task_cron_missed` event records how many windows were dropped.
- Delays shorter than one period are normal jitter and always run.
- The check happens after claiming, so it also covers gaps that occur while the manager is running (handler unregistered, worker recovery, database stalls), not just process restarts.

#### Execution modes (`cronMode`)

A cron row is a **schedule**, not a run. `cronMode` decides what a round does:

```javascript
// Default: 8.0.x semantics, unchanged.
const inline = new TaskManager({ dbConnection: 'sqlite:jobs.db' });

// Each round spawns an independent async run row; the schedule row never runs code.
const spawn = new TaskManager({ dbConnection: 'sqlite:jobs.db', cronMode: 'spawn' });
```

| Aspect | `'inline'` (default) | `'spawn'` |
|---|---|---|
| Row that runs the handler | the cron row | a new `async` run row |
| Round outcome location | cron row `result` | run row `result` |
| Handler suspends | schedule stalls until `resumeTask()` | schedule keeps ticking |
| Schedule `result` | overwritten each round | never written |
| Schedule attempts / claim events | one attempt + two events per round | none (bounded tail of round events) |
| Handler `max_concurrent_tasks` | can starve the schedule | gates runs only (manager-level limit still delays ticks in both modes) |
| Overlapping rounds | impossible (the row is `running`) | `maxInstances`, default `1`; excess rounds are skipped |
| Failure visibility | schedule row goes `failed` with the error | schedule stays `pending`; the reason is mirrored to `last_error` |
| Orphan schedule (handler gone) | row never claimed, stays `pending` | round skipped, no run rows created |
| Atomicity of "spawn + advance" | n/a | single transaction, at most one run per window |

Notes:

- The mode is a **manager-level** option, not a per-schedule column: adding a column would change the schema version and force every existing deployment to rebuild its tables.
- Switching back to `'inline'` is safe for rows spawned earlier — they are ordinary `async` rows and keep executing.
- **Rounds do not overlap by default.** Inline mode got mutual exclusion for free (its cron row was `running`, so it could not be claimed). Spawn removes that, so the equivalent cap is explicit: `maxInstances` defaults to `1` and a round that would exceed it is skipped (no run row, `next_run_time` still advances, a `task_cron_overlap_skipped` event is written, `error` untouched). Pass `maxInstances: 0` for unbounded overlap.
- **A failing round is visible on the schedule.** The run row carries the failure; the reason is mirrored onto the schedule's `error` so `last_error` is not empty while every round fails, and only a run that actually **succeeds** clears it. The schedule status stays `pending` because it keeps firing.
- **The overlap cap and the failure signal are per schedule, not per name.** Only runs spawned by that schedule count toward `maxInstances` and report their outcome. A task you enqueue by hand under the same handler name is neither a blocker nor a health signal for the schedule.
- **`runCronNow()` obeys `maxInstances`** and throws when the cap is reached, so a caller that explicitly asked for a run gets an answer it can surface. `{ ignoreMaxInstances: true }` forces one.
- **The cap is persisted** in `fib_flow_meta` (no schema change), so a restart — or a tick before the caller re-registers — sees the same value. `getCronSchedules()[].max_instances` exposes it. Omitting `maxInstances` on a later `cron()` keeps the stored value (`null` resets it, `0` means unlimited); `runRetention()` and the automatic sweep (which also runs when `retention.expire_time` is `null`) drop stored entries whose schedule row is gone. The value is recorded in **both** modes — it is inert under `'inline'`, but writing it there means the policy is already in place if the schedule is later switched to spawn.
- **A cancelled or paused schedule is never rewritten.** A run that finishes after its schedule was cancelled reports nothing: the row keeps its recorded reason and its `last_active_time`, so retention is not postponed.
- Spawn failures do not stall the schedule: `next_run_time` still advances, `getCronSchedules()[].last_error` carries the reason, and a `task_cron_spawn_failed` event is written. The round is not retried — "no order sent" is safer than "order sent twice", and the next window is at most one period away.
- The payload is copied **verbatim** from the schedule row. fib-flow does not flatten or validate it: store the shape your handler expects.
- `getCronSchedules()` exposes `last_spawned_task_id` (derived from the audit trail; no new column) so callers can follow the spawned run.

### Task Control
Task control methods provide ways to manage the TaskManager instance and individual task execution.
```javascript
/**
 * Start the TaskManager and begin processing tasks
 * Initializes task polling and monitoring
 * @throws {Error} If TaskManager is already stopped
 */
start()

/**
 * Stop the TaskManager and cleanup resources
 * Waits for running tasks to complete and closes database connections
 */
stop()

/**
 * Pause task processing without stopping the TaskManager
 * Tasks in progress will complete, but new tasks won't be started
 */
pause()

/**
 * Resume task processing after a pause
 */
resume()

/**
 * Resume a specific paused or suspended task by ID
 * @param {string} taskId Task ID
 * @param {Object} [options] Resume options
 * @param {string} [options.resume_reason] Optional reason recorded in the audit event
 */
resumeTask(taskId, options)

/**
 * Cancel a task (e.g. approval rejected, request abandoned, schedule removed)
 * @param {string} taskId Task ID
 * @param {Object} [options] Cancel options
 * @param {string} [options.reason] Cancellation reason stored as the task error
 * @throws {Error} If the task does not exist, or is currently running
 */
cancelTask(taskId, options)

/**
 * Cancel cron schedules selected by identity filter
 * @param {Object} [filters] Selector; at least one of name / cronKey / tag is required
 * @param {string} [filters.name] Task name
 * @param {string} [filters.cronKey] Schedule identity key
 * @param {string} [filters.tag] Task tag
 * @param {Object} [options] Cancel options
 * @param {string} [options.reason] Cancellation reason
 * @returns {{cancelled:number, skipped:number, taskIds:Array<number>}}
 */
cancelCron(filters, options)

/**
 * Spawn one cron run immediately, without waiting for its window
 *
 * Only a run row is created — the schedule row is untouched, so a cancelled or
 * paused schedule is not revived and next_run_time does not move. Works in both
 * execution modes.
 * @param {string|number} scheduleId Cron schedule row id
 * @param {Object} [payload] Run payload; defaults to the schedule payload
 * @param {Object} [options] Trigger options
 * @param {string} [options.trigger='manual'] Recorded on the spawned event
 * @returns {{taskId:number|string, next_run_time:number|null}}
 */
runCronNow(scheduleId, payload, options)

/**
 * Pause a specific running task by ID
 * @param {string} taskId Task ID
 */
pauseTask(taskId)

/**
 * Run retention cleanup for expired terminal tasks and their audit records
 * @param {Object} [policy] Optional retention policy override
 * @returns {Object} Deletion counters, split by row-level and global policy
 */
runRetention(policy)
```

Cancellation notes:

- `cancelled` is a distinct status. It is not a failure: the row never propagates failure to a parent workflow and is excluded from `failed_tasks` in `getWorkflowAuditSummary()`.
- Allowed source states are `pending`, `paused`, `failed`, `timeout` and `suspended`. Cancelling a `running` task is rejected — pause it first, or wait for completion.
- `cancelTask()` throws `Task <id> not found` for an unknown id, instead of failing deeper in the database layer.
- Cancelled rows are removed by the default retention policy (`statuses` defaults to `completed`, `permanently_failed`, `cancelled`).

`expire_time` remains supported as a backward-compatible shortcut. Prefer `retention` when you need explicit control over retention statuses or want to make the cleanup policy obvious in configuration.

### Task Query
Query methods allow you to retrieve task information and monitor task status across the system.
```javascript
// Get tasks with multiple filter conditions
getTasks(filters)

// Get a specific task
getTask(taskId)

// Get tasks by name
getTasksByName(name)

// Get tasks by status
getTasksByStatus(status)

// Get child tasks
getChildTasks(parentId)

// Get tasks by tag
getTasksByTag(tag)

// Get task statistics by tag
getTaskStatsByTag(tag, status)

// List cron schedules (includes an orphan flag when the handler is gone)
getCronSchedules(filters)

// Spawn one cron run immediately (schedule row untouched)
runCronNow(scheduleId, payload, options)

// Aggregate cron schedule counters
getCronScheduleStats()

// Cancel pending schedules whose handler is no longer registered
pruneCronSchedules()

// Delete tasks with multiple filter conditions
deleteTasks(filters)
```

`getTasks()` remains the lightweight snapshot query API. When you need pagination metadata or workflow-scoped task views, use `queryTasks()` from the audit query section.

#### getTasks
The `getTasks` method provides flexible task querying with multiple filter conditions:

```javascript
/**
 * Get tasks with multiple filter conditions
 * @param {Object} filters Filter conditions
 * @param {string} [filters.tag] Filter by tag
 * @param {string} [filters.status] Filter by status ("pending", "running", "completed", etc)
 * @param {string} [filters.name] Filter by task name
 * @param {string} [filters.type] Filter by task type ("async" | "cron")
 * @returns {Array<Object>} Array of matching tasks
 */
getTasks(filters)
```

Examples:

```javascript
// Get tasks with a specific tag
const taggedTasks = taskManager.getTasks({ tag: "image-processing" });

// Get pending tasks for a specific task type
const pendingImageTasks = taskManager.getTasks({ 
    name: "processImage",
    status: "pending"
});

// Complex filtering with multiple conditions
const tasks = taskManager.getTasks({
    tag: "batch-1",
    status: "running",
    name: "videoProcess"
});

// Get all tasks (empty filter)
const allTasks = taskManager.getTasks({});
```

Filter Priority:
- Multiple filters are combined with AND logic
- If a filter is not provided, that condition is not applied
- Empty filters object returns all tasks
- Invalid filter values will throw an error for status, but be ignored for tag and name

Status Values:
- pending: Task waiting to be executed
- running: Task currently being executed
- completed: Task finished successfully
- failed: Task execution failed
- timeout: Task exceeded timeout duration
- permanently_failed: Failed task that exceeded retry attempts
- paused: Task manually paused
- suspended: Task waiting for an external condition (children completion, or explicit suspend/resume)

### Audit Query
Execution audit APIs expose persisted events, attempts, and structured task/workflow audit views.

For a detailed event catalog and semantics matrix, see [Execution Audit Events](execution-audit-events.md).
For retention scope and current cleanup semantics, see [Audit Retention Policy](audit-retention-policy.md).

```javascript
// Query task events with pagination metadata
queryTaskEvents(taskId, {
    event_type,
    event_types,
    worker_id,
    attempt,
    stage,
    started_after,
    started_before,
    limit,
    offset,
    order
})

// Query workflow events with the same filters
queryWorkflowEvents(rootId, filters)

// Query attempts for a task
queryTaskAttempts(taskId, {
    worker_id,
    outcome, // e.g. completed, failed, timeout, suspended, interrupted
    started_after,
    started_before,
    ended_after,
    ended_before,
    open_only,
    limit,
    offset,
    order
})

// Query attempts for all tasks in a workflow
queryWorkflowAttempts(rootId, {
    worker_id,
    outcome, // e.g. completed, failed, timeout, suspended, interrupted
    started_after,
    started_before,
    ended_after,
    ended_before,
    open_only,
    limit,
    offset,
    order
})

// Query tasks with pagination metadata
queryTasks({
    name,
    status,
    type,
    tag,
    worker_id,
    parent_id,
    root_id,
    workflow_root_id,
    limit,
    offset,
    order
})

// Structured audit views
getTaskAudit(taskId, {
    events: { limit: 50, order: 'asc' },
    attempts: { limit: 20, order: 'asc' }
})

getWorkflowAudit(rootId, {
    tasks: { limit: 100, order: 'asc' },
    events: { limit: 200, order: 'asc' }
})

// Aggregate workflow-level diagnosis
getWorkflowAuditSummary(rootId)
```

Paged audit APIs return this shape:

```javascript
{
    items: [...],
    total: 42,
    limit: 10,
    offset: 0,
    has_more: true
}
```

`getTaskAudit()` returns the current task snapshot together with paged `events` and `attempts`.
`getWorkflowAudit()` returns the root task snapshot together with paged `tasks` and `events` for the workflow.
`getWorkflowAuditSummary()` returns a platform-oriented aggregate view including status counts, attempt outcome counts, workers, timing boundaries, root workflow stage timings, failed tasks, a best-effort critical path estimate, and the slowest workflow attempts.

Recommended query patterns:

- Use `queryTaskEvents()` when diagnosing one task execution round, especially with `attempt`, `event_type`, and `order: 'asc'`.
- Use `queryWorkflowEvents()` when reconstructing workflow timelines across parent and child tasks. Prefer `event_type` or `event_types` plus pagination rather than loading every event into an operator-facing page.
- Use `queryTaskAttempts()` or `queryWorkflowAttempts()` when the question is about worker rounds, retry cadence, open executions, or duration analysis. Prefer attempt queries over deriving rounds from event sequences.
- Use `getTaskAudit()` and `getWorkflowAudit()` for operator drill-down pages. Use `getWorkflowAuditSummary()` for aggregate diagnosis, not for exact replay.

Summary field semantics:

- `timing.first_started_at`: the earliest `started_at` among workflow attempts.
- `timing.last_ended_at`: the latest non-null `ended_at` among workflow attempts.
- `timing.last_event_time`: the latest workflow event time currently visible in the event table.
- `timing.workflow_duration_seconds`: `last_ended_at - root_task.created_at` when both values exist; otherwise `null`.
- `stage_timings`: derived from root task attempts plus root task `task_started` / `task_retry_started` events. Pending workflows or workflows without attempts return an empty array.
- `failed_tasks`: terminal workflow tasks currently in `failed`, `timeout`, `permanently_failed`, or `paused` status. This is a latest-snapshot view, not a historical list of every failed round.
- `critical_path`: a best-effort path built from the longest persisted representative attempt per task plus the longest child branch. When sibling branches tie, the implementation falls back to deterministic task id ordering.
- `slowest_attempts`: the top 5 attempts sorted by persisted duration, then start time.

Operational boundaries:

- `getWorkflowAuditSummary()` currently reads the full task, event, and attempt set for the workflow before aggregating in memory. It is intended for platform diagnosis, not for arbitrarily large workflow scans in hot paths.
- Because persisted timing is second-granularity, very short or same-second attempts may collapse to equal durations. In those cases `critical_path`, `stage_timings`, and `slowest_attempts` remain deterministic but should be read as approximations.
- After retention deletes historical rows, audit and summary APIs describe only the remaining retained data. The platform does not promise long-term completeness after deletion-based retention has run.

The current `critical_path` is an estimate derived from persisted attempt durations along the workflow tree. It is useful for platform diagnosis, but it should not be treated as a perfect replacement for a distributed trace.
Because persisted task timing is currently stored in whole seconds, very short stages or sibling tasks may collapse to the same duration and rely on deterministic tie-breaking.

### Handler Audit
Handlers can emit structured checkpoint events during execution through `task.audit()`.

```javascript
taskManager.use('import_user', async (task) => {
    task.audit('payload_validated', {
        message: 'Payload validated',
        metadata: { source: task.payload.source }
    });

    task.audit({
        code: 'remote_call_started',
        message: 'Remote call started',
        metadata: { provider: 'crm' }
    });

    return { imported: true };
});
```

Checkpoint events are written as `task_checkpoint` audit events and automatically include the current task, workflow, worker, and open attempt context.

Naming conventions:

- `checkpoint.code` must be a non-empty string. Lowercase `snake_case` such as `payload_validated` or `remote_call_started` is recommended for consistency, but not required.
- `message` is optional, but when provided it should be display text rather than another identifier.
- `metadata` should remain structured and machine-readable.

Handlers can also update the task snapshot with lightweight progress state through `task.progress()`.

```javascript
taskManager.use('import_user', async (task) => {
    task.progress('Downloading source data', {
        stage_name: 'download',
        progress_percent: 20,
        metadata: { chunk: 1 }
    });

    task.progress({
        stage_name: 'transform',
        progress_text: 'Transforming records',
        progress_percent: 75,
        message: 'Transform stage running',
        metadata: { transformed: 15 }
    });

    return { imported: true };
});
```

`task.progress()` writes a `task_progress` event and updates these task snapshot fields when provided: `current_stage_name`, `progress_text`, `progress_percent`. All audit events also keep `last_event_time` and `last_event_type` in the main task snapshot for lightweight platform queries.

These snapshot fields are convenience cache only. Platform replay, audit diagnosis, and historical reconstruction should use the persisted event and attempt records as the source of truth.

Progress conventions:

- `stage_name` must be a non-empty string. Lowercase `snake_case` such as `download_phase` or `waiting_children` is recommended for consistency, but not required.
- `progress_text` should be short user-facing text.
- `progress_percent` should describe coarse operator-visible progress, not sub-second execution precision.

#### deleteTasks
The `deleteTasks` method provides flexible task deletion with multiple filter conditions:

```javascript
/**
 * Delete tasks with multiple filter conditions
 * @param {Object} filters Filter conditions
 * @param {string} [filters.tag] Filter by tag
 * @param {string} [filters.status] Filter by status ("pending", "running", "completed", etc)
 * @param {string} [filters.name] Filter by task name
 * @param {string} [filters.type] Filter by task type ("async" | "cron")
 * @returns {number} Number of tasks deleted
 * @throws {Error} If no filter is provided, or a filter value is invalid
 */
deleteTasks(filters)
```

Examples:

```javascript
// Delete tasks with a specific tag
const deletedCount = taskManager.deleteTasks({ tag: "cleanup" });

// Delete completed tasks
const deletedCompleted = taskManager.deleteTasks({ status: "completed" });

// Delete all cron schedules
const deletedCrons = taskManager.deleteTasks({ type: "cron" });

// Delete tasks matching multiple conditions
const deletedMulti = taskManager.deleteTasks({
    tag: "batch-1",
    status: "failed",
    name: "videoProcess"
});
```

Filter Behavior:
- Multiple filters are combined with AND logic
- If a filter is not provided, that condition is not applied
- **An empty filter object throws.** Wiping the table requires the explicit `clearTasks()` API, so a typo cannot silently destroy the audit trail.
- Invalid filter values will throw an error for status and type, but be ignored for tag and name

Status Values:
- pending: Task waiting to be executed
- running: Task currently being executed
- completed: Task finished successfully
- failed: Task execution failed
- timeout: Task exceeded timeout duration
- permanently_failed: Failed task that exceeded retry attempts
- paused: Task manually paused
- suspended: Task waiting for an external condition (children completion, or explicit suspend/resume)
- cancelled: Task or schedule explicitly cancelled by an operator

### Task Lifecycle
Task handlers receive task objects that contain comprehensive information about the task and provide methods for controlling task execution.

Task Status Values:
- `pending`: Task is waiting to be executed
- `running`: Task is currently being executed
- `completed`: Task has finished successfully
- `failed`: Task execution has failed
- `timeout`: Task exceeded its configured timeout duration
- `permanently_failed`: Async task that has failed and exceeded retry attempts
- `paused`: Cron task that has failed and exceeded retry attempts
- `suspended`: Parent task waiting for child tasks, or task explicitly suspended for external interaction (resumed via `resumeTask`)
- `cancelled`: Task or cron schedule explicitly cancelled (terminal, not counted as a failure)

Task Stage:
- Stage is a numeric value starting from 0
- Stage automatically increments during task execution
- Used for controlling multi-phase task processing
- Enables conditional task creation and execution based on current stage
```javascript
// Task handler receives a task object
taskManager.use('myTask', async (task) => {
    // Access task information
    console.log(task.id);          // Unique task ID
    console.log(task.name);        // Task type name
    console.log(task.payload);     // Task data
    console.log(task.status);      // Current status
    console.log(task.parent_id);   // Parent task ID (if any)
    console.log(task.stage);       // Current execution stage
    
    // Task control methods
    task.checkTimeout();           // Check if task has timed out
    task.setProgress(50);         // Update progress percentage
    task.suspend(options);        // Return a suspension marker (see below)
    
    // Return value becomes task result
    return { success: true };
});
```

### Explicit Suspension (Human-in-the-Loop)

A handler can deliberately suspend its workflow for external interaction (e.g. human approval) by returning `task.suspend(...)`. The task transitions to `suspended` (the suspend reason is recorded in the `task_suspended` audit event), releases its execution slot, and becomes immune to heartbeat and total timeouts. It never auto-resumes — an external system must call `resumeTask` or `cancelTask`.

```javascript
// Register a handler that waits for approval
const approvalTask = taskManager.use('requestApproval', async (task) => {
    if (task.stage === 0) {
        // First run: persist a binary snapshot, notify the approver, and suspend
        const snapshot = serializeFlowState(task);
        await notifyApprover(task.payload);
        return task.suspend({
            reason: 'awaiting_approval',
            context: snapshot   // Buffer/Uint8Array stored in the task's context column
        });
    }

    // Resumed run: restore the snapshot and continue
    const state = deserializeFlowState(task.context);
    const decision = await loadInteractionResult(task.id); // caller-managed store
    if (decision === 'approved') {
        await executeOrder(task.payload);
        return { approved: true };
    }
    throw new Error('Order rejected by approver');
});

// External system resumes the task when the interaction finishes
// (e.g. from an HTTP endpoint or message queue consumer)
taskManager.resumeTask(taskId, { resume_reason: 'approved_by_ops' });

// Or reject/abandon the request
// taskManager.cancelTask(taskId, { reason: 'request abandoned' });

// List all tasks currently waiting for approval
const pendingApprovals = taskManager.getTasksByStatus('suspended', {
    suspend_reason: 'awaiting_approval'
});
```

Semantics:
- `task.suspend(options)` requires a non-empty `reason`; it returns a marker object that must be returned from the handler.
- `task.suspend({ reason, context })` optionally persists a binary snapshot to the task's `context` column (same storage as the SubTasks context). On resume the handler reads it back as `task.context` (Buffer).
- On resume, the handler re-runs from scratch with the latest registered handler and `stage` advanced by 1. Use `task.stage` to detect the resumed run (same pattern as SubTasks parents); intermediate state must live in the database (`task.audit`/`task.progress`/payload/`context`). Interaction results are managed by the caller (e.g. an external store keyed by `task.id`).
- The suspend reason stays queryable via the `task_suspended` audit event; `getTasksByStatus('suspended', { suspend_reason })` derives it from that event.
- Suspended tasks are exempt from retention cleanup (they are in-flight, not terminal).
