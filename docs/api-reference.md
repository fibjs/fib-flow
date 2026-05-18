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
 * @param {number} [options.active_update_interval=1000] Active time update interval in milliseconds
 * @param {string} [options.worker_id] Unique identifier for this worker instance (auto-generated if not provided)
 * @param {string} [options.pod_id] Stable logical node identifier used for worker recovery and peer fencing
 * @param {number} [options.worker_heartbeat_interval=5000] Worker registry heartbeat interval in milliseconds
 * @param {number} [options.worker_ttl=30000] Worker liveness TTL in milliseconds
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

### Task Options

Task execution can be configured through three levels:

1. **Global Configuration** (TaskManager level)
```javascript
const taskManager = new TaskManager({
    poll_interval: 1000,          // Poll interval in milliseconds
    max_retries: 3,              // Maximum total attempts (including initial attempt)
    retry_interval: 0,           // No delay between retries
    timeout: 60,                // Default task timeout in seconds
    max_concurrent_tasks: 10,   // Maximum concurrent tasks
    active_update_interval: 1000, // Active time update interval
    pod_id: 'scheduler-a',      // Stable logical node identity for worker recovery
    worker_heartbeat_interval: 5000, // Worker registry heartbeat interval
    worker_ttl: 30000,          // Worker liveness TTL in milliseconds
    recover_running_jobs: true, // Reclaim running jobs from dead or superseded workers
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
 */
async(taskName, payload, options)

/**
 * Create a cron task
 * @param {string} taskName Task type
 * @param {string} cronExpr Cron expression
 * @param {Object} payload Task data
 * @param {Object} options Same as async task options
 */
cron(taskName, cronExpr, payload, options)
```

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
 * Resume a specific paused task by ID
 * @param {string} taskId Task ID
 */
resumeTask(taskId)

/**
 * Pause a specific running task by ID
 * @param {string} taskId Task ID
 */
pauseTask(taskId)

/**
 * Run retention cleanup for expired terminal tasks and their audit records
 * @param {Object} [policy] Optional retention policy override
 */
runRetention(policy)
```

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
- suspended: Parent task waiting for children

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

- `checkpoint.code` should use lowercase `snake_case`, for example `payload_validated` or `remote_call_started`.
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

- `stage_name` should use lowercase `snake_case`, for example `download_phase` or `waiting_children`.
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
 * @returns {number} Number of tasks deleted
 * @throws {Error} If status is invalid
 */
deleteTasks(filters)
```

Examples:

```javascript
// Delete tasks with a specific tag
const deletedCount = taskManager.deleteTasks({ tag: "cleanup" });

// Delete completed tasks
const deletedCompleted = taskManager.deleteTasks({ status: "completed" });

// Delete tasks of a specific type
const deletedByName = taskManager.deleteTasks({ name: "processImage" });

// Delete tasks matching multiple conditions
const deletedMulti = taskManager.deleteTasks({
    tag: "batch-1",
    status: "failed",
    name: "videoProcess"
});

// Delete all tasks (empty filter)
const deletedAll = taskManager.deleteTasks({});
```

Filter Behavior:
- Multiple filters are combined with AND logic
- If a filter is not provided, that condition is not applied
- Empty filters object deletes all tasks
- Invalid filter values will throw an error for status, but be ignored for tag and name

Status Values:
- pending: Task waiting to be executed
- running: Task currently being executed
- completed: Task finished successfully
- failed: Task execution failed
- timeout: Task exceeded timeout duration
- permanently_failed: Failed task that exceeded retry attempts
- paused: Task manually paused
- suspended: Parent task waiting for children

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
- `suspended`: Parent task waiting for child tasks to complete

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
    
    // Return value becomes task result
    return { success: true };
});
```
