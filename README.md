# fib-flow

A powerful workflow management system built on fibjs for orchestrating complex task dependencies and distributed task execution.

## Key Features

- **Workflow Management**: Parent-child task relationships, automatic state propagation
- **Task Types**: Async tasks and cron jobs with priorities and delays
- **State Management**: Comprehensive task lifecycle and state transitions
- **Hot Reload**: Update or remove handlers at runtime without restarting workers
- **Reliability**: Automatic retries, timeout protection, transaction safety
- **Worker Recovery**: Reclaim `running` jobs from dead or superseded workers through the `fib_flow_workers` registry
- **Execution Audit**: Persisted task events, attempts, workflow timelines, and handler checkpoints
- **Database Support**: SQLite/MySQL/PostgreSQL with flexible connection options
- **Resource Management**: Load balancing and specialized worker support

## Installation

```bash
fibjs --install fib-flow
```

## Quick Start

```javascript
const { TaskManager } = require('fib-flow');

// Initialize task manager with an explicit backend
const taskManager = new TaskManager({
    dbConnection: 'sqlite::memory:',
    pod_id: 'scheduler-a',
    worker_heartbeat_interval: 1000,
    worker_heartbeat_timeout: 5000,
    recover_running_jobs: true
});
taskManager.db.setup();

// Basic task handler
taskManager.use('sendEmail', async (task) => {
    const { to, subject, body } = task.payload;
    if (typeof to !== 'string' || typeof subject !== 'string' || typeof body !== 'string') {
        throw new Error('Invalid email payload');
    }
    task.audit('payload_validated', {
        message: 'Email payload validated',
        metadata: { recipient: to }
    });
    task.progress('Sending email', {
        stage_name: 'delivery',
        progress_percent: 80
    });
    await sendEmail(to, subject, body);
    return { sent: true };
});

// Handler with configuration
taskManager.use('processImage', {
    handler: async (task) => {
        const { path } = task.payload;
        await processImage(path);
        return { processed: true };
    },
    timeout: 120,      // 2 minutes timeout
    max_retries: 2,    // Maximum 2 retries
    retry_interval: 30 // 30 seconds retry interval
});

// Start processing
taskManager.start();

// Add a task
taskManager.async('sendEmail', {
    to: 'user@example.com',
    subject: 'Hello',
    body: 'World'
});

// Query task audit history with pagination metadata
const taskAudit = taskManager.getTaskAudit(1, {
    events: { limit: 20, order: 'asc' },
    attempts: { limit: 10, order: 'asc' }
});

// Query workflow-level aggregate audit summary
const workflowSummary = taskManager.getWorkflowAuditSummary(1);

// Run explicit retention cleanup when needed
taskManager.runRetention();

console.log(workflowSummary.stage_timings);
console.log(workflowSummary.critical_path);

// Hot update a handler for future claims
taskManager.use('sendEmail', async (task) => {
    const { to, subject, body } = task.payload;
    await sendEmailV2(to, subject, body);
    return { sent: true, provider: 'v2' };
});

// Remove a handler when a flow is unloaded
taskManager.unuse('processImage');
```

Hot reload semantics:

- A task attempt that is already executing keeps the handler version captured when it started.
- A paused or suspended task that resumes later uses the latest registered handler.
- Child tasks created by a running parent resolve against the live handler registry when they are created.

Worker recovery semantics:

- `worker_id` identifies a single running process instance.
- `pod_id` identifies a stable logical node across restarts.
- When `pod_id` is configured, fib-flow records worker liveness in `fib_flow_workers`.
- If a worker becomes dead or is superseded by a newer worker from the same `pod_id`, its `running` tasks can be reclaimed without waiting for task timeout.
- Running-task updates are ownership-fenced by `worker_id`, so stale workers cannot safely write back after recovery.

## Documentation

### Core Concepts
- [Task States and Transitions](docs/core-concepts.md#task-states-and-transitions)
- [Task Types](docs/core-concepts.md#task-types)
- [Error Handling](docs/core-concepts.md#error-handling)
- [Workflow Support](docs/workflow-guide.md)

### Configuration & Setup
- [Installation Guide](docs/installation.md)
- [Database Configuration](docs/database-config.md)
- [Worker Recovery Design](docs/worker-recovery-design.md)
- [Task Handler System](docs/task-handler.md)

### Reference
- [API Documentation](docs/api-reference.md)
- [Retention Policy](docs/audit-retention-policy.md)
- [Execution Audit Events](docs/execution-audit-events.md)
- [Cron Syntax Guide](docs/cron-syntax.md)
- [Usage Examples](docs/usage-examples.md)
- [Common Use Cases](docs/use-cases.md)

## Example Use Cases

- Background Processing: File processing, report generation
- Scheduled Tasks: Data synchronization, backups
- Complex Workflows: Multi-step data pipelines
- Distributed Systems: Task coordination across services

For detailed examples and implementation guides, see [Usage Examples](docs/usage-examples.md).

## License

MIT License
