# Installation Guide

fib-flow is a workflow management system built on fibjs, designed for orchestrating complex task dependencies and managing distributed task execution.

## Table of Contents
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Basic Setup](#basic-setup)
  - [Task Registration](#task-registration)
  - [Running Tasks](#running-tasks)

## Installation

### Prerequisites
- fibjs >= 0.33.0
- One of the following databases:
  - SQLite3 (included)
  - MySQL 5.7+
  - PostgreSQL 12+

Install fib-flow via fibjs:

```bash
fibjs --install fib-flow
```

## Quick Start

### Basic Setup
```javascript
const { TaskManager } = require('fib-flow');

// Initialize task manager with options
const taskManager = new TaskManager({
    dbConnection: 'sqlite:tasks.db',  // Database connection string
    poll_interval: 1000,           // Poll interval in milliseconds
    max_retries: 3,               // Maximum retry attempts
    retry_interval: 0,            // No delay between retries
    timeout: 60,                  // Default task timeout in seconds
  task_heartbeat_interval: 5000, // Running task heartbeat interval in milliseconds
  task_heartbeat_timeout: 30000, // Running task heartbeat timeout window in milliseconds
  max_concurrent_tasks: 10,     // Maximum concurrent tasks
  pod_id: 'scheduler-a',        // Stable logical node identity for recovery
  worker_heartbeat_interval: 5000, // Worker registry heartbeat interval in milliseconds
  worker_heartbeat_timeout: 30000, // Worker liveness timeout window in milliseconds
  recover_running_jobs: true    // Reclaim running jobs from dead workers
});

// Initialize database
taskManager.db.setup();
```

Configuration Options:
- `dbConnection`: Database connection string (supports SQLite, MySQL, PostgreSQL)
  - SQLite: `sqlite:tasks.db` or `sqlite::memory:`
  - MySQL: `mysql://user:pass@host:3306/database`
  - PostgreSQL: `psql://user:pass@host:5432/database`
- `poll_interval`: How often to check for new tasks (in milliseconds)
- `max_retries`: Number of total attempts for failed tasks (including initial attempt)
- `retry_interval`: Time to wait before retrying (in seconds)
- `timeout`: Default task execution timeout (in seconds)
- `task_heartbeat_interval`: How often running tasks refresh `last_active_time` (in milliseconds).
- `task_heartbeat_timeout`: How long a running task can go without refreshing `last_active_time` before fib-flow marks it as timed out (in milliseconds).
- `max_concurrent_tasks`: Maximum number of tasks running simultaneously
- `worker_id`: Unique identifier for the current worker instance. If omitted, fib-flow generates one automatically.
- `pod_id`: Stable logical node identity used to group multiple worker instances across restarts.
- `worker_heartbeat_interval`: How often the current worker updates `fib_flow_workers` liveness metadata (in milliseconds).
- `worker_heartbeat_timeout`: Worker liveness timeout window (in milliseconds).
- `recover_running_jobs`: Whether startup and peer scans reclaim `running` jobs owned by dead or superseded workers.

Worker recovery behavior:
- When `pod_id` is configured, fib-flow stores worker liveness in `fib_flow_workers`.
- A newer worker from the same `pod_id` supersedes older active workers.
- Healthy peers can mark expired workers as dead and reclaim their `running` jobs.
- Recovered execution rounds are recorded with attempt outcome `interrupted` and audit event `task_recovered`.

### Task Registration
```javascript
// Basic handler registration
taskManager.use('sendEmail', async (task) => {
    const { to, subject, body } = task.payload;
    await sendEmail(to, subject, body);
    return { sent: true };
});

// Handler registration with options
taskManager.use('processImage', {
    handler: async (task) => {
        const { path } = task.payload;
        await processImage(path);
        return { processed: true };
    },
    timeout: 120,       // 2 minutes timeout
    max_retries: 2,     // Maximum 2 retries
    retry_interval: 30, // Retry every 30 seconds
    priority: 5         // Higher priority task
});
```

Task Handler Options:
- `handler`: The function that processes the task
- `timeout`: Task-specific timeout in seconds
- `max_retries`: Maximum retry attempts for this task type
- `retry_interval`: Retry interval in seconds
- `priority`: Default priority for this task type

Task Handler Parameters:
- `task.payload`: Contains the task input data
- `task.id`: Unique identifier for the task
- `task.status`: Current status of the task
- `task.attempts`: Number of execution attempts

### Running Tasks
For comprehensive examples, see [Usage Examples](usage-examples.md).

Basic startup example:
```javascript
taskManager.start();
taskManager.async('simple_task', { data: 'test' });
```

For error handling and advanced configurations, refer to the [API Reference](api-reference.md), [Database Configuration](database-config.md), and [Workflow Guide](workflow-guide.md).
