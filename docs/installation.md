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
    max_concurrent_tasks: 10      // Maximum concurrent tasks
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
- `max_retries`: Number of retry attempts for failed tasks
- `retry_interval`: Time to wait before retrying (in seconds)
- `timeout`: Default task execution timeout (in seconds)
- `max_concurrent_tasks`: Maximum number of tasks running simultaneously

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

For error handling and advanced configurations, refer to the [Advanced Guide](advanced-guide.md).
