# fib-flow

A powerful workflow management system built on fibjs for orchestrating complex task dependencies and distributed task execution.

## Key Features

- **Workflow Management**: Parent-child task relationships, automatic state propagation
- **Task Types**: Async tasks and cron jobs with priorities and delays
- **State Management**: Comprehensive task lifecycle and state transitions
- **Reliability**: Automatic retries, timeout protection, transaction safety
- **Database Support**: SQLite/MySQL/PostgreSQL with flexible connection options
- **Resource Management**: Load balancing and specialized worker support

## Installation

```bash
fibjs --install fib-flow
```

## Quick Start

```javascript
const { TaskManager } = require('fib-flow');

// Initialize task manager (uses in-memory SQLite by default)
const taskManager = new TaskManager();
taskManager.db.setup();

// Basic task handler
taskManager.use('sendEmail', async (task) => {
    const { to, subject, body } = task.payload;
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
```

## Documentation

### Core Concepts
- [Task States and Transitions](docs/core-concepts.md#task-states-and-transitions)
- [Task Types](docs/core-concepts.md#task-types)
- [Error Handling](docs/core-concepts.md#error-handling)
- [Workflow Support](docs/workflow-guide.md)

### Configuration & Setup
- [Installation Guide](docs/installation.md)
- [Database Configuration](docs/database-config.md)
- [Task Handler System](docs/task-handler.md)

### Reference
- [API Documentation](docs/api-reference.md)
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
