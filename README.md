# fib-flow

fib-flow is a workflow management system built on Fibjs, designed for orchestrating complex task dependencies and managing distributed task execution.

## Key Features

### Workflow Management
- Parent-child task relationships with automatic state propagation
- Suspended state for parent tasks while child tasks execute
- Automatic failure handling and state transitions
- Easy access to child task results and statuses

### Task Types & Scheduling
- **Async Tasks**: One-time execution with configurable delays and priorities
- **Cron Jobs**: Recurring tasks using standard cron expressions
- Priority-based task execution
- Concurrent task processing with resource limits

### State Management
- Comprehensive task lifecycle management
- Automatic state transitions based on execution results
- Different handling for async vs cron task failures
- Parent task state changes based on child task outcomes

### Reliability & Performance
- Automatic retries with configurable attempts and intervals
- Timeout protection and detection
- Connection pooling for database operations
- Transaction safety for state changes

### Database Support
- SQLite and MySQL adapters
- Efficient indexing for workflow queries
- Flexible connection options:
  - Connection strings
  - Direct database objects
  - Connection pools

## Common Use Cases

- **Background Processing**: File processing, report generation, data analysis
- **Scheduled Tasks**: Periodic cleanup, data synchronization, backups
- **Complex Workflows**: Multi-step data pipelines, approval processes
- **Distributed Systems**: Task coordination across multiple services

## Installation

Install fib-flow via fibjs:

```bash
fibjs --install fib-flow
```

## Getting Started

Here's a quick guide to using fib-flow:

```javascript
const { TaskManager } = require('fib-flow');

// Initialize task manager with options
const taskManager = new TaskManager({
    dbConnection: 'sqlite:tasks.db',  // Database connection string
    poll_interval: 1000,           // Poll interval in milliseconds
    max_retries: 3,               // Maximum retry attempts
    retry_interval: 300,          // Retry interval in seconds
    max_concurrent_tasks: 10      // Maximum concurrent tasks
});

// Initialize database
taskManager.db.setup();

// Register task handlers
taskManager.use('sendEmail', async (task) => {
    const { to, subject, body } = task.payload;
    await sendEmail(to, subject, body);
    return { sent: true };
});

// Start the task manager
taskManager.start();

// Add an async task
taskManager.async('sendEmail', {
    to: 'user@example.com',
    subject: 'Hello',
    body: 'World'
}, {
    delay: 0,           // Delay execution in seconds
    priority: 1,        // Task priority
    timeout: 30,        // Timeout in seconds
    max_retries: 3,     // Maximum retry attempts
    retry_interval: 60  // Retry interval in seconds
});
```

## Core Concepts

### Task Lifecycle and Execution

#### Task States

Tasks in fib-flow can be in the following states:

- `pending`: Task is waiting to be executed
- `running`: Task is currently being executed
- `completed`: Task has completed successfully
- `failed`: Task execution failed but may be retried
- `timeout`: Task exceeded its timeout duration
- `permanently_failed`: Async task that has failed and exceeded retry attempts
- `paused`: Cron task that has failed and exceeded retry attempts
- `suspended`: Parent task waiting for child tasks to complete

#### State Transitions

Tasks follow these state transition rules:

1. Initial State
   - All tasks start in `pending` state

2. Basic Transitions
   - `pending` → `running`: Task is claimed for execution
   - `running` → `completed`: Task completes successfully
   - `running` → `failed`: Task throws an error
   - `running` → `timeout`: Task exceeds timeout duration

3. Retry Transitions
   - `failed` → `pending`: Task has remaining retry attempts
   - `timeout` → `pending`: Task has remaining retry attempts
   - `failed` → `permanently_failed`: Async task with no retries left
   - `failed` → `paused`: Cron task with no retries left
   - `timeout` → `permanently_failed`: Async task with no retries left
   - `timeout` → `paused`: Cron task with no retries left

4. Workflow Transitions
   - `running` → `suspended`: Parent task creates child tasks
   - `suspended` → `pending`: All child tasks completed successfully
   - `suspended` → `permanently_failed`: Async parent task when any child fails
   - `suspended` → `paused`: Cron parent task when any child fails

5. Recovery Transitions
   - `paused` → `pending`: Manually resume a paused cron task

Note: State changes due to child task failures are automatic - the parent task handler is not called in these cases.

#### State Diagram

```mermaid
graph LR
    %% Nodes
    init((•)) --> pending
    pending[Pending]
    running[Running]
    completed[Completed]
    failed[Failed]
    timeout[Timeout]
    permanently_failed[Permanently Failed]
    paused[Paused]
    suspended[Suspended]
    
    %% Basic transitions
    pending --> |"claim"| running
    running --> |"success"| completed
    running --> |"error"| failed
    running --> |"timeout"| timeout
    
    %% Retry transitions
    failed --> |"has retries"| pending
    timeout --> |"has retries"| pending
    failed --> |"no retries & async"| permanently_failed
    timeout --> |"no retries & async"| permanently_failed
    failed --> |"no retries & cron"| paused
    timeout --> |"no retries & cron"| paused
    
    %% Workflow transitions
    running --> |"create children"| suspended
    suspended --> |"all children done"| pending
    suspended --> |"child failed & async"| permanently_failed
    suspended --> |"child failed & cron"| paused
    
    %% Recovery
    paused --> |"manual resume"| pending
    
    %% Styling
    classDef default fill:#f9f9f9,stroke:#333,stroke-width:2px
    classDef active fill:#d4edda,stroke:#28a745
    classDef error fill:#f8d7da,stroke:#dc3545
    classDef warning fill:#fff3cd,stroke:#ffc107
    classDef info fill:#cce5ff,stroke:#0d6efd
    
    class pending,running,suspended default
    class completed active
    class failed,timeout warning
    class permanently_failed error
    class paused info
```

### Task Types

#### Async Tasks
- One-time execution tasks
- Can be scheduled with delay
- Support priority levels
- Move to `permanently_failed` after max retries

#### Cron Tasks
- Recurring tasks based on cron expression
- Automatically schedule next run
- Can be paused and resumed
- Support same retry mechanism as async tasks

### Error Handling

1. **Task Timeout**
```javascript
taskManager.use('longTask', async (task) => {
    // Periodically check for timeout
    await step1();
    task.checkTimeout();
    
    await step2();
    task.checkTimeout();
    
    return result;
});
```

2. **Task Retry**
```javascript
// Configure retry behavior
taskManager.async('retryableTask', data, {
    max_retries: 3,        // Retry up to 3 times
    retry_interval: 300    // Wait 5 minutes between retries
});
```

## Workflow Support

fib-flow provides robust support for complex task workflows, allowing you to create parent-child task relationships and manage task dependencies.

#### Workflow Features

- **Parent-Child Relationships**: Tasks can create and manage child tasks
- **Status Propagation**: Child task failures automatically affect parent task status
- **Result Collection**: Easy access to child task results
- **Automatic Cleanup**: Proper handling of task hierarchies

#### Creating Workflows

```javascript
// Parent task that creates child tasks
taskManager.use('parent_task', (task, next) => {
    // Only called in two scenarios:
    // 1. First execution - create child tasks
    // 2. All child tasks completed successfully
    
    if (!task.completed_children) {
        // First execution - create child tasks
        return next([
            {
                name: 'child_task1',
                payload: { data: 'child1_data' }
            },
            {
                name: 'child_task2',
                payload: { data: 'child2_data' }
            }
        ]);
    }

    // All children completed successfully
    return { result: 'workflow_complete' };
});

// Child task handlers
taskManager.use('child_task1', task => {
    return { result: 'child1_result' };
});

taskManager.use('child_task2', task => {
    return { result: 'child2_result' };
});
```

#### Workflow State Management

- Parent tasks enter `suspended` state while waiting for children
- When child tasks fail:
  - For async parent tasks: automatically moves to `permanently_failed` state
  - For cron parent tasks: automatically moves to `paused` state
- No callback to parent task on child failure - state changes are automatic
- Parent task handler only runs when all children complete successfully

#### Monitoring Tasks

```javascript
// Get all child tasks for a parent
const children = taskManager.getChildTasks(parentId);

// Check task statuses
children.forEach(child => {
    console.log(`Child ${child.id}: ${child.status}`);
    if (child.status === 'completed') {
        console.log('Result:', child.result);
    } else if (child.status === 'permanently_failed') {
        console.log('Error:', child.error);
    }
});
```

## Database Configuration

fib-flow supports both SQLite and MySQL databases. You can specify the database connection in three ways:

1. **Connection String**:
```javascript
// SQLite
const taskManager = new TaskManager({
    dbConnection: 'sqlite:tasks.db'
});
```

2. **DB Connection Object**:
```javascript
const dbConn = db.open('sqlite:tasks.db');
const taskManager = new TaskManager({
    dbConnection: dbConn
});
```

3. **Connection Pool**:
```javascript
// When using a connection pool, you must specify the database type
const pool = Pool({
    create: () => db.open('sqlite:tasks.db'),
    destroy: conn => conn.close(),
    timeout: 30000,
    retry: 1,
    maxsize: 5
});

const taskManager = new TaskManager({
    dbConnection: pool,
    dbType: 'sqlite'    // Required when using connection pool
});

// MySQL example with pool
const mysqlPool = Pool({
    create: () => db.open('mysql://user:password@localhost:3306/dbname'),
    destroy: conn => conn.close(),
    timeout: 30000,
    retry: 1,
    maxsize: 5
});

const taskManager = new TaskManager({
    dbConnection: mysqlPool,
    dbType: 'mysql'    // Required when using connection pool
});
```

Note: The `dbType` parameter is only required when using a connection pool. When using a connection string, the database type is automatically inferred from the connection string prefix ('sqlite:' or 'mysql:').

## API Reference

### TaskManager

```javascript
/**
 * Create a task manager instance
 * @param {Object} options Configuration options
 * @param {string|object|function} options.dbConnection Database connection (string/object/pool)
 * @param {string} [options.dbType] Database type ('sqlite' or 'mysql') - Required when using connection pool
 * @param {number} [options.poll_interval=1000] Poll interval in milliseconds
 * @param {number} [options.max_retries=3] Default maximum retry attempts
 * @param {number} [options.retry_interval=300] Default retry interval in seconds
 * @param {number} [options.max_concurrent_tasks=10] Maximum concurrent tasks
 * @param {number} [options.active_update_interval=1000] Interval in milliseconds for updating task active time
 */
constructor(options = {}) {}

/**
 * Register a task handler
 * @param {string} name Task name
 * @param {Function} handler Task handler function
 */
use(name, handler) {}

/**
 * Add an async task
 * @param {string} name Task name
 *param {Object} payload Task data
 * @param {Object} [options] Task options
 * @param {number} [options.delay=0] Delay execution in seconds
 * @param {number} [options.priority=0] Task priority
 * @param {number} [options.timeout=60] Timeout in seconds
 * @param {number} [options.max_retries=3] Maximum retry attempts
 * @param {number} [options.retry_interval=0] Retry interval in seconds
 * @returns {number} Task ID
 */
async(name, payload, options = {}) {}

/**
 * Add a cron task
 * @param {string} name Task name
 * @param {string} cron_expr Cron expression
 * @param {Object} payload Task data
 * @param {Object} [options] Task options
 * @param {number} [options.timeout=60] Timeout in seconds
 * @param {number} [options.max_retries=3] Maximum retry attempts
 * @param {number} [options.retry_interval=0] Retry interval in seconds
 * @returns {number} Task ID
 */
cron(name, cron_expr, payload = {}, options = {}) {}

/**
 * Resume a paused cron task
 * @param {number} taskId Task ID
 * @throws {Error} If task is not found or not in paused state
 * @returns {number} Number of affected rows
 */
resumeTask(taskId) {}

/**
 * Start the task manager
 */
start() {}

/**
 * Stop the task manager
 */
stop() {}

/**
 * Retrieve task by ID
 * @param {number} taskId Task ID
 * @returns {Object} Task object
 */
getTask(taskId) {}

/**
 * Retrieve tasks by name
 * @param {string} name Task name
 * @returns {Array<Object>} Array of task objects
 */
getTasksByName(name) {}

/**
 * Retrieve tasks by status
 * @param {string} status Task status
 * @returns {Array<Object>} Array of task objects
 */
getTasksByStatus(status) {}
```

## Usage Examples

### Async Task Examples

1. **Basic Task**
```javascript
taskManager.async('processOrder', {
    orderId: '12345',
    userId: 'user789'
});
```

2. **Delayed Task**
```javascript
taskManager.async('sendReminder', {
    userId: 'user123',
    message: 'Don\'t forget to complete your profile!'
}, {
    delay: 3600  // Send reminder after 1 hour
});
```

3. **Priority Task**
```javascript
taskManager.async('sendNotification', {
    userId: 'user456',
    type: 'urgent',
    message: 'System alert!'
}, {
    priority: 10  // Higher priority task
});
```

### Cron Task Examples

1. **Daily Task**
```javascript
taskManager.cron('dailyReport', '0 0 * * *', {
    reportType: 'daily',
    recipients: ['admin@example.com']
});
```

2. **Weekly Backup**
```javascript
taskManager.cron('weeklyBackup', '0 0 * * 0', {
    backupType: 'full',
    destination: '/backups'
}, {
    timeout: 3600  // Allow up to 1 hour for backup
});
```

3. **Monthly Cleanup**
```javascript
taskManager.cron('monthlyCleanup', '0 0 1 * *', {
    older_than: '30d',
    target_dir: '/tmp'
}, {
    max_retries: 5,
    retry_interval: 600
});
```

## Cron Syntax

Cron expressions are used to define the schedule for recurring tasks. The syntax consists of six fields separated by spaces:

```
*    *    *    *    *    *
┬    ┬    ┬    ┬    ┬    ┬
│    │    │    │    │    |
│    │    │    │    │    └ day of week (0 - 7, 1L - 7L) (0 or 7 is Sun)
│    │    │    │    └───── month (1 - 12)
│    │    │    └────────── day of month (1 - 31, L)
│    │    └─────────────── hour (0 - 23)
│    └──────────────────── minute (0 - 59)
└───────────────────────── second (0 - 59, optional)
```

### Field Descriptions

1. **Second**: (optional) Specifies the exact second when the task should run. Valid values are 0-59.
2. **Minute**: Specifies the exact minute when the task should run. Valid values are 0-59.
3. **Hour**: Specifies the exact hour when the task should run. Valid values are 0-23.
4. **Day of Month**: Specifies the day of the month when the task should run. Valid values are 1-31. The character `L` can be used to specify the last day of the month.
5. **Month**: Specifies the month when the task should run. Valid values are 1-12 or JAN-DEC.
6. **Day of Week**: Specifies the day of the week when the task should run. Valid values are 0-7 (where 0 and 7 are both Sunday) or SUN-SAT. The character `L` can be used to specify the last day of the week.

### Examples

- `* * * * * *` - Every second
- `0 */5 * * * *` - Every 5 minutes
- `0 0 0 * * *` - Every day at midnight
- `0 0 9 * * 1-5` - Every weekday at 9 AM
- `0 0 12 1 * *` - At noon on the first day of every month
- `0 0 0 L * *` - At midnight on the last day of every month

### Special Characters

- `*` - Matches any value
- `,` - Separates items in a list (e.g., `MON,WED,FRI`)
- `-` - Specifies a range (e.g., `1-5`)
- `/` - Specifies increments (e.g., `*/15` for every 15 minutes)
- `L` - Last day of the month or week (e.g., `L` in the day-of-month field means the last day of the month)

Note: The `W` character (nearest weekday) is not supported.

For more complex scheduling scenarios, consult the [cron-parser documentation](https://github.com/harrisiirak/cron-parser).

## License

MIT License
