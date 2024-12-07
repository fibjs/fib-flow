# fib-flow

fib-flow is a workflow management system built on fibjs, designed for orchestrating complex task dependencies and managing distributed task execution.

## Installation

Install fib-flow via fibjs:

```bash
fibjs --install fib-flow
```

## Quick Start

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
  - Automatic in-memory SQLite database when no connection is specified

#### Database Connection Flexibility

You can now create a `TaskManager` without explicitly providing a database connection. In such cases, an in-memory SQLite database will be automatically created:

```javascript
const taskManager = new TaskManager(); // No database connection specified
taskManager.db.setup(); // Initialize database schema

// Register task handlers
taskManager.use('data_processing', async (task) => {
    const { data } = task.payload;
    
    // Perform data transformation
    const processedData = await transformData(data);
    
    // Return processed data for potential child tasks
    return { processedData };
});

taskManager.use('data_storage', async (task) => {
    const { processedData } = task.payload;
    
    // Store processed data
    await storeData(processedData);
    
    return { stored: true };
});

// Start the task manager
taskManager.start();

// Add an async task with workflow
taskManager.async('data_processing', {
    data: rawInputData
}, {
    children: ['data_storage']  // Define child task workflow
});
```

**Single-Instance Use Cases**

This feature is particularly beneficial for single-instance, in-process scenarios where:
- Distributed task management is not required
- High fault tolerance is not critical
- Simple, lightweight task orchestration is needed
- Tasks are executed within a single process or application

Example of in-process workflow:

```javascript
const taskManager = new TaskManager(); // No database connection specified
taskManager.db.setup(); // Initialize database schema

// Register task handlers
taskManager.use('data_processing', async (task) => {
    const { data } = task.payload;
    
    // Perform data transformation
    const processedData = await transformData(data);
    
    // Return processed data for potential child tasks
    return { processedData };
});

taskManager.use('data_storage', async (task) => {
    const { processedData } = task.payload;
    
    // Store processed data
    await storeData(processedData);
    
    return { stored: true };
});

// Start the task manager
taskManager.start();

// Add an async task with workflow
taskManager.async('data_processing', {
    data: rawInputData
}, {
    children: ['data_storage']  // Define child task workflow
});
```

Benefits in single-instance scenarios:
- Zero configuration overhead
- Minimal performance impact
- Simplified task management for local, non-distributed workloads
- Ideal for microservices, background processing, and event-driven architectures

## Common Use Cases

- **Background Processing**: File processing, report generation, data analysis
- **Scheduled Tasks**: Periodic cleanup, data synchronization, backups
- **Complex Workflows**: Multi-step data pipelines, approval processes
- **Distributed Systems**: Task coordination across multiple services

## Core Concepts

### Task States and Transitions

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

### Workflow Support

fib-flow provides comprehensive support for complex task workflows, enabling you to create sophisticated task hierarchies and manage dependencies effectively.

#### Core Workflow Concepts

1. **Parent-Child Relationships**
   - Parent tasks can create multiple child tasks
   - Parent task enters `suspended` state while waiting for children
   - Parent task resumes only when all children complete successfully

2. **State Management**
   - Parent tasks automatically transition to `suspended` when creating children
   - Child task failures automatically propagate to parent:
     * Async parent tasks → `permanently_failed`
     * Cron parent tasks → `paused`
   - No parent task callback on child failure - state changes are automatic

3. **Task Monitoring**
   - Track entire workflow progress through task states
   - Access child task results and errors
   - Query tasks by parent-child relationships

#### Workflow Examples

1. **Basic Parent-Child Workflow**
```javascript
// Parent task handler - creates and manages child tasks
taskManager.use('parent_task', (task, next) => {
    // First execution - create child tasks
    if (!task.completed_children) {
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

    // Called only when all children complete successfully
    return { result: 'workflow_complete' };
});

// Child task handlers
taskManager.use('child_task1', task => {
    return { result: 'child1_result' };
});

taskManager.use('child_task2', task => {
    return { result: 'child2_result' };
});

// Start the workflow
const parentId = taskManager.async('parent_task', { data: 'parent_data' });
```

2. **Nested Workflows**
```javascript
// Root task creates middle-level tasks
taskManager.use('root_task', (task, next) => {
    if (!task.completed_children) {
        return next([{
            name: 'middle_task',
            payload: { level: 1 }
        }]);
    }
    return { result: 'root_done' };
});

// Middle task creates leaf tasks
taskManager.use('middle_task', (task, next) => {
    if (!task.completed_children) {
        return next([{
            name: 'leaf_task',
            payload: { level: 2 }
        }]);
    }
    return { result: 'middle_done' };
});

// Leaf task performs actual work
taskManager.use('leaf_task', task => {
    return { result: 'leaf_done' };
});
```

3. **Error Handling in Workflows**
```javascript
// Parent task with error handling
taskManager.use('parent_task', (task, next) => {
    if (!task.completed_children) {
        return next([{
            name: 'risky_task',
            payload: { data: 'important' },
            max_retries: 3,           // Override retry settings
            retry_interval: 60        // Wait 1 minute between retries
        }]);
    }
    return { result: 'success' };
});

// Child task that might fail
taskManager.use('risky_task', task => {
    if (someErrorCondition) {
        throw new Error('Task failed');  // Parent will be notified automatically
    }
    return { result: 'success' };
});

// Monitor workflow progress
const parentId = taskManager.async('parent_task', {});
const children = taskManager.getChildTasks(parentId);

children.forEach(child => {
    console.log(`Child ${child.id}: ${child.status}`);
    if (child.status === 'completed') {
        console.log('Result:', child.result);
    } else if (child.status === 'permanently_failed') {
        console.log('Error:', child.error);
    }
});
```

### Task Handler System

#### Handler Registration
- Register specific handlers for different task types using `taskManager.use()`
- Each handler can be specialized for specific task requirements
- Multiple workers can register the same handler for load balancing
- Specialized workers can register unique handlers for specific tasks

#### Worker Specialization
- **Load Balancing**: Multiple workers can register common handlers
  - Tasks are distributed across available workers
  - Automatic failover if a worker becomes unavailable
  - Improved system throughput and reliability

- **Specialized Processing**: Workers can register unique handlers
  - GPU-dependent tasks can be routed to GPU-enabled workers
  - Memory-intensive tasks can be directed to high-memory workers
  - Special hardware requirements (e.g., TPU, FPGA) can be accommodated

#### Handler Selection
- Tasks are automatically routed to workers with matching handlers
- If multiple workers are available, load is balanced automatically
- Tasks requiring specific resources wait for appropriate workers
- Ensures optimal resource utilization across the system

## API Reference

### TaskManager

#### Constructor
```javascript
/**
 * Create a task manager instance
 * @param {Object} options Configuration options
 * @param {string|object} options.dbConnection Database connection string or object
 * @param {string} [options.dbType] Database type ('sqlite' or 'mysql')
 * @param {number} [options.poll_interval=1000] Poll interval in milliseconds
 * @param {number} [options.max_retries=3] Default maximum retry attempts
 * @param {number} [options.retry_interval=300] Default retry interval in seconds
 * @param {number} [options.max_concurrent_tasks=10] Maximum concurrent tasks
 * @param {number} [options.active_update_interval=1000] Active time update interval
 */
new TaskManager(options)
```

#### Task Registration
```javascript
/**
 * Register a task handler
 * @param {string} taskName Task type identifier
 * @param {Function} handler Async function(task, next) to handle task
 */
use(taskName, handler)
```

#### Task Creation
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

#### Task Control
```javascript
// Start the TaskManager and begin processing tasks
start()

// Stop the TaskManager and cleanup resources
stop()

// Resume a specific paused task by ID
resumeTask(taskId)
```

#### Task Query
```javascript
// Get a specific task
getTask(taskId)

// Get tasks by name
getTasksByName(name)

// Get tasks by status
getTasksByStatus(status)

// Get child tasks
getChildTasks(parentId)
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
