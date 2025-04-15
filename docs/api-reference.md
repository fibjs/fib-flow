# API Reference

The API is designed to be intuitive and developer-friendly, allowing you to quickly implement task scheduling in your applications while maintaining full control over task lifecycles and execution parameters.

## Table of Contents
- [TaskManager](#taskmanager)
  - [Constructor](#constructor)
  - [Task Registration](#task-registration)
  - [Task Creation](#task-creation)
  - [Task Control](#task-control)
  - [Task Query](#task-query)
  - [Task Lifecycle](#task-lifecycle)

## TaskManager
The TaskManager is the core component responsible for managing task lifecycles, scheduling, and execution.

### Constructor
```javascript
/**
 * Create a task manager instance
 * @param {Object} options Configuration options
 * @param {string|object} options.dbConnection Database connection string or object
 * @param {string} [options.dbType] Database type ('sqlite', 'mysql', or 'postgres')
 * @param {number} [options.poll_interval=1000] Poll interval in milliseconds
 * @param {number} [options.max_retries=3] Default maximum retry attempts
 * @param {number} [options.retry_interval=0] Default retry interval in seconds
 * @param {number} [options.timeout=60] Default task timeout in seconds
 * @param {number} [options.max_concurrent_tasks=10] Maximum concurrent tasks
 * @param {number} [options.active_update_interval=1000] Active time update interval
 */
new TaskManager(options)
```

### Task Registration

Tasks must be registered with handlers before they can be executed. The TaskManager provides flexible handler registration through the `use()` method.

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

#### Handler Options

When registering a task handler using the object form, you can specify the following options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| handler | Function | Required | The async function that processes the task |
| timeout | Number | 60 | Task execution timeout in seconds |
| max_retries | Number | 3 | Maximum number of retry attempts |
| retry_interval | Number | 0 | Delay between retries in seconds |
| priority | Number | - | Default priority for all tasks of this type |

Notes:
- Options specified during handler registration become the defaults for that task type
- These defaults can be overridden when creating individual tasks
- Handler options take precedence over global TaskManager options
- If a handler is registered as a function, it will use the global TaskManager options

### Task Options

Task execution can be configured through two levels:

1. **Global Configuration** (TaskManager level)
```javascript
const taskManager = new TaskManager({
    poll_interval: 1000,      // Poll interval in milliseconds
    max_retries: 3,          // Maximum retry attempts
    retry_interval: 0,       // No delay between retries
    timeout: 60,            // Default task timeout
    max_concurrent_tasks: 10 // Maximum concurrent tasks
});
```

2. **Task Type Configuration** (Handler registration level)
```javascript
taskManager.use('processImage', {
    handler: async (task) => { /* ... */ },
    timeout: 120,      // 2 minutes timeout
    max_retries: 2,    // Maximum 2 retries
    retry_interval: 30 // 30 seconds retry interval
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
// Start the TaskManager and begin processing tasks
start()

// Stop the TaskManager and cleanup resources
stop()

// Resume a specific paused task by ID
resumeTask(taskId)

// Pause a specific running task by ID
pauseTask(taskId)
```

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
```

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
