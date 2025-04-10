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
 * @param {number} [options.retry_interval=300] Default retry interval in seconds
 * @param {number} [options.max_concurrent_tasks=10] Maximum concurrent tasks
 * @param {number} [options.active_update_interval=1000] Active time update interval
 */
new TaskManager(options)
```

### Task Registration

Tasks must be registered with handlers before they can be executed. There are two ways to register task handlers:

#### Single Task Registration
Register a single task type with its handler:
```javascript
/**
 * Register a single task handler
 * @param {string} taskName Task type identifier
 * @param {Function} handler Async function(task, next) to handle task execution
 */
use(taskName, handler)

// Example
use('processImage', async (task) => {
  // Process single image
  const { path } = task.payload;
  // ... image processing logic
  return { processed: true };
});
```

#### Bulk Task Registration 
Register multiple task types at once using an object:
```javascript
/**
 * Register multiple task handlers at once
 * @param {Object} handlers Object mapping task types to handler functions
 */
use({
  // Property names are task types, values are handler functions
  taskName: handlerFunction
})

// Example
use({
  processImage: async (task) => {
    // Process image
    return { processed: true };
  },
  processVideo: async (task) => {
    // Process video
    return { processed: true }; 
  },
  processAudio: async (task) => {
    // Process audio
    return { processed: true };
  }
});
```

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
