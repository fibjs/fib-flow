/**
 * A distributed task scheduling and execution system that supports both async and cron tasks.
 * Provides task management, scheduling, execution, and monitoring capabilities.
 */

const coroutine = require('coroutine');
const parser = require('cron-parser');
const { createAdapter } = require('./db/index.js');
const createLogger = require('./logger');

// Create logger for task operations
const logger = createLogger('fib-flow');

/**
 * Container class for managing subtasks in a workflow
 * Used to return multiple child tasks from a task handler
 */
class SubTasks {
    /**
     * Create a new SubTasks instance
     * @param {object|Array<object>} tasks - Single task or array of tasks to execute as subtasks
     * @param {Buffer|null} context - Binary context data to be stored with parent task
     */
    constructor(tasks, context = null) {
        if (context && !(context instanceof Uint8Array)) {
            logger.error(`[SubTasks] Invalid context provided, must be a Buffer or Uint8Array`);
            throw new Error('Context must be a Buffer or Uint8Array');
        }

        if (!Array.isArray(tasks)) {
            tasks = [tasks];
        }
        this.tasks = tasks;
        this.context = context;
    }
}

/**
 * TaskManager class handles task scheduling, execution, and lifecycle management
 */
class TaskManager {
    /**
     * Initialize a new TaskManager instance
     * @param {Object} options - Configuration options
     * @param {Object} options.dbConnection - Database connection configuration
     * @param {string} options.dbType - Type of database to use
     * @param {number} options.poll_interval - Interval (ms) between task polling (default: 1000)
     * @param {number} options.max_retries - Maximum number of task retry attempts (default: 3)
     * @param {number} options.retry_interval - Interval between retries
     * @param {number} options.timeout - Task timeout in seconds
     * @param {string} options.worker_id - Unique identifier for this worker (auto-generated if not provided)
     * @param {number} options.max_concurrent_tasks - Maximum number of tasks to run simultaneously (default: 10)
     * @param {number} options.active_update_interval - Interval (ms) for updating active task status (default: 1000)
     * @param {number} options.expire_time - Time in seconds after which completed and failed tasks are deleted (default: null)
     */
    constructor(options = {}) {
        // Initialize database adapter with default in-memory SQLite if no connection provided
        const finalOptions = {
            ...options,
            dbConnection: options.dbConnection || 'sqlite::memory:',
            dbType: options.dbType || 'sqlite'
        };

        logger.info(`[TaskManager] Initializing with options:`, finalOptions);

        // Create database adapter using the determined connection
        this.db = createAdapter(finalOptions.dbConnection, finalOptions.dbType);

        // Set default options with fallback values to ensure robust configuration
        this.options = {
            poll_interval: 1000,
            max_retries: 3,
            retry_interval: 0,
            timeout: 60,
            // Generate unique worker ID to distinguish between different task manager instances
            worker_id: `worker-${Math.random().toString(36).slice(2)}`,
            max_concurrent_tasks: 10,
            active_update_interval: 1000,
            expire_time: 1 * 24 * 3600,  // Default to 1 days
            ...options
        };

        logger.info(`[TaskManager] Configuration complete:`, this.options);

        // Initialize internal state tracking for task management
        this.handlers = new Map();
        this.state = 'init';  // Initial state before starting task processing
        this.currentFiber = null;
        this.runningTasks = new Set();

        // Set up concurrency control mechanisms to manage task execution
        this.semaphore = new coroutine.Semaphore(this.options.max_concurrent_tasks);
        this.sleep = new coroutine.Semaphore(0);
        this.event = new coroutine.Event(true);
        this.activeTimer = null;
    }

    /**
     * Register task handlers for specific task types. This must be called before starting the TaskManager.
     * Supports two registration modes:
     * 1. Single handler registration with taskName and handler
     * 2. Bulk registration with an object mapping task names to handlers
     * 
     * @param {string|Object} taskName - Task type identifier or object mapping task names to handlers
     * @param {Function} [handler] - Async function to handle task execution. Required when taskName is string.
     *                              The handler receives task object with following properties:
     *                              - id: Unique task identifier
     *                              - name: Task type name
     *                              - payload: Task input data
     *                              - status: Current task status
     *                              - checkTimeout(): Method to check if task has timed out
     * @throws {Error} When:
     *  - TaskManager is not in 'init' state
     *  - Handler is missing when registering single task
     *  - Invalid handler type provided
     * @example
     * // Single handler registration
     * taskManager.use('processImage', async (task, next) => {
     *   const { path } = task.payload;
     *   // Process image
     *   return { success: true };
     * });
     * 
     * // Bulk handler registration
     * taskManager.use({
     *   processImage: async (task) => { },
     *   processVideo: async (task) => { },
     *   processAudio: async (task) => { }
     * });
     */
    use(taskName, handler) {
        if (this.state !== 'init') {
            logger.error(`[TaskManager] Cannot register handler in state: ${this.state}`);
            throw new Error('Can only register handler when TaskManager is in init state');
        }

        // Handle object parameter case
        if (arguments.length === 1 && typeof taskName === 'object') {
            logger.info(`[TaskManager] Registering multiple task handlers`);
            for (const [name, fn] of Object.entries(taskName)) {
                this.use(name, fn);
            }
            return;
        }

        logger.info(`[TaskManager] Registering handler for task type: ${taskName}`);

        // Support both function and object with handler property
        let taskHandler;
        let taskOptions = {};

        if (typeof handler === 'function') {
            taskHandler = handler;
            taskOptions = {
                max_retries: this.options.max_retries,
                retry_interval: this.options.retry_interval,
                timeout: this.options.timeout
            };
        } else if (typeof handler === 'object' && handler.handler) {
            taskHandler = handler.handler;
            // Extract supported options, use TaskManager options as defaults
            const {
                max_retries = this.options.max_retries,
                retry_interval = this.options.retry_interval,
                timeout = this.options.timeout,
                priority,
                max_concurrent_tasks // New option for task-level concurrency
            } = handler;
            taskOptions = {
                max_retries,
                retry_interval,
                timeout,
                priority,
                max_concurrent_tasks,
                running_count: 0  // 添加运行时计数器
            };
        } else {
            throw new Error('Handler must be a function or an object with handler property');
        }

        // Store both handler and options
        this.handlers.set(taskName, {
            handler: taskHandler,
            options: taskOptions
        });

        logger.info(`[TaskManager] Handler registered successfully for: ${taskName}`);
    }

    /**
     * Submit an asynchronous task for execution
     * @param {string} taskName - Name of the task type to execute
     * @param {Object} payload - Task data/parameters
     * @param {Object} options - Task execution options
     * @param {number} options.delay - Delay in seconds before task execution
     * @param {number} options.priority - Task priority level
     * @param {number} options.max_retries - Maximum retry attempts for this task
     * @param {number} options.retry_interval - Interval between retries
     * @param {number} options.timeout - Task timeout in seconds
     * @param {string} options.tag - Task tag for categorization
     * @throws {Error} If TaskManager is not running or handler not registered
     * @returns {Promise} Task creation result
     */
    async(taskName, payload = {}, options = {}) {
        logger.info(`[TaskManager] Submitting async task: ${taskName}, options:`, options);
        // Validate task can be submitted in current task manager state
        if (!this.handlers.has(taskName)) {
            logger.error(`[TaskManager] No handler found for task: ${taskName}`);
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // Get registered handler and its default options
        const registeredTask = this.handlers.get(taskName);
        const defaultOptions = {
            ...registeredTask.options,  // Task type specific defaults from registration
            ...options  // Task instance specific options
        };

        // Calculate precise next execution time considering potential delay
        const now = Math.floor(Date.now() / 1000);
        const delay = defaultOptions.delay || 0;
        const nextRunTime = now + delay;

        logger.info(`[TaskManager] Creating async task with nextRunTime: ${nextRunTime}`);

        // Persist task with comprehensive metadata for tracking and retry logic
        const task_it = this.db.insertTask({
            name: taskName,
            type: 'async',
            payload,
            priority: defaultOptions.priority,
            tag: defaultOptions.tag,
            max_retries: defaultOptions.max_retries,
            retry_interval: defaultOptions.retry_interval,
            timeout: defaultOptions.timeout,
            next_run_time: nextRunTime
        });

        logger.info(`[TaskManager] Async task created successfully: ${task_it.id}`);

        // Signal that a new task is available for processing
        this.sleep.post();
        return task_it;
    }

    /**
     * Schedule a recurring task using cron expression
     * @param {string} taskName - Name of the task type to execute
     * @param {string} cron_expr - Cron expression for scheduling
     * @param {Object} payload - Task data/parameters
     * @param {Object} options - Task execution options
     * @param {number} options.priority - Task priority level
     * @param {number} options.max_retries - Maximum retry attempts for this task
     * @param {number} options.retry_interval - Interval between retries
     * @param {number} options.timeout - Task timeout in seconds
     * @param {string} options.tag - Task tag for categorization
     * @throws {Error} If cron expression is invalid or TaskManager not running
     * @returns {Promise} Task creation result
     */
    cron(taskName, cron_expr, payload = {}, options = {}) {
        logger.info(`[TaskManager] Scheduling cron task: ${taskName}, expression: ${cron_expr}`);
        // Validate task can be scheduled in current task manager state
        if (!this.handlers.has(taskName)) {
            logger.error(`[TaskManager] No handler found for task: ${taskName}`);
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // Validate cron expression to prevent scheduling with invalid patterns
        try {
            parser.parseExpression(cron_expr);
        } catch (e) {
            logger.error(`[TaskManager] Invalid cron expression: ${cron_expr}, error: ${e.message}`);
            throw new Error(`Invalid cron expression: ${cron_expr}`);
        }

        // Get registered handler and its default options
        const registeredTask = this.handlers.get(taskName);
        const defaultOptions = {
            ...registeredTask.options,  // Task type specific defaults from registration
            ...options  // Task instance specific options
        };

        // Calculate next execution time based on cron schedule
        const nextRunTime = this._getNextRunTime(cron_expr);
        logger.info(`[TaskManager] Next run time calculated: ${nextRunTime}`);

        // Persist recurring task with comprehensive metadata
        const task_it = this.db.insertTask({
            name: taskName,
            type: 'cron',
            cron_expr: cron_expr,
            payload,
            priority: defaultOptions.priority,
            tag: defaultOptions.tag,
            max_retries: defaultOptions.max_retries,
            retry_interval: defaultOptions.retry_interval,
            timeout: defaultOptions.timeout,
            next_run_time: nextRunTime
        });

        logger.info(`[TaskManager] Cron task created successfully: ${task_it.id}`);

        // Signal that a new task is available for processing
        this.sleep.post();
        return task_it;
    }

    /**
     * Resume a paused task
     * @param {string} taskId - ID of the task to resume
     * @throws {Error} If TaskManager is not running
     * @returns {Promise} Task update result
     */
    resumeTask(taskId) {
        logger.info(`[TaskManager] Attempting to resume task: ${taskId}`);

        const now = Math.floor(Date.now() / 1000);
        logger.info(`[TaskManager] Resuming task ${taskId} with next run time: ${now}`);
        return this.db.updateTaskStatus(taskId, 'pending', {
            retry_count: 0,
            next_run_time: now
        });
    }

    /**
     * Pause a running task
     * @param {string} taskId - ID of the task to pause
     * @throws {Error} If TaskManager is not running
     * @returns {Promise} Task update result
     */
    pauseTask(taskId) {
        logger.info(`[TaskManager] Attempting to pause task: ${taskId}`);

        logger.info(`[TaskManager] Pausing task ${taskId}`);
        return this.db.updateTaskStatus(taskId, 'paused');
    }

    /**
     * Calculate the next execution time for a cron task
     * @private
     * @param {string} cron_expr - Cron expression
     * @returns {number} Unix timestamp of next execution time
     */
    _getNextRunTime(cron_expr) {
        logger.debug(`[TaskManager] Calculating next run time for cron expression: ${cron_expr}`);
        // Parse cron expression and get next valid execution time
        const interval = parser.parseExpression(cron_expr);
        const nextTime = Math.floor(interval.next().getTime() / 1000);
        logger.debug(`[TaskManager] Next run time calculated: ${nextTime}`);
        return nextTime;
    }

    /**
     * Execute a task with proper error handling and resource management
     * @private
     * @param {Object} task - Task object containing execution details
     */
    _executeTask(task) {
        logger.info(`[TaskManager] Starting execution of task: ${task.name}(${task.id})`);
        // Track task in running set
        this.runningTasks.add(task.id);

        // Update task type concurrency count if limit exists
        const taskConfig = this.handlers.get(task.name);
        if (taskConfig.options.max_concurrent_tasks) {
            taskConfig.options.running_count++;
        }

        const startTime = Date.now();
        logger.debug(`[TaskManager] Task execution started at: ${startTime}`);

        // Add timeout check method to task
        task.checkTimeout = function () {
            if (this.timeout && (Date.now() - startTime) >= (this.timeout * 1000)) {
                logger.warning(`[TaskManager] Task ${task.id} exceeded timeout of ${this.timeout}s`);
                throw new Error('Task execution timeout');
            }
        };

        if (task.result) {
            const results = task.result.split('\n');

            task.result = [];
            for (let i = 0; i < results.length; i++) {
                const splitPos = results[i].indexOf(':');
                if (splitPos !== -1) {
                    task.result.push({
                        task_id: parseInt(results[i], 10),
                        result: JSON.parse(results[i].substring(splitPos + 1))
                    });
                }
            }

            task.result.sort((a, b) => a.task_id - b.task_id);
        }

        // Execute task in new fiber for isolation
        coroutine.start(async () => {
            try {
                // Execute registered handler for task type
                logger.debug(`[TaskManager] Executing handler for task ${task.id}`);
                const result = await this.handlers.get(task.name).handler(task, (tasks, context) => new SubTasks(tasks, context));
                if (this.state !== 'running') {
                    logger.warning(`[TaskManager] Task execution aborted - manager not running`);
                    return;
                }

                if (result instanceof SubTasks) {
                    logger.info(`[TaskManager] Task ${task.id} created subtasks`);
                    const childTasks = result.tasks.map(childTask => ({
                        ...childTask,
                        type: 'async',
                        priority: childTask.priority ?? task.priority,
                        timeout: childTask.timeout ?? task.timeout,
                        max_retries: childTask.max_retries ?? task.max_retries,
                        retry_interval: childTask.retry_interval ?? task.retry_interval
                    }));

                    // If context is provided, update the parent task
                    const options = {
                        root_id: task.root_id || task.id,
                        parent_id: task.id
                    };

                    if (result.context !== null && result.context !== undefined) {
                        options.context = result.context;
                    }

                    this.db.insertTask(childTasks, options);

                    logger.info(`[TaskManager] Created ${childTasks.length} child tasks for task ${task.id}`);
                } else if (task.type === 'cron') {
                    logger.debug(`[TaskManager] Updating cron task ${task.id} for next execution`);
                    // For cron tasks, set to pending with next scheduled time
                    this.db.updateTaskStatus(task.id, 'pending', {
                        result,
                        next_run_time: this._getNextRunTime(task.cron_expr)
                    });
                } else {
                    logger.info(`[TaskManager] Completing async task ${task.id}`);
                    // For async tasks, mark as completed
                    this.db.updateTaskStatus(task.id, 'completed', {
                        result,
                        parent_id: task.parent_id
                    });
                }

                this.sleep.post();
            } catch (error) {
                logger.error(`[TaskManager] Error executing task ${task.id}:`, error);
                // Set appropriate failure status and store stack trace which includes error message
                const status = error.message.includes('timeout') ? 'timeout' : 'failed';
                this.db.updateTaskStatus(task.id, status, { error: error.stack || String(error) });
            } finally {
                logger.debug(`[TaskManager] Task ${task.id} execution cleanup`);

                // Update task type concurrency count if limit exists
                if (taskConfig.options.max_concurrent_tasks) {
                    taskConfig.options.running_count = Math.max(0, taskConfig.options.running_count - 1);
                }
                // Clean up resources
                this.semaphore.release();
                this.runningTasks.delete(task.id);
            }
        });
    }

    _canTaskRun(taskName) {
        const taskConfig = this.handlers.get(taskName);
        if (!taskConfig?.options?.max_concurrent_tasks) {
            return true;
        }

        return taskConfig.options.running_count < taskConfig.options.max_concurrent_tasks;
    }

    _filterEligibleTasks(tasks) {
        return tasks.filter(taskName => this._canTaskRun(taskName));
    }

    /**
     * Start the task processing loop
     * Initializes active time updates and begins task polling
     * @throws {Error} If TaskManager is already stopped
     */
    start() {
        logger.info(`[TaskManager] Starting task manager`);
        // Validate state transitions
        if (this.state === 'running') {
            logger.warning(`[TaskManager] Already running, start ignored`);
            return;
        }
        if (this.state === 'stopped') {
            logger.error(`[TaskManager] Cannot restart stopped manager`);
            throw new Error('Cannot restart a stopped TaskManager');
        }

        this.state = 'running';
        this.runningTasks = new Set();
        logger.info(`[TaskManager] State changed to running`);

        // Start periodic task activity monitoring
        this.activeTimer = setInterval(() => {
            if (this.runningTasks.size > 0) {
                logger.debug(`[TaskManager] Updating active time for ${this.runningTasks.size} tasks`);
                this.db.updateTaskActiveTime(Array.from(this.runningTasks));
            }
            this.db.handleTimeoutTasks(this.options.active_update_interval, this.options.expire_time);
        }, this.options.active_update_interval);

        // Start main task processing loop
        coroutine.start(() => {
            logger.debug(`[TaskManager] Task processing loop started`);
            while (this.state === 'running') {
                // Ensure we don't exceed max concurrent tasks
                this.semaphore.acquire();
                if (this.state !== 'running') {
                    logger.warning(`[TaskManager] Processing loop stopped - manager not running`);
                    break;
                }

                this.event.wait();

                // Get eligible task types based on concurrency limits
                const eligibleTaskTypes = this._filterEligibleTasks(Array.from(this.handlers.keys()));

                // Try to claim an available task from eligible types
                const task = this.db.claimTask(eligibleTaskTypes, this.options.worker_id);
                if (!task) {
                    logger.debug(`[TaskManager] No tasks available, waiting ${this.options.poll_interval}ms`);
                    // No tasks available, wait before trying again
                    this.semaphore.release();
                    this.sleep.wait(this.options.poll_interval);
                    continue;
                }

                logger.info(`[TaskManager] Claimed task ${task.id} for execution`);
                this._executeTask(task);
            }
        });
    }

    /**
     * Pause task processing without stopping the TaskManager
     * Tasks in progress will complete, but new tasks won't be started
     */
    pause() {
        logger.info(`[TaskManager] Pausing task manager`);
        // Only pause if currently running
        if (this.state !== 'running') {
            logger.info(`[TaskManager] Cannot pause - not running (state: ${this.state})`);
            return;
        }
        this.event.clear();
        logger.info(`[TaskManager] Task manager paused`);
    }

    /**
     * Resume task processing after a pause
     */
    resume() {
        logger.info(`[TaskManager] Resuming task manager`);
        // Only resume if currently running
        if (this.state !== 'running') {
            logger.info(`[TaskManager] Cannot resume - not running (state: ${this.state})`);
            return;
        }
        this.event.set();
        logger.info(`[TaskManager] Task manager resumed`);
    }

    /**
     * Stop the TaskManager and cleanup resources
     * Waits for running tasks to complete and closes database connections
     */
    stop() {
        logger.info(`[TaskManager] Stopping task manager`);
        // Only stop if currently running
        if (this.state !== 'running') {
            logger.info(`[TaskManager] Cannot stop - not running (state: ${this.state})`);
            return;
        }
        this.state = 'stopped';
        this.sleep.post();  // Wake up any waiting fibers

        // Wait for in-progress tasks to complete
        while (this.runningTasks.size > 0) {
            logger.info(`[TaskManager] Waiting for ${this.runningTasks.size} tasks to complete`);
            coroutine.sleep(10);
        }

        // Clean up resources
        if (this.activeTimer) {
            clearInterval(this.activeTimer);
            this.activeTimer = null;
        }

        if (this.db) {
            this.db.close();
            this.db = null;
        }
        logger.info(`[TaskManager] Task manager stopped and cleaned up`);
    }

    /**
     * Retrieve a specific task by ID
     * @param {string} taskId - ID of the task to retrieve
     * @returns {Object} Task object
     */
    getTask(taskId) {
        logger.debug(`[TaskManager] Getting task: ${taskId}`);
        return this.db.getTask(taskId);
    }

    /**
     * Retrieve all tasks with a specific name
     * @param {string} name - Task name to search for
     * @returns {Array} Array of matching tasks
     */
    getTasksByName(name) {
        logger.debug(`[TaskManager] Getting tasks by name: ${name}`);
        return this.db.getTasksByName(name);
    }

    /**
     * Retrieve all tasks with a specific status
     * @param {string} status - Status to filter by
     * @returns {Array} Array of matching tasks
     */
    getTasksByStatus(status) {
        logger.debug(`[TaskManager] Getting tasks by status: ${status}`);
        return this.db.getTasksByStatus(status);
    }

    /**
     * Get all child tasks for a given task
     * @param {string|number} taskId - ID of the parent task
     * @returns {Array} Array of child tasks
     */
    getChildTasks(taskId) {
        logger.debug(`[TaskManager] Getting child tasks for: ${taskId}`);
        return this.db.getChildTasks(taskId);
    }

    /**
     * Get task statistics grouped by tag
     * @param {string} tag - Optional tag to filter by
     * @param {string} status - Optional status to filter by
     * @returns {Array<object>} Array of task statistics with tag, name, status and count
     */
    getTaskStatsByTag(tag, status) {
        logger.debug(`[TaskManager] Getting task statistics by tag: ${tag}, status: ${status}`);
        return this.db.getTaskStatsByTag(tag, status);
    }

    /**
     * Get all tasks with a specific tag
     * @param {string} tag - Tag to filter by
     * @returns {Array} Array of tasks with the specified tag
     */
    getTasksByTag(tag) {
        logger.debug(`[TaskManager] Getting tasks by tag: ${tag}`);
        return this.db.getTasksByTag(tag);
    }

    /**
     * Get tasks with multiple filter conditions
     * @param {Object} filters - Filter conditions
     * @param {string} [filters.tag] - Filter by tag
     * @param {string} [filters.status] - Filter by status
     * @param {string} [filters.name] - Filter by task name
     * @returns {Array} Array of tasks matching all filter conditions
     */
    getTasks(filters = {}) {
        logger.debug(`[TaskManager] Getting tasks with filters:`, filters);
        return this.db.getTasks(filters);
    }

    /**
     * Delete tasks based on filter conditions
     * @param {Object} filters - Filter conditions
     * @param {string} [filters.tag] - Filter by tag
     * @param {string} [filters.status] - Filter by status
     * @param {string} [filters.name] - Filter by task name
     * @returns {number} Number of tasks deleted
     * @throws {Error} If TaskManager is not running
     */
    deleteTasks(filters = {}) {
        logger.debug(`[TaskManager] Deleting tasks with filters:`, filters);

        return this.db.deleteTasks(filters);
    }
}

module.exports = TaskManager;
