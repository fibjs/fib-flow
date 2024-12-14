/**
 * A distributed task scheduling and execution system that supports both async and cron tasks.
 * Provides task management, scheduling, execution, and monitoring capabilities.
 */

const coroutine = require('coroutine');
const parser = require('cron-parser');
const { createAdapter } = require('./db/index.js');
const util = require('util');

// Create logger for task operations
const logger = util.debuglog('fib-flow');

/**
 * Container class for managing subtasks in a workflow
 * Used to return multiple child tasks from a task handler
 */
class SubTasks {
    /**
     * Create a new SubTasks instance
     * @param {object|Array<object>} tasks - Single task or array of tasks to execute as subtasks
     */
    constructor(tasks) {
        if (!Array.isArray(tasks)) {
            tasks = [tasks];
        }
        this.tasks = tasks;
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
            poll_interval: options.poll_interval || 1000,
            max_retries: options.max_retries || 3,
            retry_interval: options.retry_interval || 300,
            // Generate unique worker ID to distinguish between different task manager instances
            worker_id: options.worker_id || `worker-${Math.random().toString(36).slice(2)}`,
            max_concurrent_tasks: options.max_concurrent_tasks || 10,
            active_update_interval: options.active_update_interval || 1000,
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
     * Register a task handler for a specific task type
     * @param {string} taskName - Unique identifier for the task type
     * @param {Function} handler - Async function to handle task execution
     * @throws {Error} If TaskManager is not in init state
     */
    use(taskName, handler) {
        logger.info(`[TaskManager] Registering handler for task type: ${taskName}`);
        // Prevent handler registration after task manager has started
        if (this.state !== 'init') {
            logger.error(`[TaskManager] Cannot register handler in state: ${this.state}`);
            throw new Error('Can only register handler when TaskManager is in init state');
        }
        this.handlers.set(taskName, handler);
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
     * @throws {Error} If TaskManager is not running or handler not registered
     * @returns {Promise} Task creation result
     */
    async(taskName, payload = {}, options = {}) {
        logger.info(`[TaskManager] Submitting async task: ${taskName}, options:`, options);
        // Validate task can be submitted in current task manager state
        if (this.state !== 'running') {
            logger.error(`[TaskManager] Cannot submit task in state: ${this.state}`);
            throw new Error('Can only submit task when TaskManager is running');
        }
        if (!this.handlers.has(taskName)) {
            logger.error(`[TaskManager] No handler found for task: ${taskName}`);
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // Calculate precise next execution time considering potential delay
        const now = Math.floor(Date.now() / 1000);
        const delay = options.delay || 0;
        const nextRunTime = now + delay;

        logger.info(`[TaskManager] Creating async task with nextRunTime: ${nextRunTime}`);

        // Persist task with comprehensive metadata for tracking and retry logic
        const task_it = this.db.insertTask({
            name: taskName,
            type: 'async',
            payload,
            priority: options.priority,
            // Use task-specific or global retry configuration
            max_retries: options.max_retries !== undefined ? options.max_retries : this.options.max_retries,
            retry_interval: options.retry_interval,
            timeout: options.timeout,
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
     * @throws {Error} If cron expression is invalid or TaskManager not running
     * @returns {Promise} Task creation result
     */
    cron(taskName, cron_expr, payload = {}, options = {}) {
        logger.info(`[TaskManager] Scheduling cron task: ${taskName}, expression: ${cron_expr}`);
        // Validate task can be scheduled in current task manager state
        if (this.state !== 'running') {
            logger.error(`[TaskManager] Cannot schedule cron task in state: ${this.state}`);
            throw new Error('Can only submit task when TaskManager is running');
        }
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

        // Calculate next execution time based on cron schedule
        const nextRunTime = this._getNextRunTime(cron_expr);
        logger.info(`[TaskManager] Next run time calculated: ${nextRunTime}`);

        // Persist recurring task with comprehensive metadata
        const task_it = this.db.insertTask({
            name: taskName,
            type: 'cron',
            cron_expr: cron_expr,
            payload,
            priority: options.priority,
            // Use task-specific or global retry configuration
            max_retries: options.max_retries !== undefined ? options.max_retries : this.options.max_retries,
            retry_interval: options.retry_interval,
            timeout: options.timeout,
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
     * @throws {Error} If task not found or not in paused state
     * @returns {Promise} Task update result
     */
    resumeTask(taskId) {
        logger.info(`[TaskManager] Attempting to resume task: ${taskId}`);
        // Ensure task manager is in a state that allows task resumption
        if (this.state !== 'running') {
            logger.error(`[TaskManager] Cannot resume task in state: ${this.state}`);
            throw new Error('Can only resume task when TaskManager is running');
        }

        // Retrieve task and validate its resumability
        const task = this.db.getTask(taskId);
        if (!task) {
            logger.error(`[TaskManager] Task not found: ${taskId}`);
            throw new Error('Task not found');
        }

        // Validate task is in a paused state before resuming
        if (task.status !== 'paused') {
            logger.error(`[TaskManager] Cannot resume task ${taskId} in status: ${task.status}`);
            throw new Error('Can only resume paused tasks');
        }

        // Reset task state for immediate execution
        const now = Math.floor(Date.now() / 1000);
        logger.info(`[TaskManager] Resuming task ${taskId} with next run time: ${now}`);
        return this.db.updateTaskStatus(taskId, 'pending', {
            retry_count: 0,
            next_run_time: now
        });
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

        const startTime = Date.now();
        logger.debug(`[TaskManager] Task execution started at: ${startTime}`);

        // Add timeout check method to task
        task.checkTimeout = function () {
            if (this.timeout && (Date.now() - startTime) >= (this.timeout * 1000)) {
                logger.warning(`[TaskManager] Task ${task.id} exceeded timeout of ${this.timeout}s`);
                throw new Error('Task execution timeout');
            }
        };

        // Execute task in new fiber for isolation
        coroutine.start(async () => {
            try {
                // Execute registered handler for task type
                logger.debug(`[TaskManager] Executing handler for task ${task.id}`);
                const result = await this.handlers.get(task.name)(task, tasks => new SubTasks(tasks));
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

                    this.db.insertTask(childTasks, {
                        root_id: task.root_id || task.id,
                        parent_id: task.id
                    });

                    logger.info(`[TaskManager] Created ${childTasks.length} child tasks for task ${task.id}`);
                    this.sleep.post();
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
            } catch (error) {
                logger.error(`[TaskManager] Error executing task ${task.id}:`, error);
                // Set appropriate failure status and store error
                const status = error.message.includes('timeout') ? 'timeout' : 'failed';
                this.db.updateTaskStatus(task.id, status, { error: error.message || String(error) });
            } finally {
                logger.debug(`[TaskManager] Task ${task.id} execution cleanup`);
                // Clean up resources
                this.semaphore.release();
                this.runningTasks.delete(task.id);
            }
        });
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
            this.db.handleTimeoutTasks();
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

                // Try to claim an available task
                const task = this.db.claimTask(Array.from(this.handlers.keys()));
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

        // Wait for in-progress tasks to complete
        while (this.runningTasks.size > 0) {
            logger.info(`[TaskManager] Waiting for ${this.runningTasks.size} tasks to complete`);
            coroutine.sleep(100);
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
}

module.exports = TaskManager;
