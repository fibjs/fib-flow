/**
 * A distributed task scheduling and execution system that supports both async and cron tasks.
 * Provides task management, scheduling, execution, and monitoring capabilities.
 */

const coroutine = require('coroutine');
const parser = require('cron-parser');
const { createAdapter } = require('./db/index.js');

// 内部类定义
class SubTasks {
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
     * @param {number} options.retry_interval - Interval (s) between retries (default: 300)
     * @param {string} options.worker_id - Unique identifier for this worker (auto-generated if not provided)
     * @param {number} options.max_concurrent_tasks - Maximum number of tasks to run simultaneously (default: 10)
     * @param {number} options.active_update_interval - Interval (ms) for updating active task status (default: 1000)
     */
    constructor(options = {}) {
        // Initialize database adapter with provided connection or default settings
        this.db = createAdapter(options.dbConnection, options.dbType);

        // Set default options with fallbacks for all configuration values
        this.options = {
            poll_interval: options.poll_interval || 1000,
            max_retries: options.max_retries || 3,
            retry_interval: options.retry_interval || 300,
            worker_id: options.worker_id || `worker-${Math.random().toString(36).slice(2)}`,
            max_concurrent_tasks: options.max_concurrent_tasks || 10,
            active_update_interval: options.active_update_interval || 1000,
            ...options
        };

        // Initialize internal state tracking
        this.handlers = new Map();
        this.state = 'init';  // Initial state
        this.currentFiber = null;
        this.runningTasks = new Set();

        // Initialize concurrency control mechanisms
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
        // Ensure TaskManager is in init state before registering handlers
        if (this.state !== 'init') {
            throw new Error('Can only register handler when TaskManager is in init state');
        }
        this.handlers.set(taskName, handler);
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
        // Validate task submission conditions
        if (this.state !== 'running') {
            throw new Error('Can only submit task when TaskManager is running');
        }
        if (!this.handlers.has(taskName)) {
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // Calculate task execution time
        const now = Math.floor(Date.now() / 1000);
        const delay = options.delay || 0;
        const nextRunTime = now + delay;

        // Insert task into database with all parameters
        const task_it = this.db.insertTask({
            name: taskName,
            type: 'async',
            payload,
            priority: options.priority,
            max_retries: options.max_retries !== undefined ? options.max_retries : this.options.max_retries,
            retry_interval: options.retry_interval,
            timeout: options.timeout,
            next_run_time: nextRunTime
        });

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
        // Validate task submission conditions
        if (this.state !== 'running') {
            throw new Error('Can only submit task when TaskManager is running');
        }
        if (!this.handlers.has(taskName)) {
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // Validate cron expression format
        try {
            parser.parseExpression(cron_expr);
        } catch (e) {
            throw new Error(`Invalid cron expression: ${cron_expr}`);
        }

        // Calculate next execution time based on cron expression
        const nextRunTime = this._getNextRunTime(cron_expr);

        // Insert cron task into database
        const task_it = this.db.insertTask({
            name: taskName,
            type: 'cron',
            cron_expr: cron_expr,
            payload,
            priority: options.priority,
            max_retries: options.max_retries !== undefined ? options.max_retries : this.options.max_retries,
            retry_interval: options.retry_interval,
            timeout: options.timeout,
            next_run_time: nextRunTime
        });

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
        // Validate TaskManager state
        if (this.state !== 'running') {
            throw new Error('Can only resume task when TaskManager is running');
        }

        // Retrieve and validate task exists
        const task = this.db.getTask(taskId);
        if (!task) {
            throw new Error('Task not found');
        }

        // Ensure task is in paused state
        if (task.status !== 'paused') {
            throw new Error('Can only resume paused tasks');
        }

        // Reset task state for immediate execution
        const now = Math.floor(Date.now() / 1000);
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
        // Parse cron expression and get next valid execution time
        const interval = parser.parseExpression(cron_expr);
        return Math.floor(interval.next().getTime() / 1000);
    }

    /**
     * Execute a task with proper error handling and resource management
     * @private
     * @param {Object} task - Task object containing execution details
     */
    _executeTask(task) {
        // Track task in running set
        this.runningTasks.add(task.id);
        console.log(`Executing task: ${task.name}(${task.id}), Payload:`, task.payload);

        const startTime = Date.now();

        // Add timeout check method to task
        task.checkTimeout = function () {
            if (this.timeout && (Date.now() - startTime) >= (this.timeout * 1000)) {
                throw new Error('Task execution timeout');
            }
        };

        // Execute task in new fiber for isolation
        coroutine.start(async () => {
            try {
                // Execute registered handler for task type
                const result = await this.handlers.get(task.name)(task, tasks => new SubTasks(tasks));
                if (this.state !== 'running') {
                    return;
                }

                if (result instanceof SubTasks) {
                    // 继承父任务的属性到子任务
                    const childTasks = result.tasks.map(childTask => ({
                        ...childTask,
                        type: 'async',
                        priority: childTask.priority ?? task.priority,
                        timeout: childTask.timeout ?? task.timeout,
                        max_retries: childTask.max_retries ?? task.max_retries,
                        retry_interval: childTask.retry_interval ?? task.retry_interval
                    }));

                    // 创建子任务，父任务状态会在 insertTask 中被更新为 suspended
                    this.db.insertTask(childTasks, {
                        root_id: task.root_id || task.id,
                        parent_id: task.id
                    });

                    this.sleep.post();
                } else if (task.type === 'cron') {
                    // For cron tasks, set to pending with next scheduled time
                    this.db.updateTaskStatus(task.id, 'pending', {
                        result,
                        next_run_time: this._getNextRunTime(task.cron_expr)
                    });
                } else {
                    // For async tasks, mark as completed
                    this.db.updateTaskStatus(task.id, 'completed', {
                        result,
                        parent_id: task.parent_id
                    });
                }
            } catch (error) {
                console.error('Error executing task:', error);
                // Set appropriate failure status and store error
                const status = error.message.includes('timeout') ? 'timeout' : 'failed';
                this.db.updateTaskStatus(task.id, status, { error: error.message || String(error) });
            } finally {
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
        // Validate state transitions
        if (this.state === 'running') {
            return;
        }
        if (this.state === 'stopped') {
            throw new Error('Cannot restart a stopped TaskManager');
        }

        this.state = 'running';
        this.runningTasks = new Set();

        // Start periodic task activity monitoring
        this.activeTimer = setInterval(() => {
            if (this.runningTasks.size > 0) {
                this.db.updateTaskActiveTime(Array.from(this.runningTasks));
            }
            this.db.handleTimeoutTasks();
        }, this.options.active_update_interval);

        // Start main task processing loop
        coroutine.start(() => {
            while (this.state === 'running') {
                // Ensure we don't exceed max concurrent tasks
                this.semaphore.acquire();
                if (this.state !== 'running')
                    break;

                this.event.wait();

                // Try to claim an available task
                const task = this.db.claimTask(Array.from(this.handlers.keys()));
                if (!task) {
                    // No tasks available, wait before trying again
                    this.semaphore.release();
                    this.sleep.wait(this.options.poll_interval);
                    continue;
                }

                this._executeTask(task);
            }
        });
    }

    /**
     * Pause task processing without stopping the TaskManager
     * Tasks in progress will complete, but new tasks won't be started
     */
    pause() {
        // Only pause if currently running
        if (this.state !== 'running') {
            return;
        }
        this.event.clear();
    }

    /**
     * Resume task processing after a pause
     */
    resume() {
        // Only resume if currently running
        if (this.state !== 'running') {
            return;
        }
        this.event.set();
    }

    /**
     * Stop the TaskManager and cleanup resources
     * Waits for running tasks to complete and closes database connections
     */
    stop() {
        // Only stop if currently running
        if (this.state !== 'running') {
            return;
        }
        this.state = 'stopped';

        // Wait for in-progress tasks to complete
        while (this.runningTasks.size > 0) {
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
    }

    /**
     * Retrieve a specific task by ID
     * @param {string} taskId - ID of the task to retrieve
     * @returns {Object} Task object
     */
    getTask(taskId) {
        return this.db.getTask(taskId);
    }

    /**
     * Retrieve all tasks with a specific name
     * @param {string} name - Task name to search for
     * @returns {Array} Array of matching tasks
     */
    getTasksByName(name) {
        return this.db.getTasksByName(name);
    }

    /**
     * Retrieve all tasks with a specific status
     * @param {string} status - Status to filter by
     * @returns {Array} Array of matching tasks
     */
    getTasksByStatus(status) {
        return this.db.getTasksByStatus(status);
    }

    /**
     * Get all child tasks for a given task
     * @param {string|number} taskId - ID of the parent task
     * @returns {Array} Array of child tasks
     */
    getChildTasks(taskId) {
        return this.db.getChildTasks(taskId);
    }
}

module.exports = TaskManager;
