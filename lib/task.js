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

function getAutoRetentionIntervalMs() {
    return 5 * 60 * 1000;
}

function normalizeRetentionOptions(retention, fallbackExpireTime) {
    const normalizedRetention = retention && typeof retention === 'object'
        ? { ...retention }
        : {};

    if (!Object.prototype.hasOwnProperty.call(normalizedRetention, 'expire_time')) {
        normalizedRetention.expire_time = fallbackExpireTime;
    }

    if (!Object.prototype.hasOwnProperty.call(normalizedRetention, 'statuses')) {
        normalizedRetention.statuses = ['completed', 'permanently_failed'];
    }

    return normalizedRetention;
}

const AUDIT_NAME_PATTERN = /^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$/;

function normalizeTrimmedString(value, fieldName) {
    if (typeof value !== 'string') {
        throw new Error(`${fieldName} must be a string`);
    }

    const trimmed = value.trim();
    if (!trimmed) {
        throw new Error(`${fieldName} must not be empty`);
    }

    return trimmed;
}

function normalizeAuditName(value, fieldName) {
    const normalized = normalizeTrimmedString(value, fieldName);
    if (!AUDIT_NAME_PATTERN.test(normalized)) {
        throw new Error(`${fieldName} must use lowercase snake_case`);
    }

    return normalized;
}

function normalizeMetadata(metadata, fieldName) {
    if (metadata === undefined) {
        return undefined;
    }

    if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
        throw new Error(`${fieldName} must be an object`);
    }

    return metadata;
}

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
            throw new Error('Context must be a Buffer or Uint8Array');
        }

        if (!Array.isArray(tasks)) {
            tasks = [tasks];
        }
        this.tasks = tasks;
        this.context = context;
    }
}

function cloneTaskOptions(options = {}) {
    return {
        ...options
    };
}

function assertNoSchemaOptions(taskName, handlerConfig) {
    if (Object.prototype.hasOwnProperty.call(handlerConfig, 'parameters') ||
        Object.prototype.hasOwnProperty.call(handlerConfig, 'output')) {
        throw new Error(`Handler schema is no longer supported for task: ${taskName}`);
    }
}

function createExecutionEntry({ handler, options }) {
    return {
        handler,
        options: cloneTaskOptions(options)
    };
}

function createHandlerEntry(config, runtime) {
    return {
        ...createExecutionEntry(config),
        runtime
    };
}

function snapshotRegistry(handlers) {
    const registry = new Map();

    for (const [taskName, entry] of handlers.entries()) {
        registry.set(taskName, createExecutionEntry(entry));
    }

    return registry;
}

/**
 * TaskManager class handles task scheduling, execution, and lifecycle management
 */
class TaskManager {
    /**
     * Initialize a new TaskManager instance
     * @param {Object} options - Configuration options
    * @param {Object} options.dbConnection - Required database connection configuration
    *                                        Supports: 'sqlite:path', 'mysql://...', 'psql://...'
    * @param {string} options.dbType - Type of database to use (sqlite, mysql, psql)
     * @param {number} options.poll_interval - Interval (ms) between task polling (default: 1000)
     * @param {number} options.max_retries - Maximum total attempts for tasks (including initial attempt, default: 3)
     * @param {number} options.retry_interval - Interval between retries
     * @param {number} options.timeout - Task timeout in seconds
     * @param {string} options.worker_id - Unique identifier for this worker (auto-generated if not provided)
     * @param {number} options.max_concurrent_tasks - Maximum number of tasks to run simultaneously (default: 10)
    * @param {number} options.task_heartbeat_interval - Interval (ms) for updating active task status (default: 5000)
    * @param {number} options.task_heartbeat_timeout - Timeout window (ms) before a running task is considered to have lost heartbeat (default: 30000)
    * @param {string} options.pod_id - Stable logical node identifier for worker recovery
    * @param {number} options.worker_heartbeat_interval - Interval (ms) for worker liveness updates
    * @param {number} options.worker_heartbeat_timeout - Worker liveness timeout window in milliseconds
    * @param {boolean} options.recover_running_jobs - Recover orphaned running jobs during startup and peer scans
    * @param {number} options.expire_time - Backward-compatible shortcut for retention expire time
    * @param {object} options.retention - Explicit retention policy
     */
    constructor(options) {
        if (!options || !Object.prototype.hasOwnProperty.call(options, 'dbConnection') || options.dbConnection == null) {
            throw new Error('TaskManager requires an explicit dbConnection');
        }

        const dbConnection = options.dbConnection;

        // Determine database type from connection string if not explicitly provided
        let dbType = options.dbType;
        if (!dbType) {
            if (typeof dbConnection === 'string') {
                dbType = dbConnection.split(':')[0];
            } else {
                dbType = 'sqlite';
            }
        }

        const finalOptions = {
            ...options,
            dbConnection,
            dbType
        };

        logger.info(`[TaskManager] Initializing with options:`, finalOptions);

        // Create database adapter using the determined connection
        this.db = createAdapter(finalOptions.dbConnection, finalOptions.dbType);

        // Set default expire_time based on database type
        // In-memory SQLite uses shorter expiry time to avoid memory buildup
        const defaultExpireTime = dbConnection === 'sqlite::memory:'
            ? 1 * 3600        // 1 hour for memory database
            : 1 * 24 * 3600;  // 1 day for persistent databases

        const normalizedRetention = normalizeRetentionOptions(
            options.retention,
            Object.prototype.hasOwnProperty.call(options, 'expire_time')
                ? options.expire_time
                : defaultExpireTime
        );

        // Set default options with fallback values to ensure robust configuration
        this.options = {
            poll_interval: 1000,
            max_retries: 3,
            retry_interval: 0,
            timeout: 60,
            // Generate unique worker ID to distinguish between different task manager instances
            worker_id: `worker-${Math.random().toString(36).slice(2)}`,
            pod_id: null,
            max_concurrent_tasks: 10,
            task_heartbeat_interval: 5000,
            task_heartbeat_timeout: 30000,
            worker_heartbeat_interval: 5000,
            worker_heartbeat_timeout: 30000,
            recover_running_jobs: true,
            ...options,
            expire_time: normalizedRetention.expire_time,
            retention: normalizedRetention
        };

        logger.info(`[TaskManager] Configuration complete:`, this.options);

        // Initialize internal state tracking for task management
        this.handlers = new Map();
        this.state = 'init';  // Initial state before starting task processing
        this.currentFiber = null;
        this.runningTasks = new Set();
        this.workerActiveState = true;

        // Set up concurrency control mechanisms to manage task execution
        this.semaphore = new coroutine.Semaphore(this.options.max_concurrent_tasks);
        this.sleep = new coroutine.Semaphore(0);
        this.event = new coroutine.Event(true);
        this.activeTimer = null;
        this.workerTimer = null;
        this.nextRetentionSweepAt = null;
    }

    /**
    * Register task handlers for specific task types.
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
            assertNoSchemaOptions(taskName, handler);
            taskHandler = handler.handler;
            // Extract supported options, use TaskManager options as defaults
            const {
                max_retries = this.options.max_retries,
                retry_interval = this.options.retry_interval,
                timeout = this.options.timeout,
                priority,
                max_concurrent_tasks, // New option for task-level concurrency
                description // Task description
            } = handler;
            taskOptions = {
                max_retries,
                retry_interval,
                timeout,
                priority,
                max_concurrent_tasks,
                description
            };
        } else {
            throw new Error('Handler must be a function or an object with handler property');
        }

        const existingEntry = this.handlers.get(taskName);
        const runtime = existingEntry?.runtime || { running_count: 0 };

        // Store both handler and options
        this.handlers.set(taskName, createHandlerEntry({
            handler: taskHandler,
            options: taskOptions
        }, runtime));

        logger.info(`[TaskManager] Handler registered successfully for: ${taskName}`);

        this.event.set();
        this.sleep.post();
    }

    /**
     * Unregister task handlers so they are no longer eligible for future claims.
     * Running tasks complete with the execution snapshot captured when they started.
     * @param {string|string[]} taskName - Task type identifier or list of identifiers
     * @returns {number} Number of handlers removed
     */
    unuse(taskName) {
        if (Array.isArray(taskName)) {
            logger.info(`[TaskManager] Unregistering multiple task handlers`);
            return taskName.reduce((count, name) => count + this.unuse(name), 0);
        }

        logger.info(`[TaskManager] Unregistering handler for task type: ${taskName}`);
        const deleted = this.handlers.delete(taskName);

        if (deleted) {
            this.event.set();
        }

        return deleted ? 1 : 0;
    }

    /**
     * Submit an asynchronous task for execution
     * @param {string} taskName - Name of the task type to execute
     * @param {Object} payload - Task data/parameters
     * @param {Object} options - Task execution options
     * @param {number} options.delay - Delay in seconds before task execution
     * @param {number} options.priority - Task priority level
     * @param {number} options.max_retries - Maximum total attempts for this task (including initial attempt)
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
     * @param {number} options.max_retries - Maximum total attempts for this task (including initial attempt)
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
     * Run explicit retention cleanup for expired terminal tasks and their audit records.
     * @param {Object} [policy] - Optional retention policy override
     * @returns {{tasks_deleted:number, events_deleted:number, attempts_deleted:number}}
     */
    runRetention(policy = null) {
        const effectivePolicy = policy && typeof policy === 'object'
            ? { ...this.options.retention, ...policy }
            : (policy || this.options.retention);
        logger.info(`[TaskManager] Running retention cleanup with policy:`, effectivePolicy);
        const cleanupResult = this.db.cleanupExpiredTasks(effectivePolicy);
        this._scheduleNextAutoRetention(Date.now());
        return cleanupResult;
    }

    _scheduleNextAutoRetention(nowMs = Date.now()) {
        const retention = this.options.retention;
        if (!retention || !retention.expire_time) {
            this.nextRetentionSweepAt = null;
            return;
        }

        this.nextRetentionSweepAt = nowMs + getAutoRetentionIntervalMs();
    }

    _consumeAutoRetentionPolicy(nowMs = Date.now()) {
        const retention = this.options.retention;
        if (!retention || !retention.expire_time) {
            return null;
        }

        if (this.nextRetentionSweepAt === null) {
            this.nextRetentionSweepAt = nowMs + getAutoRetentionIntervalMs();
        }

        if (nowMs < this.nextRetentionSweepAt) {
            return null;
        }

        this._scheduleNextAutoRetention(nowMs);
        return retention;
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

    _normalizeTaskCheckpoint(checkpoint, details = {}) {
        let normalizedCheckpoint;
        if (typeof checkpoint === 'string') {
            normalizedCheckpoint = {
                code: checkpoint,
                ...details
            };
        } else if (checkpoint && typeof checkpoint === 'object' && !Array.isArray(checkpoint)) {
            normalizedCheckpoint = checkpoint;
        } else {
            throw new Error('Task checkpoint must be a code string or checkpoint object');
        }

        return {
            ...normalizedCheckpoint,
            code: normalizeAuditName(normalizedCheckpoint.code, 'Checkpoint code'),
            ...(Object.prototype.hasOwnProperty.call(normalizedCheckpoint, 'message')
                ? { message: normalizeTrimmedString(normalizedCheckpoint.message, 'Checkpoint message') }
                : {}),
            ...(Object.prototype.hasOwnProperty.call(normalizedCheckpoint, 'metadata')
                ? { metadata: normalizeMetadata(normalizedCheckpoint.metadata, 'Checkpoint metadata') }
                : {})
        };
    }

    _normalizeTaskProgress(progress, details = {}) {
        let normalizedProgress;
        if (typeof progress === 'string') {
            normalizedProgress = {
                progress_text: progress,
                ...details
            };
        } else if (progress && typeof progress === 'object' && !Array.isArray(progress)) {
            normalizedProgress = progress;
        } else {
            throw new Error('Task progress must be a text string or progress object');
        }

        return {
            ...normalizedProgress,
            ...(Object.prototype.hasOwnProperty.call(normalizedProgress, 'stage_name')
                ? { stage_name: normalizeAuditName(normalizedProgress.stage_name, 'Progress stage_name') }
                : {}),
            ...(Object.prototype.hasOwnProperty.call(normalizedProgress, 'progress_text')
                ? { progress_text: normalizeTrimmedString(normalizedProgress.progress_text, 'Progress text') }
                : {}),
            ...(Object.prototype.hasOwnProperty.call(normalizedProgress, 'message')
                ? { message: normalizeTrimmedString(normalizedProgress.message, 'Progress message') }
                : {}),
            ...(Object.prototype.hasOwnProperty.call(normalizedProgress, 'metadata')
                ? { metadata: normalizeMetadata(normalizedProgress.metadata, 'Progress metadata') }
                : {})
        };
    }

    _decorateExecutionTask(task) {
        task.audit = (checkpoint, details = {}) => {
            const normalizedCheckpoint = this._normalizeTaskCheckpoint(checkpoint, details);
            return this.db.recordTaskCheckpoint(task.id, {
                ...normalizedCheckpoint,
                worker_id: normalizedCheckpoint.worker_id || task.worker_id || null,
                stage: normalizedCheckpoint.stage !== undefined ? normalizedCheckpoint.stage : task.stage
            }, task.worker_id || null);
        };

        task.progress = (progress, details = {}) => {
            const normalizedProgress = this._normalizeTaskProgress(progress, details);
            const eventId = this.db.recordTaskProgress(task.id, {
                ...normalizedProgress,
                worker_id: normalizedProgress.worker_id || task.worker_id || null,
                stage: normalizedProgress.stage !== undefined ? normalizedProgress.stage : task.stage
            }, task.worker_id || null);

            if (Object.prototype.hasOwnProperty.call(normalizedProgress, 'stage_name')) {
                task.current_stage_name = normalizedProgress.stage_name;
            }
            if (Object.prototype.hasOwnProperty.call(normalizedProgress, 'progress_text')) {
                task.progress_text = normalizedProgress.progress_text;
            }
            if (Object.prototype.hasOwnProperty.call(normalizedProgress, 'progress_percent')) {
                task.progress_percent = normalizedProgress.progress_percent;
            }

            return eventId;
        };

        return task;
    }

    _updateTaskStatusIfOwned(task, status, extra = {}) {
        try {
            this.db.updateTaskStatus(task.id, status, {
                ...extra,
                worker_id: task.worker_id || null
            });
            return true;
        } catch (error) {
            const currentTask = this.db.getTask(task.id);
            if (currentTask && (currentTask.status !== 'running' || currentTask.worker_id !== task.worker_id)) {
                logger.warning(
                    `[TaskManager] Skipping stale ${status} update for task ${task.id}, current status: ${currentTask.status}, current worker: ${currentTask.worker_id}`
                );
                return false;
            }

            throw error;
        }
    }

    _registerWorkerLifecycle() {
        if (!this.options.pod_id) {
            return;
        }

        const now = Math.floor(Date.now() / 1000);
        const workerHeartbeatTimeoutSeconds = Math.max(1, Math.ceil(this.options.worker_heartbeat_timeout / 1000));

        this.db.registerWorker({
            worker_id: this.options.worker_id,
            pod_id: this.options.pod_id,
            now,
            ttl: workerHeartbeatTimeoutSeconds,
            meta: {
                pid: process.pid,
                started_at: now
            }
        });

        const supersededWorkerIds = this.db.supersedeOlderWorkers(
            this.options.pod_id,
            this.options.worker_id,
            now
        );
        const expiredWorkerIds = this.db.reapExpiredWorkers(now);
        const workerIdsToRecover = Array.from(new Set([
            ...supersededWorkerIds,
            ...expiredWorkerIds
        ]));

        if (this.options.recover_running_jobs) {
            this.db.recoverTasksForWorkers(workerIdsToRecover, {
                worker_id: this.options.worker_id,
                pod_id: this.options.pod_id,
                now
            });
        }
    }

    _startWorkerHeartbeat() {
        if (!this.options.pod_id) {
            return;
        }

        const interval = this.options.worker_heartbeat_interval || this.options.task_heartbeat_interval;
        this.workerTimer = setInterval(() => {
            try {
                const dbAdapter = this.db;
                if (!dbAdapter || this.state !== 'running') {
                    return;
                }

                const now = Math.floor(Date.now() / 1000);
                const workerHeartbeatTimeoutSeconds = Math.max(1, Math.ceil(this.options.worker_heartbeat_timeout / 1000));
                dbAdapter.heartbeatWorker(this.options.worker_id, now, workerHeartbeatTimeoutSeconds);

                if (!this._ensureCurrentWorkerActive(now)) {
                    return;
                }

                this._reapExpiredWorkers(now);
            } catch (error) {
                logger.warning(`[TaskManager] Worker heartbeat tick failed for ${this.options.worker_id}: ${error.message}`);
            }
        }, interval);
    }

    _reapExpiredWorkers(now = Math.floor(Date.now() / 1000)) {
        const dbAdapter = this.db;
        if (!dbAdapter || !this.options.pod_id) {
            return [];
        }

        const expiredWorkerIds = dbAdapter.reapExpiredWorkers(now);
        if (this.options.recover_running_jobs && expiredWorkerIds.length > 0) {
            dbAdapter.recoverTasksForWorkers(expiredWorkerIds, {
                worker_id: this.options.worker_id,
                pod_id: this.options.pod_id,
                now
            });
            this.sleep.post();
        }

        return expiredWorkerIds;
    }

    _ensureCurrentWorkerActive(now = Math.floor(Date.now() / 1000)) {
        if (!this.options.pod_id) {
            return true;
        }

        const dbAdapter = this.db;
        if (!dbAdapter || this.state !== 'running') {
            return false;
        }

        this._reapExpiredWorkers(now);

        const worker = dbAdapter.getWorker(this.options.worker_id);
        if (worker && worker.status === 'active') {
            if (!this.workerActiveState) {
                this.workerActiveState = true;
                logger.info(`[TaskManager] Worker ${this.options.worker_id} recovered and is active again`);
                this.sleep.post();
            }
            return true;
        }

        const competingWorkers = dbAdapter.listWorkers({
            pod_id: this.options.pod_id,
            status: 'active'
        }).filter(entry => entry.worker_id !== this.options.worker_id);

        if (competingWorkers.length > 0) {
            if (this.workerActiveState) {
                logger.warning(
                    `[TaskManager] Worker ${this.options.worker_id} is fenced by active peers on pod ${this.options.pod_id}; ` +
                    `skipping task claims. Active workers: ${competingWorkers.map(entry => entry.worker_id).join(', ')}`
                );
            }
            this.workerActiveState = false;
            return false;
        }

        const workerHeartbeatTimeoutSeconds = Math.max(1, Math.ceil(this.options.worker_heartbeat_timeout / 1000));
        dbAdapter.registerWorker({
            worker_id: this.options.worker_id,
            pod_id: this.options.pod_id,
            now,
            ttl: workerHeartbeatTimeoutSeconds,
            meta: {
                pid: process.pid,
                started_at: now,
                recovered_at: now
            }
        });

        this.workerActiveState = true;
        logger.warning(`[TaskManager] Worker ${this.options.worker_id} was re-registered after losing its active lease`);
        this.sleep.post();
        return true;
    }

    _isCurrentWorkerActive() {
        if (!this.options.pod_id) {
            return true;
        }

        const isActive = this._ensureCurrentWorkerActive();

        if (isActive !== this.workerActiveState) {
            this.workerActiveState = isActive;
            if (!isActive) {
                logger.warning(
                    `[TaskManager] Worker ${this.options.worker_id} is fenced on pod ${this.options.pod_id}; skipping task claims`
                );
            }
        }

        return isActive;
    }

    /**
     * Execute a task with proper error handling and resource management
     * @private
     * @param {Object} task - Task object containing execution details
     */
    _executeTask(task) {
        logger.info(`[TaskManager] Starting execution of task: ${task.name}(${task.id})`);
        this._decorateExecutionTask(task);
        const handlerRegistry = snapshotRegistry(this.handlers);
        const executionEntry = handlerRegistry.get(task.name);
        if (!executionEntry) {
            throw new Error(`No handler registered for task: ${task.name}`);
        }

        // Track task in running set
        this.runningTasks.add(task.id);

        // Update task type concurrency count if limit exists
        const liveTaskConfig = this.handlers.get(task.name);
        const runtime = liveTaskConfig?.runtime || { running_count: 0 };
        if (executionEntry.options.max_concurrent_tasks) {
            runtime.running_count++;
        }

        const startTime = Date.now();
        logger.debug(`[TaskManager] Task execution started at: ${startTime}`);
        let hasTimedOut = false;

        // Add timeout check method to task
        task.checkTimeout = function () {
            if (this.timeout && (Date.now() - startTime) >= (this.timeout * 1000)) {
                hasTimedOut = true;
                logger.warning(`[TaskManager] Task ${task.id} exceeded timeout of ${this.timeout}s`);
                throw new Error('Task execution timeout');
            }
        };

        // Execute task in new fiber for isolation
        coroutine.start(async () => {
            try {
                // Execute registered handler for task type
                logger.debug(`[TaskManager] Executing handler for task ${task.id}`);
                const result = await executionEntry.handler(task, (tasks, context) => new SubTasks(tasks, context));
                if (this.state !== 'running') {
                    logger.warning(`[TaskManager] Task execution aborted - manager not running`);
                    return;
                }

                if (result instanceof SubTasks) {
                    logger.info(`[TaskManager] Task ${task.id} created subtasks`);

                    // Validate all subtasks
                    for (const childTask of result.tasks) {
                        // Validate if the subtask name has registered handler
                        if (!childTask.name) {
                            throw new Error('Child task missing required name property');
                        }

                        if (!this.handlers.has(childTask.name)) {
                            throw new Error(`No handler registered for child task: ${childTask.name}`);
                        }

                        // Get child task handler and default options
                        const registeredChildTask = this.handlers.get(childTask.name);
                    }

                    const childTasks = result.tasks.map(childTask => {
                        const registeredChildTask = this.handlers.get(childTask.name);

                        return {
                            ...childTask,
                            type: 'async',
                            priority: childTask.priority ?? registeredChildTask.options.priority ?? task.priority,
                            timeout: childTask.timeout ?? registeredChildTask.options.timeout ?? task.timeout,
                            max_retries: childTask.max_retries ?? registeredChildTask.options.max_retries ?? task.max_retries,
                            retry_interval: childTask.retry_interval ?? registeredChildTask.options.retry_interval ?? task.retry_interval
                        };
                    });

                    // If context is provided, update the parent task
                    const options = {
                        root_id: task.root_id || task.id,
                        parent_id: task.id,
                        worker_id: task.worker_id || null
                    };

                    if (result.context !== null && result.context !== undefined) {
                        options.context = result.context;
                    }

                    this.db.insertTask(childTasks, options);

                    logger.info(`[TaskManager] Created ${childTasks.length} child tasks for task ${task.id}`);
                } else if (task.type === 'cron') {
                    logger.debug(`[TaskManager] Updating cron task ${task.id} for next execution`);
                    // For cron tasks, set to pending with next scheduled time
                    this._updateTaskStatusIfOwned(task, 'pending', {
                        result,
                        next_run_time: this._getNextRunTime(task.cron_expr)
                    });
                } else {
                    logger.info(`[TaskManager] Completing async task ${task.id}`);
                    // For async tasks, mark as completed
                    this._updateTaskStatusIfOwned(task, 'completed', {
                        result,
                        parent_id: task.parent_id
                    });
                }

                this.sleep.post();
            } catch (error) {
                logger.error(`[TaskManager] Error executing task ${task.id}:`, error);
                // Set appropriate failure status and store stack trace which includes error message
                const status = hasTimedOut ? 'timeout' : 'failed';
                this._updateTaskStatusIfOwned(task, status, { error: error.stack || String(error) });
            } finally {
                logger.debug(`[TaskManager] Task ${task.id} execution cleanup`);

                // Update task type concurrency count if limit exists
                if (executionEntry.options.max_concurrent_tasks) {
                    runtime.running_count = Math.max(0, runtime.running_count - 1);
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

        return taskConfig.runtime.running_count < taskConfig.options.max_concurrent_tasks;
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
            throw new Error('Cannot restart a stopped TaskManager');
        }

        this.state = 'running';
        this.runningTasks = new Set();
        logger.info(`[TaskManager] State changed to running`);

        this._registerWorkerLifecycle();
        this.nextRetentionSweepAt = null;

        // Start periodic task activity monitoring
        this.activeTimer = setInterval(() => {
            try {
                if (this.runningTasks.size > 0) {
                    logger.debug(`[TaskManager] Updating active time for ${this.runningTasks.size} tasks`);
                    this.db.updateTaskActiveTime(Array.from(this.runningTasks), this.options.worker_id);
                }
                const retentionPolicy = this._consumeAutoRetentionPolicy(Date.now());
                this.db.handleTimeoutTasks({
                    task_heartbeat_interval: this.options.task_heartbeat_interval,
                    task_heartbeat_timeout: this.options.task_heartbeat_timeout
                }, retentionPolicy);
            } catch (error) {
                logger.warning(`[TaskManager] Active timer tick failed: ${error.message}`);
            }
        }, this.options.task_heartbeat_interval);

        this._startWorkerHeartbeat();

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

                if (this.state !== 'running') {
                    logger.warning(`[TaskManager] Processing loop stopped after wakeup - manager not running`);
                    this.semaphore.release();
                    break;
                }

                try {
                    if (!this._isCurrentWorkerActive()) {
                        this.semaphore.release();
                        this.sleep.wait(this.options.poll_interval);
                        continue;
                    }

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
                } catch (error) {
                    logger.warning(`[TaskManager] Task processing loop iteration failed: ${error.message}`);
                    this.semaphore.release();
                    this.sleep.wait(this.options.poll_interval);
                }
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
        this.event.set();
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

        if (this.workerTimer) {
            clearInterval(this.workerTimer);
            this.workerTimer = null;
        }

        if (this.db && this.options.pod_id) {
            this.db.markWorkerDead(this.options.worker_id, Math.floor(Date.now() / 1000));
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
     * Get audit events for a single task.
     * @param {string|number} taskId - Task id to query
     * @param {Object} [filters] - Optional event filters
     * @returns {Array<object>} Ordered task events
     */
    getTaskEvents(taskId, filters = {}) {
        logger.debug(`[TaskManager] Getting task events for: ${taskId}, filters:`, filters);
        return this.db.getTaskEvents(taskId, filters);
    }

    /**
     * Get audit events for a workflow rooted at the given task id.
     * @param {string|number} rootId - Root task id
     * @param {Object} [filters] - Optional event filters
     * @returns {Array<object>} Ordered workflow events
     */
    getWorkflowEvents(rootId, filters = {}) {
        logger.debug(`[TaskManager] Getting workflow events for: ${rootId}, filters:`, filters);
        return this.db.getWorkflowEvents(rootId, filters);
    }

    /**
     * Get attempt records for a single task.
     * @param {string|number} taskId - Task id to query
     * @returns {Array<object>} Ordered attempt records
     */
    getTaskAttempts(taskId) {
        logger.debug(`[TaskManager] Getting task attempts for: ${taskId}`);
        return this.db.getTaskAttempts(taskId);
    }

    /**
     * Query task events with pagination metadata.
     * @param {string|number} taskId - Task id to query
     * @param {Object} [filters] - Query filters and pagination options
     * @returns {{items:Array<object>, total:number, limit:number|null, offset:number, has_more:boolean}}
     */
    queryTaskEvents(taskId, filters = {}) {
        logger.debug(`[TaskManager] Querying task events for: ${taskId}, filters:`, filters);
        return this.db.queryTaskEvents(taskId, filters);
    }

    /**
     * Query workflow events with pagination metadata.
     * @param {string|number} rootId - Root task id
     * @param {Object} [filters] - Query filters and pagination options
     * @returns {{items:Array<object>, total:number, limit:number|null, offset:number, has_more:boolean}}
     */
    queryWorkflowEvents(rootId, filters = {}) {
        logger.debug(`[TaskManager] Querying workflow events for: ${rootId}, filters:`, filters);
        return this.db.queryWorkflowEvents(rootId, filters);
    }

    /**
     * Query task attempts with pagination metadata.
     * @param {string|number} taskId - Task id to query
     * @param {Object} [filters] - Query filters and pagination options
     * @returns {{items:Array<object>, total:number, limit:number|null, offset:number, has_more:boolean}}
     */
    queryTaskAttempts(taskId, filters = {}) {
        logger.debug(`[TaskManager] Querying task attempts for: ${taskId}, filters:`, filters);
        return this.db.queryTaskAttempts(taskId, filters);
    }

    /**
     * Query workflow attempts with pagination metadata.
     * @param {string|number} rootId - Root task id
     * @param {Object} [filters] - Query filters and pagination options
     * @returns {{items:Array<object>, total:number, limit:number|null, offset:number, has_more:boolean}}
     */
    queryWorkflowAttempts(rootId, filters = {}) {
        logger.debug(`[TaskManager] Querying workflow attempts for: ${rootId}, filters:`, filters);
        return this.db.queryWorkflowAttempts(rootId, filters);
    }

    /**
     * Query tasks with pagination metadata.
     * @param {Object} [filters] - Task filters and pagination options
     * @returns {{items:Array<object>, total:number, limit:number|null, offset:number, has_more:boolean}}
     */
    queryTasks(filters = {}) {
        logger.debug(`[TaskManager] Querying tasks with filters:`, filters);
        return this.db.queryTasks(filters);
    }

    /**
     * Get a structured audit view for a task.
     * @param {string|number} taskId - Task id to query
     * @param {Object} [options] - Event and attempt query options
     * @returns {{task:object|null, events:object, attempts:object}}
     */
    getTaskAudit(taskId, options = {}) {
        logger.debug(`[TaskManager] Getting task audit for: ${taskId}, options:`, options);
        return this.db.getTaskAudit(taskId, options);
    }

    /**
     * Get a structured audit view for a workflow.
     * @param {string|number} rootId - Root task id
     * @param {Object} [options] - Task and event query options
     * @returns {{root_task:object|null, tasks:object, events:object}}
     */
    getWorkflowAudit(rootId, options = {}) {
        logger.debug(`[TaskManager] Getting workflow audit for: ${rootId}, options:`, options);
        return this.db.getWorkflowAudit(rootId, options);
    }

    /**
     * Get a platform-oriented aggregate audit summary for a workflow.
     * @param {string|number} rootId - Root task id
     * @returns {object} Workflow aggregate summary
     */
    getWorkflowAuditSummary(rootId) {
        logger.debug(`[TaskManager] Getting workflow audit summary for: ${rootId}`);
        return this.db.getWorkflowAuditSummary(rootId);
    }

    /**
     * Get task definition information
     * @param {string} taskName - Name of the task to get information for
        * @returns {Object} Task definition including supported handler metadata
     * @throws {Error} If the task is not registered
     */
    getTaskInfo(taskName) {
        logger.debug(`[TaskManager] Getting task info for: ${taskName}`);

        if (!this.handlers.has(taskName)) {
            throw new Error(`No task registered with name: ${taskName}`);
        }

        const taskConfig = this.handlers.get(taskName);
        return {
            name: taskName,
            description: taskConfig.options.description || ''
        };
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
