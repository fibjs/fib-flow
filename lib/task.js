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
 * Execution modes for cron schedules.
 *
 * - `inline` (default, 8.0.x semantics): the cron row itself executes the
 *   handler; the row is reset after each round. A suspending handler freezes the
 *   whole schedule because the row stops being `pending`.
 * - `spawn`: the cron row never runs business code. Each round inserts an
 *   independent `async` run row and advances the schedule in the same
 *   transaction, so one run == one row for suspension, retry, audit and
 *   retention.
 */
const CRON_MODES = ['inline', 'spawn'];

/**
 * Validate the cron execution mode.
 * @param {string} cronMode - Requested mode
 * @returns {'inline'|'spawn'} The validated mode
 * @throws {Error} If the mode is unknown
 */
function assertCronMode(cronMode) {
    if (!CRON_MODES.includes(cronMode)) {
        throw new Error(`Invalid cronMode: ${cronMode} (expected "inline" or "spawn")`);
    }
    return cronMode;
}

/**
 * Default cap on concurrently in-flight runs of one spawn-mode schedule.
 *
 * 1 reproduces the mutual exclusion inline mode had for free (its cron row was
 * `running` and therefore not claimable), so switching to spawn does not quietly
 * start stacking overlapping runs. Callers that want the old inline cadence with
 * overlap must opt in explicitly.
 */
const DEFAULT_MAX_INSTANCES = 1;

/**
 * Task statuses that still have work ahead of them.
 *
 * Mirrors the runtime status list minus the terminal ones. Used as the default
 * selector for `cancelTasks()`: what an operator means by "clean up" is "everything
 * that has not finished", never "including the rows that already completed".
 */
const NON_TERMINAL_TASK_STATUSES = [
    'pending',
    'running',
    'failed',
    'timeout',
    'paused',
    'suspended'
];

/**
 * Validate a task status for the batch APIs.
 * @param {string} status - Candidate status
 * @returns {string} The validated status
 * @throws {Error} If the value is not a known status
 */
function assertTaskStatusForFilter(status) {
    if (!NON_TERMINAL_TASK_STATUSES.includes(status) &&
        !['completed', 'permanently_failed', 'cancelled'].includes(status)) {
        throw new Error(`Invalid status value: ${status}`);
    }
    return status;
}

/**
 * Stable identity for one schedule, used by the in-memory `maxInstances` map.
 * @param {string} name - Schedule name
 * @param {string|null} [cronKey] - Schedule identity key
 * @returns {string} Map key
 */
function scheduleIdentity(name, cronKey) {
    return `${name}\u0000${cronKey || 'default'}`;
}

/**
 * `TaskManager` constructor options whose documented names are snake_case.
 *
 * Anything listed here accepts its camelCase spelling too (`poll_interval` /
 * `pollInterval`). Names that are already camelCase in the documented API belong
 * in {@link TASK_MANAGER_CAMEL_OPTIONS} instead — keeping them apart means a new
 * option has an obvious list to join, and neither list can silently stop working
 * because a name happened to have no underscore.
 */
const TASK_MANAGER_SNAKE_OPTIONS = [
    'db_type',
    'poll_interval',
    'max_retries',
    'retry_interval',
    'timeout',
    'worker_id',
    'pod_id',
    'max_concurrent_tasks',
    'task_heartbeat_interval',
    'task_heartbeat_timeout',
    'worker_heartbeat_interval',
    'worker_heartbeat_timeout',
    'recover_running_jobs',
    'expire_time',
    'retention'
];

/**
 * `TaskManager` constructor options that are already camelCase in the documented
 * API, so they need no alias. Listed for completeness (and for the option-alias
 * test to assert the full set is covered).
 */
const TASK_MANAGER_CAMEL_OPTIONS = [
    'dbConnection',
    'dbType',
    'cronMode'
];

/**
 * `cron()` options whose documented names are snake_case.
 *
 * Same rule as the constructor: the camelCase spelling is accepted as an alias.
 * `cron_key` matters most — an ignored key silently falls back to `'default'`, so
 * a schedule registered as `{ cron_key: 'eu' }` becomes a *second* row identity
 * and fires in addition to the intended one.
 */
const CRON_SNAKE_OPTIONS = [
    'cron_key',
    'retention_expire_time',
    'max_retries',
    'retry_interval'
];

/**
 * `cron()` options that are already camelCase in the documented API.
 */
const CRON_CAMEL_OPTIONS = [
    'cronKey',
    'timezone',
    'catchUp',
    'maxInstances',
    'priority',
    'timeout',
    'tag'
];

/**
 * Turn `max_retries` into `maxRetries` / `poll_interval` into `pollInterval`.
 * @param {string} snakeCase - snake_case option name
 * @returns {string} camelCase form
 */
function toCamelCase(snakeCase) {
    return snakeCase.replace(/_([a-z])/g, (_match, letter) => letter.toUpperCase());
}

/**
 * Turn `maxRetries` into `max_retries` / `pollInterval` into `poll_interval`.
 * @param {string} camelCase - camelCase option name
 * @returns {string} snake_case form
 */
function toSnakeCase(camelCase) {
    return camelCase.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
}

/**
 * Copy alternate spellings of known options onto their documented names.
 *
 * JavaScript callers reach for the other convention reflexively, and an
 * unrecognised key is silently retained while the documented default stays in
 * force — the setting looks applied but is not. For `cron_key` that is worse than
 * an ineffective setting: the key falls back to `'default'`, so the same schedule
 * is registered under a second identity and fires twice.
 *
 * The documented spelling always wins when both are supplied, so an explicit
 * correct name is never overridden by a typo of the same name.
 *
 * @param {object} options - Raw options object (mutated in place)
 * @param {Array<string>} snakeNames - Documented snake_case names
 * @param {Array<string>} camelNames - Documented camelCase names
 * @returns {Array<{from:string, to:string}>} Aliases that were applied
 */
function applyOptionAliases(options, snakeNames, camelNames) {
    const applied = [];
    if (!options || typeof options !== 'object') {
        return applied;
    }

    const has = (key) => Object.prototype.hasOwnProperty.call(options, key);

    // snake_case documented -> accept camelCase
    for (const documented of snakeNames) {
        const alternate = toCamelCase(documented);
        if (alternate === documented || !has(alternate) || has(documented)) {
            continue;
        }
        options[documented] = options[alternate];
        applied.push({ from: alternate, to: documented });
    }

    // camelCase documented -> accept snake_case
    for (const documented of camelNames) {
        const alternate = toSnakeCase(documented);
        if (alternate === documented || !has(alternate) || has(documented)) {
            continue;
        }
        options[documented] = options[alternate];
        applied.push({ from: alternate, to: documented });
    }

    return applied;
}

/**
 * Log any option aliases that were applied, so the misspelling stays visible.
 * @param {Array<{from:string, to:string}>} applied - Aliases from `applyOptionAliases`
 * @param {string} context - Where the options came from, e.g. 'TaskManager options'
 */
function warnAboutOptionAliases(applied, context) {
    if (!applied || applied.length === 0) {
        return;
    }

    logger.warning(
        `[TaskManager] ${context} supplied with an alternate spelling were applied under ` +
        `their documented names: ${applied.map(alias => `"${alias.from}" -> "${alias.to}"`).join(', ')}. ` +
        `Please use the documented spelling.`
    );
}

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
        normalizedRetention.statuses = ['completed', 'permanently_failed', 'cancelled'];
    }

    return normalizedRetention;
}

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
    return normalizeTrimmedString(value, fieldName);
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
 * Validate an IANA time zone name.
 * cron-parser surfaces invalid zones as an opaque CronDate error, so validation
 * happens here to produce an actionable message at registration time.
 * @param {string} timeZone - IANA time zone name
 * @returns {string} The validated time zone
 */
function normalizeTimeZone(timeZone) {
    const trimmed = normalizeTrimmedString(timeZone, 'timezone');

    try {
        new Intl.DateTimeFormat('en-US', { timeZone: trimmed });
    } catch (_error) {
        throw new Error(`Invalid timezone: ${trimmed} (must be an IANA name such as "Asia/Shanghai")`);
    }

    return trimmed;
}

/**
 * Container class for suspending a task. Returning this from a task handler
 * transitions the task to 'suspended' status. The task can later be resumed
 * via TaskManager#resumeTask(taskId); the handler then re-runs with an
 * advanced stage and can detect the resumed run via task.stage.
 */
class Suspension {
    /**
     * Create a new Suspension instance
     * @param {object} options - Suspension options
     * @param {string} options.reason - Required suspend reason, e.g. 'awaiting_approval'
     * @param {object} [options.metadata] - Optional metadata describing the suspension
     * @param {Buffer|null} [options.context] - Binary snapshot data persisted to the task's context column
     */
    constructor({ reason, metadata, context } = {}) {
        if (!reason || typeof reason !== 'string' || !reason.trim()) {
            throw new Error('Suspension requires a non-empty reason');
        }
        if (context && !(context instanceof Uint8Array)) {
            throw new Error('Context must be a Buffer or Uint8Array');
        }
        this.reason = reason.trim();
        this.metadata = metadata;
        this.context = context || null;
    }
}

/**
 * A Suspension that also creates child tasks: the task suspends with reason
 * 'awaiting_subtasks' and is automatically resumed when all children complete.
 * Created via the `next(tasks, context)` handler parameter.
 */
class SubTasks extends Suspension {
    /**
     * Create a new SubTasks instance
     * @param {object|Array<object>} tasks - Single task or array of tasks to execute as subtasks
     * @param {Buffer|null} context - Binary context data to be stored with parent task
     */
    constructor(tasks, context = null) {
        if (!Array.isArray(tasks)) {
            tasks = [tasks];
        }
        super({ reason: 'awaiting_subtasks', context });
        this.tasks = tasks;
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

        // Catch alternate spellings of known options.
        //
        // Silently ignoring one is the worst outcome: the value is copied into
        // `options` and never read, so the manager runs with the default while the
        // caller believes the setting took effect. That is a misdiagnosis trap for
        // exactly the symptoms that are hardest to attribute ("the schedule is late"
        // / "it skips rounds").
        //
        // The alias is applied AND logged: the intent is unambiguous, so refusing to
        // honour it would break working callers for no benefit, while staying silent
        // would leave the mistake invisible in the configuration.
        warnAboutOptionAliases(
            applyOptionAliases(options, TASK_MANAGER_SNAKE_OPTIONS, TASK_MANAGER_CAMEL_OPTIONS),
            'TaskManager options'
        );

        // Validate configuration that can fail BEFORE the adapter opens a
        // connection pool: an invalid option must not leak pooled connections.
        const cronMode = assertCronMode(
            options.cronMode === undefined || options.cronMode === null ? 'inline' : options.cronMode
        );

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
            cronMode: 'inline',
            ...options,
            expire_time: normalizedRetention.expire_time,
            retention: normalizedRetention
        };

        // Normalized above, before the adapter was created.
        this.options.cronMode = cronMode;

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
        // Cached schedule-name list for spawn-mode claiming (see
        // _getSpawnEligibleScheduleNames).
        this._scheduleNameCache = null;
        // Per-schedule `maxInstances` (see _getScheduleMaxInstances). In memory
        // because the task table is schema-version gated.
        this.scheduleMaxInstances = new Map();
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
            this._invalidateScheduleNameCache();
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
     * @param {number} [options.retention_expire_time] - Per-task retention window in seconds
     * @throws {Error} If TaskManager is not running or handler not registered
     * @returns {number} Task id
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
        const taskId = this.db.insertTask({
            name: taskName,
            type: 'async',
            payload,
            priority: defaultOptions.priority,
            tag: defaultOptions.tag,
            max_retries: defaultOptions.max_retries,
            retry_interval: defaultOptions.retry_interval,
            timeout: defaultOptions.timeout,
            retention_expire_time: options.retention_expire_time,
            next_run_time: nextRunTime
        });

        logger.info(`[TaskManager] Async task created successfully: ${taskId}`);

        // Signal that a new task is available for processing
        this.sleep.post();
        return taskId;
    }

    /**
     * Schedule a recurring task using cron expression.
     *
     * Registration is idempotent: the schedule identity is `(taskName, cronKey)`
     * backed by a unique index, so calling `cron()` repeatedly (process restart,
     * multi-instance bootstrap, config reload) converges on a single row instead
     * of creating duplicate triggers.
     *
     * @param {string} taskName - Name of the task type to execute
     * @param {string} cron_expr - Cron expression for scheduling
     * @param {Object} payload - Task data/parameters
     * @param {Object} options - Task execution options
     * @param {string} [options.cronKey='default'] - Schedule identity key
     * @param {string} [options.timezone] - IANA time zone used to evaluate the expression
     * @param {string} [options.catchUp='once'] - 'once' runs a missed window immediately, 'skip' advances past it
     * @param {number} [options.retention_expire_time] - Per-schedule retention window in seconds
     * @param {number} options.priority - Task priority level
     * @param {number} options.max_retries - Maximum total attempts this task (including initial attempt)
     * @param {number} options.retry_interval - Interval between retries
     * @param {number} options.timeout - Task timeout in seconds
     * @param {string} options.tag - Task tag for categorization
     * @throws {Error} If the TaskManager is stopped, the handler is missing, the
     *                 expression is invalid, or the timezone is not an IANA name
     * @returns {number} Task id (same contract as async())
     */
    cron(taskName, cron_expr, payload = {}, options = {}) {
        logger.info(`[TaskManager] Scheduling cron task: ${taskName}, expression: ${cron_expr}`);

        if (this.state === 'stopped') {
            throw new Error('Cannot schedule cron on a stopped TaskManager');
        }

        // Validate task can be scheduled in current task manager state
        if (!this.handlers.has(taskName)) {
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // Normalise alternate spellings before anything is read. `cron_key` is the
        // one that matters most: an ignored key falls back to `'default'`, which
        // registers the same schedule under a second identity and fires it twice.
        warnAboutOptionAliases(
            applyOptionAliases(options, CRON_SNAKE_OPTIONS, CRON_CAMEL_OPTIONS),
            'cron() options'
        );

        const cronKey = options.cronKey === undefined ? 'default' : options.cronKey;
        if (typeof cronKey !== 'string' || cronKey.trim() === '') {
            throw new Error('cronKey must be a non-empty string');
        }

        const timezone = options.timezone === undefined || options.timezone === null
            ? null
            : normalizeTimeZone(options.timezone);
        const catchUp = options.catchUp === undefined ? 'once' : options.catchUp;
        if (catchUp !== 'once' && catchUp !== 'skip') {
            throw new Error(`Invalid catchUp value: ${catchUp} (expected "once" or "skip")`);
        }

        // Validate cron expression to prevent scheduling with invalid patterns
        try {
            parser.parseExpression(cron_expr, timezone ? { tz: timezone } : undefined);
        } catch (e) {
            logger.error(`[TaskManager] Invalid cron expression: ${cron_expr}, error: ${e.message}`);
            throw new Error(`Invalid cron expression: ${cron_expr}`);
        }

        // Only after every validation passed: a rejected registration must leave no
        // trace. Writing the policy earlier left an orphan `schedule:*` row behind
        // when the expression was invalid, and an orphan row makes
        // `_hasRetentionWork()` true, so the next ticks would run a full retention
        // pass for nothing until the sweep collected it.
        this._setScheduleMaxInstances(taskName, cronKey.trim(), options.maxInstances);

        // Get registered handler and its default options
        const registeredTask = this.handlers.get(taskName);
        const defaultOptions = {
            ...registeredTask.options,  // Task type specific defaults from registration
            ...options  // Task instance specific options
        };

        // Calculate next execution time based on cron schedule
        const nextRunTime = this._getNextRunTime(cron_expr, timezone);
        logger.info(`[TaskManager] Next run time calculated: ${nextRunTime}`);

        const schedule = this.db.upsertCronTask({
            name: taskName,
            cron_expr: cron_expr,
            cron_key: cronKey.trim(),
            cron_tz: timezone,
            cron_catch_up: catchUp,
            retention_expire_time: options.retention_expire_time,
            payload,
            priority: defaultOptions.priority,
            tag: defaultOptions.tag,
            max_retries: defaultOptions.max_retries,
            retry_interval: defaultOptions.retry_interval,
            timeout: defaultOptions.timeout,
            next_run_time: nextRunTime
        });

        // Same return contract as async(): the task id. The create/update/revive
        // outcome is recorded as an audit event on the row.
        logger.info(`[TaskManager] Cron task ${schedule.outcome}: ${schedule.id}`);

        // A new/updated/revived schedule changes what spawn mode may claim.
        this._invalidateScheduleNameCache();

        // Signal that a new task is available for processing
        this.sleep.post();
        return schedule.id;
    }

    /**
     * Resume a suspended or paused task
     * @param {string} taskId - ID of the task to resume
     * @param {object} [options] - Resume options
     * @param {string} [options.resume_reason] - Optional reason recorded in the audit event
     * @throws {Error} If TaskManager is not running
     * @returns {object|null} Updated task snapshot
     */
    resumeTask(taskId, options = {}) {
        logger.info(`[TaskManager] Attempting to resume task: ${taskId}`);

        const now = Math.floor(Date.now() / 1000);
        const extra = {
            retry_count: 0,
            next_run_time: now
        };

        if (options && typeof options === 'object' && options.resume_reason) {
            extra.resume_reason = options.resume_reason;
        }

        logger.info(`[TaskManager] Resuming task ${taskId} with next run time: ${now}`);

        // Wake the processing loop immediately so the resumed task is claimed without delay
        this.sleep.post();

        this.db.updateTaskStatus(taskId, 'pending', extra);
        return this.db.getTask(taskId);
    }

    /**
     * Cancel a task.
     *
     * Cancellation is a distinct lifecycle outcome, not a failure: cancelled rows
     * never propagate failure to a parent workflow and are not counted as failed
     * tasks in workflow summaries.
     *
     * Allowed source states: pending, paused, failed, timeout, suspended.
     * A running task is rejected — the in-flight handler owns the row until it
     * reports a terminal state (pause it, or wait for completion).
     *
     * @param {string|number} taskId - ID of the task to cancel
     * @param {object} [options] - Cancel options
     * @param {string} [options.reason] - Cancellation reason stored as the task error
     * @throws {Error} If the task does not exist or its state cannot be cancelled
     * @returns {object|null} Updated task snapshot
     */
    cancelTask(taskId, options = {}) {
        logger.info(`[TaskManager] Attempting to cancel task: ${taskId}`);

        const current = this.db.getTask(taskId);
        if (!current) {
            throw new Error(`Task ${taskId} not found`);
        }
        if (current.status === 'running') {
            throw new Error(`Cannot cancel running task ${taskId}; pause it or wait for completion (current status: running)`);
        }
        if (current.status === 'cancelled') {
            return current;
        }

        const reason = options && options.reason ? options.reason : 'Task cancelled';
        this.db.updateTaskStatus(taskId, 'cancelled', {
            error: reason,
            reason
        });
        return this.db.getTask(taskId);
    }

    /**
     * Cancel cron schedules by identity filter.
     *
     * @param {object} [filters] - Schedule selector; at least one of name / cronKey / tag
     * @param {object} [options] - Cancel options
     * @param {string} [options.reason] - Cancellation reason recorded per row
     * @returns {{cancelled:number, skipped:number, taskIds:Array<number>}}
     */
    cancelCron(filters = {}, options = {}) {
        logger.info(`[TaskManager] Cancelling cron schedules with filters:`, filters);

        const selector = [];
        const params = [];
        if (filters.name) {
            selector.push('name = ?');
            params.push(filters.name);
        }
        if (filters.cronKey) {
            selector.push('cron_key = ?');
            params.push(filters.cronKey);
        }
        if (filters.tag) {
            selector.push('tag = ?');
            params.push(filters.tag);
        }

        if (selector.length === 0) {
            throw new Error('cancelCron requires at least one filter (name, cronKey or tag)');
        }

        const rows = this.db.pool(conn => conn.execute(
            `SELECT id, status FROM fib_flow_tasks WHERE type = 'cron' AND ${selector.join(' AND ')}`,
            ...params
        ));

        const summary = { cancelled: 0, skipped: 0, taskIds: [] };
        for (const row of rows) {
            if (row.status === 'running') {
                summary.skipped += 1;
                logger.info(`[TaskManager] Skipping running cron task ${row.id}`);
                continue;
            }
            this.cancelTask(row.id, options);
            summary.cancelled += 1;
            summary.taskIds.push(row.id);
        }

        // Cancelled rows are no longer spawn-eligible.
        this._invalidateScheduleNameCache();
        return summary;
    }

    /**
     * Cancel every non-terminal task matching a filter.
     *
     * The batch counterpart of `cancelTask()`, and the shape `cancelCron()` already
     * has. Without it, a caller cleaning up "everything still in flight for this
     * handler" has to enumerate with `getTasks()` and cancel one by one, which has
     * two traps:
     *
     * - the enumeration and the cancellation are not atomic, so a row created in
     *   between is missed (for a spawn-mode schedule, the next tick creates one);
     * - `getTasks()` cannot express "any non-terminal status", so the caller has to
     *   fetch everything and filter in memory — or issue one query per status.
     *
     * `running` rows are skipped and reported, never cancelled: the in-flight
     * handler owns the row until it reports a terminal state (see `cancelTask`).
     * Terminal rows are also skipped, since cancelling them is a no-op.
     *
     * @param {object} filters - Task selector; at least one of name / tag / type / statuses
     * @param {object} [options] - Cancel options
     * @param {string} [options.reason] - Cancellation reason recorded per row
     * @returns {{cancelled:number, skipped:number, taskIds:Array<number>}} Summary
     * @throws {Error} If no selector is supplied, or a filter value is invalid
     */
    cancelTasks(filters = {}, options = {}) {
        logger.info(`[TaskManager] Cancelling tasks with filters:`, filters);

        const selector = [];
        const params = [];

        if (filters.name) {
            selector.push('name = ?');
            params.push(filters.name);
        }
        if (filters.tag) {
            selector.push('tag = ?');
            params.push(filters.tag);
        }
        if (filters.type) {
            if (filters.type !== 'async' && filters.type !== 'cron') {
                throw new Error('Invalid task type');
            }
            selector.push('type = ?');
            params.push(filters.type);
        }
        if (filters.cronKey) {
            selector.push('cron_key = ?');
            params.push(filters.cronKey);
        }

        const hasStatuses = filters.statuses !== undefined && filters.statuses !== null;
        if (selector.length === 0 && !hasStatuses) {
            throw new Error('cancelTasks requires at least one filter (name, tag, type, cronKey or statuses)');
        }

        // Default selector is "everything not finished yet": terminal rows are a
        // no-op to cancel, and `running` rows are skipped below regardless.
        const statusList = hasStatuses
            ? Array.from(new Set(filters.statuses.map(status => assertTaskStatusForFilter(status))))
            : NON_TERMINAL_TASK_STATUSES.slice();

        const where = selector.length > 0 ? `${selector.join(' AND ')} AND ` : '';
        const rows = this.db.pool(conn => conn.execute(
            `SELECT id, status FROM fib_flow_tasks WHERE ${where}status IN ? ORDER BY id ASC`,
            ...params,
            statusList
        ));

        const summary = { cancelled: 0, skipped: 0, taskIds: [] };
        for (const row of rows) {
            // The in-flight handler owns a running row until it reports a terminal
            // state; `cancelTask` rejects it, so skip and report instead of failing
            // the whole batch.
            if (row.status === 'running') {
                summary.skipped += 1;
                continue;
            }

            try {
                this.cancelTask(row.id, options);
                summary.cancelled += 1;
                summary.taskIds.push(row.id);
            } catch (error) {
                // A row that moved on between the read and the update is reported
                // rather than aborting the batch.
                summary.skipped += 1;
                logger.warning(`[TaskManager] Skipping task ${row.id} in cancelTasks: ${error.message}`);
            }
        }

        if (summary.cancelled > 0) {
            this._invalidateScheduleNameCache();
        }

        return summary;
    }

    /**
     * Pause a running task
     * @param {string|number} taskId - ID of the task to pause
     * @throws {Error} If the task does not exist or cannot transition to paused
     * @returns {object|null} Updated task snapshot
     */
    pauseTask(taskId) {
        logger.info(`[TaskManager] Attempting to pause task: ${taskId}`);

        logger.info(`[TaskManager] Pausing task ${taskId}`);
        this.db.updateTaskStatus(taskId, 'paused');
        return this.db.getTask(taskId);
    }

    /**
     * Run explicit retention cleanup for expired terminal tasks and their audit records.
     * @param {TaskRetentionPolicy|number|null} [policy] - Optional retention policy override
     * @returns {TaskRetentionResult}
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

    /**
     * Whether automatic retention sweeps should run.
     *
     * Row-level windows (`retention_expire_time` on a task) are independent of the
     * global window, so a manager configured with `retention: { expire_time: null }`
     * still needs sweeps — otherwise those rows would only ever be collected by an
     * explicit `runRetention()` call.
     *
     * The sweep also prunes per-schedule metadata whose schedule row is gone, so
     * outstanding metadata counts as work too. Without this, the narrow
     * configuration "global window disabled, no row carries its own window" would
     * gate the sweep off entirely and leave the metadata growing — the same shape of
     * bug this gate was originally introduced to fix.
     *
     * @private
     * @returns {boolean} True when a sweep has work it could do
     */
    _hasRetentionWork() {
        const retention = this.options.retention;
        if (retention && retention.expire_time) {
            return true;
        }

        if (!this.db) {
            return false;
        }

        try {
            // No global window: sweep when a row carries its own window…
            const rows = this.db.pool(conn => conn.execute(
                'SELECT 1 AS present FROM fib_flow_tasks WHERE retention_expire_time IS NOT NULL LIMIT 1'
            ));
            if (rows.length > 0) {
                return true;
            }
        } catch (error) {
            logger.debug(`[TaskManager] Could not probe row-level retention rows: ${error.message}`);
            return false;
        }

        // …or when per-schedule metadata exists (it is swept by the same pass).
        try {
            const metaRows = this.db.pool(conn => conn.execute(
                `SELECT 1 AS present FROM fib_flow_meta WHERE meta_key LIKE 'schedule:%' LIMIT 1`
            ));
            return metaRows.length > 0;
        } catch (error) {
            logger.debug(`[TaskManager] Could not probe schedule metadata: ${error.message}`);
            return false;
        }
    }

    _scheduleNextAutoRetention(nowMs = Date.now()) {
        if (!this._hasRetentionWork()) {
            this.nextRetentionSweepAt = null;
            return;
        }

        this.nextRetentionSweepAt = nowMs + getAutoRetentionIntervalMs();
    }

    _consumeAutoRetentionPolicy(nowMs = Date.now()) {
        const retention = this.options.retention;
        if (!retention) {
            return null;
        }

        // A null/absent global window is not "retention disabled": row-level
        // windows may still need collection, so probe for them before arming.
        if (!this._hasRetentionWork()) {
            this.nextRetentionSweepAt = null;
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
     * @param {string|null} [timeZone] - IANA time zone; null evaluates in the process zone
     * @param {number} [after] - Unix seconds to start from; defaults to now
     * @returns {number} Unix timestamp of next execution time
     */
    _getNextRunTime(cron_expr, timeZone = null, after = null) {
        logger.debug(`[TaskManager] Calculating next run time for cron expression: ${cron_expr}, tz: ${timeZone || 'local'}`);
        const options = {};
        if (timeZone) {
            options.tz = timeZone;
        }
        if (after !== null && after !== undefined) {
            options.currentDate = new Date(after * 1000);
        }
        // Parse cron expression and get next valid execution time
        const interval = parser.parseExpression(cron_expr, Object.keys(options).length > 0 ? options : undefined);
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

        // Explicit suspension marker: return task.suspend({ reason }) from a handler
        // to pause the workflow until resumeTask() is called externally.
        task.suspend = (options = {}) => new Suspension(options);


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

    /**
     * Persist a terminal/intermediate status for a task this worker owns.
     *
     * Never throws: a worker fiber that already finished its handler has no
     * useful recovery path, and an uncaught error here kills the whole process
     * (fibjs `coroutine.start` propagates it). Two cases are benign:
     *
     * - the row disappeared (operator delete, `clearTasks`, retention, another
     *   process) — nothing left to update;
     * - the row was taken over or already moved on (stale update).
     *
     * Everything else (database closed, lock timeout, connection loss) is logged
     * and swallowed for the same reason: losing one status write must not take
     * the process down.
     *
     * @private
     * @param {Object} task - Executing task
     * @param {string} status - Target status
     * @param {Object} [extra] - Extra columns to write
     * @returns {boolean} True when the write was applied
     */
    _updateTaskStatusIfOwned(task, status, extra = {}) {
        try {
            this.db.updateTaskStatus(task.id, status, {
                ...extra,
                worker_id: task.worker_id || null
            });
            return true;
        } catch (error) {
            let currentTask = null;
            try {
                currentTask = this.db ? this.db.getTask(task.id) : null;
            } catch (lookupError) {
                logger.warning(
                    `[TaskManager] Could not re-read task ${task.id} after a failed ${status} update: ${lookupError.message}`
                );
                currentTask = null;
            }

            if (!currentTask) {
                logger.warning(
                    `[TaskManager] Task ${task.id} disappeared before its ${status} update; skipping (${error.message})`
                );
                return false;
            }

            if (currentTask.status !== 'running' || currentTask.worker_id !== task.worker_id) {
                logger.warning(
                    `[TaskManager] Skipping stale ${status} update for task ${task.id}, current status: ${currentTask.status}, current worker: ${currentTask.worker_id}`
                );
                return false;
            }

            // The row is still ours but the write failed for another reason.
            logger.error(
                `[TaskManager] Failed to persist ${status} for task ${task.id}: ${error.message}`
            );
            return false;
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

        // Reap expired workers first so that competing peers that have silently
        // expired are visible as 'dead' before we check for active competitors.
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

        // No competing peers — the worker was transiently evicted (e.g. lease
        // expired because heartbeats failed silently when status was not
        // 'active').  Re-register and immediately heartbeat so that subsequent
        // _reapExpiredWorkers calls (from the main processing loop) will not
        // reap the just-registered worker before the next timer-driven heartbeat
        // has a chance to extend its lease.
        const workerHeartbeatTimeoutSeconds = Math.max(1, Math.ceil(this.options.worker_heartbeat_timeout / 1000));
        const registeredWorker = dbAdapter.registerWorker({
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

        if (!registeredWorker) {
            logger.warning(`[TaskManager] Worker ${this.options.worker_id} re-registration failed — worker record is null`);
            this.workerActiveState = false;
            return false;
        }

        // Immediately heartbeat so that the expires_at written by registerWorker
        // is doubled down.  When the main loop calls _ensureCurrentWorkerActive
        // moments later it will see a fresh lease and a status of 'active'.
        dbAdapter.heartbeatWorker(this.options.worker_id, now, workerHeartbeatTimeoutSeconds);

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
     * Decide whether a claimed cron round should be skipped instead of executed.
     *
     * A round is "missed" when the scheduled instant is at least one full period
     * in the past. Anything smaller is normal scheduling jitter and must run.
     *
     * @private
     * @param {Object} task - Claimed task
     * @returns {boolean} True when the round should be skipped
     */
    _shouldSkipMissedCronWindow(task) {
        if (task.type !== 'cron' || task.cron_catch_up !== 'skip') {
            return false;
        }
        if (!task.cron_expr || !task.next_run_time) {
            return false;
        }

        const now = Math.floor(Date.now() / 1000);
        const scheduled = Number(task.next_run_time);
        if (now < scheduled) {
            return false;
        }

        const periodSeconds = this._getCronPeriodSeconds(task.cron_expr, task.cron_tz || null, scheduled);
        return periodSeconds > 0 && (now - scheduled) >= periodSeconds;
    }

    /**
     * Estimate the period of a cron expression from a given instant.
     * @private
     * @param {string} cronExpr - Cron expression
     * @param {string|null} timeZone - IANA time zone
     * @param {number} from - Unix seconds to start from
     * @returns {number} Seconds between two consecutive occurrences, or 0 when unknown
     */
    _getCronPeriodSeconds(cronExpr, timeZone, from) {
        try {
            const options = [{ currentDate: new Date(from * 1000) }];
            if (timeZone) {
                options[0].tz = timeZone;
            }
            const interval = parser.parseExpression(cronExpr, options[0]);
            const first = Math.floor(interval.next().getTime() / 1000);
            const second = Math.floor(interval.next().getTime() / 1000);
            return second - first;
        } catch (error) {
            logger.warning(`[TaskManager] Failed to estimate cron period for ${cronExpr}: ${error.message}`);
            return 0;
        }
    }

    /**
     * Record a skipped cron window and advance the schedule past it.
     * The attempt row keeps the audit trail complete even though no handler ran.
     * @private
     * @param {Object} task - Claimed task
     */
    _recordSkippedCronWindow(task) {
        const now = Math.floor(Date.now() / 1000);
        const nextRunTime = this._getNextRunTime(task.cron_expr, task.cron_tz || null);
        const skippedWindows = this._countMissedCronWindows(task, now);

        logger.warning(`[TaskManager] Skipping ${skippedWindows} missed window(s) for cron task ${task.id} (catchUp=skip)`);

        this.db.recordCronWindowSkipped(task.id, {
            worker_id: task.worker_id || this.options.worker_id,
            next_run_time: nextRunTime,
            skipped_windows: skippedWindows,
            scheduled_run_time: task.next_run_time,
            now,
            // Spawn rounds never opened an attempt at claim time; writing one here
            // would reintroduce per-round growth on a row retention never collects.
            track_attempt: this.options.cronMode !== 'spawn'
        });
    }

    /**
     * Count how many scheduled occurrences were missed between the persisted
     * next_run_time and now.
     * @private
     * @param {Object} task - Claimed task
     * @param {number} now - Current unix seconds
     * @returns {number} Number of missed windows (>= 1)
     */
    _countMissedCronWindows(task, now) {
        const scheduled = Number(task.next_run_time);
        let count = 0;
        let cursor = scheduled;

        try {
            while (cursor <= now && count < 1000) {
                const options = { currentDate: new Date(cursor * 1000) };
                if (task.cron_tz) {
                    options.tz = task.cron_tz;
                }
                cursor = Math.floor(parser.parseExpression(task.cron_expr, options).next().getTime() / 1000);
                count += 1;
            }
        } catch (_error) {
            return Math.max(count, 1);
        }

        return Math.max(count, 1);
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

                if (result instanceof Suspension) {
                    if (result.tasks) {
                        // SubTasks: suspension awaiting child task completion
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
                                retry_interval: childTask.retry_interval ?? registeredChildTask.options.retry_interval ?? task.retry_interval,
                                // A child lives inside the parent's workflow, so it
                                // inherits the parent's retention window unless it
                                // defines its own.
                                retention_expire_time: childTask.retention_expire_time ?? task.retention_expire_time ?? null
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
                    } else {
                        logger.info(`[TaskManager] Task ${task.id} explicitly suspended with reason: ${result.reason}`);
                        this._updateTaskStatusIfOwned(task, 'suspended', {
                            suspend_reason: result.reason,
                            ...(result.context ? { context: result.context } : {}),
                            worker_id: task.worker_id || null
                        });
                    }
                } else if (task.type === 'cron') {
                    logger.debug(`[TaskManager] Updating cron task ${task.id} for next execution`);
                    // Re-read the persisted schedule: a re-registration during this round
                    // may have changed the expression, so the in-memory snapshot is stale.
                    const schedule = this.db.getCronScheduleState(task.id) || {};
                    const cronExpr = schedule.cron_expr || task.cron_expr;
                    const cronTz = schedule.cron_tz || null;
                    const nextRunTime = this._getNextRunTime(cronExpr, cronTz);

                    // A successful round resets the retry budget: retry_count counts
                    // attempts inside one cron round, not the lifetime of the schedule.
                    this._updateTaskStatusIfOwned(task, 'pending', {
                        result,
                        error: null,
                        retry_count: 0,
                        next_run_time: nextRunTime
                    });
                } else {
                    logger.info(`[TaskManager] Completing async task ${task.id}`);
                    // For async tasks, mark as completed
                    this._updateTaskStatusIfOwned(task, 'completed', {
                        result,
                        parent_id: task.parent_id
                    });
                    // A run spawned by a schedule reports its outcome back, so the
                    // schedule does not look healthy while every round fails.
                    this._reportRunOutcomeToSchedule(task, null);
                }

                this.sleep.post();
            } catch (error) {
                logger.error(`[TaskManager] Error executing task ${task.id}:`, error);
                // Set appropriate failure status and store stack trace which includes error message
                const status = hasTimedOut ? 'timeout' : 'failed';
                const errorText = error.stack || String(error);
                this._updateTaskStatusIfOwned(task, status, { error: errorText });
                this._reportRunOutcomeToSchedule(task, errorText);
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

    /**
     * Report a finished run's outcome to the schedule that spawned it.
     *
     * Inline mode reported failures on the schedule row because the row was the
     * execution. Spawn mode moved execution to a separate row, so without this the
     * schedule would stay a pristine `pending` with `error = NULL` while every
     * round failed: a consumer reading `getCronSchedules()[].last_error` would show
     * "active, no errors" forever.
     *
     * Only spawn-mode managers report, and only for runs of a name that actually
     * has a schedule — a plain `async()` task is never touched.
     *
     * Best-effort: a failure here must never affect the run's own bookkeeping.
     *
     * @private
     * @param {object} task - Finished run row
     * @param {string|null} error - Failure text, or `null` on success
     */
    _reportRunOutcomeToSchedule(task, error) {
        if (this.options.cronMode !== 'spawn' || !task || task.type !== 'async' || !this.db) {
            return;
        }
        if (!this._hasScheduleForName(task.name)) {
            return;
        }

        try {
            this.db.recordScheduleRunOutcome({ runTaskId: task.id, error });
        } catch (reportError) {
            logger.warning(
                `[TaskManager] Could not report run outcome for task ${task.id} to its schedule: ${reportError.message}`
            );
        }
    }

    /**
     * Whether a schedule row exists for a handler name.
     *
     * Uses the `known` set (every schedule, orphaned or not) rather than the
     * spawn-eligible one: a run that finishes after its schedule was cancelled
     * still has an outcome worth reporting. The set also keeps plain `async()`
     * tasks from paying a database round trip on every completion.
     *
     * @private
     * @param {string} taskName - Handler name
     * @returns {boolean} True when a schedule row exists for it
     */
    _hasScheduleForName(taskName) {
        const registered = Array.from(this.handlers.keys());
        if (registered.length === 0) {
            return false;
        }

        const ttl = Math.max(1, Number(this.options.poll_interval) || 1000);
        if (!this._scheduleNameCache || (Date.now() - this._scheduleNameCache.at) >= ttl) {
            this._refreshScheduleNameCache(registered);
        }

        return this._scheduleNameCache.known.has(taskName);
    }

    _filterEligibleTasks(tasks) {
        return tasks.filter(taskName => this._canTaskRun(taskName));
    }

    /**
     * Names of every registered handler that currently has a cron schedule.
     *
     * Only names that are BOTH registered and in a runnable state qualify. An
     * orphan schedule (its handler was removed while the row stayed `pending`)
     * must be excluded: spawn mode would otherwise keep inserting `async` run
     * rows named after a handler nobody registered, and those rows can never be
     * claimed — so they never reach a terminal state and never become retention
     * candidates. Inline mode is immune by construction because `claimTask`
     * already filters on the registered name list.
     *
     * In spawn mode these names must stay claimable regardless of the handler's
     * `max_concurrent_tasks`: the schedule row holds a slot only for the duration
     * of the spawn transaction, and dropping the name from the claim list when the
     * handler is saturated would stall the schedule — exactly the failure mode
     * spawn mode removes.
     *
     * Cached for one poll interval: without it every tick would run an extra
     * full-table scan just to rebuild a name list that changes only on
     * registration. `cron()` / `cancelCron()` / `unuse()` clear the cache.
     *
     * @private
     * @returns {Array<string>} Registered names that own at least one schedule
     */
    _getSpawnEligibleScheduleNames() {
        const registered = Array.from(this.handlers.keys());
        if (registered.length === 0) {
            return [];
        }

        const ttl = Math.max(1, Number(this.options.poll_interval) || 1000);
        if (this._scheduleNameCache && (Date.now() - this._scheduleNameCache.at) < ttl) {
            return this._scheduleNameCache.eligible;
        }

        this._refreshScheduleNameCache(registered);
        return this._scheduleNameCache.eligible;
    }

    /**
     * Rebuild the cached schedule-name sets.
     *
     * Two sets, because they answer different questions: `eligible` gates which
     * schedules may spawn right now (registered + runnable + not orphaned), while
     * `known` is every schedule that exists at all — a run finishing after its
     * schedule was cancelled still has an outcome worth reporting.
     *
     * @private
     * @param {Array<string>} registered - Currently registered handler names
     */
    _refreshScheduleNameCache(registered) {
        const schedules = this.db.getCronSchedules({}, registered);
        const eligible = new Set();
        const known = new Set();

        for (const schedule of schedules) {
            known.add(schedule.name);
            if (schedule.orphan === true) {
                // Registered-name list is the authority: never spawn for a
                // schedule no handler can execute.
                continue;
            }
            if (schedule.status === 'pending' || schedule.status === 'running') {
                eligible.add(schedule.name);
            }
        }

        this._scheduleNameCache = {
            at: Date.now(),
            eligible: Array.from(eligible),
            known
        };
    }

    /**
     * Drop the cached schedule-name list so the next tick re-reads it.
     * @private
     */
    _invalidateScheduleNameCache() {
        this._scheduleNameCache = null;
    }

    /**
     * Claim the next runnable row.
     *
     * Ordering matters: cron schedules are probed first and with `type='cron'`,
     * so a crowded async queue cannot delay a schedule and a saturated handler
     * cannot hide it. Inline mode keeps the original single-probe behaviour.
     *
     * @private
     * @returns {{task:object|null, scheduleOnly:boolean}} Claim result
     */
    _claimNextTask() {
        if (this.options.cronMode === 'spawn') {
            const scheduleNames = this._getSpawnEligibleScheduleNames();
            if (scheduleNames.length > 0) {
                const scheduleTask = this.db.claimTask(scheduleNames, this.options.worker_id, {
                    skipAttempt: true,
                    skipEvents: true,
                    typeFilter: 'cron'
                });
                if (scheduleTask) {
                    return { task: scheduleTask, scheduleOnly: true };
                }
            }

            // Never let the execution probe pick up a cron row. Both types share
            // the same name in spawn mode, so an unfiltered claim could hand a
            // schedule row to the user handler — the exact thing spawn mode
            // exists to prevent. The schedule above already had its chance; any
            // cron row still `pending` here is not spawn-eligible (cancelled,
            // paused, or its handler is gone) and must wait, not execute.
            const eligibleTaskTypes = this._filterEligibleTasks(Array.from(this.handlers.keys()));
            const asyncTask = this.db.claimTask(eligibleTaskTypes, this.options.worker_id, {
                typeFilter: 'async'
            });
            return { task: asyncTask || null, scheduleOnly: false };
        }

        const eligibleTaskTypes = this._filterEligibleTasks(Array.from(this.handlers.keys()));
        const task = this.db.claimTask(eligibleTaskTypes, this.options.worker_id);
        return { task: task || null, scheduleOnly: false };
    }

    /**
     * Run one spawn round for a claimed cron row.
     *
     * The whole point of this path: the row never executes user code. Inserting
     * the run row and advancing the schedule happen in one transaction, so a
     * crash in between cannot produce a second run for the same window.
     *
     * Two guards the inline mode got for free (its row was `running` and therefore
     * not claimable) have to be explicit here:
     *
     * - **Overlap**: `maxInstances` caps how many of this schedule's runs may be
     *   in flight. A handler slower than its cron period would otherwise stack
     *   overlapping runs, and the queued ones can never reach a terminal state, so
     *   retention would never collect them.
     * - **Orphan**: a schedule whose handler is gone must not spawn at all.
     *
     * Missed-window policy (`catchUp: 'skip'`) is evaluated before either check so
     * the existing skip semantics still apply.
     *
     * @private
     * @param {object} task - Claimed cron row
     */
    _runCronRound(task) {
        try {
            // Guard against the window between "claim" and "spawn": the handler can
            // be unregistered while the round is in flight. Spawning here would
            // create a run row nobody can claim, which then never reaches a
            // terminal state and never becomes a retention candidate.
            if (!this.handlers.has(task.name)) {
                logger.warning(
                    `[TaskManager] Skipping spawn for cron task ${task.id}(${task.name}): no handler registered`
                );
                this._failCronSpawn(task, new Error(`No handler registered for task: ${task.name}`));
                return;
            }

            if (this._shouldSkipMissedCronWindow(task)) {
                this._recordSkippedCronWindow(task);
                return;
            }

            // Re-read the persisted schedule: a re-registration during this round
            // may have changed the expression, so the in-memory snapshot is stale.
            const schedule = this.db.getCronScheduleState(task.id) || {};
            const cronExpr = schedule.cron_expr || task.cron_expr;
            const cronTz = schedule.cron_tz || null;
            const nextRunTime = this._getNextRunTime(cronExpr, cronTz);

            const maxInstances = this._getScheduleMaxInstances(task);
            if (maxInstances !== null) {
                const inFlight = this.db.countScheduleRunsInFlight(task.id);
                if (inFlight >= maxInstances) {
                    logger.warning(
                        `[TaskManager] Skipping cron round for ${task.name}(${task.id}): ` +
                        `${inFlight} run(s) still in flight (maxInstances=${maxInstances})`
                    );
                    this.db.skipCronRoundForOverlap({
                        schedule: task,
                        next_run_time: nextRunTime,
                        in_flight: inFlight
                    });
                    return;
                }
            }

            this._spawnCronRun(task, { cronExpr, cronTz, nextRunTime });
        } catch (error) {
            logger.error(`[TaskManager] Cron spawn failed for schedule ${task.id}:`, error);
            this._failCronSpawn(task, error);
        } finally {
            this.sleep.post();
        }
    }

    /**
     * Resolve how many runs of one schedule may be in flight at once.
     *
     * Defaults to 1 — the mutual exclusion inline mode had for free — so switching
     * `cronMode` to `'spawn'` cannot silently start overlapping runs for a handler
     * that is slower than its period.
     *
     * The value is persisted in `fib_flow_meta` (see `cron()`), so a restart — or a
     * tick that happens before the caller re-registers — still sees the policy the
     * caller asked for. The in-memory map only avoids a metadata round trip on the
     * hot path.
     *
     * @private
     * @param {object} task - Schedule row
     * @returns {number|null} Cap, or `null` for unlimited
     */
    _getScheduleMaxInstances(task) {
        const cronKey = task.cron_key || 'default';
        const key = scheduleIdentity(task.name, cronKey);

        if (this.scheduleMaxInstances.has(key)) {
            return this.scheduleMaxInstances.get(key);
        }

        // Not registered in this process yet: fall back to the persisted policy.
        let persisted;
        try {
            persisted = this.db.getScheduleMaxInstances(task.name, cronKey);
        } catch (error) {
            logger.warning(
                `[TaskManager] Could not read the persisted maxInstances for ${task.name}: ${error.message}`
            );
            persisted = undefined;
        }

        const resolved = persisted === undefined ? DEFAULT_MAX_INSTANCES : persisted;
        this.scheduleMaxInstances.set(key, resolved);
        return resolved;
    }

    /**
     * Record the caller's `maxInstances` preference for a schedule.
     *
     * Omitting the option **keeps the stored value** instead of resetting it to the
     * default. `cron()` is an idempotent upsert — every call restates the same
     * configuration — so an absent field means "leave it alone". Resetting on
     * omission would silently wipe a non-default cap on any re-registration, and
     * because the value is persisted, the wipe would survive the restart too.
     *
     * Pass `maxInstances: 0` for unlimited; pass `null` explicitly to reset to the
     * default.
     *
     * Written through to the database so it survives a restart: the in-memory map
     * alone would silently revert to the default on any path that does not
     * re-register the schedule (a first tick before `cron()`, a partially failed
     * definition load).
     *
     * @private
     * @param {string} name - Schedule name
     * @param {string} cronKey - Schedule identity key
     * @param {number|null|undefined} maxInstances - Raw option value
     */
    _setScheduleMaxInstances(name, cronKey, maxInstances) {
        const key = scheduleIdentity(name, cronKey);
        let resolved;

        if (maxInstances === undefined) {
            // Keep whatever is already in force: the in-memory value when this
            // process has seen it, otherwise the persisted one, otherwise the
            // default.
            if (this.scheduleMaxInstances.has(key)) {
                return;
            }

            let persisted;
            try {
                persisted = this.db.getScheduleMaxInstances(name, cronKey);
            } catch (error) {
                logger.warning(
                    `[TaskManager] Could not read the persisted maxInstances for ${name}: ${error.message}`
                );
                persisted = undefined;
            }

            resolved = persisted === undefined ? DEFAULT_MAX_INSTANCES : persisted;
            this.scheduleMaxInstances.set(key, resolved);
            if (persisted === undefined) {
                this.db.setScheduleMaxInstances(name, cronKey, resolved);
            }
            return;
        }

        if (maxInstances === null) {
            // Explicit reset.
            resolved = DEFAULT_MAX_INSTANCES;
        } else {
            const normalized = Number(maxInstances);
            if (!Number.isInteger(normalized) || normalized < 0) {
                throw new Error('maxInstances must be a non-negative integer (0 = unlimited)');
            }
            // 0 means "no cap".
            resolved = normalized === 0 ? null : normalized;
        }

        this.scheduleMaxInstances.set(key, resolved);
        this.db.setScheduleMaxInstances(name, cronKey, resolved);
    }

    /**
     * Spawn one run row for a claimed schedule.
     *
     * Never leaves the schedule in `running`: on failure the row is advanced the
     * same way a successful round would be, with the error recorded, so the next
     * tick still fires. Deliberately not retried — "no order sent" is safer than
     * "order sent twice", and the next window is at most one period away.
     *
     * @private
     * @param {object} task - Schedule row
     * @param {object} options - Spawn options
     * @param {string} options.cronExpr - Expression to advance from
     * @param {string|null} options.cronTz - IANA time zone
     * @param {number} options.nextRunTime - Next instant
     * @returns {{taskId:number|string, next_run_time:number}|null} Spawn result
     */
    _spawnCronRun(task, { cronExpr, cronTz, nextRunTime }) {
        try {
            const result = this.db.spawnCronRun({
                schedule: task,
                next_run_time: nextRunTime
            });

            logger.info(`[TaskManager] Cron task ${task.name}(${task.id}) spawned run ${result.taskId}`);
            return result;
        } catch (error) {
            logger.error(`[TaskManager] Failed to spawn run for cron task ${task.id}: ${error.message}`);
            this._failCronSpawn(task, error, nextRunTime);
            return null;
        }
    }

    /**
     * Advance a schedule past a failed spawn without creating a run row.
     * @private
     * @param {object} task - Schedule row
     * @param {Error} error - Failure to record
     * @param {number} [nextRunTime] - Next instant; recomputed when omitted
     */
    _failCronSpawn(task, error, nextRunTime) {
        try {
            const fallbackNext = nextRunTime !== undefined && nextRunTime !== null
                ? nextRunTime
                : this._getNextRunTime(task.cron_expr, task.cron_tz || null);

            this.db.failCronSpawn({
                schedule: task,
                next_run_time: fallbackNext,
                error: (error && error.message) || String(error)
            });
        } catch (failError) {
            // The row is still `running` at this point, which means the schedule
            // will only resume through worker recovery. Log loudly: this is the
            // one path where the schedule can still stall.
            logger.error(
                `[TaskManager] Could not record spawn failure for cron task ${task.id}; schedule may stall until recovery: ${failError.message}`
            );
        }
    }

    /**
     * Trigger one cron schedule immediately, without waiting for its window.
     *
     * Only a run row is created: the schedule row is left exactly as it was, so a
     * cancelled or paused schedule is not revived by a manual trigger, and
     * `next_run_time` does not move.
     *
     * The same `maxInstances` cap applies. A manual trigger creates a run of this
     * schedule, so it must count toward the cap — otherwise the invariant would be
     * "at most N runs, except when you trigger manually", and the runs produced by
     * a manual trigger would then block the scheduled rounds while evading the cap
     * themselves. Exceeding it throws rather than skipping silently: a caller that
     * explicitly asked for a run must get an answer it can show.
     *
     * @param {string|number} scheduleId - Cron schedule row id
     * @param {object} [payload] - Run payload; defaults to the schedule payload
     * @param {object} [options] - Trigger options
     * @param {string} [options.trigger='manual'] - Trigger tag recorded on the event
     * @param {boolean} [options.ignoreMaxInstances=false] - Bypass the cap
     * @returns {{taskId:number|string, next_run_time:number|null}} Spawn result
     * @throws {Error} If the schedule does not exist, is not a cron row, has no
     *                 registered handler, or the cap would be exceeded
     */
    runCronNow(scheduleId, payload, options = {}) {
        logger.info(`[TaskManager] Triggering cron schedule ${scheduleId} immediately`);

        if (scheduleId === undefined || scheduleId === null || scheduleId === '') {
            throw new Error('runCronNow requires a schedule id');
        }

        const task = this.db.getTask(scheduleId);
        if (!task) {
            throw new Error(`Cron schedule ${scheduleId} not found`);
        }
        if (task.type !== 'cron') {
            throw new Error(`Task ${scheduleId} is not a cron schedule (type: ${task.type})`);
        }
        if (!this.handlers.has(task.name)) {
            throw new Error(`No handler registered for cron schedule ${scheduleId} (name: ${task.name})`);
        }

        if (options.ignoreMaxInstances !== true) {
            const maxInstances = this._getScheduleMaxInstances(task);
            if (maxInstances !== null) {
                const inFlight = this.db.countScheduleRunsInFlight(task.id);
                if (inFlight >= maxInstances) {
                    throw new Error(
                        `Cron schedule ${scheduleId} already has ${inFlight} run(s) in flight ` +
                        `(maxInstances=${maxInstances}); pass ignoreMaxInstances to force a run`
                    );
                }
            }
        }

        const result = this.db.spawnCronRun({
            schedule: task,
            payload,
            trigger: options.trigger || 'manual',
            advance: false
        });

        // Wake the loop so the new row is claimed without waiting a full poll.
        this.sleep.post();
        return result;
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

                    // Spawn mode: schedules are probed FIRST and without the
                    // handler-concurrency filter. A saturated per-handler limit
                    // would otherwise remove the name from `claimTask`'s name
                    // list, which is the same "schedule stalls" failure the
                    // spawn mode exists to remove.
                    const claimed = this._claimNextTask();
                    const task = claimed.task;

                    if (claimed.scheduleOnly && task) {
                        this._runCronRound(task);
                        this.semaphore.release();
                        continue;
                    }

                    if (!task) {
                        logger.debug(`[TaskManager] No tasks available, waiting ${this.options.poll_interval}ms`);
                        // No tasks available, wait before trying again
                        this.semaphore.release();
                        this.sleep.wait(this.options.poll_interval);
                        continue;
                    }

                    logger.info(`[TaskManager] Claimed task ${task.id} for execution`);

                    // Missed-window policy is evaluated here, after the claim, so it
                    // also covers gaps that appear while the manager is running
                    // (handler unregistered, worker recovered, DB hiccup) — not only
                    // process startup.
                    if (this._shouldSkipMissedCronWindow(task)) {
                        this._recordSkippedCronWindow(task);
                        this.semaphore.release();
                        continue;
                    }

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
     * @param {object} [filters] - Optional filters, e.g. { suspend_reason: 'awaiting_approval' }
     * @returns {Array} Array of matching tasks
     */
    getTasksByStatus(status, filters = {}) {
        logger.debug(`[TaskManager] Getting tasks by status: ${status}, filters:`, filters);
        return this.db.getTasksByStatus(status, filters);
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
     * List cron schedules, including an `orphan` flag when a handler is missing.
     * @param {object} [filters] - Optional filters (name / tag / status)
     * @returns {Array<object>} Schedule rows
     */
    getCronSchedules(filters = {}) {
        logger.debug(`[TaskManager] Getting cron schedules with filters:`, filters);
        return this.db.getCronSchedules(filters, Array.from(this.handlers.keys()), {
            include_spawned: true
        });
    }

    /**
     * Cancel pending cron schedules whose handler is not registered.
     * Never runs automatically: reclaiming an orphan is an explicit operator action.
     * @returns {{cancelled:number, taskIds:Array<number>}}
     */
    pruneCronSchedules() {
        logger.info(`[TaskManager] Pruning orphan cron schedules`);
        const result = this.db.pruneCronSchedules(Array.from(this.handlers.keys()));
        if (result && result.cancelled > 0) {
            this._invalidateScheduleNameCache();
        }
        return result;
    }

    /**
     * Aggregate cron schedule counters (active / paused / cancelled / orphan).
     * @returns {{total:number, active:number, paused:number, cancelled:number, orphan:number}}
     */
    getCronScheduleStats() {
        logger.debug(`[TaskManager] Getting cron schedule stats`);
        return this.db.getCronScheduleStats(Array.from(this.handlers.keys()));
    }

    /**
     * Get tasks with multiple filter conditions
     * @param {Object} filters - Filter conditions
     * @param {string} [filters.tag] - Filter by tag
     * @param {string} [filters.status] - Filter by status
     * @param {string} [filters.name] - Filter by task name
     * @param {string} [filters.type] - Filter by task type ('async' | 'cron')
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
module.exports.Suspension = Suspension;
