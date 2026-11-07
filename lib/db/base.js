/**
 * Provides a unified interface for task persistence across different database systems.
 * Abstracts database-specific complexities to ensure consistent task management.
 */

const db = require('db');
const Pool = require('fib-pool');
const coroutine = require('coroutine');
const createLogger = require('../logger');
const { parseTask } = require('./util');

// Create logger for database operations
const logger = createLogger('fib-flow');

function serializeEventMetadata(metadata) {
    if (metadata === undefined || metadata === null) {
        return null;
    }

    return typeof metadata === 'string' ? metadata : JSON.stringify(metadata);
}

function parseEventRow(event) {
    if (!event) return null;

    if (typeof event.metadata === 'string' && event.metadata !== '') {
        try {
            event.metadata = JSON.parse(event.metadata);
        } catch (_error) {
            // Keep original metadata when parsing fails.
        }
    }

    return event;
}

function parseAttemptRow(attempt) {
    if (!attempt) return null;

    if (attempt.timeout_flag !== undefined && attempt.timeout_flag !== null) {
        attempt.timeout_flag = Boolean(attempt.timeout_flag);
    }

    return attempt;
}

function normalizeRootId(taskId, rootId) {
    return rootId || taskId;
}

function hasFilterValue(value) {
    return value !== undefined && value !== null;
}

function normalizePagination(filters = {}) {
    const order = String(filters.order || 'asc').toLowerCase() === 'desc' ? 'DESC' : 'ASC';

    let limit = null;
    if (hasFilterValue(filters.limit)) {
        limit = Number(filters.limit);
        if (!Number.isInteger(limit) || limit <= 0) {
            throw new Error('limit must be a positive integer');
        }
    }

    let offset = 0;
    if (hasFilterValue(filters.offset)) {
        offset = Number(filters.offset);
        if (!Number.isInteger(offset) || offset < 0) {
            throw new Error('offset must be a non-negative integer');
        }
    }

    return { limit, offset, order };
}

function toPageResult(items, total, limit, offset) {
    return {
        items,
        total,
        limit,
        offset,
        has_more: limit !== null ? offset + items.length < total : false
    };
}

function incrementCounter(bucket, key) {
    bucket[key] = (bucket[key] || 0) + 1;
}

function sortObjectKeys(input) {
    const output = {};
    Object.keys(input).sort().forEach(key => {
        output[key] = input[key];
    });
    return output;
}

function compareNullableNumberAsc(left, right) {
    const normalizedLeft = left === null || left === undefined ? Number.MAX_SAFE_INTEGER : left;
    const normalizedRight = right === null || right === undefined ? Number.MAX_SAFE_INTEGER : right;
    return normalizedLeft - normalizedRight;
}

function hasOwn(obj, key) {
    return Object.prototype.hasOwnProperty.call(obj, key);
}

/**
 * Resolve the status part of a task query into a single list, or `null`.
 *
 * `status` and `statuses` are alternatives; supplying both is rejected rather than
 * silently resolved, because the two readings ("this one" vs "any of these")
 * disagree and guessing would hide the mistake.
 *
 * @param {object} filters - Query filters
 * @returns {Array<string>|null} Statuses to match, or `null` for no constraint
 * @throws {Error} If both are supplied, or a value is not a known status
 */
function normalizeStatusFilter(filters) {
    const hasSingle = filters.status !== undefined && filters.status !== null;
    const hasMany = filters.statuses !== undefined && filters.statuses !== null;

    if (hasSingle && hasMany) {
        throw new Error('Query filters accept either "status" or "statuses", not both');
    }

    if (hasSingle) {
        return [assertTaskStatus(filters.status)];
    }

    if (!hasMany) {
        return null;
    }

    if (!Array.isArray(filters.statuses) || filters.statuses.length === 0) {
        throw new Error('"statuses" must be a non-empty array of task statuses');
    }

    return Array.from(new Set(filters.statuses.map(status => assertTaskStatus(status))));
}

/**
 * Build the `fib_flow_meta` key for per-schedule policy.
 *
 * `fib_flow_meta` is a plain key-value table, so per-schedule settings live there
 * without touching the schema version. JSON-encoding the tuple keeps the key
 * collision-free (a schedule name may contain any character, including the
 * separator) and safe to round-trip through SQL.
 * @param {string} setting - Setting name, e.g. 'max_instances'
 * @param {string} name - Schedule name
 * @param {string} cronKey - Schedule identity key
 * @returns {string} Meta key
 */
function buildScheduleMetaKey(setting, name, cronKey) {
    return `schedule:${JSON.stringify([setting, name, cronKey])}`;
}

/**
 * Identity of one schedule, independent of which setting a meta key carries.
 * @param {string} name - Schedule name
 * @param {string} cronKey - Schedule identity key
 * @returns {string} Identity key
 */
function buildScheduleIdentityKey(name, cronKey) {
    return JSON.stringify([name, cronKey]);
}

/**
 * Decode a `schedule:*` meta key.
 * @param {string} metaKey - Stored key
 * @returns {{setting:string, name:string, cronKey:string}|null} Parsed parts, or
 *          `null` when the key is not one of ours
 */
function parseScheduleMetaKey(metaKey) {
    const prefix = 'schedule:';
    if (typeof metaKey !== 'string' || !metaKey.startsWith(prefix)) {
        return null;
    }

    try {
        const parsed = JSON.parse(metaKey.slice(prefix.length));
        if (!Array.isArray(parsed) || parsed.length !== 3) {
            return null;
        }
        return { setting: String(parsed[0]), name: String(parsed[1]), cronKey: String(parsed[2]) };
    } catch (_error) {
        return null;
    }
}

/**
 * Stable map key for one schedule row.
 * @param {number|string} taskId - Schedule row id
 * @returns {string} Map key
 */
function scheduleIdentity(taskId) {
    return String(taskId);
}

const DEFAULT_RETENTION_STATUSES = ['completed', 'permanently_failed', 'cancelled'];
const ALLOWED_RETENTION_STATUSES = ['completed', 'permanently_failed', 'paused', 'cancelled'];
const INTERNAL_RETENTION_BATCH_SIZE = 100;

/**
 * Audit events that describe a single cron round rather than a row lifecycle.
 *
 * A schedule row is never collected by retention, so these would accumulate
 * forever. Each spawn round keeps only the most recent
 * {@link CRON_ROUND_EVENT_HISTORY} entries: enough to see whether the schedule is
 * firing and which run came last, without turning the event table into a
 * per-minute ledger.
 */
const CRON_ROUND_EVENT_TYPES = [
    'task_cron_spawned',
    'task_cron_spawn_failed',
    'task_cron_missed',
    'task_cron_overlap_skipped',
    'task_cron_run_failed',
    'task_cron_run_recovered'
];
const CRON_ROUND_EVENT_HISTORY = 20;

/**
 * Canonical task status list. Single source of truth for validation paths and
 * documentation; the per-engine DDL CHECK/ENUM lists must stay in sync.
 */
const TASK_STATUSES = [
    'pending',
    'running',
    'completed',
    'failed',
    'timeout',
    'permanently_failed',
    'paused',
    'suspended',
    'cancelled'
];

/**
 * Statuses that represent a finished task lifecycle. Used by retention defaults
 * and workflow failure statistics.
 */
const TERMINAL_TASK_STATUSES = ['completed', 'permanently_failed', 'cancelled'];

const CURRENT_SCHEMA_VERSION = '2';


function normalizeRetentionPolicy(policy, fallbackExpireTime = null) {
    let input = policy;

    if (typeof input === 'number') {
        input = { expire_time: input };
    }

    if (!input || typeof input !== 'object') {
        input = {};
    }

    const rawExpireTime = hasOwn(input, 'expire_time') ? input.expire_time : fallbackExpireTime;
    const now = hasOwn(input, 'now') ? Number(input.now) : Math.floor(Date.now() / 1000);

    // Statuses are validated identically whether or not a global window is set:
    // row-level retention reuses this same gate, so an unsupported status must
    // fail here instead of silently widening what can be deleted.
    const rawStatuses = hasOwn(input, 'statuses') ? input.statuses : DEFAULT_RETENTION_STATUSES;
    if (!Array.isArray(rawStatuses) || rawStatuses.length === 0) {
        throw new Error('retention statuses must be a non-empty array');
    }

    const normalizedStatuses = rawStatuses.map(status => String(status));
    normalizedStatuses.forEach(status => {
        if (!ALLOWED_RETENTION_STATUSES.includes(status)) {
            throw new Error(`Unsupported retention status: ${status}`);
        }
    });

    const statuses = Array.from(new Set(normalizedStatuses));

    if (rawExpireTime === null || rawExpireTime === undefined || rawExpireTime === false) {
        return {
            expire_time: null,
            statuses,
            batch_size: INTERNAL_RETENTION_BATCH_SIZE,
            now
        };
    }

    const expireTime = Number(rawExpireTime);
    if (!Number.isFinite(expireTime) || expireTime <= 0) {
        throw new Error('retention expire_time must be a positive number');
    }

    return {
        expire_time: expireTime,
        statuses,
        batch_size: INTERNAL_RETENTION_BATCH_SIZE,
        now
    };
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

/**
 * Validate a task status against the canonical status list.
 * @param {string} status - Status value to validate
 * @param {string} [fieldName='status'] - Field name used in the error message
 * @returns {string} The validated status
 * @throws {Error} If the status is not a known task status
 */
function assertTaskStatus(status, fieldName = 'status') {
    if (!TASK_STATUSES.includes(status)) {
        throw new Error(`Invalid ${fieldName} value: ${status}`);
    }

    return status;
}

const TASK_TYPES = ['async', 'cron'];

/**
 * Validate a task type.
 * @param {string} type - Task type to validate
 * @param {string} [message] - Error message to use when the type is invalid
 * @returns {string} The validated type
 * @throws {Error} If the type is not a known task type
 */
function assertTaskType(type, message = 'Invalid task type') {
    if (!TASK_TYPES.includes(type)) {
        throw new Error(message);
    }

    return type;
}

/**
 * Validate an IANA time zone name using the runtime ICU data.
 * cron-parser reports invalid zones as an opaque CronDate timestamp error, so
 * validation must happen here to produce an actionable message.
 * @param {string} timeZone - IANA time zone name
 * @returns {string} The validated time zone
 * @throws {Error} If the time zone is not a valid IANA name
 */
function assertIanaTimeZone(timeZone) {
    const normalized = normalizeTrimmedString(timeZone, 'timezone');

    try {
        new Intl.DateTimeFormat('en-US', { timeZone: normalized });
    } catch (_error) {
        throw new Error(`Invalid timezone: ${normalized} (must be an IANA name such as "Asia/Shanghai")`);
    }

    return normalized;
}

/**
 * Validate a cron catch-up policy.
 * @param {string} catchUp - Either 'once' or 'skip'
 * @returns {string} The validated policy
 * @throws {Error} If the policy is unknown
 */
function assertCronCatchUp(catchUp) {
    if (catchUp !== 'once' && catchUp !== 'skip') {
        throw new Error(`Invalid catchUp value: ${catchUp} (expected "once" or "skip")`);
    }

    return catchUp;
}

/**
 * Validate a cron identity key. Keys become part of a unique index, so they
 * must be non-empty strings; async tasks store NULL instead.
 * @param {string} cronKey - Cron identity key
 * @returns {string} The validated key
 * @throws {Error} If the key is not a non-empty string
 */
function assertCronKey(cronKey) {
    return normalizeTrimmedString(cronKey, 'cronKey');
}

/**
 * Validate a per-row retention window in seconds.
 * @param {number} value - Retention window
 * @returns {number} The validated window
 * @throws {Error} If the value is not a positive finite number
 */
function assertRetentionExpireTime(value) {
    const normalized = Number(value);
    if (!Number.isFinite(normalized) || normalized <= 0) {
        throw new Error('retention_expire_time must be a positive number of seconds');
    }

    return Math.floor(normalized);
}

function normalizeAuditName(value, fieldName) {
    return normalizeTrimmedString(value, fieldName);
}

function normalizeWorkerIdentifier(value, fieldName) {
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

function normalizeCheckpointPayload(checkpoint) {
    if (!checkpoint || !checkpoint.code) {
        throw new Error('Checkpoint code is required');
    }

    return {
        ...checkpoint,
        code: normalizeAuditName(checkpoint.code, 'Checkpoint code'),
        ...(hasOwn(checkpoint, 'message') ? { message: normalizeTrimmedString(checkpoint.message, 'Checkpoint message') } : {}),
        ...(hasOwn(checkpoint, 'metadata') ? { metadata: normalizeMetadata(checkpoint.metadata, 'Checkpoint metadata') } : {})
    };
}

function normalizeProgressPayload(progress) {
    const hasProgressField = hasOwn(progress, 'stage_name')
        || hasOwn(progress, 'progress_text')
        || hasOwn(progress, 'progress_percent');

    if (!hasProgressField) {
        throw new Error('Task progress requires stage_name, progress_text, or progress_percent');
    }

    if (hasOwn(progress, 'progress_percent')) {
        const percent = Number(progress.progress_percent);
        if (!Number.isFinite(percent) || percent < 0 || percent > 100) {
            throw new Error('progress_percent must be a number between 0 and 100');
        }
    }

    return {
        ...progress,
        ...(hasOwn(progress, 'stage_name') ? { stage_name: normalizeAuditName(progress.stage_name, 'Progress stage_name') } : {}),
        ...(hasOwn(progress, 'progress_text') ? { progress_text: normalizeTrimmedString(progress.progress_text, 'Progress text') } : {}),
        ...(hasOwn(progress, 'message') ? { message: normalizeTrimmedString(progress.message, 'Progress message') } : {}),
        ...(hasOwn(progress, 'metadata') ? { metadata: normalizeMetadata(progress.metadata, 'Progress metadata') } : {})
    };
}

function uniqueTaskIds(rows) {
    const seen = new Set();
    const ids = [];

    (rows || []).forEach(row => {
        if (!row || row.id === undefined || row.id === null || seen.has(row.id)) {
            return;
        }

        seen.add(row.id);
        ids.push(row.id);
    });

    return ids;
}

/**
 * Centralizes task management logic to support distributed, resilient task processing.
 * Enables flexible connection handling and provides a standardized task lifecycle.
 */
class BaseDBAdapter {
    /**
     * Configures connection pooling to optimize database resource utilization.
     * Supports multiple connection initialization strategies for maximum flexibility.
     * 
     * @param {string|object|function} conn - Flexible connection configuration
     * @param {number} [poolSize=5] - Limits concurrent database connections
     * @throws {Error} Prevents misconfigured database connections
     */
    constructor(conn, poolSize = 5) {
        logger.info(`[BaseDBAdapter] Initializing with connection type: ${typeof conn}, pool size: ${poolSize}`);

        // Default lock clause for row-level locking
        this.lockClause = 'FOR UPDATE SKIP LOCKED';

        /**
         * Normalizes different connection types to a consistent pool-like interface.
         * Ensures uniform connection management regardless of underlying database.
         * 
         * @param {*} conn - Raw database connection
         * @returns {function} Standardized connection pool wrapper
         */
        function wrap_conn(conn) {
            logger.debug(`[BaseDBAdapter] Wrapping connection with thread-safe interface`);
            // Thread-safe connection wrapper using coroutine lock
            const locker = new coroutine.Lock();

            // Provides a consistent execution and cleanup mechanism
            const pool = function (callback) {
                let result;

                try {
                    // Ensure thread-safe execution of database operations
                    locker.acquire();
                    logger.debug(`[BaseDBAdapter] Acquired lock for database operation`);
                    result = callback(conn);
                } finally {
                    // Always release the lock, even if an error occurs
                    logger.debug(`[BaseDBAdapter] Releasing lock after database operation`);
                    locker.release();
                }

                return result;
            };

            // Ensures thread-safe connection closure
            pool.clear = function () {
                try {
                    // Prevent concurrent connection closure
                    logger.debug(`[BaseDBAdapter] Acquiring lock for connection closure`);
                    locker.acquire();
                    conn.close();
                    logger.info(`[BaseDBAdapter] Connection closed successfully`);
                } finally {
                    locker.release();
                }
            }.bind(this);

            return pool;
        }

        // Prioritizes connection initialization based on connection type
        // Prevents runtime errors and supports diverse database configurations
        // NOTE: the string check must be evaluated first — comparing a native
        // connection object with `==` against a string coerces it via toString(),
        // which throws for fibjs database handles.
        if (typeof conn === 'string' && conn === 'sqlite::memory:') {
            logger.info(`[BaseDBAdapter] Creating in-memory SQLite connection`);
            this.pool = wrap_conn(db.open(conn));
        }
        else if (typeof conn === 'function') {
            logger.debug(`[BaseDBAdapter] Using provided connection function`);
            this.pool = conn;
        }
        else if (typeof conn === 'string') {
            logger.info(`[BaseDBAdapter] Creating connection pool with connection string`);
            this.pool = Pool({
                create: () => {
                    logger.debug(`[BaseDBAdapter] Creating new connection in pool`);
                    return this.createConnection(conn);
                },
                destroy: conn => {
                    logger.debug(`[BaseDBAdapter] Destroying connection in pool`);
                    this.destroyConnection(conn);
                },
                timeout: 30000,
                retry: 1,
                maxsize: poolSize
            });
        }
        else if (typeof conn === 'object') {
            logger.debug(`[BaseDBAdapter] Wrapping provided connection object`);
            this.pool = wrap_conn(conn);
        }
        else {
            throw new Error('Invalid connection type: ' + conn);
        }
        logger.info(`[BaseDBAdapter] Connection pool initialized successfully`);
    }

    /**
     * Prepares database schema for task tracking.
     * Enforces implementation requirement for specific database adapters.
     * @throws {Error} If not overridden by subclass
     */
    setup() {
        logger.info(`[BaseDBAdapter] Setting up database schema`);
        throw new Error('setup() must be implemented by subclass');
    }

    /**
     * Enforce the schema version recorded in fib_flow_meta.
     *
     * fib-flow does not migrate column layouts: the version gate exists so an
     * outdated table layout fails loudly at startup instead of producing
     * "no such column" errors on the first cron registration.
     *
     * @throws {Error} If the existing schema predates the current version
     */
    _assertSchemaVersion() {
        const rebuildHint = 'Drop the fib_flow_* tables and re-run setup() to rebuild them.';

        // Portable column probe: selecting the v2 columns from an empty result set
        // fails on every engine when the layout is older, without engine-specific
        // information_schema / PRAGMA queries.
        let columnsPresent = true;
        try {
            this.pool(conn => conn.execute(
                'SELECT cron_key, cron_tz, cron_catch_up, retention_expire_time FROM fib_flow_tasks WHERE 1 = 0'
            ));
        } catch (_error) {
            columnsPresent = false;
        }

        const rows = this.pool(conn => conn.execute(
            "SELECT meta_value FROM fib_flow_meta WHERE meta_key = 'schema_version'"
        ));

        if (rows.length > 0) {
            const version = String(rows[0].meta_value || '');
            if (version !== CURRENT_SCHEMA_VERSION || !columnsPresent) {
                throw new Error(
                    `fib-flow schema v${version} is not supported; expected v${CURRENT_SCHEMA_VERSION}. ${rebuildHint}`
                );
            }
            return;
        }

        const taskRows = this.pool(conn => conn.execute('SELECT COUNT(*) AS total FROM fib_flow_tasks'));
        const total = taskRows[0] ? Number(taskRows[0].total) : 0;
        if (total > 0 || !columnsPresent) {
            throw new Error(
                `fib-flow task table predates schema v${CURRENT_SCHEMA_VERSION} ` +
                `(${total} row(s), missing scheduling columns). ${rebuildHint}`
            );
        }

        this.pool(conn => conn.execute(
            "INSERT INTO fib_flow_meta (meta_key, meta_value) VALUES ('schema_version', ?)",
            CURRENT_SCHEMA_VERSION
        ));
    }

    /**
     * Establishes a database connection with error handling.
     * Allows database-specific connection logic in subclasses.
     * 
     * @param {string} connStr - Database connection parameters
     * @returns {object} Initialized database connection
     * @protected
     */
    createConnection(connStr) {
        logger.debug(`[BaseDBAdapter] Creating database connection with string: ${connStr}`);
        return db.open(connStr);
    }

    /**
     * Safely terminates database connection to prevent resource leaks.
     * Provides a hook for custom connection cleanup in subclasses.
     * 
     * @param {object} conn - Active database connection
     * @protected
     */
    destroyConnection(conn) {
        logger.debug(`[BaseDBAdapter] Destroying database connection`);
        conn.close();
    }

    /**
     * Retrieves the ID of the last inserted row from database operation
     * Provides a default implementation that works with common databases
     * Each database adapter can override this method for specific implementations
     * 
     * @protected
     * @param {Object} conn - Database connection object
     * @param {Object} rs - Result set from the insert operation
     * @returns {number} The ID of the last inserted row
     * @throws {Error} If the database doesn't support auto-incrementing IDs
     */
    _getLastInsertedId(conn, rs) {
        return rs.insertId;
    }

    /**
     * Insert one task row inside an already-open transaction.
     *
     * Connection-scoped on purpose: every public entry point (`insertTask`,
     * `updateTaskStatus`, `insertTaskEvents`) opens its own `conn.trans()`, and
     * nesting those is fatal — a single-connection wrapper (sqlite::memory:)
     * raises "cannot start a transaction within a transaction", a pooled one
     * blocks until the pool timeout ("database is locked"). Callers that must be
     * atomic therefore use this helper instead of the public API.
     *
     * @private
     * @param {object} conn - Open connection
     * @param {object} task - Task definition
     * @param {object} [options] - Insertion context (root_id / parent_id)
     * @param {number} [now] - Unix seconds
     * @returns {{taskId:number|string, name:string, event:object}}
     */
    _insertTaskRowWithConnection(conn, task, options = {}, now = Math.floor(Date.now() / 1000)) {
        // Enforces strict task definition requirements
        if (!task) {
            throw new Error('Task object is required');
        }
        if (!task.name) {
            throw new Error('Task name is required');
        }
        if (!task.type) {
            throw new Error('Task type is required');
        }
        if (options.parent_id && task.type !== 'async') {
            throw new Error('Parent tasks can only be of type "async"');
        }
        assertTaskType(task.type, 'Task type must be either "async" or "cron"');
        // Validate task status if provided
        if (task.status) {
            assertTaskStatus(task.status);
        }

        // Schedule identity: cron rows must carry a key (defaulting to
        // 'default'), async rows store NULL so they never collide in the
        // unique identity index.
        const cronKey = task.type === 'cron'
            ? BaseDBAdapter.normalizeCronKey(task.cron_key)
            : null;
        const cronTz = task.cron_tz === undefined || task.cron_tz === null
            ? null
            : assertIanaTimeZone(task.cron_tz);
        const cronCatchUp = task.cron_catch_up === undefined || task.cron_catch_up === null
            ? 'once'
            : assertCronCatchUp(task.cron_catch_up);
        const retentionExpireTime = task.retention_expire_time === undefined || task.retention_expire_time === null
            ? null
            : assertRetentionExpireTime(task.retention_expire_time);

        logger.info(`[BaseDBAdapter] Inserting task with name: ${task.name}`);
        const rs = conn.execute(
            `INSERT INTO fib_flow_tasks (
                    name, type, status, priority, payload, cron_expr,
                    max_retries, retry_interval, next_run_time, timeout,
                    created_at, root_id, parent_id, total_children, completed_children,
                    tag, cron_key, cron_tz, cron_catch_up, retention_expire_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?)`,
            task.name,
            task.type,
            task.status || 'pending', // Use provided status or default to pending
            task.priority || 0,
            task.payload ? JSON.stringify(task.payload) : null,
            task.cron_expr || null,
            task.max_retries !== undefined ? task.max_retries : 3,
            task.retry_interval || 0,
            task.next_run_time || now,
            task.timeout || 60,
            now,
            options.root_id || null,
            options.parent_id || null,
            task.tag || null,
            cronKey,
            cronTz,
            cronCatchUp,
            retentionExpireTime
        );

        const taskId = this._getLastInsertedId(conn, rs);

        return {
            taskId,
            name: task.name,
            event: {
                task_id: taskId,
                root_id: normalizeRootId(taskId, options.root_id || null),
                parent_id: options.parent_id || null,
                event_type: 'task_created',
                to_status: task.status || 'pending',
                stage: 0,
                event_time: now,
                message: `Task ${task.name} created`,
                metadata: {
                    name: task.name,
                    type: task.type,
                    tag: task.tag || null,
                    ...(cronKey ? { cron_key: cronKey } : {}),
                    // Set only for runs spawned by a cron schedule: the exact link
                    // between a run and the schedule that created it. Matching by
                    // name + tag would also catch a manually enqueued task, and
                    // two schedules can share both.
                    ...(options.schedule_task_id !== undefined && options.schedule_task_id !== null
                        ? { schedule_task_id: options.schedule_task_id }
                        : {})
                }
            }
        };
    }

    /**
     * Resolve which schedule spawned a run row, if any.
     *
     * The link is the audit trail: `spawnCronRun` records `spawned_task_id` on the
     * schedule's `task_cron_spawned` event. Matching by `name` + `tag` is wrong
     * because a manually enqueued task shares both with the schedule's own runs
     * (they use one handler name), and two schedules can share `name` + `tag` and
     * differ only by `cron_key`.
     *
     * `root_id` / `parent_id` are not usable either: `parent_id` would turn the
     * schedule into a suspended workflow parent, and `root_id` would make
     * `getWorkflowAuditSummary(scheduleId)` swallow every historical round.
     *
     * @private
     * @param {object} conn - Open connection
     * @param {string|number} runTaskId - Run row id
     * @returns {object|null} `{ id, name, cron_key, root_id, parent_id, stage, error, status }`
     */
    _findScheduleForRunWithConnection(conn, runTaskId) {
        // The link lives on the run's own `task_created` event, written by
        // `spawnCronRun`. Reading it by `task_id` uses
        // `idx_task_events_task_time`, so this is an exact single-row lookup
        // instead of a scan over recent spawn events.
        const linkRows = conn.execute(
            `SELECT metadata
               FROM fib_flow_task_events
              WHERE task_id = ?
                AND event_type = 'task_created'
              ORDER BY id DESC
              LIMIT 1`,
            runTaskId
        );

        let scheduleId = null;
        if (linkRows.length > 0) {
            let metadata = linkRows[0].metadata;
            if (typeof metadata === 'string') {
                try {
                    metadata = JSON.parse(metadata);
                } catch (_error) {
                    metadata = null;
                }
            }
            const linked = metadata ? metadata.schedule_task_id : null;
            if (linked !== undefined && linked !== null) {
                scheduleId = linked;
            }
        }

        if (scheduleId === null) {
            return null;
        }

        const scheduleRows = conn.execute(
            `SELECT id, name, cron_key, root_id, parent_id, stage, error, status
               FROM fib_flow_tasks
              WHERE id = ? AND type = 'cron'`,
            scheduleId
        );

        return scheduleRows.length > 0 ? scheduleRows[0] : null;
    }

    /**
     * Persist the per-schedule `maxInstances` policy.
     *
     * Stored in `fib_flow_meta` (a key-value table that already exists) rather than
     * in a task column: a new column would change the schema version and force
     * every deployment to rebuild its tables. Keyed by the schedule identity, so a
     * restart — or any tick that runs before the caller re-registers — still sees
     * the value the caller asked for.
     *
     * @param {string} scheduleName - Schedule name
     * @param {string} cronKey - Schedule identity key
     * @param {number|null} maxInstances - Cap; `null` means unlimited
     * @returns {void}
     */
    setScheduleMaxInstances(scheduleName, cronKey, maxInstances) {
        const name = normalizeTrimmedString(scheduleName, 'schedule name');
        const key = normalizeTrimmedString(cronKey, 'cronKey');
        const metaKey = buildScheduleMetaKey('max_instances', name, key);
        const value = maxInstances === null || maxInstances === undefined
            ? 'null'
            : String(maxInstances);

        // `meta_key` is the primary key, so a plain upsert would need ON CONFLICT /
        // ON DUPLICATE KEY UPDATE — not portable across the three engines. The
        // competing-insert path is: write, and if the row appears underneath us
        // (another process created it between our check and our insert, reachable on
        // PostgreSQL / MySQL at READ COMMITTED during a multi-pod boot), retry once
        // with the update.
        //
        // Deliberately NOT wrapped in `conn.trans()`: on PostgreSQL a failed
        // statement aborts the surrounding transaction, so a recovery statement in
        // the catch block would fail too ("current transaction is aborted"). A
        // single-row upsert needs no atomicity beyond each statement's own.
        const attemptWrite = (conn) => {
            const existing = conn.execute(
                `SELECT meta_value FROM fib_flow_meta WHERE meta_key = ?`,
                metaKey
            );

            // Unchanged: skip the write entirely so a re-registration does not touch
            // the row.
            if (existing.length > 0 && String(existing[0].meta_value) === value) {
                return true;
            }

            const updated = conn.execute(
                `UPDATE fib_flow_meta SET meta_value = ? WHERE meta_key = ?`,
                value,
                metaKey
            );

            if (updated && updated.affected) {
                return true;
            }

            try {
                conn.execute(
                    `INSERT INTO fib_flow_meta (meta_key, meta_value) VALUES (?, ?)`,
                    metaKey,
                    value
                );
                return true;
            } catch (_conflict) {
                // Lost the race: the row exists now, so the next pass takes the
                // update branch.
                return false;
            }
        };

        this.pool(conn => {
            if (attemptWrite(conn)) {
                return;
            }

            if (attemptWrite(conn)) {
                return;
            }

            // Two consecutive losses means something other than a simple insert race
            // (the row is being churned concurrently). Report it rather than spinning.
            throw new Error(`Could not persist maxInstances for schedule ${name}/${key}`);
        });
    }

    /**
     * Read one persisted `maxInstances` policy.
     * @param {string} scheduleName - Schedule name
     * @param {string} cronKey - Schedule identity key
     * @returns {number|null|undefined} Cap, `null` for unlimited, `undefined` when
     *          the schedule has no persisted policy
     */
    getScheduleMaxInstances(scheduleName, cronKey) {
        const metaKey = buildScheduleMetaKey('max_instances', scheduleName, cronKey);
        const rows = this.pool(conn => conn.execute(
            `SELECT meta_value FROM fib_flow_meta WHERE meta_key = ?`,
            metaKey
        ));

        if (rows.length === 0) {
            return undefined;
        }

        const raw = String(rows[0].meta_value);
        if (raw === 'null') {
            return null;
        }

        const parsed = Number(raw);
        return Number.isFinite(parsed) ? parsed : undefined;
    }

    /**
     * Count the run rows a schedule produced that are still in flight.
     *
     * Spawn mode removes the mutual exclusion inline mode had for free: an inline
     * cron row is `running` and therefore not claimable, so rounds cannot overlap.
     * Without this count a handler slower than its cron period accumulates
     * overlapping runs forever, and those `pending` / `running` rows never reach a
     * terminal state — so retention never collects them either.
     *
     * Only runs this schedule actually spawned are counted. A manually enqueued
     * task shares `name` and `tag` with them (one handler name) but is not this
     * schedule's round, and a long manual run must not starve the tick.
     *
     * @param {string|number} scheduleId - Schedule row id
     * @returns {number} Number of in-flight runs spawned by this schedule
     */
    countScheduleRunsInFlight(scheduleId) {
        if (scheduleId === undefined || scheduleId === null) {
            throw new Error('countScheduleRunsInFlight requires a schedule id');
        }

        return this.pool(conn => {
            // Two index-bounded steps instead of one table scan:
            //
            // 1. the runs this schedule spawned, from its own `task_cron_spawned`
            //    events (bounded by the round-event cap, and read through
            //    `idx_task_events_task_time`);
            // 2. how many of those ids are still in flight (primary-key lookup).
            //
            // The alternative — scanning the in-flight set of *every* handler and
            // filtering afterwards — is linear in the global queue length, on every
            // tick, for every schedule.
            const eventRows = conn.execute(
                `SELECT metadata
                   FROM fib_flow_task_events
                  WHERE task_id = ?
                    AND event_type = 'task_cron_spawned'
                  ORDER BY id DESC
                  LIMIT ?`,
                scheduleId,
                CRON_ROUND_EVENT_HISTORY
            );

            const runIds = [];
            for (const row of eventRows) {
                let metadata = row.metadata;
                if (typeof metadata === 'string') {
                    try {
                        metadata = JSON.parse(metadata);
                    } catch (_error) {
                        metadata = null;
                    }
                }
                const spawnedId = metadata ? metadata.spawned_task_id : null;
                if (spawnedId !== undefined && spawnedId !== null) {
                    runIds.push(spawnedId);
                }
            }

            if (runIds.length === 0) {
                return 0;
            }

            const rows = conn.execute(
                `SELECT COUNT(*) AS total
                   FROM fib_flow_tasks
                  WHERE id IN ?
                    AND status IN ('pending', 'running')`,
                runIds
            );

            const counted = rows[0] ? Number(rows[0].total) : 0;

            // The round-event trail is capped, so a schedule with more than
            // CRON_ROUND_EVENT_HISTORY runs in flight at once (only reachable with
            // `maxInstances: 0`) would be undercounted. Fall back to the exact
            // form when the answer is pinned to that boundary.
            if (eventRows.length < CRON_ROUND_EVENT_HISTORY || counted < CRON_ROUND_EVENT_HISTORY) {
                return counted;
            }

            return this._countScheduleRunsInFlightExactWithConnection(conn, scheduleId);
        });
    }

    /**
     * Exact in-flight count, used when the bounded trail may have been truncated.
     *
     * Scans the in-flight set and checks each run's `schedule_task_id` marker. Only
     * reached for schedules with many concurrent runs, so the linear scan is not on
     * the common path.
     *
     * @private
     * @param {object} conn - Open connection
     * @param {string|number} scheduleId - Schedule row id
     * @returns {number} In-flight run count
     */
    _countScheduleRunsInFlightExactWithConnection(conn, scheduleId) {
        const rows = conn.execute(
            `SELECT e.task_id, e.metadata
               FROM fib_flow_task_events e
               JOIN fib_flow_tasks t ON t.id = e.task_id
              WHERE e.event_type = 'task_created'
                AND t.status IN ('pending', 'running')`
        );

        let total = 0;
        for (const row of rows) {
            let metadata = row.metadata;
            if (typeof metadata === 'string') {
                try {
                    metadata = JSON.parse(metadata);
                } catch (_error) {
                    metadata = null;
                }
            }
            const linked = metadata ? metadata.schedule_task_id : null;
            if (linked !== undefined && linked !== null && String(linked) === String(scheduleId)) {
                total += 1;
            }
        }

        return total;
    }

    /**
     * Mirror a run's outcome onto the schedule that spawned it.
     *
     * Inline mode reported failures on the schedule row itself because the row was
     * the execution. Spawn mode moved execution to a separate row, so without this
     * the schedule would stay a pristine `pending` with `error = NULL` while every
     * round failed — an observability regression: a consumer reading
     * `getCronSchedules()[].last_error` would show "active, no errors" forever.
     *
     * Only runs this schedule actually spawned report back. A manually enqueued
     * task shares the handler name, so matching by `name` + `tag` would let a user
     * clicking "run now" rewrite the schedule's health.
     *
     * Only `error` is written; the status is untouched, because a spawn round still
     * advances `next_run_time`.
     *
     * @param {object} params - Outcome parameters
     * @param {string|number} params.runTaskId - Finished run row id
     * @param {string|null} [params.error] - Failure text; `null` clears it
     * @returns {number} Number of schedule rows updated
     */
    recordScheduleRunOutcome({ runTaskId, error }) {
        if (runTaskId === undefined || runTaskId === null) {
            throw new Error('recordScheduleRunOutcome requires a run task id');
        }

        const message = error === undefined || error === null || error === ''
            ? null
            : String(error);

        let updated = 0;
        this.pool(conn => conn.trans(() => {
            const schedule = this._findScheduleForRunWithConnection(conn, runTaskId);
            if (!schedule) {
                // Not a spawned run: a manual enqueue, a subtask, or an event that
                // has already been trimmed. Nothing to report against.
                return;
            }

            // Only a live schedule owns a health signal. A cancelled or paused row
            // is history: rewriting its `error` and refreshing `last_active_time`
            // would both falsify the record and postpone its retention. `running`
            // is allowed because a run can finish while the next round is being
            // claimed.
            if (schedule.status !== 'pending' && schedule.status !== 'running') {
                return;
            }

            const now = Math.floor(Date.now() / 1000);
            const previous = schedule.error || null;
            if (previous === message) {
                return;
            }

            conn.execute(
                `UPDATE fib_flow_tasks SET error = ?, last_active_time = ? WHERE id = ?`,
                message,
                now,
                schedule.id
            );
            updated = 1;

            this._insertTaskEventsWithConnection(conn, [{
                task_id: schedule.id,
                root_id: normalizeRootId(schedule.id, schedule.root_id),
                parent_id: schedule.parent_id || null,
                event_type: message === null ? 'task_cron_run_recovered' : 'task_cron_run_failed',
                from_status: schedule.status,
                to_status: schedule.status,
                stage: schedule.stage,
                event_time: now,
                message: message === null
                    ? `Cron task ${schedule.name} recovered: latest run succeeded`
                    : `Cron task ${schedule.name} latest run failed: ${message}`,
                metadata: {
                    spawned_task_id: runTaskId,
                    ...(message === null ? {} : { error: message })
                }
            }]);

            this._pruneCronRoundEventsWithConnection(conn, schedule.id);
        }));

        return updated;
    }

    /**
     * Advance a schedule past a round that was skipped because too many runs are
     * still in flight, without creating a run row.
     *
     * Mirrors `failCronSpawn` (the schedule must never stay `running`) but records
     * a distinct event and does not write `error`: an overlap skip is expected
     * behaviour, not a failure, and writing it to `error` would make a healthy
     * schedule look broken.
     *
     * @param {object} params - Skip parameters
     * @param {object} params.schedule - Claimed cron row
     * @param {number} params.next_run_time - Next instant for the schedule
     * @param {number} params.in_flight - Runs still in flight when the round was skipped
     * @returns {{next_run_time:number}}
     */
    skipCronRoundForOverlap({ schedule, next_run_time, in_flight }) {
        if (!schedule || schedule.id === undefined || schedule.id === null) {
            throw new Error('skipCronRoundForOverlap requires a schedule row');
        }
        const nextRunTime = Number(next_run_time);
        if (!Number.isFinite(nextRunTime) || nextRunTime <= 0) {
            throw new Error('skipCronRoundForOverlap requires a positive next_run_time');
        }

        this.pool(conn => conn.trans(() => {
            const now = Math.floor(Date.now() / 1000);

            const advance = conn.execute(
                `UPDATE fib_flow_tasks
                    SET status = 'pending',
                        stage = 0,
                        retry_count = 0,
                        next_run_time = ?,
                        last_active_time = ?,
                        worker_id = NULL,
                        start_time = NULL
                  WHERE id = ? AND type = 'cron'`,
                nextRunTime,
                now,
                schedule.id
            );

            if (advance.affected !== 1) {
                throw new Error(`Cron schedule ${schedule.id} disappeared before the overlap skip could be recorded`);
            }

            this._finishOpenTaskAttemptWithConnection(conn, schedule.id, {
                ended_at: now,
                outcome: 'skipped',
                timeout_flag: false,
                error: null
            });

            this._insertTaskEventsWithConnection(conn, [{
                task_id: schedule.id,
                root_id: normalizeRootId(schedule.id, schedule.root_id),
                parent_id: schedule.parent_id || null,
                event_type: 'task_cron_overlap_skipped',
                from_status: schedule.status || 'running',
                to_status: 'pending',
                stage: 0,
                worker_id: schedule.worker_id || null,
                event_time: now,
                message: `Cron task ${schedule.name} skipped this window: ${in_flight} run(s) still in flight`,
                metadata: {
                    in_flight: Number(in_flight) || 0,
                    scheduled_run_time: schedule.next_run_time || null,
                    next_run_time: nextRunTime
                }
            }]);

            this._pruneCronRoundEventsWithConnection(conn, schedule.id);
        }));

        return { next_run_time: nextRunTime };
    }

    /**
     * Spawn one run row for a cron schedule and advance the schedule, atomically.
     *
     * This is the whole point of spawn mode: "insert the run row" and "move the
     * schedule forward" happen in ONE transaction, so a crash in between cannot
     * produce a second run for the same window on the next tick. Nothing here
     * touches the user handler — the cron row never executes business code.
     *
     * @param {object} params - Spawn parameters
     * @param {object} params.schedule - Claimed cron row (parsed task)
     * @param {number} params.next_run_time - Next instant for the schedule
     * @param {object|null} [params.payload] - Run payload; defaults to the schedule payload
     * @param {string} [params.trigger] - 'cron' or 'manual' (recorded on the event)
     * @param {boolean} [params.advance=true] - Advance the schedule row. Manual
     *        triggers pass `false`: they must not move `next_run_time`, reset the
     *        retry budget, or revive a cancelled/paused schedule.
     * @returns {{taskId:number|string, next_run_time:number}} Spawn result
     */
    spawnCronRun({ schedule, next_run_time, payload, trigger = 'cron', advance = true }) {
        if (!schedule || schedule.id === undefined || schedule.id === null) {
            throw new Error('spawnCronRun requires a schedule row');
        }
        const shouldAdvance = advance !== false;
        const nextRunTime = Number(next_run_time);
        if (shouldAdvance && (!Number.isFinite(nextRunTime) || nextRunTime <= 0)) {
            throw new Error('spawnCronRun requires a positive next_run_time');
        }

        let result = null;
        this.pool(conn => conn.trans(() => {
            const now = Math.floor(Date.now() / 1000);

            // The schedule payload is copied verbatim. Shaping it into the
            // handler's expected input is the caller's job: the engine has no
            // idea what the business payload looks like.
            const runPayload = payload === undefined ? schedule.payload : payload;
            const inserted = this._insertTaskRowWithConnection(conn, {
                name: schedule.name,
                type: 'async',
                payload: runPayload,
                priority: schedule.priority,
                tag: schedule.tag || null,
                timeout: schedule.timeout,
                max_retries: schedule.max_retries,
                retry_interval: schedule.retry_interval,
                // A run lives inside the schedule's retention window, mirroring
                // the child-task inheritance rule (insertTask parent handling).
                retention_expire_time: schedule.retention_expire_time || null,
                next_run_time: now
            }, {
                // Exact run -> schedule link, carried on the run's own
                // `task_created` event. Neither scheduling column works:
                // `parent_id` would suspend the schedule into a workflow parent,
                // and `root_id` would make `getWorkflowAuditSummary(scheduleId)`
                // swallow every historical round.
                schedule_task_id: schedule.id
            }, now);

            if (shouldAdvance) {
                // Reset the schedule row only. `error` is deliberately NOT cleared
                // here: clearing at spawn time means the signal is empty for the
                // whole duration of a slow run, so a failing schedule looks healthy
                // two thirds of the time (measured 18/24 samples with a 2.5s
                // handler). It is cleared by `recordScheduleRunOutcome` when a run
                // actually succeeds.
                //
                // `result` IS cleared, but only because a value can only be an
                // inline-era leftover: spawn mode never writes it, so whatever is
                // there was produced by the round that ran the handler in place.
                // Leaving it would make the schedule report a previous execution's
                // result while the run rows report the current ones.
                const advanceResult = conn.execute(
                    `UPDATE fib_flow_tasks
                        SET status = 'pending',
                            stage = 0,
                            retry_count = 0,
                            result = NULL,
                            next_run_time = ?,
                            last_active_time = ?,
                            worker_id = NULL,
                            start_time = NULL
                      WHERE id = ? AND type = 'cron'`,
                    nextRunTime,
                    now,
                    schedule.id
                );

                if (advanceResult.affected !== 1) {
                    throw new Error(`Cron schedule ${schedule.id} disappeared before it could be advanced`);
                }

                // The claim attempt (if this round was claimed) is closed here with
                // a dedicated outcome: reporting 'completed' would make a spawn
                // round indistinguishable from an executed one when triaging.
                this._finishOpenTaskAttemptWithConnection(conn, schedule.id, {
                    ended_at: now,
                    outcome: 'spawned',
                    timeout_flag: false,
                    error: null
                });
            }

            this._insertTaskEventsWithConnection(conn, [
                inserted.event,
                {
                    task_id: schedule.id,
                    root_id: normalizeRootId(schedule.id, schedule.root_id),
                    parent_id: schedule.parent_id || null,
                    event_type: 'task_cron_spawned',
                    from_status: shouldAdvance ? (schedule.status || 'running') : schedule.status || null,
                    to_status: shouldAdvance ? 'pending' : schedule.status || null,
                    stage: 0,
                    worker_id: schedule.worker_id || null,
                    event_time: now,
                    message: `Cron task ${schedule.name} spawned run ${inserted.taskId}`,
                    metadata: {
                        spawned_task_id: inserted.taskId,
                        scheduled_run_time: schedule.next_run_time || null,
                        next_run_time: shouldAdvance ? nextRunTime : (schedule.next_run_time || null),
                        trigger,
                        advanced: shouldAdvance
                    }
                }
            ]);

            // Bounded round history: the run rows carry the full trail. Applied
            // for manual triggers too — they add a round event each call and the
            // schedule row is never collected by retention.
            this._pruneCronRoundEventsWithConnection(conn, schedule.id);

            result = {
                taskId: inserted.taskId,
                next_run_time: shouldAdvance ? nextRunTime : (schedule.next_run_time || null)
            };
        }));

        return result;
    }

    /**
     * Advance a cron schedule past a failed spawn without creating a run row.
     *
     * Never leaves the row in `running`: that would stall the schedule forever,
     * which is the exact failure spawn mode exists to remove. The error stays
     * visible through `getCronSchedules().last_error`.
     *
     * @param {object} params - Failure parameters
     * @param {object} params.schedule - Claimed cron row
     * @param {number} params.next_run_time - Next instant for the schedule
     * @param {string} params.error - Failure text recorded on the row
     * @returns {{next_run_time:number}}
     */
    failCronSpawn({ schedule, next_run_time, error }) {
        if (!schedule || schedule.id === undefined || schedule.id === null) {
            throw new Error('failCronSpawn requires a schedule row');
        }
        const nextRunTime = Number(next_run_time);
        if (!Number.isFinite(nextRunTime) || nextRunTime <= 0) {
            throw new Error('failCronSpawn requires a positive next_run_time');
        }
        const message = error === undefined || error === null || error === ''
            ? 'Cron spawn failed'
            : String(error);

        this.pool(conn => conn.trans(() => {
            const now = Math.floor(Date.now() / 1000);

            const advance = conn.execute(
                `UPDATE fib_flow_tasks
                    SET status = 'pending',
                        stage = 0,
                        retry_count = 0,
                        error = ?,
                        next_run_time = ?,
                        last_active_time = ?,
                        worker_id = NULL,
                        start_time = NULL
                  WHERE id = ? AND type = 'cron'`,
                message,
                nextRunTime,
                now,
                schedule.id
            );

            if (advance.affected !== 1) {
                throw new Error(`Cron schedule ${schedule.id} disappeared before the failure could be recorded`);
            }

            this._finishOpenTaskAttemptWithConnection(conn, schedule.id, {
                ended_at: now,
                outcome: 'spawn_failed',
                timeout_flag: false,
                error: message
            });

            this._insertTaskEventsWithConnection(conn, [{
                task_id: schedule.id,
                root_id: normalizeRootId(schedule.id, schedule.root_id),
                parent_id: schedule.parent_id || null,
                event_type: 'task_cron_spawn_failed',
                from_status: schedule.status || 'running',
                to_status: 'pending',
                stage: 0,
                worker_id: schedule.worker_id || null,
                event_time: now,
                message: `Cron task ${schedule.name} failed to spawn a run: ${message}`,
                metadata: {
                    error: message,
                    scheduled_run_time: schedule.next_run_time || null,
                    next_run_time: nextRunTime
                }
            }]);

            this._pruneCronRoundEventsWithConnection(conn, schedule.id);
        }));

        return { next_run_time: nextRunTime };
    }

    /**
     * Manages task insertion with comprehensive validation and workflow tracking.
     * Supports atomic insertion of single or multiple tasks with parent-child relationships.
     *
     * @param {object|Array<object>} tasks - Tasks to be inserted
     * @param {object} [options] - Insertion context and workflow metadata
     * @returns {number|Array<number>} Assigned task ID(s)
     * @throws {Error} If tasks fail validation or workflow constraints
     */
    insertTask(tasks, options = {}) {
        logger.info(`[BaseDBAdapter] Inserting tasks with options:`, options);
        const isArray = Array.isArray(tasks);
        const taskArray = isArray ? tasks : [tasks];

        const taskIds = [];
        this.pool(conn => conn.trans(() => {
            const now = Math.floor(Date.now() / 1000);
            const events = [];
            let parentTask = null;
            const childTaskNames = [];

            // Updates parent task state to reflect child task creation
            if (options.parent_id) {
                logger.info(`[BaseDBAdapter] Updating parent task ${options.parent_id}`);

                const parentRs = conn.execute(
                    `SELECT id, name, status, stage, retry_count, root_id, parent_id, worker_id
                     FROM fib_flow_tasks WHERE id = ?`,
                    options.parent_id
                );

                if (parentRs.length === 0) {
                    throw new Error(`Parent task ${options.parent_id} not found`);
                }

                parentTask = parentRs[0];

                let rs;
                // Check if context update is needed
                if (options.context !== undefined) {
                    logger.info(`[BaseDBAdapter] Updating parent task ${options.parent_id} with context data`);
                    const params = [taskArray.length, options.context, options.parent_id];
                    let sql = `UPDATE fib_flow_tasks 
                             SET total_children = total_children + ?,
                                 status = 'suspended',
                                 context = ?,
                                 result = null
                             WHERE id = ? AND status = 'running'`;
                    if (options.worker_id) {
                        sql += ' AND worker_id = ?';
                        params.push(options.worker_id);
                    }
                    rs = conn.execute(
                        sql,
                        ...params
                    );
                } else {
                    const params = [taskArray.length, options.parent_id];
                    let sql = `UPDATE fib_flow_tasks 
                             SET total_children = total_children + ?,
                                 status = 'suspended',
                                 result = null
                             WHERE id = ? AND status = 'running'`;
                    if (options.worker_id) {
                        sql += ' AND worker_id = ?';
                        params.push(options.worker_id);
                    }
                    rs = conn.execute(
                        sql,
                        ...params
                    );
                }

                if (rs.affected === 0) {
                    throw new Error(`Parent task ${options.parent_id} is not owned by worker ${options.worker_id || 'unknown'} or is not in running state`);
                }

                this._finishOpenTaskAttemptWithConnection(conn, parentTask.id, {
                    ended_at: now,
                    outcome: 'suspended',
                    timeout_flag: false
                });
            }

            for (const task of taskArray) {
                const inserted = this._insertTaskRowWithConnection(conn, task, options, now);
                taskIds.push(inserted.taskId);
                childTaskNames.push(inserted.name);
                events.push(inserted.event);
            }

            if (parentTask && taskIds.length > 0) {
                events.push({
                    task_id: parentTask.id,
                    root_id: normalizeRootId(parentTask.id, parentTask.root_id),
                    parent_id: parentTask.parent_id || null,
                    event_type: 'task_subtasks_created',
                    from_status: parentTask.status,
                    to_status: 'suspended',
                    stage: parentTask.stage,
                    worker_id: parentTask.worker_id || null,
                    event_time: now,
                    message: `Task ${parentTask.name} created ${taskIds.length} subtasks`,
                    metadata: {
                        child_task_ids: taskIds,
                        child_task_names: childTaskNames,
                        child_count: taskIds.length
                    }
                });
                events.push({
                    task_id: parentTask.id,
                    root_id: normalizeRootId(parentTask.id, parentTask.root_id),
                    parent_id: parentTask.parent_id || null,
                    event_type: 'task_status_changed',
                    from_status: parentTask.status,
                    to_status: 'suspended',
                    stage: parentTask.stage,
                    worker_id: parentTask.worker_id || null,
                    event_time: now,
                    message: `Task ${parentTask.name} suspended while waiting for child tasks`,
                    metadata: {
                        suspend_reason: 'awaiting_subtasks',
                        child_count: taskIds.length,
                        child_task_ids: taskIds
                    }
                });
            }

            if (events.length > 0) {
                this._insertTaskEventsWithConnection(conn, events);
            }
        }));

        return isArray ? taskIds : taskIds[0];
    }

    /**
     * Resolve the cron identity key for a schedule definition.
     * @param {string} [cronKey] - Caller supplied key
     * @returns {string} Non-empty identity key
     */
    static normalizeCronKey(cronKey) {
        return cronKey === undefined || cronKey === null
            ? 'default'
            : assertCronKey(cronKey);
    }

    /**
     * Create or update a cron schedule identified by (name, cron_key).
     *
     * The unique index ux_task_cron_identity is the authority: a concurrent
     * caller that inserts first wins and the loser falls back to an update.
     * `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` are not portable across the three
     * supported engines, so the competing-insert path is the portable one.
     *
     * @param {object} schedule - Schedule definition
     * @param {object} [options] - Insertion context (root_id / parent_id)
     * @returns {{task:object, outcome:'created'|'updated'|'revived'}}
     */
    upsertCronTask(schedule, options = {}) {
        if (!schedule || typeof schedule !== 'object') {
            throw new Error('Schedule definition is required');
        }
        if (!schedule.name) {
            throw new Error('Task name is required');
        }
        if (!schedule.cron_expr) {
            throw new Error('cron_expr is required for a cron schedule');
        }

        const cronKey = BaseDBAdapter.normalizeCronKey(schedule.cron_key);
        const cronTz = schedule.cron_tz === undefined || schedule.cron_tz === null
            ? null
            : assertIanaTimeZone(schedule.cron_tz);
        const cronCatchUp = assertCronCatchUp(schedule.cron_catch_up || 'once');
        const retentionExpireTime = schedule.retention_expire_time === undefined || schedule.retention_expire_time === null
            ? null
            : assertRetentionExpireTime(schedule.retention_expire_time);
        const now = Math.floor(Date.now() / 1000);

        let result = null;

        const applyUpdate = (conn, existing) => {
            const scheduleChanged = existing.cron_expr !== schedule.cron_expr
                || (existing.cron_tz || null) !== cronTz;

            let nextRunTime = existing.next_run_time;
            if (scheduleChanged) {
                nextRunTime = schedule.next_run_time !== undefined && schedule.next_run_time !== null
                    ? schedule.next_run_time
                    : existing.next_run_time;
            }

            // Only rows that can still run are revived. failed/timeout rows keep
            // their retry budget so an in-flight retry chain is not reset.
            const shouldRevive = existing.status === 'paused' || existing.status === 'cancelled';
            const status = shouldRevive ? 'pending' : existing.status;
            const retryCount = shouldRevive ? 0 : existing.retry_count;
            const error = shouldRevive ? null : existing.error;

            conn.execute(
                `UPDATE fib_flow_tasks
                    SET cron_expr = ?,
                        cron_tz = ?,
                        cron_catch_up = ?,
                        retention_expire_time = ?,
                        payload = ?,
                        priority = ?,
                        tag = ?,
                        max_retries = ?,
                        retry_interval = ?,
                        timeout = ?,
                        status = ?,
                        retry_count = ?,
                        error = ?,
                        next_run_time = ?,
                        last_active_time = ?
                  WHERE id = ?`,
                schedule.cron_expr,
                cronTz,
                cronCatchUp,
                retentionExpireTime,
                schedule.payload ? JSON.stringify(schedule.payload) : null,
                schedule.priority === undefined || schedule.priority === null ? existing.priority : schedule.priority,
                schedule.tag === undefined ? existing.tag : schedule.tag,
                schedule.max_retries === undefined ? existing.max_retries : schedule.max_retries,
                schedule.retry_interval === undefined ? existing.retry_interval : schedule.retry_interval,
                schedule.timeout === undefined ? existing.timeout : schedule.timeout,
                status,
                retryCount,
                error,
                nextRunTime,
                existing.status === 'running' ? existing.last_active_time : now,
                existing.id
            );

            const events = [];
            if (shouldRevive) {
                events.push({
                    task_id: existing.id,
                    root_id: normalizeRootId(existing.id, existing.root_id),
                    parent_id: existing.parent_id || null,
                    event_type: 'task_resumed',
                    from_status: existing.status,
                    to_status: 'pending',
                    stage: existing.stage || 0,
                    worker_id: existing.worker_id || null,
                    event_time: now,
                    message: `Cron task ${existing.name} revived by schedule re-registration`,
                    metadata: { resume_reason: 'schedule_reregistered', cron_key: cronKey }
                });
            }
            if (scheduleChanged) {
                events.push({
                    task_id: existing.id,
                    root_id: normalizeRootId(existing.id, existing.root_id),
                    parent_id: existing.parent_id || null,
                    event_type: 'task_cron_rescheduled',
                    from_status: existing.status,
                    to_status: status,
                    stage: existing.stage || 0,
                    worker_id: existing.worker_id || null,
                    event_time: now,
                    message: `Cron task ${existing.name} rescheduled`,
                    metadata: {
                        cron_key: cronKey,
                        previous_cron_expr: existing.cron_expr,
                        cron_expr: schedule.cron_expr,
                        previous_cron_tz: existing.cron_tz || null,
                        cron_tz: cronTz,
                        previous_next_run_time: existing.next_run_time,
                        next_run_time: nextRunTime
                    }
                });
            }
            if (events.length > 0) {
                this._insertTaskEventsWithConnection(conn, events);
            }

            const rows = conn.execute('SELECT * FROM fib_flow_tasks WHERE id = ?', existing.id);
            const task = parseTask(rows[0]);
            task.outcome = shouldRevive ? 'revived' : 'updated';
            return task;
        };

        this.pool(conn => conn.trans(() => {
            const rows = conn.execute(
                `SELECT * FROM fib_flow_tasks
                  WHERE type = 'cron' AND name = ? AND cron_key = ?`,
                schedule.name,
                cronKey
            );

            if (rows.length > 0) {
                result = applyUpdate(conn, rows[0]);
            }
        }));

        if (result) {
            return result;
        }

        // No row yet: insert outside the transaction above. insertTask() opens its
        // own pooled transaction, so nesting it here would deadlock the single
        // connection wrapper used by sqlite::memory:.
        const payload = {
            name: schedule.name,
            type: 'cron',
            cron_expr: schedule.cron_expr,
            cron_key: cronKey,
            cron_tz: cronTz,
            cron_catch_up: cronCatchUp,
            retention_expire_time: retentionExpireTime,
            payload: schedule.payload,
            priority: schedule.priority,
            tag: schedule.tag,
            max_retries: schedule.max_retries,
            retry_interval: schedule.retry_interval,
            timeout: schedule.timeout,
            next_run_time: schedule.next_run_time || now
        };

        let insertId = null;
        try {
            insertId = this.insertTask(payload, options);
        } catch (error) {
            // A concurrent caller may have won the identity race. The driver error
            // is opaque on PostgreSQL ([20024] Exception occurred.), so the identity
            // is re-read instead of matching engine-specific message text.
            const raced = this.pool(conn => conn.execute(
                `SELECT id FROM fib_flow_tasks
                  WHERE type = 'cron' AND name = ? AND cron_key = ?`,
                schedule.name,
                cronKey
            ));

            if (raced.length === 0) {
                throw error;
            }
        }

        if (insertId === null) {
            this.pool(conn => conn.trans(() => {
                const raced = conn.execute(
                    `SELECT * FROM fib_flow_tasks
                      WHERE type = 'cron' AND name = ? AND cron_key = ?`,
                    schedule.name,
                    cronKey
                );

                if (raced.length === 0) {
                    throw new Error(`Failed to create or update cron schedule ${schedule.name}/${cronKey}`);
                }

                result = applyUpdate(conn, raced[0]);
            }));

            return result;
        }

        const created = this.getTask(insertId);
        created.outcome = 'created';
        return created;
    }

    /**
     * Update only the scheduling fields of an existing cron row.
     * Deliberately does NOT reuse updateTaskStatus(): that path rewrites result,
     * resets stage and enforces the status transition table.
     *
     * @param {string|number} taskId - Task id
     * @param {object} updates - Fields to update
     * @returns {object|null} Updated task snapshot
     */
    updateCronSchedule(taskId, updates = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        const fields = [];
        const params = [];

        if (updates.cron_expr !== undefined) {
            fields.push('cron_expr = ?');
            params.push(updates.cron_expr);
        }
        if (updates.cron_tz !== undefined) {
            fields.push('cron_tz = ?');
            params.push(updates.cron_tz === null ? null : assertIanaTimeZone(updates.cron_tz));
        }
        if (updates.cron_catch_up !== undefined) {
            fields.push('cron_catch_up = ?');
            params.push(assertCronCatchUp(updates.cron_catch_up));
        }
        if (updates.next_run_time !== undefined) {
            fields.push('next_run_time = ?');
            params.push(updates.next_run_time);
        }
        if (updates.retention_expire_time !== undefined) {
            fields.push('retention_expire_time = ?');
            params.push(updates.retention_expire_time === null ? null : assertRetentionExpireTime(updates.retention_expire_time));
        }

        if (fields.length === 0) {
            return this.getTask(taskId);
        }

        this.pool(conn => conn.execute(
            `UPDATE fib_flow_tasks SET ${fields.join(', ')} WHERE id = ? AND type = 'cron'`,
            ...params,
            taskId
        ));

        return this.getTask(taskId);
    }

    /**
     * Read the persisted scheduling fields for a task.
     * Used by the completion path so a changed cron expression takes effect on
     * the round that just finished, rather than one round later.
     * @param {string|number} taskId - Task id
     * @returns {{cron_expr:string|null, cron_tz:string|null, cron_catch_up:string}|null}
     */
    getCronScheduleState(taskId) {
        return this.pool(conn => {
            const rows = conn.execute(
                'SELECT cron_expr, cron_tz, cron_catch_up, retry_count FROM fib_flow_tasks WHERE id = ?',
                taskId
            );
            return rows.length > 0 ? rows[0] : null;
        });
    }

    /**
     * Insert one audit event.
     * @param {object} event - Audit event to persist
     * @returns {number} Inserted event id
     */
    insertTaskEvent(event) {
        return this.insertTaskEvents([event])[0];
    }

    /**
     * Insert multiple audit events in one transaction.
     * @param {Array<object>} events - Audit events to persist
     * @returns {Array<number>} Inserted event ids
     */
    insertTaskEvents(events) {
        if (!Array.isArray(events) || events.length === 0) {
            return [];
        }

        let eventIds = [];
        this.pool(conn => conn.trans(() => {
            eventIds = this._insertTaskEventsWithConnection(conn, events);
        }));
        return eventIds;
    }

    _insertTaskEventsWithConnection(conn, events) {
        const eventIds = [];

        for (const event of events) {
            if (!event || !event.task_id) {
                throw new Error('Task event task_id is required');
            }
            if (!event.event_type) {
                throw new Error('Task event event_type is required');
            }

            const eventTime = event.event_time || Math.floor(Date.now() / 1000);
            const rs = conn.execute(
                `INSERT INTO fib_flow_task_events (
                        task_id, root_id, parent_id, event_type,
                        from_status, to_status, stage, worker_id,
                        attempt, event_time, message, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                event.task_id,
                event.root_id || null,
                event.parent_id || null,
                event.event_type,
                event.from_status || null,
                event.to_status || null,
                event.stage !== undefined ? event.stage : null,
                event.worker_id || null,
                event.attempt !== undefined ? event.attempt : null,
                eventTime,
                event.message || null,
                serializeEventMetadata(event.metadata)
            );

            conn.execute(
                `UPDATE fib_flow_tasks
                    SET last_event_time = ?,
                        last_event_type = ?
                  WHERE id = ?`,
                eventTime,
                event.event_type,
                event.task_id
            );

            eventIds.push(this._getLastInsertedId(conn, rs));
        }

        return eventIds;
    }

    _startTaskAttemptWithConnection(conn, attempt) {
        const rs = conn.execute(
            `INSERT INTO fib_flow_task_attempts (
                    task_id, attempt, worker_id, started_at
                ) VALUES (?, ?, ?, ?)`,
            attempt.task_id,
            attempt.attempt,
            attempt.worker_id || null,
            attempt.started_at
        );

        return this._getLastInsertedId(conn, rs);
    }

    _getNextAttemptNumberWithConnection(conn, taskId) {
        const rs = conn.execute(
            `SELECT COALESCE(MAX(attempt), 0) AS max_attempt
             FROM fib_flow_task_attempts
             WHERE task_id = ?`,
            taskId
        );

        return (rs[0] && rs[0].max_attempt ? rs[0].max_attempt : 0) + 1;
    }

    _finishTaskAttemptWithConnection(conn, attempt) {
        return conn.execute(
            `UPDATE fib_flow_task_attempts
                SET ended_at = ?,
                    outcome = ?,
                    error = ?,
                    timeout_flag = ?
              WHERE task_id = ? AND attempt = ? AND ended_at IS NULL`,
            attempt.ended_at,
            attempt.outcome || null,
            attempt.error || null,
            Boolean(attempt.timeout_flag),
            attempt.task_id,
            attempt.attempt
        );
    }

    _finishOpenTaskAttemptWithConnection(conn, taskId, values) {
        const openAttemptRs = conn.execute(
            `SELECT attempt
             FROM fib_flow_task_attempts
             WHERE task_id = ? AND ended_at IS NULL
             ORDER BY attempt DESC
             LIMIT 1`,
            taskId
        );

        if (openAttemptRs.length === 0) {
            return null;
        }

        const attempt = openAttemptRs[0].attempt;
        this._finishTaskAttemptWithConnection(conn, {
            task_id: taskId,
            attempt,
            ...values
        });

        return attempt;
    }

    _deleteTaskAuditDataWithConnection(conn, taskIds) {
        if (!taskIds || taskIds.length === 0) {
            return {
                events_deleted: 0,
                attempts_deleted: 0
            };
        }

        const deletedEvents = conn.execute('DELETE FROM fib_flow_task_events WHERE task_id IN ?', taskIds);
        const deletedAttempts = conn.execute('DELETE FROM fib_flow_task_attempts WHERE task_id IN ?', taskIds);

        return {
            events_deleted: deletedEvents && deletedEvents.affected ? deletedEvents.affected : 0,
            attempts_deleted: deletedAttempts && deletedAttempts.affected ? deletedAttempts.affected : 0
        };
    }

    _cleanupExpiredTasksWithConnection(conn, policy) {
        const retentionPolicy = normalizeRetentionPolicy(policy);

        const emptyResult = {
            tasks_deleted: 0,
            events_deleted: 0,
            attempts_deleted: 0,
            tasks_deleted_by_row_policy: 0,
            tasks_deleted_by_global_policy: 0,
            has_more: false
        };

        // Row-level policies run even when the global window is disabled: a row
        // that carries its own retention_expire_time does not depend on the
        // manager-wide configuration. The status gate is shared with the global
        // policy: a row-level window changes *how long* a row is kept, never
        // *which states* may be dropped. Without it, a long-gap cron schedule, a
        // suspended approval or a task waiting for its retry would be deleted
        // while the runtime still needs them.
        const rowPolicyRows = conn.execute(
            `SELECT id
             FROM fib_flow_tasks
             WHERE retention_expire_time IS NOT NULL
               AND status IN ?
               AND last_active_time < ? - retention_expire_time
             ORDER BY last_active_time ASC, id ASC
             LIMIT ?`,
            retentionPolicy.statuses,
            retentionPolicy.now,
            retentionPolicy.batch_size
        );
        const rowPolicyIds = uniqueTaskIds(rowPolicyRows);

        let globalRowsToDelete = [];
        if (retentionPolicy.expire_time) {
            const remaining = retentionPolicy.batch_size - rowPolicyIds.length;
            if (remaining > 0) {
                const globalRows = conn.execute(
                    `SELECT id
                     FROM fib_flow_tasks
                     WHERE retention_expire_time IS NULL
                     AND status IN ?
                     AND last_active_time < ?
                     ORDER BY last_active_time ASC, id ASC
                     LIMIT ?`,
                    retentionPolicy.statuses,
                    retentionPolicy.now - retentionPolicy.expire_time,
                    remaining
                );
                globalRowsToDelete = uniqueTaskIds(globalRows);
            }
        }

        const expiredTaskIds = rowPolicyIds.concat(globalRowsToDelete);
        if (expiredTaskIds.length === 0) {
            return emptyResult;
        }

        const policyByTaskId = new Map();
        rowPolicyIds.forEach(taskId => policyByTaskId.set(String(taskId), 'row'));
        globalRowsToDelete.forEach(taskId => policyByTaskId.set(String(taskId), 'global'));

        const auditCleanup = this._deleteTaskAuditDataWithConnection(conn, expiredTaskIds);
        conn.execute('DELETE FROM fib_flow_tasks WHERE id IN ?', expiredTaskIds);

        this._logRetentionSummary(expiredTaskIds, retentionPolicy, policyByTaskId);

        return {
            tasks_deleted: expiredTaskIds.length,
            events_deleted: auditCleanup.events_deleted,
            attempts_deleted: auditCleanup.attempts_deleted,
            tasks_deleted_by_row_policy: rowPolicyIds.length,
            tasks_deleted_by_global_policy: globalRowsToDelete.length,
            has_more: expiredTaskIds.length === retentionPolicy.batch_size
        };
    }

    /**
     * Report what retention deleted, and under which policy.
     *
     * Retention cannot leave a persistent per-task marker: the marker's `task_id`
     * would point at a row that no longer exists, so no later sweep would ever
     * collect it and the event table would grow without bound — breaking the
     * "retention leaves no orphaned audit rows" invariant. The machine-readable
     * channel is therefore the returned counters; this method adds the human
     * channel (a structured info log) with the same grouping.
     *
     * @private
     * @param {Array<number>} taskIds - Deleted task ids
     * @param {object} retentionPolicy - Normalized retention policy
     * @param {Map<string,string>} policyByTaskId - Policy that selected each row
     */
    _logRetentionSummary(taskIds, retentionPolicy, policyByTaskId) {
        const byPolicy = { row: [], global: [] };
        taskIds.forEach(taskId => {
            const policy = policyByTaskId ? policyByTaskId.get(String(taskId)) : 'unknown';
            (byPolicy[policy] || byPolicy.global).push(taskId);
        });

        logger.info(
            `[retention] Deleted ${taskIds.length} task(s): ` +
            `${byPolicy.row.length} by row policy, ${byPolicy.global.length} by global policy ` +
            `(global window: ${retentionPolicy.expire_time === null ? 'disabled' : `${retentionPolicy.expire_time}s`}, ` +
            `statuses: ${retentionPolicy.statuses.join(',')})`
        );

        if (taskIds.length > 0) {
            logger.debug(
                `[retention] Deleted task ids — row policy: [${byPolicy.row.join(',')}], ` +
                `global policy: [${byPolicy.global.join(',')}]`
            );
        }
    }

    /**
     * Close out a cron round that was skipped by the catch-up policy.
     *
     * The round still produces an attempt row (outcome 'skipped') so the audit
     * trail reflects that the schedule advanced without running the handler.
     *
     * @param {string|number} taskId - Task id
     * @param {object} details - Skip details
     * @param {string} details.worker_id - Worker that claimed the round
     * @param {number} details.next_run_time - Next scheduled time to persist
     * @param {number} [details.skipped_windows] - Number of missed occurrences
     * @param {number} [details.scheduled_run_time] - Originally scheduled instant
     * @param {number} [details.now] - Event timestamp
     * @param {boolean} [details.track_attempt=true] - Close the open attempt with
     *        outcome `skipped`. Spawn-mode rounds pass `false`: no attempt was
     *        opened at claim time, so writing one here would resurrect the
     *        unbounded per-round growth that spawn mode removes.
     * @returns {boolean} True when the row was updated
     */
    recordCronWindowSkipped(taskId, details = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        const now = Number.isFinite(Number(details.now)) ? Number(details.now) : Math.floor(Date.now() / 1000);
        const nextRunTime = Number(details.next_run_time);
        if (!Number.isFinite(nextRunTime)) {
            throw new Error('next_run_time is required');
        }

        let updated = false;
        this.pool(conn => conn.trans(() => {
            const rows = conn.execute(
                `SELECT id, name, status, stage, root_id, parent_id, worker_id, retry_count
                   FROM fib_flow_tasks WHERE id = ?`,
                taskId
            );
            if (rows.length === 0) {
                return;
            }

            const task = rows[0];
            const rs = conn.execute(
                `UPDATE fib_flow_tasks
                    SET status = 'pending',
                        stage = 0,
                        next_run_time = ?,
                        retry_count = 0,
                        last_active_time = ?
                  WHERE id = ? AND status = 'running'`,
                nextRunTime,
                now,
                taskId
            );

            if (rs.affected !== 1) {
                return;
            }
            updated = true;

            if (details.track_attempt !== false) {
                this._finishOpenTaskAttemptWithConnection(conn, taskId, {
                    ended_at: now,
                    outcome: 'skipped',
                    timeout_flag: false,
                    error: null
                });
            }

            this._insertTaskEventsWithConnection(conn, [{
                task_id: task.id,
                root_id: normalizeRootId(task.id, task.root_id),
                parent_id: task.parent_id || null,
                event_type: 'task_cron_missed',
                from_status: 'running',
                to_status: 'pending',
                stage: 0,
                worker_id: details.worker_id || task.worker_id || null,
                event_time: now,
                message: `Cron task ${task.name} skipped ${details.skipped_windows || 1} missed window(s)`,
                metadata: {
                    catch_up: 'skip',
                    skipped_windows: details.skipped_windows || 1,
                    scheduled_run_time: details.scheduled_run_time || null,
                    next_run_time: nextRunTime
                }
            }]);

            this._pruneCronRoundEventsWithConnection(conn, taskId);
        }));

        return updated;
    }

    _resumeParentTaskWithConnection(conn, parentId, resultEntry, eventTime) {
        const parentRs = conn.execute(
            `SELECT id, name, status, stage, root_id, parent_id, worker_id
             FROM fib_flow_tasks WHERE id = ?`,
            parentId
        );

        if (parentRs.length === 0) {
            throw new Error(`Parent task ${parentId} not found`);
        }

        const parentTask = parentRs[0];

        conn.execute(
            `UPDATE fib_flow_tasks
                SET
                    completed_children = completed_children + 1,
                    result = CONCAT(COALESCE(result, ''), ?)
              WHERE id = ?`,
            resultEntry,
            parentId
        );

        const resumeRs = conn.execute(
            `UPDATE fib_flow_tasks
                SET
                    status = 'pending',
                    stage = stage + 1
              WHERE id = ? AND status = 'suspended'
                AND total_children > 0
                AND completed_children = total_children`,
            parentId
        );

        if (resumeRs.affected === 1) {
            this._insertTaskEventsWithConnection(conn, [{
                task_id: parentTask.id,
                root_id: normalizeRootId(parentTask.id, parentTask.root_id),
                parent_id: parentTask.parent_id || null,
                event_type: 'task_status_changed',
                from_status: parentTask.status,
                to_status: 'pending',
                stage: parentTask.stage + 1,
                worker_id: parentTask.worker_id || null,
                event_time: eventTime,
                message: `Parent task ${parentTask.name} resumed after child completion`,
                metadata: {
                    resume_reason: 'children_completed'
                }
            }]);
        }
    }

    /**
     * Get all audit events for a task.
     * @param {string|number} taskId - Task id
     * @param {object} [filters] - Optional event filters
     * @returns {Array<object>} Ordered event rows
     */
    getTaskEvents(taskId, filters = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        return this._getEvents({
            ...filters,
            task_id: taskId
        });
    }

    /**
     * Get all audit events for a workflow rooted at rootId.
     * @param {string|number} rootId - Workflow root task id
     * @param {object} [filters] - Optional event filters
     * @returns {Array<object>} Ordered event rows
     */
    getWorkflowEvents(rootId, filters = {}) {
        if (!rootId) {
            throw new Error('Root task ID is required');
        }

        return this._getEvents({
            ...filters,
            root_id: rootId
        });
    }

    getTaskAttempts(taskId, filters = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        return this._queryAttempts({
            ...filters,
            task_id: taskId
        }).items;
    }

    queryTaskEvents(taskId, filters = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        return this._queryEvents({
            ...filters,
            task_id: taskId
        });
    }

    queryWorkflowEvents(rootId, filters = {}) {
        if (!rootId) {
            throw new Error('Root task ID is required');
        }

        return this._queryEvents({
            ...filters,
            root_id: rootId
        });
    }

    queryTaskAttempts(taskId, filters = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        return this._queryAttempts({
            ...filters,
            task_id: taskId
        });
    }

    queryWorkflowAttempts(rootId, filters = {}) {
        if (!rootId) {
            throw new Error('Root task ID is required');
        }

        return this._queryAttempts({
            ...filters,
            workflow_root_id: rootId
        });
    }

    queryTasks(filters = {}) {
        logger.info(`[queryTasks] Retrieving paged tasks with filters:`, filters);

        if (filters.status) {
            assertTaskStatus(filters.status);
        }
        const statusList = normalizeStatusFilter(filters);
        if (filters.type) {
            assertTaskType(filters.type, 'Invalid task type');
        }

        const pagination = normalizePagination(filters);

        return this.pool(conn => {
            const params = [];
            const conditions = [];

            if (filters.tag) {
                conditions.push('tag = ?');
                params.push(filters.tag);
            }
            if (statusList) {
                conditions.push('status IN ?');
                params.push(statusList);
            }
            if (filters.name) {
                conditions.push('name = ?');
                params.push(filters.name);
            }
            if (filters.worker_id) {
                conditions.push('worker_id = ?');
                params.push(filters.worker_id);
            }
            if (filters.parent_id) {
                conditions.push('parent_id = ?');
                params.push(filters.parent_id);
            }
            if (filters.root_id) {
                conditions.push('root_id = ?');
                params.push(filters.root_id);
            }
            if (filters.type) {
                conditions.push('type = ?');
                params.push(filters.type);
            }
            if (filters.workflow_root_id) {
                conditions.push('(id = ? OR root_id = ?)');
                params.push(filters.workflow_root_id, filters.workflow_root_id);
            }

            let whereSql = '';
            if (conditions.length > 0) {
                whereSql = ' WHERE ' + conditions.join(' AND ');
            }

            const totalRs = conn.execute(`SELECT COUNT(*) AS total FROM fib_flow_tasks${whereSql}`, ...params);
            const total = totalRs[0] ? totalRs[0].total : 0;

            let sql = `SELECT * FROM fib_flow_tasks${whereSql} ORDER BY created_at ${pagination.order}, id ${pagination.order}`;
            if (pagination.limit !== null) {
                sql += ` LIMIT ${pagination.limit}`;
                if (pagination.offset > 0) {
                    sql += ` OFFSET ${pagination.offset}`;
                }
            }

            const items = conn.execute(sql, ...params).map(task => parseTask(task));
            return toPageResult(items, total, pagination.limit, pagination.offset);
        });
    }

    getTaskAudit(taskId, options = {}) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        return {
            task: this.getTask(taskId),
            events: this.queryTaskEvents(taskId, options.events || {}),
            attempts: this.queryTaskAttempts(taskId, options.attempts || {})
        };
    }

    getWorkflowAudit(rootId, options = {}) {
        if (!rootId) {
            throw new Error('Root task ID is required');
        }

        return {
            root_task: this.getTask(rootId),
            tasks: this.queryTasks({
                workflow_root_id: rootId,
                ...(options.tasks || {})
            }),
            events: this.queryWorkflowEvents(rootId, options.events || {})
        };
    }

    getWorkflowAuditSummary(rootId) {
        if (!rootId) {
            throw new Error('Root task ID is required');
        }

        const rootTask = this.getTask(rootId);
        const tasksPage = this.queryTasks({ workflow_root_id: rootId, order: 'asc' });
        const attemptsPage = this.queryWorkflowAttempts(rootId, { order: 'asc' });
        const eventsPage = this.queryWorkflowEvents(rootId, { order: 'asc' });
        const rootEventsPage = this.queryTaskEvents(rootId, { order: 'asc' });

        const tasks = tasksPage.items;
        const attempts = attemptsPage.items;
        const events = eventsPage.items;
        const rootEvents = rootEventsPage.items;

        const taskStatusCounts = {};
        const attemptOutcomeCounts = {};
        const taskNameCounts = {};
        const workers = new Set();
        const stageTaskCounts = {};
        const failedTasks = [];
        const cancelledTasks = [];
        const tasksById = new Map();
        const childrenByParentId = new Map();
        let maxAttempt = 0;
        let firstStartedAt = null;
        let lastEndedAt = null;
        let lastEventTime = null;

        tasks.forEach(task => {
            tasksById.set(task.id, task);
            incrementCounter(taskStatusCounts, task.status);
            incrementCounter(taskNameCounts, task.name);
            incrementCounter(stageTaskCounts, String(task.stage || 0));

            if (task.parent_id !== null && task.parent_id !== undefined) {
                if (!childrenByParentId.has(task.parent_id)) {
                    childrenByParentId.set(task.parent_id, []);
                }
                childrenByParentId.get(task.parent_id).push(task);
            }

            if (task.worker_id) {
                workers.add(task.worker_id);
            }

            if (['failed', 'timeout', 'permanently_failed', 'paused'].includes(task.status)) {
                failedTasks.push({
                    task_id: task.id,
                    name: task.name,
                    status: task.status,
                    error: task.error || null
                });
            } else if (task.status === 'cancelled') {
                // Cancellation is a deliberate outcome, not a failure. It is
                // reported separately so failure dashboards stay meaningful.
                cancelledTasks.push({
                    task_id: task.id,
                    name: task.name,
                    error: task.error || null
                });
            }
        });

        const attemptsByTaskId = new Map();

        const attemptsWithDuration = attempts.map(attempt => {
            if (!attemptsByTaskId.has(attempt.task_id)) {
                attemptsByTaskId.set(attempt.task_id, []);
            }

            if (attempt.worker_id) {
                workers.add(attempt.worker_id);
            }

            if (attempt.outcome) {
                incrementCounter(attemptOutcomeCounts, attempt.outcome);
            }

            maxAttempt = Math.max(maxAttempt, attempt.attempt || 0);

            if (attempt.started_at !== null && attempt.started_at !== undefined) {
                firstStartedAt = firstStartedAt === null
                    ? attempt.started_at
                    : Math.min(firstStartedAt, attempt.started_at);
            }

            if (attempt.ended_at !== null && attempt.ended_at !== undefined) {
                lastEndedAt = lastEndedAt === null
                    ? attempt.ended_at
                    : Math.max(lastEndedAt, attempt.ended_at);
            }

            const duration_seconds = attempt.ended_at !== null && attempt.ended_at !== undefined
                ? Math.max(0, attempt.ended_at - attempt.started_at)
                : null;

            const attemptWithDuration = {
                ...attempt,
                duration_seconds
            };

            attemptsByTaskId.get(attempt.task_id).push(attemptWithDuration);
            return attemptWithDuration;
        });

        events.forEach(event => {
            if (event.worker_id) {
                workers.add(event.worker_id);
            }
            lastEventTime = lastEventTime === null
                ? event.event_time
                : Math.max(lastEventTime, event.event_time);
        });

        attemptsWithDuration.sort((left, right) => {
            const leftDuration = left.duration_seconds === null ? -1 : left.duration_seconds;
            const rightDuration = right.duration_seconds === null ? -1 : right.duration_seconds;
            if (rightDuration !== leftDuration) {
                return rightDuration - leftDuration;
            }
            if (left.started_at !== right.started_at) {
                return left.started_at - right.started_at;
            }
            return left.id - right.id;
        });

        const rootStartEventsByAttempt = new Map();
        rootEvents.forEach(event => {
            if ((event.event_type === 'task_started' || event.event_type === 'task_retry_started') && event.attempt !== null && event.attempt !== undefined) {
                if (!rootStartEventsByAttempt.has(event.attempt)) {
                    rootStartEventsByAttempt.set(event.attempt, event);
                }
            }
        });

        const stageTimings = (attemptsByTaskId.get(rootId) || [])
            .slice()
            .sort((left, right) => left.attempt - right.attempt)
            .map(attempt => {
                const startEvent = rootStartEventsByAttempt.get(attempt.attempt);
                return {
                    stage: startEvent && startEvent.stage !== null && startEvent.stage !== undefined
                        ? startEvent.stage
                        : Math.max(0, attempt.attempt - 1),
                    attempt: attempt.attempt,
                    started_at: attempt.started_at,
                    ended_at: attempt.ended_at || null,
                    duration_seconds: attempt.duration_seconds,
                    outcome: attempt.outcome || null,
                    worker_id: attempt.worker_id || null
                };
            });

        function selectRepresentativeAttempt(taskId) {
            const taskAttempts = (attemptsByTaskId.get(taskId) || []).slice();
            if (taskAttempts.length === 0) {
                return null;
            }

            taskAttempts.sort((left, right) => {
                const durationCompare = (right.duration_seconds === null ? -1 : right.duration_seconds)
                    - (left.duration_seconds === null ? -1 : left.duration_seconds);
                if (durationCompare !== 0) {
                    return durationCompare;
                }

                const endCompare = compareNullableNumberAsc(left.ended_at, right.ended_at);
                if (endCompare !== 0) {
                    return endCompare;
                }

                return left.attempt - right.attempt;
            });

            return taskAttempts[0];
        }

        const criticalPathCache = new Map();
        const buildCriticalPath = (taskId) => {
            if (criticalPathCache.has(taskId)) {
                return criticalPathCache.get(taskId);
            }

            const task = tasksById.get(taskId);
            if (!task) {
                const emptyPath = { total_duration_seconds: 0, nodes: [] };
                criticalPathCache.set(taskId, emptyPath);
                return emptyPath;
            }

            const representativeAttempt = selectRepresentativeAttempt(taskId);
            const ownDuration = representativeAttempt && representativeAttempt.duration_seconds !== null
                ? representativeAttempt.duration_seconds
                : 0;
            const ownNode = {
                task_id: task.id,
                name: task.name,
                status: task.status,
                stage: task.stage,
                attempt: representativeAttempt ? representativeAttempt.attempt : null,
                outcome: representativeAttempt ? representativeAttempt.outcome || null : null,
                duration_seconds: representativeAttempt ? representativeAttempt.duration_seconds : null,
                started_at: representativeAttempt ? representativeAttempt.started_at : null,
                ended_at: representativeAttempt ? representativeAttempt.ended_at || null : null
            };

            const childTasks = (childrenByParentId.get(taskId) || []).slice().sort((left, right) => left.id - right.id);
            let bestChildPath = null;

            childTasks.forEach(childTask => {
                const candidate = buildCriticalPath(childTask.id);
                if (!bestChildPath || candidate.total_duration_seconds > bestChildPath.total_duration_seconds) {
                    bestChildPath = candidate;
                    return;
                }

                if (candidate.total_duration_seconds === bestChildPath.total_duration_seconds) {
                    const candidateFirstId = candidate.nodes.length > 0 ? candidate.nodes[0].task_id : Number.MAX_SAFE_INTEGER;
                    const bestFirstId = bestChildPath.nodes.length > 0 ? bestChildPath.nodes[0].task_id : Number.MAX_SAFE_INTEGER;
                    if (candidateFirstId < bestFirstId) {
                        bestChildPath = candidate;
                    }
                }
            });

            const path = {
                total_duration_seconds: ownDuration + (bestChildPath ? bestChildPath.total_duration_seconds : 0),
                nodes: [ownNode].concat(bestChildPath ? bestChildPath.nodes : [])
            };

            criticalPathCache.set(taskId, path);
            return path;
        };

        const criticalPath = buildCriticalPath(rootId);

        return {
            root_task: rootTask,
            totals: {
                tasks: tasks.length,
                attempts: attempts.length,
                events: events.length,
                checkpoints: events.filter(event => event.event_type === 'task_checkpoint').length,
                max_attempt: maxAttempt
            },
            statuses: sortObjectKeys(taskStatusCounts),
            attempt_outcomes: sortObjectKeys(attemptOutcomeCounts),
            task_names: sortObjectKeys(taskNameCounts),
            stages: sortObjectKeys(stageTaskCounts),
            workers: Array.from(workers).sort(),
            timing: {
                created_at: rootTask ? rootTask.created_at || null : null,
                first_started_at: firstStartedAt,
                last_ended_at: lastEndedAt,
                last_event_time: lastEventTime,
                workflow_duration_seconds: rootTask && lastEndedAt !== null && rootTask.created_at !== undefined && rootTask.created_at !== null
                    ? Math.max(0, lastEndedAt - rootTask.created_at)
                    : null
            },
            stage_timings: stageTimings,
            failed_tasks: failedTasks,
            cancelled_tasks: cancelledTasks,
            critical_path: criticalPath,
            slowest_attempts: attemptsWithDuration.slice(0, 5)
        };
    }

    recordTaskProgress(taskId, progress = {}, workerId = null) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        const normalizedProgress = normalizeProgressPayload(progress);

        let eventId = null;
        this.pool(conn => conn.trans(() => {
            const taskRs = conn.execute(
                `SELECT id, status, stage, root_id, parent_id, worker_id
                 FROM fib_flow_tasks WHERE id = ?`,
                taskId
            );

            if (taskRs.length === 0) {
                throw new Error(`Task ${taskId} not found`);
            }

            const task = taskRs[0];
            if (workerId && (task.status !== 'running' || task.worker_id !== workerId)) {
                throw new Error(`Task ${taskId} is no longer owned by worker ${workerId}`);
            }
            const eventTime = normalizedProgress.event_time || Math.floor(Date.now() / 1000);
            const updates = ['last_active_time = ?'];
            const params = [eventTime];

            if (hasOwn(normalizedProgress, 'stage_name')) {
                updates.push('current_stage_name = ?');
                params.push(normalizedProgress.stage_name);
            }
            if (hasOwn(normalizedProgress, 'progress_text')) {
                updates.push('progress_text = ?');
                params.push(normalizedProgress.progress_text);
            }
            if (hasOwn(normalizedProgress, 'progress_percent')) {
                updates.push('progress_percent = ?');
                params.push(normalizedProgress.progress_percent);
            }

            params.push(taskId);
            let whereClause = 'id = ?';
            if (workerId) {
                whereClause += ' AND status = \'running\' AND worker_id = ?';
                params.push(workerId);
            }

            conn.execute(
                `UPDATE fib_flow_tasks
                    SET ${updates.join(', ')}
                  WHERE ${whereClause}`,
                ...params
            );

            const openAttemptRs = conn.execute(
                `SELECT attempt
                 FROM fib_flow_task_attempts
                 WHERE task_id = ? AND ended_at IS NULL
                 ORDER BY attempt DESC
                 LIMIT 1`,
                taskId
            );

            const metadata = {
                ...(hasOwn(normalizedProgress, 'stage_name') ? { stage_name: normalizedProgress.stage_name } : {}),
                ...(hasOwn(normalizedProgress, 'progress_text') ? { progress_text: normalizedProgress.progress_text } : {}),
                ...(hasOwn(normalizedProgress, 'progress_percent') ? { progress_percent: normalizedProgress.progress_percent } : {}),
                ...(normalizedProgress.metadata || {})
            };

            eventId = this._insertTaskEventsWithConnection(conn, [{
                task_id: task.id,
                root_id: normalizeRootId(task.id, task.root_id),
                parent_id: task.parent_id || null,
                event_type: 'task_progress',
                from_status: task.status,
                to_status: task.status,
                stage: normalizedProgress.stage !== undefined ? normalizedProgress.stage : task.stage,
                worker_id: normalizedProgress.worker_id || task.worker_id || null,
                attempt: openAttemptRs.length > 0 ? openAttemptRs[0].attempt : null,
                event_time: eventTime,
                message: normalizedProgress.message || normalizedProgress.progress_text || normalizedProgress.stage_name || 'task progress updated',
                metadata
            }])[0];
        }));

        return eventId;
    }

    recordTaskCheckpoint(taskId, checkpoint = {}, workerId = null) {
        if (!taskId) {
            throw new Error('Task ID is required');
        }

        const normalizedCheckpoint = normalizeCheckpointPayload(checkpoint);

        let eventId = null;
        this.pool(conn => conn.trans(() => {
            const taskRs = conn.execute(
                `SELECT id, name, status, stage, root_id, parent_id, worker_id
                 FROM fib_flow_tasks WHERE id = ?`,
                taskId
            );

            if (taskRs.length === 0) {
                throw new Error(`Task ${taskId} not found`);
            }

            const task = taskRs[0];
            if (workerId && (task.status !== 'running' || task.worker_id !== workerId)) {
                throw new Error(`Task ${taskId} is no longer owned by worker ${workerId}`);
            }
            const openAttemptRs = conn.execute(
                `SELECT attempt
                 FROM fib_flow_task_attempts
                 WHERE task_id = ? AND ended_at IS NULL
                 ORDER BY attempt DESC
                 LIMIT 1`,
                taskId
            );

            const metadata = {
                code: normalizedCheckpoint.code,
                ...(normalizedCheckpoint.metadata || {})
            };

            eventId = this._insertTaskEventsWithConnection(conn, [{
                task_id: task.id,
                root_id: normalizeRootId(task.id, task.root_id),
                parent_id: task.parent_id || null,
                event_type: 'task_checkpoint',
                from_status: task.status,
                to_status: task.status,
                stage: normalizedCheckpoint.stage !== undefined ? normalizedCheckpoint.stage : task.stage,
                worker_id: normalizedCheckpoint.worker_id || task.worker_id || null,
                attempt: openAttemptRs.length > 0 ? openAttemptRs[0].attempt : null,
                event_time: normalizedCheckpoint.event_time || Math.floor(Date.now() / 1000),
                message: normalizedCheckpoint.message || normalizedCheckpoint.code,
                metadata
            }])[0];
        }));

        return eventId;
    }

    _getEvents(filters = {}) {
        return this._queryEvents(filters).items;
    }

    _queryEvents(filters = {}) {
        const pagination = normalizePagination(filters);

        return this.pool(conn => {
            const params = [];
            const conditions = [];

            if (filters.task_id) {
                conditions.push('task_id = ?');
                params.push(filters.task_id);
            }
            if (filters.root_id) {
                conditions.push('root_id = ?');
                params.push(filters.root_id);
            }
            if (filters.parent_id) {
                conditions.push('parent_id = ?');
                params.push(filters.parent_id);
            }
            if (filters.worker_id) {
                conditions.push('worker_id = ?');
                params.push(filters.worker_id);
            }
            if (filters.event_type) {
                conditions.push('event_type = ?');
                params.push(filters.event_type);
            }
            if (Array.isArray(filters.event_types) && filters.event_types.length > 0) {
                conditions.push('event_type IN ?');
                params.push(filters.event_types);
            }
            if (hasFilterValue(filters.attempt)) {
                conditions.push('attempt = ?');
                params.push(filters.attempt);
            }
            if (hasFilterValue(filters.stage)) {
                conditions.push('stage = ?');
                params.push(filters.stage);
            }
            if (filters.started_after) {
                conditions.push('event_time >= ?');
                params.push(filters.started_after);
            }
            if (filters.started_before) {
                conditions.push('event_time <= ?');
                params.push(filters.started_before);
            }

            let whereSql = '';
            if (conditions.length > 0) {
                whereSql = ' WHERE ' + conditions.join(' AND ');
            }

            const totalRs = conn.execute(`SELECT COUNT(*) AS total FROM fib_flow_task_events${whereSql}`, ...params);
            const total = totalRs[0] ? totalRs[0].total : 0;

            let sql = `SELECT * FROM fib_flow_task_events${whereSql} ORDER BY event_time ${pagination.order}, id ${pagination.order}`;
            if (pagination.limit !== null) {
                sql += ` LIMIT ${pagination.limit}`;
                if (pagination.offset > 0) {
                    sql += ` OFFSET ${pagination.offset}`;
                }
            }

            const items = conn.execute(sql, ...params).map(event => parseEventRow(event));
            return toPageResult(items, total, pagination.limit, pagination.offset);
        });
    }

    _queryAttempts(filters = {}) {
        const pagination = normalizePagination(filters);

        return this.pool(conn => {
            const params = [];
            const conditions = [];
            const attemptTable = filters.workflow_root_id
                ? 'fib_flow_task_attempts a INNER JOIN fib_flow_tasks t ON t.id = a.task_id'
                : 'fib_flow_task_attempts';
            const selectSql = filters.workflow_root_id ? 'SELECT a.*' : 'SELECT *';
            const countSql = 'SELECT COUNT(*) AS total';

            if (filters.task_id) {
                conditions.push('task_id = ?');
                params.push(filters.task_id);
            }
            if (filters.workflow_root_id) {
                conditions.push('(t.id = ? OR t.root_id = ?)');
                params.push(filters.workflow_root_id, filters.workflow_root_id);
            }
            if (filters.worker_id) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.worker_id = ?`);
                params.push(filters.worker_id);
            }
            if (filters.outcome) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.outcome = ?`);
                params.push(filters.outcome);
            }
            if (filters.started_after) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.started_at >= ?`);
                params.push(filters.started_after);
            }
            if (filters.started_before) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.started_at <= ?`);
                params.push(filters.started_before);
            }
            if (filters.ended_after) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.ended_at >= ?`);
                params.push(filters.ended_after);
            }
            if (filters.ended_before) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.ended_at <= ?`);
                params.push(filters.ended_before);
            }
            if (filters.open_only) {
                conditions.push(`${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.ended_at IS NULL`);
            }

            let whereSql = '';
            if (conditions.length > 0) {
                whereSql = ' WHERE ' + conditions.join(' AND ');
            }

            const totalRs = conn.execute(`${countSql} FROM ${attemptTable}${whereSql}`, ...params);
            const total = totalRs[0] ? totalRs[0].total : 0;

            let sql = `${selectSql} FROM ${attemptTable}${whereSql} ORDER BY ${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.attempt ${pagination.order}, ${filters.workflow_root_id ? 'a' : 'fib_flow_task_attempts'}.id ${pagination.order}`;
            if (pagination.limit !== null) {
                sql += ` LIMIT ${pagination.limit}`;
                if (pagination.offset > 0) {
                    sql += ` OFFSET ${pagination.offset}`;
                }
            }

            const items = conn.execute(sql, ...params).map(attempt => parseAttemptRow(attempt));
            return toPageResult(items, total, pagination.limit, pagination.offset);
        });
    }

    /**
     * Implements a robust task claiming mechanism with concurrency control.
     * Prevents race conditions and ensures fair task distribution among workers.
     *
     * @param {Array<string>} taskNames - Eligible task names for execution
     * @param {string} workerId - ID of the worker claiming the task
     * @param {object} [options] - Claim options
     * @param {boolean} [options.skipAttempt=false] - Do not open an attempt row.
     *        Spawn-mode cron rows use this: a spawn round is not an execution, so
     *        an attempt row would grow the table by one row per tick forever
     *        (cron rows are never collected by retention).
     * @param {boolean} [options.skipEvents=false] - Do not write `task_claimed` /
     *        `task_started` events for the same reason.
     * @param {string} [options.typeFilter] - Restrict the claim to one task type
     *        ('async' | 'cron'). Spawn mode probes cron rows separately so a
     *        saturated handler concurrency limit cannot starve the schedule.
     * @returns {object|null} Next available task or null if no tasks are ready
     * @throws {Error} If task name selection is invalid
     */
    claimTask(taskNames, workerId, options = {}) {
        logger.info(`[claimTask] Attempting to claim task for worker ${workerId}, names:`, taskNames);

        if (!Array.isArray(taskNames)) {
            throw new Error('Task names array is required');
        }

        if (!workerId || workerId.trim() === '') {
            throw new Error('Worker ID is required');
        }

        // Return early if taskNames is empty as no tasks can be claimed
        if (taskNames.length === 0) {
            logger.debug(`[claimTask] Empty task names array, skipping DB query`);
            return null;
        }

        const skipAttempt = options.skipAttempt === true;
        const skipEvents = options.skipEvents === true;
        const typeFilter = options.typeFilter === undefined || options.typeFilter === null
            ? null
            : assertTaskType(options.typeFilter, 'Invalid task type');

        let task = null;
        const now = Math.floor(Date.now() / 1000);

        this.pool(conn => {
            while (true) {
                // Find executable tasks
                logger.debug(`[claimTask] Searching for pending tasks`);
                const claimParams = [taskNames, now];
                let typeClause = '';
                if (typeFilter) {
                    typeClause = ' AND type = ?';
                    claimParams.push(typeFilter);
                }
                const rs = conn.execute(
                    `SELECT * FROM fib_flow_tasks 
                        WHERE status = 'pending' 
                        AND name IN ? 
                        AND next_run_time <= ?${typeClause}
                        ORDER BY priority DESC, next_run_time ASC 
                        LIMIT 1 
                        ${this.lockClause}`,
                    ...claimParams
                );

                if (rs.length > 0) {
                    // Update task status
                    logger.info(`[claimTask] Found pending task ${rs[0].id}, attempting to claim`);
                    const updateResult = conn.execute(
                        `UPDATE fib_flow_tasks 
                            SET status = 'running',
                                last_active_time = ?,
                                worker_id = ?,
                                start_time = ?
                            WHERE id = ? AND status = 'pending'`,
                        now,
                        workerId,
                        now,
                        rs[0].id
                    );

                    if (updateResult.affected) {
                        task = rs[0];
                        const nextAttempt = skipAttempt
                            ? null
                            : this._getNextAttemptNumberWithConnection(conn, task.id);
                        task.status = 'running';
                        task.last_active_time = now;
                        task.worker_id = workerId;
                        task.start_time = now;
                        task.attempt = nextAttempt;
                        const claimEvents = skipEvents ? [] : [{
                            task_id: task.id,
                            root_id: normalizeRootId(task.id, task.root_id),
                            parent_id: task.parent_id || null,
                            event_type: 'task_claimed',
                            from_status: 'pending',
                            to_status: 'running',
                            stage: task.stage,
                            worker_id: workerId,
                            event_time: now,
                            message: `Task ${task.name} claimed by ${workerId}`,
                            metadata: {
                                name: task.name
                            }
                        }, {
                            task_id: task.id,
                            root_id: normalizeRootId(task.id, task.root_id),
                            parent_id: task.parent_id || null,
                            event_type: 'task_started',
                            from_status: 'pending',
                            to_status: 'running',
                            stage: task.stage,
                            worker_id: workerId,
                            attempt: nextAttempt,
                            event_time: now,
                            message: `Task ${task.name} started by ${workerId}`,
                            metadata: {
                                name: task.name,
                                retry_count: task.retry_count || 0
                            }
                        }];

                        if ((task.retry_count || 0) > 0 && !skipEvents) {
                            claimEvents.push({
                                task_id: task.id,
                                root_id: normalizeRootId(task.id, task.root_id),
                                parent_id: task.parent_id || null,
                                event_type: 'task_retry_started',
                                from_status: 'pending',
                                to_status: 'running',
                                stage: task.stage,
                                worker_id: workerId,
                                attempt: nextAttempt,
                                event_time: now,
                                message: `Task ${task.name} retry attempt ${nextAttempt} started by ${workerId}`,
                                metadata: {
                                    name: task.name,
                                    retry_count: task.retry_count || 0
                                }
                            });
                        }

                        if (nextAttempt !== null) {
                            this._startTaskAttemptWithConnection(conn, {
                                task_id: task.id,
                                attempt: nextAttempt,
                                worker_id: workerId,
                                started_at: now
                            });
                        }

                        this._insertTaskEventsWithConnection(conn, claimEvents);
                        logger.info(`[claimTask] Successfully claimed task ${task.id}`);
                        break;
                    }
                    logger.warning(`[claimTask] Task ${rs[0].id} was claimed by another worker`);
                } else {
                    logger.debug(`[claimTask] No pending tasks found`);
                    break;
                }
            }
        });

        // Parse task using the utility function for complete processing
        if (task) {
            task = parseTask(task);
        }

        return task;
    }

    /**
     * Update task status with state transition validation
     * Enforces valid state transitions to maintain task lifecycle integrity
     * @param {string|number} taskId - ID of task to update
     * @param {string} status - New status value (must be valid task state)
     * @param {object} [extra] - Additional fields to update:
     *                        - result: Task execution result
     *                        - error: Error message if failed
     *                        - next_run_time: Next scheduled run
     *                        - retry_count: Current retry attempt
     * @throws {Error} If status transition is invalid or update fails
     */
    updateTaskStatus(taskId, status, extra = {}) {
        logger.info(`[updateTaskStatus] Updating task ${taskId} to status '${status}' with extra:`, extra);
        const ownershipWorkerId = extra.worker_id || null;
        const allowedPreviousStatuses = {
            'running': ['pending'],
            'completed': ['running'],
            'failed': ['running'],
            'timeout': ['running'],
            'pending': ['running', 'failed', 'timeout', 'paused', 'suspended'],  // suspended: manual resume
            'permanently_failed': ['failed', 'timeout', 'suspended'],  // suspended: explicit cancel/reject
            'paused': ['running', 'pending', 'failed', 'timeout', 'suspended'],
            'suspended': ['running'],  // running: explicit suspend (task.suspend())
            // Explicit cancellation. Running rows are intentionally excluded:
            // the in-flight handler owns the row until it reports a terminal state.
            'cancelled': ['pending', 'paused', 'failed', 'timeout', 'suspended']
        };

        if (!allowedPreviousStatuses[status]) {
            throw new Error('Invalid status value');
        }

        this.pool(conn => {
            // Build dynamic SQL update statement
            let updates = ['status = ?'];
            let params = [status];
            const eventTime = Math.floor(Date.now() / 1000);

            // Always update last_active_time for status changes
            updates.push('last_active_time = ?');
            params.push(eventTime);

            // Only touch the result column when the caller actually supplies one,
            // otherwise a status transition would overwrite a stored result with 'null'.
            if ('result' in extra) {
                updates.push('result = ?');
                params.push(extra.result === undefined || extra.result === null ? null : JSON.stringify(extra.result));
            }

            if ('error' in extra) {
                updates.push('error = ?');
                params.push(extra.error);
            }

            // Binary context snapshot (e.g. task.suspend({ context }))
            if ('context' in extra) {
                updates.push('context = ?');
                params.push(extra.context === undefined || extra.context === null ? null : extra.context);
            }

            // NOTE: 'stage' handling is deferred into the transaction below because
            // it depends on the task's previous status (a suspended resume advances stage).

            if ('next_run_time' in extra) {
                updates.push('next_run_time = ?');
                params.push(extra.next_run_time);
            }

            if ('retry_count' in extra) {
                updates.push('retry_count = ?');
                params.push(extra.retry_count);
            }

            // Add WHERE clause parameters
            const whereParams = [taskId, allowedPreviousStatuses[status]];
            let whereClause = 'id = ? AND status IN ?';
            if (ownershipWorkerId && allowedPreviousStatuses[status].includes('running')) {
                whereClause += ` AND (status != 'running' OR worker_id = ?)`;
                whereParams.push(ownershipWorkerId);
            }
            logger.debug(`[updateTaskStatus] Executing update query with params:`, params);

            conn.trans(() => {
                const currentTaskRs = conn.execute(
                    `SELECT id, name, type, status, stage, retry_count, root_id, parent_id, worker_id
                     FROM fib_flow_tasks WHERE id = ?`,
                    taskId
                );

                if (currentTaskRs.length === 0) {
                    throw new Error(`Task ${taskId} not found`);
                }

                const currentTask = currentTaskRs[0];
                if (ownershipWorkerId && currentTask.status === 'running' && currentTask.worker_id !== ownershipWorkerId) {
                    throw new Error(`Task ${taskId} is no longer owned by worker ${ownershipWorkerId}`);
                }

                // Resuming from an explicit suspension advances the workflow stage
                // instead of resetting it, and clears the suspend reason.
                const resumingFromSuspended = currentTask.status === 'suspended' && status === 'pending';
                if (status === 'pending') {
                    if (resumingFromSuspended) {
                        updates.push('stage = stage + 1');
                    } else {
                        updates.push('stage = 0');
                    }
                }

                const rs = conn.execute(`
                        UPDATE fib_flow_tasks 
                        SET ${updates.join(', ')}
                        WHERE ${whereClause}
                    `, ...params, ...whereParams);

                if (rs.affected === 0) {
                    const rs1 = conn.execute(`SELECT status, worker_id FROM fib_flow_tasks WHERE id = ?`, taskId);
                    throw new Error(`Failed to update task ${taskId}. Current status: ${rs1[0].status}, worker_id: ${rs1[0].worker_id}`);
                }
                logger.info(`[updateTaskStatus] Successfully updated task ${taskId}, affected rows: ${rs.affected}`);

                if (currentTask.status === 'running') {
                    const attemptOutcome = status === 'pending' && currentTask.type === 'cron'
                        ? 'completed'
                        : status;

                    this._finishOpenTaskAttemptWithConnection(conn, currentTask.id, {
                        ended_at: eventTime,
                        outcome: attemptOutcome,
                        error: 'error' in extra ? extra.error : null,
                        timeout_flag: status === 'timeout'
                    });
                }

                this._insertTaskEventsWithConnection(conn, [{
                    task_id: currentTask.id,
                    root_id: normalizeRootId(currentTask.id, currentTask.root_id),
                    parent_id: currentTask.parent_id || null,
                    event_type: status === 'completed'
                        ? 'task_completed'
                        : status === 'failed'
                            ? 'task_failed'
                            : status === 'timeout'
                                ? 'task_timed_out'
                                : status === 'paused'
                                    ? 'task_paused'
                                    : status === 'permanently_failed'
                                        ? 'task_permanently_failed'
                                        : status === 'cancelled'
                                            ? 'task_cancelled'
                                            : status === 'suspended'
                                                ? 'task_suspended'
                                                : resumingFromSuspended
                                                    ? 'task_resumed'
                                                    : 'task_status_changed',
                    from_status: currentTask.status,
                    to_status: status,
                    stage: status === 'pending'
                        ? (resumingFromSuspended ? currentTask.stage + 1 : 0)
                        : currentTask.stage,
                    worker_id: currentTask.worker_id || null,
                    event_time: eventTime,
                    message: status === 'cancelled'
                        ? `Task ${currentTask.name} cancelled${extra.error ? `: ${extra.error}` : ''}`
                        : `Task ${currentTask.name} status changed from ${currentTask.status} to ${status}`,
                    metadata: {
                        error: 'error' in extra ? extra.error : null,
                        retry_count: 'retry_count' in extra ? extra.retry_count : null,
                        next_run_time: 'next_run_time' in extra ? extra.next_run_time : null,
                        ...('reason' in extra ? { reason: extra.reason } : {}),
                        ...('suspend_reason' in extra ? { suspend_reason: extra.suspend_reason } : {}),
                        ...(resumingFromSuspended ? { resume_reason: extra.resume_reason || 'manual' } : {})
                    }
                }]);

                if (extra.parent_id && status === 'completed') {
                    logger.info(`[updateTaskStatus] Updating parent task ${extra.parent_id}`);
                    const resultEntry = extra.result === undefined || extra.result === null
                        ? 'null'
                        : JSON.stringify(extra.result);
                    this._resumeParentTaskWithConnection(conn, extra.parent_id, `${taskId}:${resultEntry}\n`, eventTime);
                    logger.info(`[updateTaskStatus] Parent task update completed for ${extra.parent_id}`);
                }
            });
        });
    }

    registerWorker(worker) {
        if (!worker || typeof worker !== 'object') {
            throw new Error('Worker registration payload is required');
        }

        const workerId = normalizeWorkerIdentifier(worker.worker_id, 'worker_id');
        const podId = normalizeWorkerIdentifier(worker.pod_id, 'pod_id');
        const now = Number.isFinite(Number(worker.now)) ? Number(worker.now) : Math.floor(Date.now() / 1000);
        const ttl = Number(worker.ttl);

        if (!Number.isFinite(ttl) || ttl <= 0) {
            throw new Error('worker ttl must be a positive number');
        }

        const expiresAt = now + ttl;
        const meta = worker.meta === undefined ? null : JSON.stringify(worker.meta);

        let registeredWorker = null;
        this.pool(conn => conn.trans(() => {
            conn.execute(`DELETE FROM fib_flow_workers WHERE worker_id = ?`, workerId);
            conn.execute(
                `INSERT INTO fib_flow_workers (
                    worker_id, pod_id, status, registered_at, last_seen_at, expires_at, superseded_at, dead_at, meta
                ) VALUES (?, ?, 'active', ?, ?, ?, NULL, NULL, ?)`,
                workerId,
                podId,
                now,
                now,
                expiresAt,
                meta
            );

            const rows = conn.execute(`SELECT * FROM fib_flow_workers WHERE worker_id = ?`, workerId);
            registeredWorker = rows[0] || null;
        }));

        return registeredWorker;
    }

    heartbeatWorker(workerId, now = Math.floor(Date.now() / 1000), ttl) {
        const normalizedWorkerId = normalizeWorkerIdentifier(workerId, 'worker_id');
        const normalizedTtl = Number(ttl);
        if (!Number.isFinite(normalizedTtl) || normalizedTtl <= 0) {
            throw new Error('worker ttl must be a positive number');
        }

        return this.pool(conn => conn.execute(
            `UPDATE fib_flow_workers
                SET last_seen_at = ?,
                    expires_at = ?,
                    status = 'active'
              WHERE worker_id = ? AND status = 'active'`,
            now,
            now + normalizedTtl,
            normalizedWorkerId
        ));
    }

    getWorker(workerId) {
        const normalizedWorkerId = normalizeWorkerIdentifier(workerId, 'worker_id');
        return this.pool(conn => {
            const rows = conn.execute(`SELECT * FROM fib_flow_workers WHERE worker_id = ?`, normalizedWorkerId);
            return rows[0] || null;
        });
    }

    listWorkers(filters = {}) {
        return this.pool(conn => {
            const conditions = [];
            const params = [];

            if (filters.pod_id) {
                conditions.push('pod_id = ?');
                params.push(filters.pod_id);
            }
            if (filters.status) {
                conditions.push('status = ?');
                params.push(filters.status);
            }

            let sql = 'SELECT * FROM fib_flow_workers';
            if (conditions.length > 0) {
                sql += ` WHERE ${conditions.join(' AND ')}`;
            }
            sql += ' ORDER BY registered_at ASC, worker_id ASC';
            return conn.execute(sql, ...params);
        });
    }

    supersedeOlderWorkers(podId, currentWorkerId, now = Math.floor(Date.now() / 1000)) {
        const normalizedPodId = normalizeWorkerIdentifier(podId, 'pod_id');
        const normalizedWorkerId = normalizeWorkerIdentifier(currentWorkerId, 'worker_id');

        let supersededWorkerIds = [];
        this.pool(conn => conn.trans(() => {
            const rows = conn.execute(
                `SELECT worker_id
                   FROM fib_flow_workers
                  WHERE pod_id = ?
                    AND worker_id != ?
                    AND status = 'active'`,
                normalizedPodId,
                normalizedWorkerId
            );

            if (rows.length === 0) {
                supersededWorkerIds = [];
                return;
            }

            const workerIds = rows.map(row => row.worker_id);
            conn.execute(
                `UPDATE fib_flow_workers
                    SET status = 'superseded',
                        superseded_at = ?
                  WHERE worker_id IN ?`,
                now,
                workerIds
            );

            supersededWorkerIds = workerIds;
        }));

        return supersededWorkerIds;
    }

    reapExpiredWorkers(now = Math.floor(Date.now() / 1000)) {
        let expiredWorkerIds = [];
        this.pool(conn => conn.trans(() => {
            const rows = conn.execute(
                `SELECT worker_id
                   FROM fib_flow_workers
                  WHERE status = 'active'
                    AND expires_at < ?`,
                now
            );

            if (rows.length === 0) {
                expiredWorkerIds = [];
                return;
            }

            const workerIds = rows.map(row => row.worker_id);
            conn.execute(
                `UPDATE fib_flow_workers
                    SET status = 'dead',
                        dead_at = ?
                  WHERE worker_id IN ?
                    AND status = 'active'`,
                now,
                workerIds
            );

            expiredWorkerIds = workerIds;
        }));

        return expiredWorkerIds;
    }

    markWorkerDead(workerId, now = Math.floor(Date.now() / 1000)) {
        const normalizedWorkerId = normalizeWorkerIdentifier(workerId, 'worker_id');
        return this.pool(conn => conn.execute(
            `UPDATE fib_flow_workers
                SET status = 'dead',
                    dead_at = ?
              WHERE worker_id = ? AND status != 'dead'`,
            now,
            normalizedWorkerId
        ));
    }

    recoverTasksForWorkers(workerIds, recoveredBy = {}) {
        if (!Array.isArray(workerIds) || workerIds.length === 0) {
            return 0;
        }

        const uniqueWorkerIds = Array.from(new Set(workerIds.filter(Boolean)));
        if (uniqueWorkerIds.length === 0) {
            return 0;
        }

        const recoveredByWorkerId = recoveredBy.worker_id || null;
        const recoveredByPodId = recoveredBy.pod_id || null;
        const now = Number.isFinite(Number(recoveredBy.now)) ? Number(recoveredBy.now) : Math.floor(Date.now() / 1000);

        let recoveredCount = 0;
        this.pool(conn => conn.trans(() => {
            const tasks = conn.execute(
                `SELECT id, name, root_id, parent_id, worker_id
                   FROM fib_flow_tasks
                  WHERE status = 'running'
                    AND worker_id IN ?`,
                uniqueWorkerIds
            );

            tasks.forEach(task => {
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                        SET status = 'pending',
                            stage = 0,
                            last_active_time = ?,
                            next_run_time = ?,
                            worker_id = NULL,
                            start_time = NULL,
                            result = NULL
                      WHERE id = ? AND status = 'running' AND worker_id = ?`,
                    now,
                    now,
                    task.id,
                    task.worker_id
                );

                if (rs.affected !== 1) {
                    return;
                }

                recoveredCount += 1;
                this._finishOpenTaskAttemptWithConnection(conn, task.id, {
                    ended_at: now,
                    outcome: 'interrupted',
                    timeout_flag: false,
                    error: 'Task ownership recovered after worker became unavailable'
                });

                this._insertTaskEventsWithConnection(conn, [{
                    task_id: task.id,
                    root_id: normalizeRootId(task.id, task.root_id),
                    parent_id: task.parent_id || null,
                    event_type: 'task_recovered',
                    from_status: 'running',
                    to_status: 'pending',
                    stage: 0,
                    worker_id: recoveredByWorkerId,
                    event_time: now,
                    message: `Task ${task.id} recovered from worker ${task.worker_id}`,
                    metadata: {
                        recovery_reason: 'worker_unavailable',
                        recovered_from_worker_id: task.worker_id,
                        recovered_by_worker_id: recoveredByWorkerId,
                        recovered_by_pod_id: recoveredByPodId
                    }
                }]);
            });

        }));

        return recoveredCount;
    }

    /**
     * Update last active time for running tasks
     * Used to track task health and detect timeouts
     * @param {Array<string|number>} taskIds - IDs of tasks to update
     */
    updateTaskActiveTime(taskIds, workerId = null) {
        logger.info(`[updateTaskActiveTime] Updating active time for tasks:`, taskIds);

        if (!Array.isArray(taskIds) || taskIds.length === 0) {
            return;
        }
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => {
            logger.debug(`[updateTaskActiveTime] Executing update query`);
            if (workerId) {
                return conn.execute(
                    `UPDATE fib_flow_tasks
                        SET last_active_time = ?
                      WHERE id IN ?
                        AND status = 'running'
                        AND worker_id = ?`,
                    now,
                    taskIds,
                    workerId
                );
            }

            return conn.execute(
                'UPDATE fib_flow_tasks SET last_active_time = ? WHERE id IN ?',
                now, taskIds
            );
        });
    }

    _runBestEffortTimeoutSideEffect(action, callback) {
        try {
            return callback();
        } catch (error) {
            logger.warning(`[handleTimeoutTasks] Failed to ${action}: ${error.message}`);
            return null;
        }
    }

    _isClosingConnectionError(error) {
        if (!error) {
            return false;
        }

        const message = String(error.message || error);
        return message.includes('database is closed') || message.includes('Invalid procedure call');
    }

    _runTimeoutSweepStep(stepName, defaultValue, callback) {
        if (!this.pool) {
            return defaultValue;
        }

        try {
            return callback();
        } catch (error) {
            if (this._isClosingConnectionError(error)) {
                logger.debug(`[handleTimeoutTasks] Skipping ${stepName} because the database is closing`);
                return defaultValue;
            }

            throw error;
        }
    }

    _hasParentResultEntry(resultText, entryPrefix) {
        if (!resultText || !entryPrefix) {
            return false;
        }

        return resultText.startsWith(entryPrefix) || resultText.indexOf(`\n${entryPrefix}`) !== -1;
    }

    _resumeParentIfReadyWithConnection(conn, parentTask, eventTime) {
        const resumeRs = conn.execute(
            `UPDATE fib_flow_tasks
                SET
                    status = 'pending',
                    stage = stage + 1
              WHERE id = ? AND status = 'suspended' AND completed_children = total_children`,
            parentTask.id
        );

        if (resumeRs.affected === 1) {
            this._runBestEffortTimeoutSideEffect('record parent resume event', () => {
                this._insertTaskEventsWithConnection(conn, [{
                    task_id: parentTask.id,
                    root_id: normalizeRootId(parentTask.id, parentTask.root_id),
                    parent_id: parentTask.parent_id || null,
                    event_type: 'task_status_changed',
                    from_status: parentTask.status,
                    to_status: 'pending',
                    stage: parentTask.stage + 1,
                    worker_id: parentTask.worker_id || null,
                    event_time: eventTime,
                    message: `Parent task ${parentTask.name} resumed after child completion`,
                    metadata: {
                        resume_reason: 'children_completed'
                    }
                }]);
            });
        }

        return resumeRs.affected;
    }

    _propagateChildFailureToParentWithConnection(conn, task, eventTime) {
        if (!task.parent_id) {
            return 0;
        }

        const parentRs = conn.execute(
            `SELECT id, name, status, stage, root_id, parent_id, worker_id, result
             FROM fib_flow_tasks WHERE id = ?`,
            task.parent_id
        );

        if (parentRs.length === 0) {
            throw new Error(`Parent task ${task.parent_id} not found`);
        }

        const parentTask = parentRs[0];
        const resultEntry = `${task.id}!${JSON.stringify(task.error)}\n`;
        const entryPrefix = `${task.id}!`;

        if (!this._hasParentResultEntry(parentTask.result, entryPrefix)) {
            conn.execute(
                `UPDATE fib_flow_tasks
                    SET
                        completed_children = completed_children + 1,
                        result = CONCAT(COALESCE(result, ''), ?)
                  WHERE id = ?`,
                resultEntry,
                task.parent_id
            );
        }

        return this._resumeParentIfReadyWithConnection(conn, parentTask, eventTime);
    }

    _handleTotalTimeoutTasks(now) {
        return this._runTimeoutSweepStep('total-timeout sweep', 0, () => this.pool(conn => {
            logger.debug(`[handleTimeoutTasks] Checking for total timeout tasks`);
            const totalTimeoutTasks = conn.execute(
                `SELECT id, name, stage, root_id, parent_id, worker_id FROM fib_flow_tasks
                     WHERE status = 'running'
                     AND start_time + timeout < ?`,
                now
            );

            let affected = 0;
            for (const task of totalTimeoutTasks) {
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'timeout',
                             error = 'Task exceeded total timeout limit',
                             last_active_time = ?
                         WHERE id = ? AND status = 'running'`,
                    now,
                    task.id
                );

                if (rs.affected !== 1) {
                    continue;
                }

                affected += 1;
                this._runBestEffortTimeoutSideEffect(`close timeout attempt for task ${task.id}`, () => {
                    this._finishOpenTaskAttemptWithConnection(conn, task.id, {
                        ended_at: now,
                        outcome: 'timeout',
                        error: 'Task exceeded total timeout limit',
                        timeout_flag: true
                    });
                });
                this._runBestEffortTimeoutSideEffect(`record timeout event for task ${task.id}`, () => {
                    this._insertTaskEventsWithConnection(conn, [{
                        task_id: task.id,
                        root_id: normalizeRootId(task.id, task.root_id),
                        parent_id: task.parent_id || null,
                        event_type: 'task_timed_out',
                        from_status: 'running',
                        to_status: 'timeout',
                        stage: task.stage,
                        worker_id: task.worker_id || null,
                        event_time: now,
                        message: `Task ${task.name} exceeded total timeout`,
                        metadata: {
                            timeout_type: 'total'
                        }
                    }]);
                });
            }

            return affected;
        }));
    }

    _handleHeartbeatTimeoutTasks(now, timeoutConfig) {
        return this._runTimeoutSweepStep('heartbeat-timeout sweep', 0, () => this.pool(conn => {
            logger.debug(`[handleTimeoutTasks] Checking for heartbeat timeout tasks`);
            const heartbeatTimeoutMs = Number(timeoutConfig.task_heartbeat_timeout);
            const heartbeatThreshold = now - Math.max(1, Math.ceil(heartbeatTimeoutMs / 1000));
            const heartbeatTimeoutTasks = conn.execute(
                `SELECT id, name, stage, root_id, parent_id, worker_id FROM fib_flow_tasks
                     WHERE status = 'running'
                     AND last_active_time IS NOT NULL
                     AND last_active_time < ?`,
                heartbeatThreshold
            );

            let affected = 0;
            for (const task of heartbeatTimeoutTasks) {
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'timeout',
                             error = ?,
                             last_active_time = ?
                         WHERE id = ? AND status = 'running'`,
                    'Task heartbeat timed out',
                    now,
                    task.id
                );

                if (rs.affected !== 1) {
                    continue;
                }

                affected += 1;
                this._runBestEffortTimeoutSideEffect(`close heartbeat-timeout attempt for task ${task.id}`, () => {
                    this._finishOpenTaskAttemptWithConnection(conn, task.id, {
                        ended_at: now,
                        outcome: 'timeout',
                        error: 'Task heartbeat timed out',
                        timeout_flag: true
                    });
                });
                this._runBestEffortTimeoutSideEffect(`record heartbeat-timeout event for task ${task.id}`, () => {
                    this._insertTaskEventsWithConnection(conn, [{
                        task_id: task.id,
                        root_id: normalizeRootId(task.id, task.root_id),
                        parent_id: task.parent_id || null,
                        event_type: 'task_timed_out',
                        from_status: 'running',
                        to_status: 'timeout',
                        stage: task.stage,
                        worker_id: task.worker_id || null,
                        event_time: now,
                        message: `Task ${task.name} lost heartbeat`,
                        metadata: {
                            timeout_type: 'heartbeat'
                        }
                    }]);
                });
            }

            return affected;
        }));
    }

    _handleRetryTasks(now) {
        return this._runTimeoutSweepStep('retry sweep', 0, () => this.pool(conn => {
            logger.debug(`[handleTimeoutTasks] Checking for tasks eligible for retry`);
            const retryTasks = conn.execute(
                `SELECT id, name, status, stage, retry_count, root_id, parent_id, worker_id FROM fib_flow_tasks
                     WHERE status IN ('timeout','failed')
                     AND retry_count + 1 < max_retries
                     AND (last_active_time + retry_interval < ?)`,
                now
            );

            let affected = 0;
            for (const task of retryTasks) {
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'pending',
                             stage = 0,
                             result = null,
                             context = null,
                             retry_count = retry_count + 1,
                             last_active_time = ?,
                             next_run_time = ? + retry_interval
                         WHERE id = ? AND status IN ('timeout','failed')`,
                    now,
                    now,
                    task.id
                );

                if (rs.affected !== 1) {
                    continue;
                }

                affected += 1;
                this._runBestEffortTimeoutSideEffect(`record retry event for task ${task.id}`, () => {
                    this._insertTaskEventsWithConnection(conn, [{
                        task_id: task.id,
                        root_id: normalizeRootId(task.id, task.root_id),
                        parent_id: task.parent_id || null,
                        event_type: 'task_retry_scheduled',
                        from_status: task.status,
                        to_status: 'pending',
                        stage: 0,
                        worker_id: task.worker_id || null,
                        event_time: now,
                        message: `Task ${task.name} scheduled for retry`,
                        metadata: {
                            retry_count: (task.retry_count || 0) + 1,
                            previous_status: task.status
                        }
                    }]);
                });
            }

            return affected;
        }));
    }

    _handleExhaustedCronTasks(now) {
        return this._runTimeoutSweepStep('cron exhaustion sweep', 0, () => this.pool(conn => {
            logger.debug(`[handleTimeoutTasks] Checking for cron tasks that have exhausted retries`);
            const exhaustedTasks = conn.execute(
                `SELECT id, name, status, stage, root_id, parent_id, worker_id FROM fib_flow_tasks
                     WHERE status IN ('timeout','failed')
                     AND type = 'cron'
                     AND retry_count + 1 >= max_retries`
            );

            let affected = 0;
            for (const task of exhaustedTasks) {
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'paused',
                             last_active_time = ?
                         WHERE id = ? AND status IN ('timeout','failed')`,
                    now,
                    task.id
                );

                if (rs.affected !== 1) {
                    continue;
                }

                affected += 1;
                this._runBestEffortTimeoutSideEffect(`record paused event for task ${task.id}`, () => {
                    this._insertTaskEventsWithConnection(conn, [{
                        task_id: task.id,
                        root_id: normalizeRootId(task.id, task.root_id),
                        parent_id: task.parent_id || null,
                        event_type: 'task_paused',
                        from_status: task.status,
                        to_status: 'paused',
                        stage: task.stage,
                        worker_id: task.worker_id || null,
                        event_time: now,
                        message: `Cron task ${task.name} paused after retry exhaustion`,
                        metadata: {
                            retry_exhausted: true
                        }
                    }]);
                });
            }

            return affected;
        }));
    }

    _handlePermanentlyFailedAsyncTasks(now) {
        return this._runTimeoutSweepStep('permanent-failure sweep', 0, () => this.pool(conn => {
            const failedTasks = conn.execute(
                `SELECT id, name, status, stage, root_id, parent_id, worker_id, error
                    FROM fib_flow_tasks
                    WHERE type = 'async'
                      AND (
                            (status IN ('timeout', 'failed') AND retry_count + 1 >= max_retries)
                         OR (status = 'permanently_failed' AND parent_id IS NOT NULL)
                      )`
            );

            let affected = 0;
            for (const task of failedTasks) {
                let stateChanged = false;

                if (task.status !== 'permanently_failed') {
                    const rs = conn.execute(
                        `UPDATE fib_flow_tasks
                            SET status = 'permanently_failed',
                                last_active_time = ?
                          WHERE id = ? AND status IN ('timeout', 'failed')`,
                        now,
                        task.id
                    );

                    if (rs.affected !== 1) {
                        continue;
                    }

                    stateChanged = true;
                    affected += 1;
                    this._runBestEffortTimeoutSideEffect(`record permanent-failure event for task ${task.id}`, () => {
                        this._insertTaskEventsWithConnection(conn, [{
                            task_id: task.id,
                            root_id: normalizeRootId(task.id, task.root_id),
                            parent_id: task.parent_id || null,
                            event_type: 'task_permanently_failed',
                            from_status: task.status,
                            to_status: 'permanently_failed',
                            stage: task.stage,
                            worker_id: task.worker_id || null,
                            event_time: now,
                            message: `Task ${task.name} permanently failed after retry exhaustion`,
                            metadata: {
                                error: task.error || null,
                                retry_exhausted: true
                            }
                        }]);
                    });
                }

                if (task.parent_id) {
                    this._propagateChildFailureToParentWithConnection(conn, task, now);
                }

                if (stateChanged && task.parent_id) {
                    logger.info(`[handleTimeoutTasks] Parent task ${task.parent_id} checked after child ${task.id} permanently failed`);
                }
            }

            return affected;
        }));
    }

    _cleanupExpiredTasks(policy) {
        const result = this._runTimeoutSweepStep('retention cleanup', {
            tasks_deleted: 0,
            events_deleted: 0,
            attempts_deleted: 0
        }, () => this.pool(conn => this._cleanupExpiredTasksWithConnection(conn, policy)));

        // This is the path the automatic sweep takes (activeTimer →
        // handleTimeoutTasks → here), and it is the only sweep most deployments ever
        // run: `cleanupExpiredTasks()` / `runRetention()` are not called by an
        // application that relies on the default retention window. Sweeping the
        // per-schedule metadata here — not only in the public `runRetention()` —
        // is what keeps `schedule:*` proportional to the registered schedules.
        //
        // Best-effort: a bookkeeping failure must not fail the retention pass.
        this._runBestEffortTimeoutSideEffect('prune schedule metadata', () => {
            this.pruneScheduleMeta();
        });

        return result;
    }

    /**
     * Handle tasks that have exceeded their timeout period
     * Implements the following timeout handling logic:
     * 1. Marks tasks as permanently_failed if max retries reached
     * 2. Marks running tasks as timeout if inactive
     * 3. Schedules retry attempts for failed tasks within retry limit
     * 4. Cleans up expired terminal tasks according to the retention policy
    * @param {object} timeoutConfig - Task heartbeat timeout configuration
     * @param {number|object} [retention] - Retention expire time or retention policy
     * @returns {object} Count of tasks in each state transition
     */
    handleTimeoutTasks(timeoutConfig, retention = null) {
        const retentionPolicy = normalizeRetentionPolicy(retention, null);
        // `retention === null` means the caller did not arm a sweep this tick (the
        // manager throttles them). A policy object — even one whose `expire_time` is
        // null — means "run the pass": row-level windows and per-schedule metadata
        // both need it.
        const shouldSweep = retention !== null && retention !== undefined;
        logger.info(`[handleTimeoutTasks] Starting timeout task handling${retentionPolicy.expire_time ? `, with expire time: ${retentionPolicy.expire_time}s` : ''}`);
        const now = Math.floor(Date.now() / 1000);
        const result = {
            timed_out_total: this._handleTotalTimeoutTasks(now),
            timed_out_heartbeat: this._handleHeartbeatTimeoutTasks(now, timeoutConfig),
            retried: this._handleRetryTasks(now),
            paused: this._handleExhaustedCronTasks(now),
            permanently_failed: this._handlePermanentlyFailedAsyncTasks(now),
            tasks_deleted: 0,
            events_deleted: 0,
            attempts_deleted: 0
        };

        if (result.timed_out_total) {
            logger.warning(`[handleTimeoutTasks] ${result.timed_out_total} tasks exceeded total timeout`);
        }
        if (result.timed_out_heartbeat) {
            logger.warning(`[handleTimeoutTasks] ${result.timed_out_heartbeat} tasks timed out waiting for heartbeat`);
        }
        if (result.retried) {
            logger.info(`[handleTimeoutTasks] ${result.retried} tasks scheduled for retry`);
        }
        if (result.paused) {
            logger.warning(`[handleTimeoutTasks] ${result.paused} cron tasks paused due to no retries left`);
        }

        // Run the sweep whenever the caller armed one. A policy whose `expire_time`
        // is null still needs the pass: row-level windows and leftover per-schedule
        // metadata are collected by it, and gating on `expire_time` here would
        // disable both for a manager configured with `expire_time: null`.
        if (shouldSweep) {
            const cleanupResult = this._cleanupExpiredTasks({
                ...retentionPolicy,
                now
            });

            result.tasks_deleted = cleanupResult.tasks_deleted;
            result.events_deleted = cleanupResult.events_deleted;
            result.attempts_deleted = cleanupResult.attempts_deleted;

            if (cleanupResult.tasks_deleted) {
                logger.info(`[handleTimeoutTasks] Cleaned up ${cleanupResult.tasks_deleted} expired tasks`);
            }
        }

        logger.debug(`[handleTimeoutTasks] Timeout task handling completed`);
        return result;
    }

    /**
     * Retrieve task by ID with payload parsing
     * @param {string|number} taskId - Task ID to retrieve
     * @returns {object|null} Task object if found, null otherwise
     */
    getTask(taskId) {
        logger.info(`[getTask] Retrieving task with ID: ${taskId}`);
        return this.pool(conn => {
            const rs = conn.execute('SELECT * FROM fib_flow_tasks WHERE id = ?', taskId);
            if (rs.length === 0) return null;
            return parseTask(rs[0]);
        });
    }

    cleanupExpiredTasks(policy = {}) {
        const retentionPolicy = normalizeRetentionPolicy(policy);

        const totals = {
            tasks_deleted: 0,
            events_deleted: 0,
            attempts_deleted: 0,
            tasks_deleted_by_row_policy: 0,
            tasks_deleted_by_global_policy: 0
        };

        while (true) {
            const cleanupResult = this.pool(conn => {
                let result;
                conn.trans(() => {
                    result = this._cleanupExpiredTasksWithConnection(conn, retentionPolicy);
                });
                return result;
            });

            totals.tasks_deleted += cleanupResult.tasks_deleted;
            totals.events_deleted += cleanupResult.events_deleted;
            totals.attempts_deleted += cleanupResult.attempts_deleted;
            totals.tasks_deleted_by_row_policy += cleanupResult.tasks_deleted_by_row_policy;
            totals.tasks_deleted_by_global_policy += cleanupResult.tasks_deleted_by_global_policy;

            if (!cleanupResult.has_more || cleanupResult.tasks_deleted === 0) {
                break;
            }
        }

        // Per-schedule policy rows in `fib_flow_meta` describe a schedule that may no
        // longer exist. Sweeping them here keeps the table proportional to what is
        // actually registered, instead of growing with every definition that was
        // ever created and deleted.
        this.pruneScheduleMeta();

        return totals;
    }

    /**
     * Delete per-schedule `fib_flow_meta` rows whose schedule row is gone.
     *
     * Only the `schedule:*` namespace is touched, and only by comparing against the
     * schedule rows that still exist — an operator-created key is never removed.
     *
     * @returns {number} Number of meta rows deleted
     */
    pruneScheduleMeta() {
        let pruned = 0;

        // `conn.trans()` returns a success flag, not the callback's value, so the
        // count is carried out through a closure (the same pattern the other
        // transactional writers here use).
        this.pool(conn => conn.trans(() => {
            const metaRows = conn.execute(
                `SELECT meta_key FROM fib_flow_meta WHERE meta_key LIKE 'schedule:%'`
            );

            if (metaRows.length === 0) {
                return;
            }

            const scheduleRows = conn.execute(
                `SELECT name, cron_key FROM fib_flow_tasks WHERE type = 'cron'`
            );
            const live = new Set(scheduleRows.map(row =>
                buildScheduleIdentityKey(row.name, BaseDBAdapter.normalizeCronKey(row.cron_key))
            ));

            const orphanKeys = [];
            for (const row of metaRows) {
                const parsed = parseScheduleMetaKey(row.meta_key);
                if (!parsed) {
                    // Not one of ours (an operator key, or a future format): leave it.
                    continue;
                }
                if (!live.has(buildScheduleIdentityKey(parsed.name, parsed.cronKey))) {
                    orphanKeys.push(row.meta_key);
                }
            }

            if (orphanKeys.length === 0) {
                return;
            }

            conn.execute(
                `DELETE FROM fib_flow_meta WHERE meta_key IN ?`,
                orphanKeys
            );
            pruned = orphanKeys.length;
        }));

        return pruned;
    }

    /**
     * Get all tasks with specified name
     * @param {string} name - Task name to search for
     * @returns {Array<object>} Array of matching tasks with parsed payloads
     * @throws {Error} If name parameter is missing
     */
    getTasksByName(name) {
        logger.info(`[getTasksByName] Retrieving tasks with name: ${name}`);

        if (!name) {
            throw new Error('Task name is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE name = ?', name);
            return tasks.map(task => parseTask(task));
        });
    }

    /**
     * Get all tasks with specified status
     * @param {string} status - Status to filter by (must be valid task state)
     * @param {object} [filters] - Optional filters:
     *                             - suspend_reason: Only return tasks with this suspend reason
     * @returns {Array<object>} Array of matching tasks with parsed payloads
     * @throws {Error} If status is invalid or missing
     */
    getTasksByStatus(status, filters = {}) {
        logger.info(`[getTasksByStatus] Retrieving tasks with status: ${status}`);

        if (!status) {
            throw new Error('Status is required');
        }
        assertTaskStatus(status);

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE status = ?', status)
                .map(task => parseTask(task));

            if (!filters.suspend_reason) {
                return tasks;
            }

            // suspend_reason is not a task column: derive it from the latest
            // task_suspended audit event metadata for each task.
            const ids = tasks.map(task => task.id);
            if (ids.length === 0) {
                return tasks;
            }

            const eventRows = conn.execute(
                `SELECT task_id, id, metadata
                   FROM fib_flow_task_events
                  WHERE task_id IN ? AND event_type = 'task_suspended'
                  ORDER BY id ASC`,
                ids
            );

            const reasonByTaskId = new Map();
            for (const row of eventRows) {
                if (!row.metadata) {
                    continue;
                }
                try {
                    const metadata = JSON.parse(row.metadata);
                    if (metadata.suspend_reason) {
                        // Ordered by id ASC, so the last event per task wins
                        reasonByTaskId.set(row.task_id, metadata.suspend_reason);
                    }
                } catch (e) {
                    // Ignore malformed metadata
                }
            }

            return tasks.filter(task => reasonByTaskId.get(task.id) === filters.suspend_reason);
        });
    }

    /**
     * Get tasks statistics by tag
     * @param {string} tag - Tag to filter by (optional)
     * @param {string} status - Status to filter by (optional)
     * @returns {Array<object>} Array of task statistics grouped by tag and name
     */
    getTaskStatsByTag(tag, status) {
        logger.info(`[BaseDBAdapter] Getting task stats by tag: ${tag}, status: ${status}`);

        return this.pool(conn => {
            let sql = 'SELECT tag, name, status, COUNT(*) as count FROM fib_flow_tasks';
            const params = [];
            const conditions = [];

            if (tag) {
                conditions.push('tag = ?');
                params.push(tag);
            }
            if (status) {
                conditions.push('status = ?');
                params.push(status);
            }

            if (conditions.length > 0) {
                sql += ' WHERE ' + conditions.join(' AND ');
            }

            sql += ' GROUP BY tag, name, status ORDER BY tag, name, status';

            logger.debug(`[BaseDBAdapter] Executing stats query: ${sql}`);
            const result = conn.execute(sql, ...params);
            logger.debug(`[BaseDBAdapter] Stats query returned ${result.length} rows`);
            return result;
        });
    }

    /**
     * Get tasks by tag
     * @param {string} tag - Tag to filter by
     * @returns {Array<object>} Array of tasks with the specified tag
     */
    getTasksByTag(tag) {
        logger.info(`[BaseDBAdapter] Getting tasks by tag: ${tag}`);

        if (!tag) {
            throw new Error('Tag is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE tag = ?', tag);
            return tasks.map(task => parseTask(task));
        });
    }

    /**
     * List cron schedules.
     *
     * `orphan` marks schedules whose handler is no longer registered: their rows
     * stay pending forever because claimTask() only matches registered names.
     *
     * @param {object} [filters] - Optional filters (name, cronKey, tag, status, statuses)
     * @param {Array<string>} [registeredNames] - Registered handler names used to
     *        compute the orphan flag; omit to skip orphan evaluation
     * @param {object} [options] - Query options
     * @param {boolean} [options.include_spawned=false] - Also resolve
     *        `last_spawned_task_id`. That costs an extra audit-table lookup, so
     *        callers on a hot path (claim eligibility) leave it off.
     * @returns {Array<object>} Schedule rows
     */
    getCronSchedules(filters = {}, registeredNames = null, options = {}) {
        const statusList = normalizeStatusFilter(filters);

        const conditions = ["type = 'cron'"];
        const params = [];

        if (filters.name) {
            conditions.push('name = ?');
            params.push(filters.name);
        }
        if (filters.cronKey !== undefined && filters.cronKey !== null) {
            // Without this, a caller filtering by `cronKey` silently got every
            // schedule of that name — with several schedules per name (P2) that is
            // an arbitrary row, not an error.
            conditions.push('cron_key = ?');
            params.push(assertCronKey(filters.cronKey));
        }
        if (filters.tag) {
            conditions.push('tag = ?');
            params.push(filters.tag);
        }
        if (statusList) {
            conditions.push('status IN ?');
            params.push(statusList);
        }

        return this.pool(conn => {
            const rows = conn.execute(
                `SELECT * FROM fib_flow_tasks WHERE ${conditions.join(' AND ')} ORDER BY id ASC`,
                ...params
            );

            const known = registeredNames ? new Set(registeredNames) : null;
            const scheduleIds = options.include_spawned === true ? rows.map(row => row.id) : null;
            // Spawn mode hands the run id to the engine instead of the caller, so
            // the schedule view has to expose it — otherwise a consumer cannot
            // subscribe to the spawned run's state. Derived from the audit trail
            // (no schema change, no new column).
            const lastSpawned = scheduleIds
                ? this._getLastSpawnedTaskIds(conn, scheduleIds)
                : null;
            // The overlap cap lives in `fib_flow_meta`; expose it so operators can
            // see the value that is actually in force.
            const maxInstancesBySchedule = this._getScheduleMaxInstancesWithConnection(conn, rows);

            return rows.map(row => {
                const task = parseTask(row);
                const identity = scheduleIdentity(row.id);
                return {
                    id: task.id,
                    name: task.name,
                    tag: task.tag || null,
                    cron_key: task.cron_key || null,
                    cron_expr: task.cron_expr || null,
                    cron_tz: task.cron_tz || null,
                    cron_catch_up: task.cron_catch_up || 'once',
                    retention_expire_time: task.retention_expire_time || null,
                    max_instances: maxInstancesBySchedule.has(identity)
                        ? maxInstancesBySchedule.get(identity)
                        : undefined,
                    next_run_time: task.next_run_time,
                    status: task.status,
                    retry_count: task.retry_count || 0,
                    last_error: task.error || null,
                    ...(lastSpawned ? { last_spawned_task_id: lastSpawned.get(task.id) || null } : {}),
                    orphan: known ? !known.has(task.name) : undefined
                };
            });
        });
    }

    /**
     * Bulk-load persisted `maxInstances` policies for the given schedule rows.
     * @private
     * @param {object} conn - Open connection
     * @param {Array<object>} rows - Raw schedule rows
     * @returns {Map<string, number|null>} schedule id -> cap
     */
    _getScheduleMaxInstancesWithConnection(conn, rows) {
        const result = new Map();
        if (!Array.isArray(rows) || rows.length === 0) {
            return result;
        }

        const keys = rows.map(row => buildScheduleMetaKey(
            'max_instances',
            row.name,
            BaseDBAdapter.normalizeCronKey(row.cron_key)
        ));

        const metaRows = conn.execute(
            `SELECT meta_key, meta_value FROM fib_flow_meta WHERE meta_key IN ?`,
            keys
        );

        const byKey = new Map();
        for (const metaRow of metaRows) {
            const raw = String(metaRow.meta_value);
            if (raw === 'null') {
                byKey.set(metaRow.meta_key, null);
                continue;
            }
            const parsed = Number(raw);
            if (Number.isFinite(parsed)) {
                byKey.set(metaRow.meta_key, parsed);
            }
        }

        rows.forEach((row, index) => {
            if (byKey.has(keys[index])) {
                result.set(scheduleIdentity(row.id), byKey.get(keys[index]));
            }
        });

        return result;
    }

    /**
     * Resolve the most recently spawned run id for each schedule.
     *
     * Reads the audit trail rather than adding a column: `fib_flow_tasks` is
     * schema-version gated, so a new column would force a rebuild of every
     * existing deployment.
     *
     * @private
     * @param {object} conn - Open connection
     * @param {Array<number|string>} scheduleIds - Schedule row ids
     * @returns {Map<number|string, number|string>} schedule id -> spawned run id
     */
    _getLastSpawnedTaskIds(conn, scheduleIds) {
        const latest = new Map();
        if (!Array.isArray(scheduleIds) || scheduleIds.length === 0) {
            return latest;
        }

        const rows = conn.execute(
            `SELECT task_id, metadata
               FROM fib_flow_task_events
              WHERE task_id IN ?
                AND event_type = 'task_cron_spawned'
              ORDER BY task_id ASC, id DESC`,
            scheduleIds
        );

        for (const row of rows) {
            if (latest.has(row.task_id)) {
                continue;
            }
            let metadata = row.metadata;
            if (typeof metadata === 'string') {
                try {
                    metadata = JSON.parse(metadata);
                } catch (_error) {
                    metadata = null;
                }
            }
            const spawnedId = metadata ? metadata.spawned_task_id : null;
            if (spawnedId !== undefined && spawnedId !== null) {
                latest.set(row.task_id, spawnedId);
            }
        }

        return latest;
    }

    /**
     * Keep the per-round audit trail of a schedule bounded.
     *
     * A schedule row is never collected by retention (`pending` is outside the
     * retention status gate), so every per-round event would stay forever: a
     * one-minute schedule writes half a million rows a year. The spawned run rows
     * are the authoritative round history — they carry their own events and are
     * collected normally — so the schedule only needs a bounded tail for "did this
     * schedule fire recently, and which run did it produce".
     *
     * Deletes by id watermark instead of `NOT IN (subquery)`: MySQL rejects
     * deleting from a table referenced in the subquery, and the watermark form is
     * portable across all three engines.
     *
     * @private
     * @param {object} conn - Open connection
     * @param {number|string} scheduleId - Schedule row id
     */
    _pruneCronRoundEventsWithConnection(conn, scheduleId) {
        const rows = conn.execute(
            `SELECT id
               FROM fib_flow_task_events
              WHERE task_id = ?
                AND event_type IN ?
              ORDER BY id DESC
              LIMIT ?`,
            scheduleId,
            CRON_ROUND_EVENT_TYPES,
            CRON_ROUND_EVENT_HISTORY
        );

        // Fewer rows than the cap: nothing to trim.
        if (rows.length < CRON_ROUND_EVENT_HISTORY) {
            return;
        }

        const watermark = rows[rows.length - 1].id;
        conn.execute(
            `DELETE FROM fib_flow_task_events
              WHERE task_id = ?
                AND event_type IN ?
                AND id < ?`,
            scheduleId,
            CRON_ROUND_EVENT_TYPES,
            watermark
        );
    }

    /**
     * Cancel pending cron schedules whose handler is not registered.
     * Rows are cancelled (never deleted); retention removes them afterwards.
     *
     * @param {Array<string>} registeredNames - Currently registered handler names
     * @param {string} [reason] - Cancellation reason
     * @returns {{cancelled:number, taskIds:Array<number>}}
     */
    pruneCronSchedules(registeredNames, reason = 'handler no longer registered') {
        if (!Array.isArray(registeredNames)) {
            throw new Error('registeredNames array is required');
        }

        const known = new Set(registeredNames);
        const orphans = this.getCronSchedules({ status: 'pending' })
            .filter(schedule => !known.has(schedule.name));

        const result = { cancelled: 0, taskIds: [] };
        for (const schedule of orphans) {
            this.updateTaskStatus(schedule.id, 'cancelled', { error: reason, reason });
            result.cancelled += 1;
            result.taskIds.push(schedule.id);
        }

        return result;
    }

    /**
     * Aggregate cron schedule counters.
     * @param {Array<string>} [registeredNames] - Registered handler names; enables orphan counting
     * @returns {{total:number, active:number, paused:number, cancelled:number, orphan:number}}
     */
    getCronScheduleStats(registeredNames = null) {
        const schedules = this.getCronSchedules({}, registeredNames);
        const stats = {
            total: schedules.length,
            active: 0,
            paused: 0,
            cancelled: 0,
            orphan: 0
        };

        for (const schedule of schedules) {
            if (schedule.status === 'pending' || schedule.status === 'running') {
                stats.active += 1;
            } else if (schedule.status === 'paused') {
                stats.paused += 1;
            } else if (schedule.status === 'cancelled') {
                stats.cancelled += 1;
            }

            if (schedule.orphan) {
                stats.orphan += 1;
            }
        }

        return stats;
    }

    /**
     * Get tasks by multiple filter conditions
     * @param {object} filters - Filter conditions
     * @param {string} [filters.tag] - Filter by tag
     * @param {string} [filters.status] - Filter by status (must be valid task state)
     * @param {string} [filters.name] - Filter by task name
     * @returns {Array<object>} Array of tasks matching all filter conditions
     * @throws {Error} If status is invalid
     */
    getTasks(filters = {}) {
        return this.queryTasks(filters).items;
    }

    /**
     * Delete tasks by filter conditions
     * @param {object} filters - Filter conditions
     * @param {string} [filters.tag] - Filter by tag
     * @param {string} [filters.status] - Filter by status (must be valid task state)
     * @param {string} [filters.name] - Filter by task name
     * @param {string} [filters.type] - Filter by task type ('async' | 'cron')
     * @returns {number} Number of tasks deleted
     * @throws {Error} If no filter is provided, or a filter value is invalid
     */
    deleteTasks(filters = {}) {
        logger.info(`[deleteTasks] Deleting tasks with filters:`, filters);

        if (filters.status) {
            assertTaskStatus(filters.status);
        }
        if (filters.type) {
            assertTaskType(filters.type, 'Invalid task type');
        }

        return this.pool(conn => {
            const conditions = [];
            const params = [];

            if (filters.tag) {
                conditions.push('tag = ?');
                params.push(filters.tag);
            }
            if (filters.status) {
                conditions.push('status = ?');
                params.push(filters.status);
            }
            if (filters.name) {
                conditions.push('name = ?');
                params.push(filters.name);
            }
            if (filters.type) {
                conditions.push('type = ?');
                params.push(filters.type);
            }

            if (conditions.length === 0) {
                throw new Error('deleteTasks requires at least one filter (type, name, tag or status); use clearTasks() to wipe the table');
            }

            const whereSql = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';
            const taskRows = conn.execute(`SELECT id FROM fib_flow_tasks${whereSql}`, ...params);
            const taskIds = uniqueTaskIds(taskRows);


            this._deleteTaskAuditDataWithConnection(conn, taskIds);

            const sql = `DELETE FROM fib_flow_tasks${whereSql}`;

            logger.debug(`[deleteTasks] Executing query: ${sql} with params:`, params);
            const result = conn.execute(sql, ...params);
            logger.info(`[deleteTasks] Deleted ${result.affected} tasks`);
            return result.affected;
        });
    }

    /**
     * Close database connection and cleanup resources
     * Should be called when adapter is no longer needed
     */
    close() {
        logger.info(`[close] Closing database connection`);
        if (this.pool) {
            this.pool.clear();
            this.pool = null;
        }
    }

    /**
     * Provides real-time visibility into active task processing.
     * Enables monitoring and potential intervention for long-running tasks.
     * 
     * @returns {Array<object>} Currently executing tasks with parsed metadata
     */
    getRunningTasks() {
        logger.info(`[getRunningTasks] Retrieving running tasks`);
        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE status = ?', 'running');
            return tasks.map(task => parseTask(task));
        });
    }

    /**
     * Allows complete task database reset for testing or maintenance.
     * Provides a controlled mechanism to clear all task records.
     * 
     * @returns {number} Count of tasks permanently removed
     */
    clearTasks() {
        logger.info(`[clearTasks] Clearing all tasks`);
        return this.pool(conn => {
            conn.execute('DELETE FROM fib_flow_task_events');
            conn.execute('DELETE FROM fib_flow_task_attempts');
            conn.execute('DELETE FROM fib_flow_workers');
            const rs = conn.execute('DELETE FROM fib_flow_tasks');
            return rs.affected;
        });
    }

    /**
     * Retrieves task hierarchy to support complex workflow tracking.
     * Enables understanding of task dependencies and execution context.
     * 
     * @param {string|number} parentId - Identifier for parent task
     * @returns {Array<object>} Detailed child task information
     * @throws {Error} Prevents queries without a valid parent task ID
     */
    getChildTasks(parentId) {
        logger.info(`[getChildTasks] Retrieving child tasks for parent ID: ${parentId}`);

        if (!parentId) {
            throw new Error('Parent task ID is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute(`SELECT * FROM fib_flow_tasks WHERE parent_id = ?`, parentId);
            return tasks.map(task => parseTask(task));
        });
    }
}

module.exports = BaseDBAdapter;