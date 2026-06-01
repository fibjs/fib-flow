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

const DEFAULT_RETENTION_STATUSES = ['completed', 'permanently_failed'];
const ALLOWED_RETENTION_STATUSES = ['completed', 'permanently_failed', 'paused'];
const AUDIT_NAME_PATTERN = /^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$/;
const INTERNAL_RETENTION_BATCH_SIZE = 100;

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

    if (rawExpireTime === null || rawExpireTime === undefined || rawExpireTime === false) {
        return {
            expire_time: null,
            statuses: DEFAULT_RETENTION_STATUSES.slice(),
            now
        };
    }

    const expireTime = Number(rawExpireTime);
    if (!Number.isFinite(expireTime) || expireTime <= 0) {
        throw new Error('retention expire_time must be a positive number');
    }

    const statuses = hasOwn(input, 'statuses') ? input.statuses : DEFAULT_RETENTION_STATUSES;
    if (!Array.isArray(statuses) || statuses.length === 0) {
        throw new Error('retention statuses must be a non-empty array');
    }

    const normalizedStatuses = statuses.map(status => String(status));
    normalizedStatuses.forEach(status => {
        if (!ALLOWED_RETENTION_STATUSES.includes(status)) {
            throw new Error(`Unsupported retention status: ${status}`);
        }
    });

    return {
        expire_time: expireTime,
        statuses: Array.from(new Set(normalizedStatuses)),
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

function normalizeAuditName(value, fieldName) {
    const normalized = normalizeTrimmedString(value, fieldName);
    if (!AUDIT_NAME_PATTERN.test(normalized)) {
        throw new Error(`${fieldName} must use lowercase snake_case`);
    }

    return normalized;
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
        if (conn == "sqlite::memory:") {
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
                if (!['async', 'cron'].includes(task.type)) {
                    throw new Error('Task type must be either "async" or "cron"');
                }
                // Validate task status if provided
                if (task.status && !['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'].includes(task.status)) {
                    throw new Error(`Invalid task status: ${task.status}`);
                }

                logger.info(`[BaseDBAdapter] Inserting task with name: ${task.name}`);
                const rs = conn.execute(
                    `INSERT INTO fib_flow_tasks (
                            name, type, status, priority, payload, cron_expr,
                            max_retries, retry_interval, next_run_time, timeout,
                            created_at, root_id, parent_id, total_children, completed_children,
                            tag
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)`,
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
                    task.tag || null
                );

                const taskId = this._getLastInsertedId(conn, rs);
                taskIds.push(taskId);
                childTaskNames.push(task.name);
                events.push({
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
                        tag: task.tag || null
                    }
                });
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
        if (!retentionPolicy.expire_time) {
            return {
                tasks_deleted: 0,
                events_deleted: 0,
                attempts_deleted: 0,
                has_more: false
            };
        }

        const expiredTaskRows = conn.execute(
            `SELECT id
             FROM fib_flow_tasks
             WHERE status IN ?
             AND last_active_time < ?
             ORDER BY last_active_time ASC, id ASC
             LIMIT ?`,
            retentionPolicy.statuses,
            retentionPolicy.now - retentionPolicy.expire_time,
            retentionPolicy.batch_size
        );
        const expiredTaskIds = uniqueTaskIds(expiredTaskRows);
        if (expiredTaskIds.length === 0) {
            return {
                tasks_deleted: 0,
                events_deleted: 0,
                attempts_deleted: 0,
                has_more: false
            };
        }

        const auditCleanup = this._deleteTaskAuditDataWithConnection(conn, expiredTaskIds);
        conn.execute('DELETE FROM fib_flow_tasks WHERE id IN ?', expiredTaskIds);

        return {
            tasks_deleted: expiredTaskIds.length,
            events_deleted: auditCleanup.events_deleted,
            attempts_deleted: auditCleanup.attempts_deleted,
            has_more: expiredTaskIds.length === retentionPolicy.batch_size
        };
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
              WHERE id = ? AND status = 'suspended' AND completed_children = total_children`,
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

        if (filters.status && !['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'].includes(filters.status)) {
            throw new Error('Invalid status value');
        }
        if (filters.type && !['async', 'cron'].includes(filters.type)) {
            throw new Error('Invalid task type');
        }

        const pagination = normalizePagination(filters);

        return this.pool(conn => {
            const params = [];
            const conditions = [];

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
     * @returns {object|null} Next available task or null if no tasks are ready
     * @throws {Error} If task name selection is invalid
     */
    claimTask(taskNames, workerId) {
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

        let task = null;
        const now = Math.floor(Date.now() / 1000);

        this.pool(conn => {
            while (true) {
                // Find executable tasks
                logger.debug(`[claimTask] Searching for pending tasks`);
                const rs = conn.execute(
                    `SELECT * FROM fib_flow_tasks 
                        WHERE status = 'pending' 
                        AND name IN ? 
                        AND next_run_time <= ? 
                        ORDER BY priority DESC, next_run_time ASC 
                        LIMIT 1 
                        ${this.lockClause}`,
                    taskNames,
                    now
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
                        const nextAttempt = this._getNextAttemptNumberWithConnection(conn, task.id);
                        task.status = 'running';
                        task.last_active_time = now;
                        task.worker_id = workerId;
                        task.start_time = now;
                        task.attempt = nextAttempt;
                        const claimEvents = [{
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

                        if ((task.retry_count || 0) > 0) {
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

                        this._startTaskAttemptWithConnection(conn, {
                            task_id: task.id,
                            attempt: nextAttempt,
                            worker_id: workerId,
                            started_at: now
                        });

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
            'pending': ['running', 'failed', 'timeout', 'paused', 'suspended'],  // Add suspended
            'permanently_failed': ['failed', 'timeout'],
            'paused': ['running', 'pending', 'failed', 'timeout'],
            'suspended': ['running']  // Only running tasks can be suspended
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

            const result_json = extra.result ? JSON.stringify(extra.result) : 'null';
            updates.push('result = ?');
            params.push(result_json);

            if ('error' in extra) {
                updates.push('error = ?');
                params.push(extra.error);
            }

            if (status === 'pending') {
                updates.push('stage = 0');
            }

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
                                        : 'task_status_changed',
                    from_status: currentTask.status,
                    to_status: status,
                    stage: status === 'pending' ? 0 : currentTask.stage,
                    worker_id: currentTask.worker_id || null,
                    event_time: eventTime,
                    message: `Task ${currentTask.name} status changed from ${currentTask.status} to ${status}`,
                    metadata: {
                        error: 'error' in extra ? extra.error : null,
                        retry_count: 'retry_count' in extra ? extra.retry_count : null,
                        next_run_time: 'next_run_time' in extra ? extra.next_run_time : null
                    }
                }]);

                if (extra.parent_id && status === 'completed') {
                    logger.info(`[updateTaskStatus] Updating parent task ${extra.parent_id}`);
                    this._resumeParentTaskWithConnection(conn, extra.parent_id, `${taskId}:${result_json}\n`, eventTime);
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
        return this._runTimeoutSweepStep('retention cleanup', {
            tasks_deleted: 0,
            events_deleted: 0,
            attempts_deleted: 0
        }, () => this.pool(conn => this._cleanupExpiredTasksWithConnection(conn, policy)));
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

        if (retentionPolicy.expire_time) {
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
        if (!retentionPolicy.expire_time) {
            return {
                tasks_deleted: 0,
                events_deleted: 0,
                attempts_deleted: 0
            };
        }

        const totals = {
            tasks_deleted: 0,
            events_deleted: 0,
            attempts_deleted: 0
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

            if (!cleanupResult.has_more || cleanupResult.tasks_deleted === 0) {
                return totals;
            }
        }
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
     * @returns {Array<object>} Array of matching tasks with parsed payloads
     * @throws {Error} If status is invalid or missing
     */
    getTasksByStatus(status) {
        logger.info(`[getTasksByStatus] Retrieving tasks with status: ${status}`);

        if (!status) {
            throw new Error('Status is required');
        }
        if (!['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'].includes(status)) {
            throw new Error('Invalid status value');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE status = ?', status);
            return tasks.map(task => parseTask(task));
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
     * @returns {number} Number of tasks deleted
     * @throws {Error} If status is invalid
     */
    deleteTasks(filters = {}) {
        logger.info(`[deleteTasks] Deleting tasks with filters:`, filters);

        // Validate status if provided
        if (filters.status && !['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'].includes(filters.status)) {
            throw new Error('Invalid status value');
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