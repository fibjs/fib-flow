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

            // Updates parent task state to reflect child task creation
            if (options.parent_id) {
                logger.info(`[BaseDBAdapter] Updating parent task ${options.parent_id}`);

                let rs;
                // Check if context update is needed
                if (options.context !== undefined) {
                    logger.info(`[BaseDBAdapter] Updating parent task ${options.parent_id} with context data`);
                    rs = conn.execute(
                        `UPDATE fib_flow_tasks 
                             SET total_children = total_children + ?,
                                 status = 'suspended',
                                 context = ?,
                                 result = null
                             WHERE id = ? AND status = 'running'`,
                        taskArray.length,
                        options.context,
                        options.parent_id
                    );
                } else {
                    rs = conn.execute(
                        `UPDATE fib_flow_tasks 
                             SET total_children = total_children + ?,
                                 status = 'suspended',
                                 result = null
                             WHERE id = ? AND status = 'running'`,
                        taskArray.length,
                        options.parent_id
                    );
                }

                if (rs.affected === 0) {
                    throw new Error(`Parent task ${options.parent_id} is not in running state`);
                }
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

                taskIds.push(this._getLastInsertedId(conn, rs));
            }
        }));

        return isArray ? taskIds : taskIds[0];
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

            // Always update last_active_time for status changes
            updates.push('last_active_time = ?');
            params.push(Math.floor(Date.now() / 1000));

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
            params.push(taskId, allowedPreviousStatuses[status]);
            logger.debug(`[updateTaskStatus] Executing update query with params:`, params);

            conn.trans(() => {
                const rs = conn.execute(`
                        UPDATE fib_flow_tasks 
                        SET ${updates.join(', ')}
                        WHERE id = ? AND status IN ?
                    `, ...params);

                if (rs.affected === 0) {
                    const rs1 = conn.execute(`SELECT status FROM fib_flow_tasks WHERE id = ?`, taskId);
                    throw new Error(`Failed to update task ${taskId}. Current status: ${rs1[0].status}`);
                }
                logger.info(`[updateTaskStatus] Successfully updated task ${taskId}, affected rows: ${rs.affected}`);

                if (extra.parent_id && status === 'completed') {
                    logger.info(`[updateTaskStatus] Updating parent task ${extra.parent_id}`);
                    conn.execute(`
                            UPDATE fib_flow_tasks
                            SET
                                completed_children = completed_children + 1,
                                result = CONCAT(result, ?)
                            WHERE id = ?;
                            UPDATE fib_flow_tasks
                            SET
                                status = 'pending',
                                stage = stage + 1
                            WHERE id = ? AND status = 'suspended' AND completed_children = total_children
                        `, `${taskId}:${result_json}\n`, extra.parent_id, extra.parent_id);
                    logger.info(`[updateTaskStatus] Parent task update completed for ${extra.parent_id}`);
                }
            });
        });
    }

    /**
     * Update last active time for running tasks
     * Used to track task health and detect timeouts
     * @param {Array<string|number>} taskIds - IDs of tasks to update
     */
    updateTaskActiveTime(taskIds) {
        logger.info(`[updateTaskActiveTime] Updating active time for tasks:`, taskIds);

        if (!Array.isArray(taskIds) || taskIds.length === 0) {
            return;
        }
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => {
            logger.debug(`[updateTaskActiveTime] Executing update query`);
            return conn.execute(
                'UPDATE fib_flow_tasks SET last_active_time = ? WHERE id IN ?',
                now, taskIds
            );
        });
    }

    /**
     * Handle tasks that have exceeded their timeout period
     * Implements the following timeout handling logic:
     * 1. Marks tasks as permanently_failed if max retries reached
     * 2. Marks running tasks as timeout if inactive
     * 3. Schedules retry attempts for failed tasks within retry limit
     * 4. Cleans up expired completed and permanently_failed tasks
     * @param {number} active_update_interval - Interval between active updates in ms
     * @param {number} [expireTime] - Time in seconds after which completed and failed tasks are deleted
     * @returns {object} Count of tasks in each state transition
     */
    handleTimeoutTasks(active_update_interval, expireTime = null) {
        logger.info(`[handleTimeoutTasks] Starting timeout task handling${expireTime ? `, with expire time: ${expireTime}s` : ''}`);
        const now = Math.floor(Date.now() / 1000);

        return this.pool(conn => {
            // 1. total timeout: select ids first, then update by PK in small batches
            logger.debug(`[handleTimeoutTasks] Checking for total timeout tasks`);
            const totalTimeoutIds = conn.execute(
                `SELECT id FROM fib_flow_tasks
                     WHERE status = 'running'
                     AND start_time + timeout < ?`,
                now
            ).map(r => r.id);

            let totalTimeoutAffected = 0;
            for (let i = 0; i < totalTimeoutIds.length; i += 100) {
                const batch = totalTimeoutIds.slice(i, i + 100);
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'timeout',
                             error = 'Task exceeded total timeout limit',
                             last_active_time = ?
                         WHERE id IN ? AND status = 'running'`,
                    now, batch
                );
                totalTimeoutAffected += rs.affected;
            }

            if (totalTimeoutAffected) {
                logger.warning(`[handleTimeoutTasks] ${totalTimeoutAffected} tasks exceeded total timeout`);
            }

            // 2. Check tasks that have lost heartbeat (5 consecutive missing updates)
            logger.debug(`[handleTimeoutTasks] Checking for heartbeat timeout tasks`);

            const heartbeatThreshold = now - Math.floor(active_update_interval * 5 / 1000);
            const heartbeatTimeoutIds = conn.execute(
                `SELECT id, last_active_time FROM fib_flow_tasks
                     WHERE status = 'running'
                     AND last_active_time IS NOT NULL
                     AND last_active_time < ?`,
                heartbeatThreshold
            ).map(r => r.id);

            let heartbeatTimeoutAffected = 0;
            for (let i = 0; i < heartbeatTimeoutIds.length; i += 100) {
                const batch = heartbeatTimeoutIds.slice(i, i + 100);
                let last_active_time = heartbeatTimeoutIds[i].last_active_time;
                let error_msg = `Task heartbeat lost - worker may be dead - last_active_time=${last_active_time} - now=${now}`;
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'timeout',
                             error = ${error_msg},
                             last_active_time = ?
                         WHERE id IN ? AND status = 'running'`,
                    now, batch
                );
                heartbeatTimeoutAffected += rs.affected;
            }

            if (heartbeatTimeoutAffected) {
                logger.warning(`[handleTimeoutTasks] ${heartbeatTimeoutAffected} tasks lost heartbeat`);
            }

            // Update tasks to 'pending' if retry interval has passed
            logger.debug(`[handleTimeoutTasks] Checking for tasks eligible for retry`);
            const retryIds = conn.execute(
                `SELECT id FROM fib_flow_tasks
                     WHERE status IN ('timeout','failed')
                     AND retry_count+ 1 < max_retries
                     AND (last_active_time + retry_interval < ?)`,
                now
            ).map(r => r.id);

            let retryAffected = 0;
            for (let i = 0; i < retryIds.length; i += 100) {
                const batch = retryIds.slice(i, i + 100);
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = 'pending',
                             stage = 0,
                             result = null,
                             context = null,
                             retry_count = retry_count + 1,
                             last_active_time = ?,
                             next_run_time = ? + retry_interval
                         WHERE id IN ? AND status IN ('timeout','failed')`,
                    now, now, batch
                );
                retryAffected += rs.affected;
            }

            if (retryAffected) {
                logger.info(`[handleTimeoutTasks] ${retryAffected} tasks scheduled for retry`);
            }


            // Update cron tasks with no retries left to 'paused'
            logger.debug(`[handleTimeoutTasks] Checking for cron tasks that have exhausted retries`);

            const exhaustedIds = conn.execute(
                `SELECT id FROM fib_flow_tasks
                     WHERE status IN ('timeout','failed')
                     AND type = 'cron'
                     AND retry_count + 1 >= max_retries`,
                now
            ).map(r => r.id);

            let exhaustedAffected = 0;
            for (let i = 0; i < exhaustedIds.length; i += 100) {
                const batch = exhaustedIds.slice(i, i + 100);
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks
                         SET status = CASE type WHEN 'cron' THEN 'paused' ELSE 'permanently_failed' END,
                             last_active_time = ?
                         WHERE id IN ? AND status IN ('timeout','failed')`,
                    now, batch
                );
                exhaustedAffected += rs.affected;
            }

            if (exhaustedAffected) {
                logger.warning(`[handleTimeoutTasks] ${exhaustedAffected} tasks permanently failed`);
            }

            const failedRs = conn.execute(
                `SELECT id, parent_id, error
                    FROM fib_flow_tasks 
                    WHERE status IN ('timeout', 'failed') 
                    AND type = 'async' 
                    AND retry_count + 1 >= max_retries 
                    `
            );

            failedRs.forEach(task => {
                this.pool(conn => {
                    conn.trans(() => {
                        const rs = conn.execute(`
                        UPDATE fib_flow_tasks 
                            SET status = 'permanently_failed',
                            last_active_time = ?
                            WHERE id = ? AND status IN ('timeout', 'failed')
                        `, now, task.id);

                        if (rs.affected === 1 && task.parent_id) {
                            logger.info(`[handleTimeoutTasks] Updating parent task ${task.parent_id}`);
                            conn.execute(`
                                UPDATE fib_flow_tasks
                                SET
                                    completed_children = completed_children + 1,
                                    result = CONCAT(result, ?)
                                WHERE id = ?;
                                UPDATE fib_flow_tasks
                                SET
                                    status = 'pending',
                                    stage = stage + 1
                                WHERE id = ? AND status = 'suspended' AND completed_children = total_children
                        `, `${task.id}!${JSON.stringify(task.error)}\n`, task.parent_id, task.parent_id);
                            logger.info(`[handleTimeoutTasks] Parent task update completed for ${task.parent_id}`);
                        }
                    });
                });

            });

            if (expireTime) {
                // Clean up expired tasks based on last_active_time
                const expiredTasksRs = conn.execute(
                    `DELETE FROM fib_flow_tasks 
                        WHERE status IN ('completed', 'permanently_failed') 
                        AND last_active_time < ?`,
                    now - expireTime
                );

                if (expiredTasksRs.affected) {
                    logger.info(`[handleTimeoutTasks] Cleaned up ${expiredTasksRs.affected} expired tasks`);
                }
            }

            logger.debug(`[handleTimeoutTasks] Timeout task handling completed`);
        });
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
        logger.info(`[getTasks] Retrieving tasks with filters:`, filters);

        // Validate status if provided
        if (filters.status && !['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'].includes(filters.status)) {
            throw new Error('Invalid status value');
        }

        return this.pool(conn => {
            let sql = 'SELECT * FROM fib_flow_tasks';
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

            if (conditions.length > 0) {
                sql += ' WHERE ' + conditions.join(' AND ');
            }

            // Order by creation time descending to show newest first
            sql += ' ORDER BY created_at DESC';

            logger.debug(`[getTasks] Executing query: ${sql} with params:`, params);
            const tasks = conn.execute(sql, ...params);
            return tasks.map(task => parseTask(task));
        });
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
            let sql = 'DELETE FROM fib_flow_tasks';
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

            if (conditions.length > 0) {
                sql += ' WHERE ' + conditions.join(' AND ');
            }

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