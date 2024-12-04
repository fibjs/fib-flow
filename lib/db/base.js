/**
 * Base database adapter providing common functionality for task persistence and management.
 * Implements connection pooling, task CRUD operations, and task state management.
 * This class serves as an abstract base class that specific database adapters should extend.
 */

const db = require('db');
const Pool = require('fib-pool');

/**
 * Abstract base class for database adapters that provides core task management functionality.
 * Implements connection pooling and essential task operations including:
 * - Task creation and persistence
 * - Task state management (pending, running, completed, failed, etc.)
 * - Task scheduling and retry logic
 * - Task querying and filtering
 * - Connection pooling and resource management
 */
class BaseDBAdapter {
    /**
     * Initialize database adapter with connection pooling support
     * @param {string|object|function} conn - Connection configuration:
     *                                       - string: Connection string for database
     *                                       - object: Existing database connection
     *                                       - function: Custom pool implementation
     * @param {number} [poolSize=5] - Maximum number of connections in the pool
     * @throws {Error} If connection type is invalid
     */
    constructor(conn, poolSize = 5) {
        // is function
        if (typeof conn === 'function') {
            this.pool = conn;
        } else if (typeof conn === 'string') {
            this.pool = Pool({
                create: () => {
                    return this.createConnection(conn);
                },
                destroy: conn => this.destroyConnection(conn),
                timeout: 30000,
                retry: 1,
                maxsize: poolSize
            });
        } else if (typeof conn === 'object') {
            this.pool = function (callback) {
                return callback(conn);
            };
            this.pool.clear = function () {
                this.destroyConnection(conn);
            }.bind(this);
        } else {
            throw new Error('Invalid connection type: ' + conn);
        }
    }

    /**
     * Initialize database schema
     * This is an abstract method that should be implemented by specific adapters
     * @throws {Error} If called directly on BaseDBAdapter
     */
    setup() {
        throw new Error('setup() must be implemented by subclass');
    }

    /**
     * Create new database connection
     * Protected method that should be implemented by specific database adapters
     * @param {string} connStr - Database connection string
     * @returns {object} Database connection instance
     * @protected
     */
    createConnection(connStr) {
        return db.open(connStr);
    }

    /**
     * Close and cleanup database connection
     * Protected method that should be implemented by specific database adapters
     * @param {object} conn - Database connection to destroy
     * @protected
     */
    destroyConnection(conn) {
        conn.close();
    }

    /**
     * Insert new task(s) into database with validation
     * @param {object|Array<object>} tasks - Single task object or array of task objects
     * @param {object} [options] - Optional parameters for workflow
     * @param {number} [options.root_id] - ID of the root task (workflow instance)
     * @param {number} [options.parent_id] - ID of the parent task
     * @returns {number|Array<number>} ID(s) of created task(s)
     * @throws {Error} If required fields are missing or invalid
     */
    insertTask(tasks, options = {}) {
        const isArray = Array.isArray(tasks);
        const taskArray = isArray ? tasks : [tasks];

        const taskIds = [];
        this.pool(conn => conn.trans(() => {
            const now = Math.floor(Date.now() / 1000);

            if (options.parent_id) {
                const rs = conn.execute(
                    `UPDATE dcron_tasks 
                         SET total_children = total_children + ?,
                             status = 'suspended'
                         WHERE id = ? AND status = 'running'`,
                    taskArray.length,
                    options.parent_id
                );

                if (rs.affected === 0) {
                    throw new Error(`Parent task ${options.parent_id} is not in running state`);
                }
            }

            for (const task of taskArray) {
                if (!task) {
                    throw new Error('Task object is required');
                }
                if (!task.name) {
                    throw new Error('Task name is required');
                }
                if (!task.type) {
                    throw new Error('Task type is required');
                }
                if (!['async', 'cron'].includes(task.type)) {
                    throw new Error('Task type must be either "async" or "cron"');
                }

                const rs = conn.execute(
                    `INSERT INTO dcron_tasks (
                            name, type, status, priority, payload, cron_expr,
                            max_retries, retry_interval, next_run_time, timeout,
                            created_at, root_id, parent_id, total_children, completed_children
                        ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)`,
                    task.name,
                    task.type,
                    task.priority || 0,
                    task.payload ? JSON.stringify(task.payload) : null,
                    task.cron_expr || null,
                    task.max_retries !== undefined ? task.max_retries : 3,
                    task.retry_interval || 0,
                    task.next_run_time || now,
                    task.timeout || 60,
                    now,
                    options.root_id || null,
                    options.parent_id || null
                );

                taskIds.push(rs.insertId);
            }
        }));

        return isArray ? taskIds : taskIds[0];
    }

    /**
     * Claim next available task for processing with priority and timing consideration
     * Implements optimistic locking to handle concurrent workers
     * @param {Array<string>} taskNames - List of task names to consider for execution
     * @returns {object|null} Task object if available, null if no tasks are ready
     * @throws {Error} If taskNames is not a non-empty array
     */
    claimTask(taskNames) {
        if (!Array.isArray(taskNames) || taskNames.length === 0) {
            throw new Error('Task names array is required');
        }

        let task = null;
        const now = Math.floor(Date.now() / 1000);

        try {
            this.pool(conn => {
                while (true) {
                    // Find executable tasks
                    const rs = conn.execute(
                        `SELECT * FROM dcron_tasks 
                        WHERE status = 'pending' 
                        AND name IN ?
                        AND next_run_time <= ?
                        ORDER BY priority DESC, next_run_time ASC
                        LIMIT 1`,
                        taskNames,
                        now
                    );

                    if (rs.length > 0) {
                        // Update task status
                        const updateResult = conn.execute(
                            `UPDATE dcron_tasks 
                            SET status = 'running',
                                last_active_time = ?
                            WHERE id = ? AND status = 'pending'`,
                            now,
                            rs[0].id
                        );

                        if (updateResult.affected) {
                            task = rs[0];
                            break;
                        }
                    } else {
                        break; // No pending tasks available
                    }
                }
            });
        } catch (e) {
            console.error('Failed to claim task:', e);
        }

        if (task && task.payload) {
            try {
                task.payload = JSON.parse(task.payload);
            } catch (e) {
                console.error('Failed to parse task payload:', e);
            }
        }

        return task;
    }

    /**
     * Update task status with state transition validation
     * Enforces valid state transitions to maintain task lifecycle integrity
     * @param {string|number} taskId - ID of task to update
     * @param {string} status - New status value (must be valid state)
     * @param {object} [extra] - Additional fields to update:
     *                        - result: Task execution result
     *                        - error: Error message if failed
     *                        - next_run_time: Next scheduled run
     *                        - retry_count: Current retry attempt
     * @throws {Error} If status transition is invalid or update fails
     */
    updateTaskStatus(taskId, status, extra = {}) {
        const allowedPreviousStatuses = {
            'running': ['pending'],
            'completed': ['running'],
            'failed': ['running'],
            'timeout': ['running'],
            'pending': ['running', 'failed', 'timeout', 'paused', 'suspended'],  // 添加 suspended
            'permanently_failed': ['failed', 'timeout'],
            'paused': ['running', 'failed', 'timeout'],
            'suspended': ['running']  // 只有 running 状态的任务可以转为 suspended
        };

        if (!allowedPreviousStatuses[status])
            throw new Error('Invalid status value');

        this.pool(conn => {
            // Build dynamic SQL update statement
            let updates = ['status = ?'];
            let params = [status];

            if ('result' in extra) {
                updates.push('result = ?');
                params.push(extra.result ? JSON.stringify(extra.result) : null);
            }

            if ('error' in extra) {
                updates.push('error = ?');
                params.push(extra.error);
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

            conn.trans(() => {
                const rs = conn.execute(`
                    UPDATE dcron_tasks 
                    SET ${updates.join(', ')}
                    WHERE id = ? AND status IN ?
                `, ...params);

                if (rs.affected === 0) {
                    throw new Error(`Failed to update task ${taskId}`);
                }

                if (extra.parent_id) {
                    const rs = conn.execute(`
                        UPDATE dcron_tasks 
                        SET status = CASE
                            WHEN completed_children + 1 = total_children THEN 'pending'
                            ELSE 'suspended'
                        END,
                        completed_children = completed_children + 1
                        WHERE id = ? AND status = 'suspended'
                    `, extra.parent_id);

                    if (rs.affected === 0) {
                        throw new Error(`Failed to update parent task ${extra.parent_id}`);
                    }
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
        if (!Array.isArray(taskIds) || taskIds.length === 0) {
            return;
        }
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => {
            return conn.execute(
                'UPDATE dcron_tasks SET last_active_time = ? WHERE id IN ?',
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
     * @returns {object} Count of tasks in each state transition
     */
    handleTimeoutTasks() {
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => conn.trans(() => {
            // Set remaining timed-out tasks to 'timeout'
            const timeoutRs = conn.execute(
                `UPDATE dcron_tasks 
                SET status = 'timeout', last_active_time = ? 
                WHERE status = 'running' 
                AND (last_active_time IS NOT NULL AND last_active_time + timeout < ?)`,
                now, now
            );

            if (timeoutRs.affected)
                console.log(`${timeoutRs.affected} tasks set to timeout.`);

            // Update tasks to 'pending' if retry interval has passed
            const retryRs = conn.execute(
                `UPDATE dcron_tasks 
                SET status = 'pending', 
                    retry_count = retry_count + 1, 
                    last_active_time = ?,
                    next_run_time = ? + retry_interval
                WHERE status IN ('timeout', 'failed') 
                AND retry_count < max_retries 
                AND (last_active_time + retry_interval < ?)`,
                now, now, now
            );

            if (retryRs.affected)
                console.log(`${retryRs.affected} tasks set to pending for retry.`);

            // Update tasks with no retries left to 'permanently_failed' or 'paused' based on type
            const permanentlyFailedRs = conn.execute(
                `UPDATE dcron_tasks 
                SET status = CASE type
                    WHEN 'cron' THEN 'paused'
                    ELSE 'permanently_failed'
                END,
                last_active_time = ? 
                WHERE status IN ('running', 'timeout', 'failed') 
                AND retry_count >= max_retries 
                AND (status != 'running' OR (last_active_time IS NOT NULL AND last_active_time + timeout < ?))`,
                now, now
            );

            if (permanentlyFailedRs.affected)
                console.log(`${permanentlyFailedRs.affected} tasks set to permanently failed or paused.`);

            let workflowFailedCount = 0;
            while (true) {
                const workflowFailedRs = conn.execute(
                    `UPDATE dcron_tasks 
                    SET status = CASE type
                        WHEN 'cron' THEN 'paused'
                        ELSE 'permanently_failed'
                    END
                    WHERE status = 'suspended'
                    AND EXISTS (
                        SELECT 1
                        FROM (
                            SELECT id, parent_id, status 
                            FROM dcron_tasks
                            WHERE status = 'permanently_failed'
                            AND parent_id is not null
                            AND parent_id IN (
                                SELECT id FROM dcron_tasks WHERE status = 'suspended'
                            )
                        ) child 
                        WHERE child.parent_id = dcron_tasks.id
                    )`
                );

                workflowFailedCount += workflowFailedRs.affected;
                if (workflowFailedRs.affected == 0)
                    break;
            }

            if (workflowFailedCount)
                console.log(`${workflowFailedCount} workflow tasks set to failed or paused.`);
        }));
    }

    /**
     * Retrieve task by ID with payload parsing
     * @param {string|number} taskId - Task ID to retrieve
     * @returns {object|null} Task object if found, null otherwise
     */
    getTask(taskId) {
        return this.pool(conn => {
            const rs = conn.execute('SELECT * FROM dcron_tasks WHERE id = ?', taskId);
            if (rs.length === 0) return null;
            return this._parseTask(rs[0]);
        });
    }

    /**
     * Get all tasks with specified name
     * @param {string} name - Task name to search for
     * @returns {Array<object>} Array of matching tasks with parsed payloads
     * @throws {Error} If name parameter is missing
     */
    getTasksByName(name) {
        if (!name) {
            throw new Error('Task name is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM dcron_tasks WHERE name = ?', name);
            return tasks.map(task => this._parseTask(task));
        });
    }

    /**
     * Get all tasks with specified status
     * @param {string} status - Status to filter by (must be valid task state)
     * @returns {Array<object>} Array of matching tasks with parsed payloads
     * @throws {Error} If status is invalid or missing
     */
    getTasksByStatus(status) {
        if (!status) {
            throw new Error('Status is required');
        }
        if (!['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'].includes(status)) {
            throw new Error('Invalid status value');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM dcron_tasks WHERE status = ?', status);
            return tasks.map(task => this._parseTask(task));
        });
    }

    /**
     * Close database connection and cleanup resources
     * Should be called when adapter is no longer needed
     */
    close() {
        if (this.pool) {
            this.pool.clear();
            this.pool = null;
        }
    }

    /**
     * Parse task object from database result
     * Handles JSON parsing for payload and result fields
     * @param {object} task - Raw task object from database
     * @returns {object|null} Parsed task object or null if input is invalid
     * @private
     */
    _parseTask(task) {
        if (!task) return null;

        if (task.payload) {
            try {
                task.payload = JSON.parse(task.payload);
            } catch (e) {
                // Keep original string if parsing fails
                task.payload = task.payload;
            }
        }
        if (task.result) {
            try {
                task.result = JSON.parse(task.result);
            } catch (e) {
                // Keep original string if parsing fails
                task.result = task.result;
            }
        }
        return task;
    }

    /**
     * Get all currently running tasks
     * Useful for monitoring active task execution
     * @returns {Array<object>} Array of running tasks with parsed payloads
     */
    getRunningTasks() {
        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM dcron_tasks WHERE status = ?', 'running');
            return tasks.map(task => this._parseTask(task));
        });
    }

    /**
     * Remove all tasks from the database
     * Warning: This operation cannot be undone
     * @returns {number} Number of tasks deleted
     */
    clearTasks() {
        return this.pool(conn => {
            const rs = conn.execute('DELETE FROM dcron_tasks');
            return rs.affected;
        });
    }

    /**
     * Get all child tasks for a given parent task
     * @param {string|number} parentId - ID of the parent task
     * @returns {Array<object>} Array of child tasks with parsed payloads and results
     * @throws {Error} If parentId is invalid or missing
     */
    getChildTasks(parentId) {
        if (!parentId) {
            throw new Error('Parent task ID is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute(`SELECT * FROM dcron_tasks WHERE parent_id = ?`, parentId);
            return tasks.map(task => this._parseTask(task));
        });
    }
}

module.exports = BaseDBAdapter;
