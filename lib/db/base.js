/**
 * Provides a unified interface for task persistence across different database systems.
 * Abstracts database-specific complexities to ensure consistent task management.
 */

const db = require('db');
const Pool = require('fib-pool');
const coroutine = require('coroutine');

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
        /**
         * Normalizes different connection types to a consistent pool-like interface.
         * Ensures uniform connection management regardless of underlying database.
         * 
         * @param {*} conn - Raw database connection
         * @returns {function} Standardized connection pool wrapper
         */
        function wrap_conn(conn) {
            // Thread-safe connection wrapper using coroutine lock
            const locker = new coroutine.Lock();

            // Provides a consistent execution and cleanup mechanism
            const pool = function (callback) {
                let result;

                try {
                    // Ensure thread-safe execution of database operations
                    locker.acquire();
                    result = callback(conn);
                } finally {
                    // Always release the lock, even if an error occurs
                    locker.release();
                }

                return result;
            };

            // Ensures thread-safe connection closure
            pool.clear = function () {
                try {
                    // Prevent concurrent connection closure
                    locker.acquire();
                    conn.close();
                } finally {
                    locker.release();
                }
            }.bind(this);

            return pool;
        }

        // Prioritizes connection initialization based on connection type
        // Prevents runtime errors and supports diverse database configurations
        if (conn == "sqlite::memory:")
            this.pool = wrap_conn(db.open(conn));
        
        else if (typeof conn === 'function')
            this.pool = conn;
        
        else if (typeof conn === 'string')
            this.pool = Pool({
                create: () => {
                    return this.createConnection(conn);
                },
                destroy: conn => this.destroyConnection(conn),
                timeout: 30000,
                retry: 1,
                maxsize: poolSize
            });
        
        else if (typeof conn === 'object')
            this.pool = wrap_conn(conn);

        else
            throw new Error('Invalid connection type: ' + conn);
    }

    /**
     * Prepares database schema for task tracking.
     * Enforces implementation requirement for specific database adapters.
     * @throws {Error} If not overridden by subclass
     */
    setup() {
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
        conn.close();
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
        const isArray = Array.isArray(tasks);
        const taskArray = isArray ? tasks : [tasks];

        const taskIds = [];
        this.pool(conn => conn.trans(() => {
            const now = Math.floor(Date.now() / 1000);

            // Updates parent task state to reflect child task creation
            if (options.parent_id) {
                const rs = conn.execute(
                    `UPDATE fib_flow_tasks 
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
                if (!['async', 'cron'].includes(task.type)) {
                    throw new Error('Task type must be either "async" or "cron"');
                }

                const rs = conn.execute(
                    `INSERT INTO fib_flow_tasks (
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
     * Implements a robust task claiming mechanism with concurrency control.
     * Prevents race conditions and ensures fair task distribution among workers.
     * 
     * @param {Array<string>} taskNames - Eligible task names for execution
     * @returns {object|null} Next available task or null if no tasks are ready
     * @throws {Error} If task name selection is invalid
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
                        `SELECT * FROM fib_flow_tasks 
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
                            `UPDATE fib_flow_tasks 
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
                    UPDATE fib_flow_tasks 
                    SET ${updates.join(', ')}
                    WHERE id = ? AND status IN ?
                `, ...params);

                if (rs.affected === 0) {
                    throw new Error(`Failed to update task ${taskId}`);
                }

                if (extra.parent_id) {
                    const rs = conn.execute(`
                        UPDATE fib_flow_tasks
                        SET completed_children = completed_children + 1
                        WHERE id = ?;
                        UPDATE fib_flow_tasks
                        SET status = 'pending'
                        WHERE id = ? AND status = 'suspended' AND completed_children = total_children
                    `, extra.parent_id, extra.parent_id);
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
     * @returns {object} Count of tasks in each state transition
     */
    handleTimeoutTasks() {
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => conn.trans(() => {
            // Set remaining timed-out tasks to 'timeout'
            const timeoutRs = conn.execute(
                `UPDATE fib_flow_tasks 
                SET status = 'timeout', last_active_time = ? 
                WHERE status = 'running' 
                AND (last_active_time IS NOT NULL AND last_active_time + timeout < ?)`,
                now, now
            );

            if (timeoutRs.affected)
                console.log(`${timeoutRs.affected} tasks set to timeout.`);

            // Update tasks to 'pending' if retry interval has passed
            const retryRs = conn.execute(
                `UPDATE fib_flow_tasks 
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
                `UPDATE fib_flow_tasks 
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
                    `UPDATE fib_flow_tasks 
                    SET status = CASE type
                        WHEN 'cron' THEN 'paused'
                        ELSE 'permanently_failed'
                    END
                    WHERE status = 'suspended'
                    AND EXISTS (
                        SELECT 1
                        FROM (
                            SELECT id, parent_id, status 
                            FROM fib_flow_tasks
                            WHERE status = 'permanently_failed'
                            AND parent_id is not null
                            AND parent_id IN (
                                SELECT id FROM fib_flow_tasks WHERE status = 'suspended'
                            )
                        ) child 
                        WHERE child.parent_id = fib_flow_tasks.id
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
            const rs = conn.execute('SELECT * FROM fib_flow_tasks WHERE id = ?', taskId);
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
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE name = ?', name);
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
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE status = ?', status);
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
     * Parses complex JSON fields to ensure data integrity and type safety.
     * Gracefully handles parsing errors to prevent task processing interruption.
     * 
     * @param {object} task - Unparsed task object from database
     * @returns {object|null} Processed task with safely parsed JSON fields
     * @private
     */
    _parseTask(task) {
        if (!task) return null;

        if (task.payload) {
            try {
                task.payload = JSON.parse(task.payload);
            } catch (e) {
                // Preserves original data if JSON parsing fails
                task.payload = task.payload;
            }
        }
        if (task.result) {
            try {
                task.result = JSON.parse(task.result);
            } catch (e) {
                // Preserves original data if JSON parsing fails
                task.result = task.result;
            }
        }
        return task;
    }

    /**
     * Provides real-time visibility into active task processing.
     * Enables monitoring and potential intervention for long-running tasks.
     * 
     * @returns {Array<object>} Currently executing tasks with parsed metadata
     */
    getRunningTasks() {
        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM fib_flow_tasks WHERE status = ?', 'running');
            return tasks.map(task => this._parseTask(task));
        });
    }

    /**
     * Allows complete task database reset for testing or maintenance.
     * Provides a controlled mechanism to clear all task records.
     * 
     * @returns {number} Count of tasks permanently removed
     */
    clearTasks() {
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
        if (!parentId) {
            throw new Error('Parent task ID is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute(`SELECT * FROM fib_flow_tasks WHERE parent_id = ?`, parentId);
            return tasks.map(task => this._parseTask(task));
        });
    }
}

module.exports = BaseDBAdapter;
