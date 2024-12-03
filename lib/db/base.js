const db = require('db');
const Pool = require('fib-pool');

class BaseDBAdapter {
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

    createConnection(connStr) {
        return db.open(connStr);
    }

    destroyConnection(conn) {
        conn.close();
    }

    setup() {
        this.pool(conn => {
            conn.execute(`
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    type TEXT CHECK(type IN ('async', 'cron')) NOT NULL,
                    status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused')) NOT NULL,
                    priority INTEGER DEFAULT 0,
                    payload TEXT,
                    cron_expr TEXT,
                    next_run_time INTEGER NOT NULL,
                    last_active_time INTEGER,
                    timeout INTEGER DEFAULT 60,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    retry_interval INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    result TEXT,
                    error TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_status_priority_next_run_time ON tasks(status, priority, next_run_time);
                CREATE INDEX IF NOT EXISTS idx_name ON tasks(name);
            `);
        });
    }

    close() {
        if (this.pool) {
            this.pool.clear();
            this.pool = null;
        }
    }

    insertTask(task) {
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

        return this.pool(conn => {
            try {
                const now = Math.floor(Date.now() / 1000);
                const rs = conn.execute(
                    `INSERT INTO tasks (
                        name, type, status, priority, payload, cron_expr, max_retries, 
                        retry_interval, next_run_time, timeout, created_at
                    ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)`,
                    task.name,
                    task.type,
                    task.priority || 0,
                    task.payload ? JSON.stringify(task.payload) : null,
                    task.cron_expr || null,
                    task.max_retries !== undefined ? task.max_retries : 3,
                    task.retry_interval || 0,
                    task.next_run_time || now,
                    task.timeout || 60,
                    now
                );
                return rs.insertId;
            } catch (error) {
                throw new Error(`Failed to insert task: ${error.message}`);
            }
        });
    }

    claimTask(taskNames) {
        if (!Array.isArray(taskNames) || taskNames.length === 0) {
            throw new Error('Task names array is required');
        }

        let task = null;
        const now = Math.floor(Date.now() / 1000);

        try {
            this.pool(conn => {
                let updated = false;
                while (!updated) {
                    // 查找可执行的任务
                    const rs = conn.execute(
                        `SELECT id, name, type, payload, cron_expr, timeout, priority, max_retries, retry_count, next_run_time
                        FROM tasks 
                        WHERE status = 'pending' 
                        AND name IN ?
                        AND next_run_time <= ?
                        ORDER BY priority DESC, next_run_time ASC
                        LIMIT 1`,
                        taskNames,
                        now
                    );

                    if (rs.length > 0) {
                        task = rs[0];
                        // 更新任务状态
                        const updateResult = conn.execute(
                            `UPDATE tasks 
                            SET status = 'running',
                                last_active_time = ?
                            WHERE id = ? AND status = 'pending'`,
                            now,
                            task.id
                        );

                        updated = updateResult.affected > 0;
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

    updateTaskActiveTime(taskId) {
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => {
            return conn.execute(
                'UPDATE tasks SET last_active_time = ? WHERE id = ?',
                now, taskId
            );
        });
    }

    updateTaskStatus(taskId, status, options = {}) {
        const allowedPreviousStatuses = {
            'running': ['pending'],
            'completed': ['running'],
            'failed': ['running'],
            'timeout': ['running'],
            'pending': ['running', 'failed', 'timeout', 'paused'],
            'permanently_failed': ['failed', 'timeout'],
            'paused': ['running', 'failed', 'timeout']
        };

        if (!allowedPreviousStatuses[status])
            throw new Error('Invalid status value');

        return this.pool(conn => {
            // 构建动态 SQL 更新语句
            let updates = ['status = ?'];
            let params = [status];

            if ('result' in options) {
                updates.push('result = ?');
                params.push(options.result ? JSON.stringify(options.result) : null);
            }

            if ('error' in options) {
                updates.push('error = ?');
                params.push(options.error);
            }

            if ('next_run_time' in options) {
                updates.push('next_run_time = ?');
                params.push(options.next_run_time);
            }

            if ('retry_count' in options) {
                updates.push('retry_count = ?');
                params.push(options.retry_count);
            }

            // 添加 WHERE 条件的参数
            params.push(taskId, allowedPreviousStatuses[status]);

            const rs = conn.execute(`
                UPDATE tasks 
                SET ${updates.join(', ')}
                WHERE id = ? AND status IN ?
            `, ...params);

            if (rs.affected === 0) {
                throw new Error(`Failed to update task ${taskId}`);
            }

            return rs.affected;
        });
    }

    _parseTask(task) {
        if (!task) return null;

        if (task.payload) {
            try {
                task.payload = JSON.parse(task.payload);
            } catch (e) {
                // 解析失败时保持原始字符串
                task.payload = task.payload;
            }
        }
        if (task.result) {
            try {
                task.result = JSON.parse(task.result);
            } catch (e) {
                // 解析失败时保持原始字符串
                task.result = task.result;
            }
        }
        return task;
    }

    getRunningTasks() {
        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM tasks WHERE status = ?', 'running');
            return tasks.map(task => this._parseTask(task));
        });
    }

    getTask(taskId) {
        return this.pool(conn => {
            const rs = conn.execute('SELECT * FROM tasks WHERE id = ?', taskId);
            if (rs.length === 0) return null;
            return this._parseTask(rs[0]);
        });
    }

    getTasksByName(name) {
        if (!name) {
            throw new Error('Task name is required');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM tasks WHERE name = ?', name);
            return tasks.map(task => this._parseTask(task));
        });
    }

    getTasksByStatus(status) {
        if (!status) {
            throw new Error('Status is required');
        }
        if (!['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused'].includes(status)) {
            throw new Error('Invalid status value');
        }

        return this.pool(conn => {
            const tasks = conn.execute('SELECT * FROM tasks WHERE status = ?', status);
            return tasks.map(task => this._parseTask(task));
        });
    }

    handleTimeoutTasks() {
        const now = Math.floor(Date.now() / 1000);
        return this.pool(conn => {
            // Update tasks with no retries left to 'permanently_failed' or 'paused' based on type
            const permanentlyFailedRs = conn.execute(
                `UPDATE tasks 
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

            // Then, set remaining timed-out tasks to 'timeout'
            const timeoutRs = conn.execute(
                `UPDATE tasks 
                SET status = 'timeout', last_active_time = ? 
                WHERE status = 'running' 
                AND (last_active_time IS NOT NULL AND last_active_time + timeout < ?)`,
                now, now
            );

            if (timeoutRs.affected)
                console.log(`${timeoutRs.affected} tasks set to timeout.`);

            // Then, update tasks to 'pending' if retry interval has passed
            const retryRs = conn.execute(
                `UPDATE tasks 
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

            return { permanentlyFailedChanges: permanentlyFailedRs.affected, timeoutChanges: timeoutRs.affected, retryChanges: retryRs.affected };
        });
    }

    clearTasks() {
        return this.pool(conn => {
            const rs = conn.execute('DELETE FROM tasks');
            return rs.affected;
        });
    }
}

module.exports = BaseDBAdapter;
