/**
 * SQLite-specific database adapter implementation
 * Extends BaseDBAdapter with SQLite-specific schema and constraints
 */

const BaseDBAdapter = require('./base');

/**
 * SQLite adapter for task persistence
 * Uses CHECK constraints and separate index creation for data integrity
 */
class SQLiteAdapter extends BaseDBAdapter {
    /**
     * Initialize SQLite-specific schema
     * Creates tasks table with appropriate column constraints and indexes
     * Uses INTEGER PRIMARY KEY for auto-incrementing IDs
     */
    setup() {
        this.pool(conn => {
            conn.execute(`
                CREATE TABLE IF NOT EXISTS fib_flow_tasks (
                    -- 基础信息
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    type TEXT CHECK(type IN ('async', 'cron')) NOT NULL,
                    priority INTEGER DEFAULT 0,
                    payload TEXT,
                    created_at INTEGER,

                    -- 任务状态
                    status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended')) NOT NULL,
                    next_run_time INTEGER NOT NULL,
                    last_active_time INTEGER,
                    result TEXT,
                    error TEXT,

                    -- 任务控制
                    timeout INTEGER DEFAULT 60,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    retry_interval INTEGER DEFAULT 0,
                    cron_expr TEXT,

                    -- 工作流关系
                    root_id INTEGER,
                    parent_id INTEGER,
                    total_children INTEGER DEFAULT 0,
                    completed_children INTEGER DEFAULT 0
                );

                -- 任务调度索引
                CREATE INDEX IF NOT EXISTS idx_fib_flow_tasks_status_priority_next_run_time ON fib_flow_tasks(status, priority, next_run_time);
                CREATE INDEX IF NOT EXISTS idx_fib_flow_tasks_name ON fib_flow_tasks(name);
                CREATE INDEX IF NOT EXISTS idx_fib_flow_tasks_parent_status ON fib_flow_tasks(parent_id, status);
            `);
        });
    }
}

module.exports = SQLiteAdapter;
