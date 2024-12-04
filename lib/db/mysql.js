/**
 * MySQL-specific database adapter implementation
 * Extends BaseDBAdapter with MySQL-specific schema and optimizations
 */

const BaseDBAdapter = require('./base');

/**
 * MySQL adapter for task persistence
 * Uses InnoDB engine and includes indexes for optimal query performance
 */
class MySQLAdapter extends BaseDBAdapter {
    /**
     * Initialize MySQL-specific schema
     * Creates tasks table with appropriate column types and indexes
     */
    setup() {
        this.pool(conn => {
            conn.execute(`
                CREATE TABLE IF NOT EXISTS fib_flow_tasks (
                    -- 基础信息
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type ENUM('async', 'cron') NOT NULL,
                    priority INT DEFAULT 0,
                    payload TEXT,
                    created_at BIGINT,

                    -- 任务状态
                    status ENUM('pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended') NOT NULL,
                    next_run_time BIGINT NOT NULL,
                    last_active_time BIGINT,
                    result TEXT,
                    error TEXT,

                    -- 任务控制
                    timeout INT DEFAULT 60,
                    retry_count INT DEFAULT 0,
                    max_retries INT DEFAULT 3,
                    retry_interval INT DEFAULT 0,
                    cron_expr VARCHAR(100),

                    -- 工作流关系
                    root_id BIGINT,
                    parent_id BIGINT,
                    total_children INT DEFAULT 0,
                    completed_children INT DEFAULT 0,

                    -- 索引和约束
                    KEY idx_fib_flow_tasks_status_priority_next_run_time (status, priority, next_run_time),
                    KEY idx_fib_flow_tasks_name (name),
                    KEY idx_fib_flow_tasks_parent_status (parent_id, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            `);
        });
    }
}

module.exports = MySQLAdapter;
