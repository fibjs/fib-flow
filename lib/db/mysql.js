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
                CREATE TABLE IF NOT EXISTS dcron_tasks (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type ENUM('async', 'cron') NOT NULL,
                    status ENUM('pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused') NOT NULL,
                    priority INT DEFAULT 0,
                    payload TEXT,
                    cron_expr VARCHAR(100),
                    next_run_time BIGINT NOT NULL,
                    last_active_time BIGINT,
                    timeout INT DEFAULT 60,
                    retry_count INT DEFAULT 0,
                    max_retries INT DEFAULT 3,
                    retry_interval INT DEFAULT 0,
                    created_at BIGINT,
                    result TEXT,
                    error TEXT,
                    KEY dcron_idx_status_priority_next_run_time (status, priority, next_run_time),
                    KEY dcron_idx_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            `);
        });
    }
}

module.exports = MySQLAdapter;
