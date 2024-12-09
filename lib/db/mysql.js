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
     * 
     * Table Structure:
     * - Task identification and basic info:
     *   - id: Unique task identifier
     *   - name: Task type name, used to match with registered handlers
     *   - type: Task type (async: one-time, cron: recurring)
     *   - priority: Task priority for execution ordering (-20 to 20, higher runs first)
     *   - payload: JSON encoded task parameters and data
     *   - created_at: Task creation timestamp (Unix seconds)
     * 
     * - Task execution status and results:
     *   - status: Current task state
     *   - next_run_time: Next scheduled execution time (Unix seconds)
     *   - last_active_time: Last time task reported activity (Unix seconds)
     *   - result: JSON encoded task execution result
     *   - error: Error message if task failed
     * 
     * - Task execution settings:
     *   - timeout: Task execution timeout in seconds
     *   - retry_count: Number of retry attempts made
     *   - max_retries: Maximum number of retry attempts allowed
     *   - retry_interval: Delay between retry attempts in seconds
     *   - cron_expr: Cron expression for recurring tasks
     * 
     * - Workflow relationships:
     *   - root_id: ID of the root task in workflow
     *   - parent_id: ID of the parent task
     *   - total_children: Total number of child tasks
     *   - completed_children: Number of completed child tasks
     * 
     * Indexes:
     * - idx_fib_flow_tasks_status_priority_next_run_time: For task scheduling and claiming
     * - idx_fib_flow_tasks_name: For task type lookups
     * - idx_fib_flow_tasks_parent_status: For workflow management
     */
    setup() {
        this.pool(conn => {
            conn.execute(`
                CREATE TABLE IF NOT EXISTS fib_flow_tasks (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type ENUM('async', 'cron') NOT NULL,
                    priority INT DEFAULT 0,
                    payload TEXT,
                    created_at BIGINT,

                    status ENUM('pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended') NOT NULL,
                    next_run_time BIGINT NOT NULL,
                    last_active_time BIGINT,
                    result TEXT,
                    error TEXT,

                    stage INT DEFAULT 0,
                    timeout INT DEFAULT 60,
                    retry_count INT DEFAULT 0,
                    max_retries INT DEFAULT 3,
                    retry_interval INT DEFAULT 0,
                    cron_expr VARCHAR(100),

                    root_id BIGINT,
                    parent_id BIGINT,
                    total_children INT DEFAULT 0,
                    completed_children INT DEFAULT 0,

                    KEY idx_fib_flow_tasks_status_priority_next_run_time (status, priority, next_run_time),
                    KEY idx_fib_flow_tasks_name (name),
                    KEY idx_fib_flow_tasks_parent_status (parent_id, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            `);
        });
    }
}

module.exports = MySQLAdapter;
