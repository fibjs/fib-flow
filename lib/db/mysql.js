/**
 * MySQL-specific database adapter implementation
 * Extends BaseDBAdapter with MySQL-specific schema and optimizations
 */

const BaseDBAdapter = require('./base');
const createLogger = require('../logger');

// Create logger for MySQL adapter operations
const logger = createLogger('fib-flow');
/**
 * MySQL adapter for task persistence
 * Uses InnoDB engine and includes indexes for optimal query performance
 */
class MySQLAdapter extends BaseDBAdapter {
    constructor(config, poolSize = 5) {
        logger.info(`[MySQLAdapter] Initializing with config:`, config);
        super(config, poolSize);
    }

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
     * - Worker information:
     *   - worker_id: ID of the worker that executed the task
     *   - start_time: Timestamp when task started execution (Unix seconds)
     * 
     * Indexes:
     * - idx_fib_flow_tasks_status_priority_next_run_time: For task scheduling and claiming
     * - idx_fib_flow_tasks_name: For task type lookups
     * - idx_fib_flow_tasks_parent_status: For workflow management
     */
    setup() {
        logger.notice(`[MySQLAdapter] Setting up database schema`);
        try {
            this.pool(conn => {
                logger.notice(`[MySQLAdapter] Creating tasks table if not exists`);
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
                        worker_id VARCHAR(255),
                        start_time BIGINT,

                        -- Task scheduling index: Optimizes task claiming and status filtering (high frequency)
                        INDEX idx_task_scheduling (status, next_run_time, priority),
                        
                        -- Timeout detection index: Optimizes monitoring of running tasks (medium frequency)
                        INDEX idx_task_timeout (status, last_active_time),
                        
                        -- Workflow index: Optimizes parent-child relationship queries (low frequency)
                        INDEX idx_task_workflow (parent_id, status, completed_children),
                        
                        -- Foreign key constraints: Ensures workflow integrity
                        FOREIGN KEY (parent_id) REFERENCES fib_flow_tasks(id) ON DELETE CASCADE,
                        FOREIGN KEY (root_id) REFERENCES fib_flow_tasks(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                `);
                logger.notice(`[MySQLAdapter] Database schema setup completed successfully`);
            });
        } catch (error) {
            logger.error(`[MySQLAdapter] Failed to setup database schema:`, error);
            throw error;
        }
    }
}

module.exports = MySQLAdapter;
