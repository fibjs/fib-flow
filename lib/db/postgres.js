/**
 * PostgreSQL-specific database adapter implementation
 * Extends BaseDBAdapter with PostgreSQL-specific schema and optimizations
 */

const BaseDBAdapter = require('./base');
const createLogger = require('../logger');

// Create logger for PostgreSQL adapter operations
const logger = createLogger('fib-flow');

/**
 * PostgreSQL adapter for task persistence
 * Uses native PostgreSQL features and includes indexes for optimal query performance
 */
class PSQLAdapter extends BaseDBAdapter {
    constructor(config, poolSize = 5) {
        logger.info(`[PSQLAdapter] Initializing with config:`, config);
        super(config, poolSize);
    }

    /**
     * Initialize PostgreSQL-specific schema
     * Creates tasks table with appropriate column types and indexes
     * 
     * Table Structure:
     * - Task identification and basic info:
     *   - id: BIGSERIAL primary key
     *   - name: Task type name, used to match with registered handlers
     *   - type: Task type (async: one-time, cron: recurring)
     *   - priority: Task priority for execution ordering (-20 to 20, higher runs first)
     *   - payload: JSON encoded task parameters and data
     *   - created_at: Task creation timestamp (Unix seconds)
     *   - tag: Task tag for categorization
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
     * - idx_fib_flow_tasks_status_priority_next_run_time: B-tree index for task scheduling
     * - idx_fib_flow_tasks_name: B-tree index for task type lookups
     * - idx_fib_flow_tasks_parent_status: B-tree index for workflow management
     * - idx_fib_flow_tasks_tag_name_status: B-tree index for task statistics
     */
    setup() {
        logger.notice(`[PSQLAdapter] Setting up database schema`);
        try {
            this.pool(conn => {
                logger.notice(`[PSQLAdapter] Creating tasks table if not exists`);
                conn.execute(`
                    CREATE TABLE IF NOT EXISTS fib_flow_tasks (
                        id BIGSERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        type VARCHAR(10) NOT NULL,
                        priority INTEGER DEFAULT 0,
                        payload TEXT,
                        created_at BIGINT,
                        tag VARCHAR(255),

                        status VARCHAR(20) NOT NULL,
                        next_run_time BIGINT NOT NULL,
                        last_active_time BIGINT,
                        result TEXT,
                        error TEXT,

                        stage INTEGER DEFAULT 0,
                        timeout INTEGER DEFAULT 60,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        retry_interval INTEGER DEFAULT 0,
                        cron_expr VARCHAR(100),

                        root_id BIGINT,
                        parent_id BIGINT,
                        total_children INTEGER DEFAULT 0,
                        completed_children INTEGER DEFAULT 0,
                        worker_id VARCHAR(255),
                        start_time BIGINT,
                        context BYTEA
                    );

                    -- Task scheduling index: Optimizes high-frequency task claiming queries
                    -- Uses partial index to improve query performance
                    CREATE INDEX IF NOT EXISTS idx_task_scheduling 
                    ON fib_flow_tasks(status, next_run_time, priority DESC)
                    WHERE status = 'pending';

                    -- Timeout detection index: Only indexes running tasks
                    CREATE INDEX IF NOT EXISTS idx_task_timeout 
                    ON fib_flow_tasks(last_active_time)
                    WHERE status = 'running';

                    -- Workflow index: Supports parent-child relationship queries
                    -- Uses INCLUDE to add commonly accessed fields to reduce table lookups
                    CREATE INDEX IF NOT EXISTS idx_task_workflow 
                    ON fib_flow_tasks(parent_id, status)
                    INCLUDE (completed_children, total_children)
                    WHERE parent_id IS NOT NULL;

                    -- Task statistics index: Optimizes tag-based queries
                    -- Uses B-tree index for exact matches and range scans
                    CREATE INDEX IF NOT EXISTS idx_task_stats 
                    ON fib_flow_tasks(tag, name, status);
                `);
                logger.info(`[PSQLAdapter] Database schema setup completed successfully`);
            });
        } catch (error) {
            logger.error(`[PSQLAdapter] Failed to setup database schema:`, error);
            throw error;
        }
    }

    /**
     * Get the ID of the last inserted row in PostgreSQL
     * Uses PostgreSQL's lastval() function to retrieve the last value from a sequence
     * 
     * @protected
     * @param {Object} conn - Database connection object
     * @param {Object} rs - Result set from the previous insert operation
     * @returns {number} The ID of the last inserted row
     * @throws {Error} If lastval() fails or no sequence has been used in the current session
     */
    _getLastInsertedId(conn, rs) {
        const rs1 = conn.execute('SELECT lastval()');
        return rs1[0].lastval;
    }
}

module.exports = PSQLAdapter;
