/**
 * SQLite-specific database adapter implementation
 * Extends BaseDBAdapter with SQLite-specific schema and constraints
 */

const BaseDBAdapter = require('./base');
const createLogger = require('../logger');

// Create logger for SQLite adapter operations
const logger = createLogger('fib-flow');

/**
 * SQLite adapter for task persistence
 * Uses CHECK constraints and separate index creation for data integrity
 */
class SQLiteAdapter extends BaseDBAdapter {
    constructor(config, poolSize = 5) {
        super(config, poolSize);
        // Override lock clause since SQLite doesn't support FOR UPDATE SKIP LOCKED
        this.lockClause = '';
    }

    /**
     * Initialize SQLite-specific schema
     * Creates tasks table with appropriate column constraints and indexes
     * 
     * Table Structure:
     * - Task identification and basic info:
     *   - id: Unique task identifier
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
     *   - start_time: Start time of task execution (Unix seconds)
     * 
     * Indexes:
     * - idx_fib_flow_tasks_status_priority_next_run_time: For task scheduling and claiming
     * - idx_fib_flow_tasks_name: For task type lookups
     * - idx_fib_flow_tasks_parent_status: For workflow management
     * - idx_fib_flow_tasks_tag_name_status: For task statistics queries by tag
     */
    setup() {
        logger.info(`[SQLiteAdapter] Setting up database schema`);
        this.pool(conn => {
            logger.info(`[SQLiteAdapter] Creating tasks table if not exists`);
            conn.execute(`
                    CREATE TABLE IF NOT EXISTS fib_flow_tasks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        type TEXT CHECK(type IN ('async', 'cron')) NOT NULL,
                        priority INTEGER DEFAULT 0,
                        payload TEXT,
                        created_at INTEGER,
                        tag TEXT,

                        status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended')) NOT NULL DEFAULT 'pending',
                        next_run_time INTEGER NOT NULL,
                        last_active_time INTEGER,
                        result TEXT,
                        error TEXT,

                        stage INTEGER DEFAULT 0,
                        timeout INTEGER DEFAULT 60,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        retry_interval INTEGER DEFAULT 0,
                        cron_expr TEXT,

                        root_id INTEGER,
                        parent_id INTEGER,
                        total_children INTEGER DEFAULT 0,
                        completed_children INTEGER DEFAULT 0,
                        worker_id TEXT,
                        start_time INTEGER,
                        context BLOB
                    );

                    -- Task scheduling index: Supports high-frequency task claiming queries
                    CREATE INDEX IF NOT EXISTS idx_task_scheduling 
                    ON fib_flow_tasks(status, next_run_time, priority);

                    -- Task timeout detection index: Supports monitoring active tasks
                    CREATE INDEX IF NOT EXISTS idx_task_timeout 
                    ON fib_flow_tasks(status, last_active_time) 
                    WHERE status = 'running';

                    -- Workflow index: Supports parent-child task relationships and completion tracking
                    CREATE INDEX IF NOT EXISTS idx_task_workflow 
                    ON fib_flow_tasks(parent_id, status, completed_children);

                    -- Task statistics index: Supports efficient statistics queries by tag
                    CREATE INDEX IF NOT EXISTS idx_task_stats
                    ON fib_flow_tasks(tag, name, status);
                `);
            logger.info(`[SQLiteAdapter] Database schema setup completed successfully`);
        });
    }
}

module.exports = SQLiteAdapter;
