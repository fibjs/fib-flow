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
                CREATE TABLE IF NOT EXISTS dcron_tasks (
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
                    created_at INTEGER,
                    result TEXT,
                    error TEXT
                );

                CREATE INDEX IF NOT EXISTS dcron_idx_status_priority_next_run_time ON dcron_tasks(status, priority, next_run_time);
                CREATE INDEX IF NOT EXISTS dcron_idx_name ON dcron_tasks(name);
            `);
        });
    }
}

module.exports = SQLiteAdapter;
