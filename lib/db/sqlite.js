const BaseDBAdapter = require('./base');

class SQLiteAdapter extends BaseDBAdapter {
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
                    created_at INTEGER,
                    result TEXT,
                    error TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_status_priority_next_run_time ON tasks(status, priority, next_run_time);
                CREATE INDEX IF NOT EXISTS idx_name ON tasks(name);
            `);
        });
    }
}

module.exports = SQLiteAdapter;
