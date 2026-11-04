const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');

const { TaskManager } = require('..');
const config = require('./config.js');

describe('Schema contract', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            dbConnection: config.dbConnection,
            poll_interval: 100
        });
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
        }
    });

    it('should record schema_version when creating a fresh database', () => {
        taskManager.db.setup();

        const rows = taskManager.db.pool(conn => conn.execute(
            "SELECT meta_value FROM fib_flow_meta WHERE meta_key = 'schema_version'"
        ));

        assert.equal(rows.length, 1);
        assert.equal(String(rows[0].meta_value), '2');
    });

    it('should be idempotent across repeated setup() calls', () => {
        taskManager.db.setup();
        taskManager.db.setup();

        const rows = taskManager.db.pool(conn => conn.execute(
            "SELECT meta_value FROM fib_flow_meta WHERE meta_key = 'schema_version'"
        ));
        assert.equal(rows.length, 1);
    });

    it('should expose the scheduling columns on the task table', () => {
        taskManager.db.setup();
        taskManager.use('schema_probe', () => 'ok');
        taskManager.start();

        const taskId = taskManager.cron('schema_probe', '0 9 * * *', {}, { cronKey: 'probe' });
        const task = taskManager.getTask(taskId);

        assert.equal(task.cron_key, 'probe');
        assert.equal(task.cron_tz, null);
        assert.equal(task.cron_catch_up, 'once');
        assert.equal(task.retention_expire_time, null);
    });

    it('should reject an outdated schema version', () => {
        taskManager.db.setup();
        taskManager.db.pool(conn => conn.execute(
            "UPDATE fib_flow_meta SET meta_value = '1' WHERE meta_key = 'schema_version'"
        ));

        assert.throws(() => {
            taskManager.db.setup();
        }, /is not supported; expected v2/);

        // Restore so the shared database used by other suites stays usable.
        taskManager.db.pool(conn => conn.execute(
            "UPDATE fib_flow_meta SET meta_value = '2' WHERE meta_key = 'schema_version'"
        ));
    });

    it('should expose the cron identity unique index', () => {
        taskManager.db.setup();
        taskManager.use('schema_unique', () => 'ok');
        taskManager.start();

        taskManager.cron('schema_unique', '0 9 * * *', {}, { cronKey: 'one' });

        // The identity is enforced by the database: a second insert with the same
        // (name, cron_key) must fail rather than silently create a duplicate.
        // Driver error text is engine-specific (PostgreSQL reports an opaque
        // "[20024] Exception occurred."), so the invariant is asserted through
        // the row count instead of the message.
        let threw = false;
        try {
            taskManager.db.insertTask({
                name: 'schema_unique',
                type: 'cron',
                cron_expr: '0 10 * * *',
                cron_key: 'one'
            });
        } catch (_error) {
            threw = true;
        }

        assert.equal(threw, true, 'duplicate (name, cron_key) must be rejected');
        assert.equal(taskManager.getTasks({ type: 'cron', name: 'schema_unique' }).length, 1);
    });

    it('should converge on one row when the identity already exists', () => {
        taskManager.db.setup();
        taskManager.use('schema_converge', () => 'ok');
        taskManager.start();

        const first = taskManager.cron('schema_converge', '0 9 * * *', {}, { cronKey: 'k' });
        const second = taskManager.cron('schema_converge', '0 11 * * *', {}, { cronKey: 'k' });

        assert.equal(first, second);
        assert.equal(taskManager.getTasks({ type: 'cron', name: 'schema_converge' }).length, 1);
    });
});
