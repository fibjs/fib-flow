const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');
const coroutine = require('coroutine');

const { TaskManager } = require('..');
const config = require('./config.js');

function sleep(ms) {
    coroutine.sleep(ms);
}

function waitFor(predicate, timeoutMs = 5000, intervalMs = 20) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (predicate()) {
            return true;
        }
        coroutine.sleep(intervalMs);
    }

    return predicate();
}

function nowSeconds() {
    return Math.floor(Date.now() / 1000);
}

describe('Resilience', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            dbConnection: config.dbConnection,
            poll_interval: 20
        });
        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
        }
    });

    describe('status write resilience', () => {
        it('should survive the row disappearing while the handler runs', () => {
            let release = false;
            taskManager.use('row_vanishes', () => {
                while (!release) {
                    coroutine.sleep(10);
                }
                return 'done';
            });
            taskManager.start();

            const taskId = taskManager.async('row_vanishes', {});
            assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'running'));
            assert.equal(taskManager.getTask(taskId).status, 'running');

            // The row is removed while the handler is still executing.
            assert.equal(taskManager.deleteTasks({ name: 'row_vanishes' }), 1);
            assert.equal(taskManager.getTask(taskId), null);

            // Reaching this point at all proves nothing threw: the fiber must not
            // propagate an uncaught error (that would kill the process).
            release = true;
            sleep(300);

            assert.equal(taskManager.getTask(taskId), null);
            assert.equal(taskManager.state, 'running');
        });

        it('should ignore a stale status write after ownership moved on', () => {
            taskManager.use('stale_owner', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const taskId = taskManager.async('stale_owner', {});

            // Claim it as this worker so the task snapshot carries an owner.
            const claimed = taskManager.db.claimTask(['stale_owner'], 'worker-a');
            assert.equal(claimed.id, taskId);
            assert.equal(claimed.worker_id, 'worker-a');

            // Another worker takes the row over (worker recovery / supersede).
            taskManager.db.pool(conn => conn.execute(
                "UPDATE fib_flow_tasks SET worker_id = 'other-worker' WHERE id = ?",
                taskId
            ));

            const applied = taskManager._updateTaskStatusIfOwned(claimed, 'completed', { result: 'x' });

            assert.equal(applied, false);
            assert.equal(taskManager.getTask(taskId).status, 'running');
            assert.equal(taskManager.getTask(taskId).worker_id, 'other-worker');
        });

        it('should not throw when the database is already closed', () => {
            taskManager.use('closed_db', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const taskId = taskManager.async('closed_db', {});
            const task = taskManager.getTask(taskId);

            taskManager.db.close();
            taskManager.db = null;

            assert.doesNotThrow(() => {
                taskManager._updateTaskStatusIfOwned(task, 'completed', { result: 'x' });
            });
        });
    });

    describe('retention safety', () => {
        it('should keep rows the runtime still needs even with an expired row window', () => {
            taskManager.use('retention_keep', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const statuses = ['pending', 'running', 'suspended', 'failed', 'timeout'];
            const ids = statuses.map(status => {
                const taskId = taskManager.async('retention_keep', {}, { retention_expire_time: 60 });
                taskManager.db.pool(conn => conn.execute(
                    'UPDATE fib_flow_tasks SET status = ?, last_active_time = ? WHERE id = ?',
                    status,
                    nowSeconds() - 30 * 86400,
                    taskId
                ));
                return taskId;
            });

            const cleanup = taskManager.runRetention({ expire_time: 86400 });

            assert.equal(cleanup.tasks_deleted_by_row_policy, 0);
            ids.forEach((taskId, index) => {
                assert.ok(taskManager.getTask(taskId), `${statuses[index]} must survive retention`);
            });
        });

        it('should delete an expired row window for a terminal status', () => {
            taskManager.use('retention_drop', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const taskId = taskManager.async('retention_drop', {}, { retention_expire_time: 60 });
            taskManager.db.pool(conn => conn.execute(
                "UPDATE fib_flow_tasks SET status = 'completed', last_active_time = ? WHERE id = ?",
                nowSeconds() - 3600,
                taskId
            ));

            const cleanup = taskManager.runRetention({ expire_time: 86400 });

            assert.equal(cleanup.tasks_deleted_by_row_policy, 1);
            assert.equal(taskManager.getTask(taskId), null);
        });

        it('should not throw when the global window is disabled', () => {
            const manager = new TaskManager({
                dbConnection: config.dbConnection,
                poll_interval: 20,
                retention: { expire_time: null }
            });
            manager.db.setup();

            try {
                // Row-level only configuration: the sweep still has to work.
                assert.doesNotThrow(() => manager.runRetention());
                const result = manager.runRetention({ expire_time: null });
                assert.equal(result.tasks_deleted, 0);
            } finally {
                manager.stop();
            }
        });

        it('should arm automatic sweeps when only row-level windows are enabled', () => {
            const manager = new TaskManager({
                dbConnection: config.dbConnection,
                poll_interval: 20,
                task_heartbeat_interval: 50,
                retention: { expire_time: null }
            });
            manager.db.setup();
            manager.db.clearTasks();
            manager.use('row_window', () => 'ok');
            manager.start();
            manager.pause();

            try {
                const taskId = manager.async('row_window', {}, { retention_expire_time: 60 });
                manager.db.pool(conn => conn.execute(
                    "UPDATE fib_flow_tasks SET status = 'completed', last_active_time = ? WHERE id = ?",
                    nowSeconds() - 3600,
                    taskId
                ));

                const firstTick = manager._consumeAutoRetentionPolicy(Date.now());
                assert.equal(firstTick, null, 'first tick only arms the timer');
                assert.notEqual(manager.nextRetentionSweepAt, null);

                const secondTick = manager._consumeAutoRetentionPolicy(Date.now() + 6 * 60 * 1000);
                assert.ok(secondTick, 'row-level-only configuration must produce a sweep policy');

                const cleanup = manager.db.cleanupExpiredTasks(secondTick);
                assert.equal(cleanup.tasks_deleted_by_row_policy, 1);
                assert.equal(manager.getTask(taskId), null);
            } finally {
                manager.db.clearTasks();
                manager.stop();
            }
        });

        it('should leave no orphaned audit rows behind a retention sweep', () => {
            taskManager.use('retention_residue', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const taskId = taskManager.async('retention_residue', {});
            taskManager.db.pool(conn => conn.execute(
                "UPDATE fib_flow_tasks SET status = 'completed', last_active_time = ? WHERE id = ?",
                nowSeconds() - 7200,
                taskId
            ));

            const cleanup = taskManager.runRetention({ expire_time: 3600 });
            assert.equal(cleanup.tasks_deleted, 1);

            const residue = taskManager.db.pool(conn => conn.execute(
                'SELECT COUNT(*) AS total FROM fib_flow_task_events WHERE task_id = ?',
                taskId
            ));
            assert.equal(Number(residue[0].total), 0);
        });
    });

    describe('child retention inheritance', () => {
        it('should inherit the parent retention window', () => {
            taskManager.use('inherit_parent', (task, next) => {
                if (task.stage === 0) {
                    return next([{ name: 'inherit_child' }]);
                }
                return 'ok';
            });
            taskManager.use('inherit_child', () => 'ok');
            taskManager.start();

            const parentId = taskManager.async('inherit_parent', {}, { retention_expire_time: 7 * 86400 });
            assert.ok(waitFor(() => taskManager.getChildTasks(parentId).length === 1));

            const parent = taskManager.getTask(parentId);
            const child = taskManager.getChildTasks(parentId)[0];

            assert.equal(parent.retention_expire_time, 7 * 86400);
            assert.equal(child.retention_expire_time, 7 * 86400);
        });

        it('should let a child override the inherited window', () => {
            taskManager.use('override_parent', (task, next) => {
                if (task.stage === 0) {
                    return next([{ name: 'override_child', retention_expire_time: 3600 }]);
                }
                return 'ok';
            });
            taskManager.use('override_child', () => 'ok');
            taskManager.start();

            const parentId = taskManager.async('override_parent', {}, { retention_expire_time: 7 * 86400 });
            assert.ok(waitFor(() => taskManager.getChildTasks(parentId).length === 1));

            const child = taskManager.getChildTasks(parentId)[0];
            assert.equal(child.retention_expire_time, 3600);
        });
    });
});
