const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');
const coroutine = require('coroutine');
const fs = require('fs');
const path = require('path');

const { createAdapter, TaskManager } = require('..');
const config = require('./config.js');

function createSqliteFileConnection(name) {
    const dbPath = path.join(__dirname, `${name}.sqlite`);
    if (fs.exists(dbPath)) {
        fs.unlink(dbPath);
    }

    return {
        dbPath,
        connection: `sqlite:${dbPath}`
    };
}

function cleanupSqliteFile(dbPath) {
    if (fs.exists(dbPath)) {
        fs.unlink(dbPath);
    }
}

function createTestDatabaseTarget(name) {
    if (typeof config.dbConnection === 'string' && config.dbConnection.startsWith('sqlite')) {
        const sqliteFile = createSqliteFileConnection(name);
        return {
            connection: sqliteFile.connection,
            cleanup: () => cleanupSqliteFile(sqliteFile.dbPath)
        };
    }

    return {
        connection: config.dbConnection,
        cleanup: () => {}
    };
}

function clearWorkers(adapter) {
    adapter.pool(conn => {
        conn.execute('DELETE FROM fib_flow_workers');
    });
}

function waitFor(predicate, timeoutMs = 3000, intervalMs = 20) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (predicate()) {
            return true;
        }
        coroutine.sleep(intervalMs);
    }

    return predicate();
}

describe('Worker Recovery', () => {
    describe('Adapter worker registry and fencing', () => {
        let testDb;
        let adapter;

        beforeEach(() => {
            testDb = createTestDatabaseTarget('worker-recovery-db');
            adapter = createAdapter(testDb.connection);
            adapter.setup();
            adapter.clearTasks();
            clearWorkers(adapter);
        });

        afterEach(() => {
            if (adapter) {
                adapter.close();
            }
            testDb.cleanup();
        });

        it('should register workers, supersede older workers, and mark expired workers dead', () => {
            const oldWorker = adapter.registerWorker({
                worker_id: 'worker-old',
                pod_id: 'pod-a',
                now: 100,
                ttl: 5,
                meta: { pid: 1 }
            });
            const newWorker = adapter.registerWorker({
                worker_id: 'worker-new',
                pod_id: 'pod-a',
                now: 101,
                ttl: 5,
                meta: { pid: 2 }
            });

            assert.equal(oldWorker.worker_id, 'worker-old');
            assert.equal(newWorker.worker_id, 'worker-new');

            const superseded = adapter.supersedeOlderWorkers('pod-a', 'worker-new', 101);
            assert.deepEqual(superseded, ['worker-old']);
            assert.equal(adapter.getWorker('worker-old').status, 'superseded');
            assert.equal(adapter.getWorker('worker-new').status, 'active');

            adapter.registerWorker({
                worker_id: 'worker-expired',
                pod_id: 'pod-b',
                now: 90,
                ttl: 5,
                meta: { pid: 3 }
            });
            const expired = adapter.reapExpiredWorkers(96);
            assert.deepEqual(expired, ['worker-expired']);
            assert.equal(adapter.getWorker('worker-expired').status, 'dead');
        });

        it('should recover running tasks for dead workers and record interrupted attempts', () => {
            adapter.registerWorker({
                worker_id: 'worker-dead',
                pod_id: 'pod-a',
                now: 100,
                ttl: 5,
                meta: { pid: 1 }
            });

            const taskId = adapter.insertTask({
                name: 'recover_me',
                type: 'async'
            });

            adapter.claimTask(['recover_me'], 'worker-dead');
            adapter.markWorkerDead('worker-dead', 110);

            const recoveredCount = adapter.recoverTasksForWorkers(['worker-dead'], {
                worker_id: 'worker-rescuer',
                pod_id: 'pod-b',
                now: 111
            });

            assert.equal(recoveredCount, 1);

            const task = adapter.getTask(taskId);
            assert.equal(task.status, 'pending');
            assert.equal(task.worker_id, null);
            assert.equal(task.start_time, null);
            assert.equal(task.next_run_time, 111);

            const attempts = adapter.getTaskAttempts(taskId);
            assert.equal(attempts.length, 1);
            assert.equal(attempts[0].outcome, 'interrupted');
            assert.equal(attempts[0].ended_at, 111);

            const recoveryEvents = adapter.getTaskEvents(taskId, { event_type: 'task_recovered' });
            assert.equal(recoveryEvents.length, 1);
            assert.equal(recoveryEvents[0].worker_id, 'worker-rescuer');
            assert.equal(recoveryEvents[0].metadata.recovered_from_worker_id, 'worker-dead');
            assert.equal(recoveryEvents[0].metadata.recovered_by_pod_id, 'pod-b');
        });

        it('should fence running task updates when worker ownership changes', () => {
            const taskId = adapter.insertTask({
                name: 'owned_task',
                type: 'async'
            });

            adapter.claimTask(['owned_task'], 'worker-1');

            assert.throws(() => {
                adapter.updateTaskStatus(taskId, 'completed', { worker_id: 'worker-2' });
            }, /no longer owned/);

            assert.throws(() => {
                adapter.recordTaskProgress(taskId, {
                    stage_name: 'processing',
                    progress_text: 'halfway'
                }, 'worker-2');
            }, /no longer owned/);

            assert.throws(() => {
                adapter.recordTaskCheckpoint(taskId, {
                    code: 'ownership_lost',
                    message: 'stale worker'
                }, 'worker-2');
            }, /no longer owned/);

            assert.throws(() => {
                adapter.insertTask({
                    name: 'child_task',
                    type: 'async'
                }, {
                    parent_id: taskId,
                    worker_id: 'worker-2'
                });
            }, /not owned by worker/);
        });

        it('should not reap a worker at the exact expiry boundary after heartbeat renewal', () => {
            adapter.registerWorker({
                worker_id: 'worker-boundary',
                pod_id: 'pod-boundary',
                now: 100,
                ttl: 2,
                meta: { pid: 1 }
            });

            adapter.heartbeatWorker('worker-boundary', 101, 2);

            const exactBoundary = adapter.reapExpiredWorkers(103);
            assert.deepEqual(exactBoundary, []);
            assert.equal(adapter.getWorker('worker-boundary').status, 'active');

            const afterBoundary = adapter.reapExpiredWorkers(104);
            assert.deepEqual(afterBoundary, ['worker-boundary']);
            assert.equal(adapter.getWorker('worker-boundary').status, 'dead');
        });

        it('should ignore heartbeat attempts from superseded workers', () => {
            adapter.registerWorker({
                worker_id: 'worker-old',
                pod_id: 'pod-heartbeat-race',
                now: 100,
                ttl: 5,
                meta: { pid: 1 }
            });
            adapter.registerWorker({
                worker_id: 'worker-new',
                pod_id: 'pod-heartbeat-race',
                now: 101,
                ttl: 5,
                meta: { pid: 2 }
            });

            adapter.supersedeOlderWorkers('pod-heartbeat-race', 'worker-new', 101);

            const beforeHeartbeat = adapter.getWorker('worker-old');
            adapter.heartbeatWorker('worker-old', 110, 5);
            const afterHeartbeat = adapter.getWorker('worker-old');

            assert.equal(beforeHeartbeat.status, 'superseded');
            assert.equal(afterHeartbeat.status, 'superseded');
            assert.equal(afterHeartbeat.last_seen_at, beforeHeartbeat.last_seen_at);
            assert.equal(afterHeartbeat.expires_at, beforeHeartbeat.expires_at);
        });
    });

    describe('TaskManager worker lifecycle integration', () => {
        let testDb;
        let bootstrapAdapter;
        let taskManagers;

        beforeEach(() => {
            testDb = createTestDatabaseTarget('worker-recovery-manager');
            bootstrapAdapter = createAdapter(testDb.connection);
            bootstrapAdapter.setup();
            bootstrapAdapter.clearTasks();
            clearWorkers(bootstrapAdapter);
            taskManagers = [];
        });

        afterEach(() => {
            taskManagers.forEach(manager => manager.stop());
            if (bootstrapAdapter) {
                bootstrapAdapter.close();
            }
            testDb.cleanup();
        });

        it('should register and mark the current worker dead on stop', () => {
            const taskManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-stop',
                worker_id: 'worker-stop',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(taskManager);
            taskManager.db.setup();
            taskManager.start();

            let worker = taskManager.db.getWorker('worker-stop');
            assert.ok(worker);
            assert.equal(worker.status, 'active');

            taskManager.stop();
            taskManagers = taskManagers.filter(manager => manager !== taskManager);

            worker = bootstrapAdapter.getWorker('worker-stop');
            assert.ok(worker);
            assert.equal(worker.status, 'dead');
        });

        it('should recover tasks from a superseded worker during startup', () => {
            const taskId = bootstrapAdapter.insertTask({
                name: 'resume_after_restart',
                type: 'async'
            });

            bootstrapAdapter.claimTask(['resume_after_restart'], 'worker-old');
            bootstrapAdapter.registerWorker({
                worker_id: 'worker-old',
                pod_id: 'pod-a',
                now: 100,
                ttl: 60,
                meta: { pid: 1 }
            });

            const taskManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-a',
                worker_id: 'worker-new',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(taskManager);
            taskManager.db.setup();
            taskManager.use('resume_after_restart', () => ({ resumed: true }));
            taskManager.start();

            assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'completed', 3000));

            const oldWorker = bootstrapAdapter.getWorker('worker-old');
            const newWorker = bootstrapAdapter.getWorker('worker-new');
            const task = taskManager.getTask(taskId);
            const attempts = taskManager.getTaskAttempts(taskId);

            assert.equal(oldWorker.status, 'superseded');
            assert.equal(newWorker.status, 'active');
            assert.equal(task.status, 'completed');
            assert.equal(task.worker_id, 'worker-new');
            assert.equal(attempts.length, 2);
            assert.equal(attempts[0].outcome, 'interrupted');
            assert.equal(attempts[1].outcome, 'completed');
        });

        it('should recover tasks for expired workers discovered by another pod', () => {
            const taskId = bootstrapAdapter.insertTask({
                name: 'recover_from_peer',
                type: 'async'
            });

            bootstrapAdapter.claimTask(['recover_from_peer'], 'worker-expired');
            bootstrapAdapter.registerWorker({
                worker_id: 'worker-expired',
                pod_id: 'pod-expired',
                now: 100,
                ttl: 1,
                meta: { pid: 1 }
            });

            const taskManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-peer',
                worker_id: 'worker-peer',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(taskManager);
            taskManager.db.setup();
            taskManager.use('recover_from_peer', () => ({ rescued: true }));
            taskManager.start();

            assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'completed', 3000));

            const expiredWorker = bootstrapAdapter.getWorker('worker-expired');
            const task = taskManager.getTask(taskId);
            const recoveryEvents = taskManager.getTaskEvents(taskId, { event_type: 'task_recovered' });

            assert.equal(expiredWorker.status, 'dead');
            assert.equal(task.status, 'completed');
            assert.equal(recoveryEvents.length, 1);
            assert.equal(recoveryEvents[0].metadata.recovered_by_worker_id, 'worker-peer');
        });

        it('should leave superseded running tasks untouched when startup recovery is disabled', () => {
            let resumedRuns = 0;

            const taskId = bootstrapAdapter.insertTask({
                name: 'disabled_startup_recovery',
                type: 'async'
            });

            bootstrapAdapter.claimTask(['disabled_startup_recovery'], 'worker-old');
            bootstrapAdapter.registerWorker({
                worker_id: 'worker-old',
                pod_id: 'pod-disabled-startup',
                now: 100,
                ttl: 60,
                meta: { pid: 1 }
            });

            const taskManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-disabled-startup',
                worker_id: 'worker-new',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                recover_running_jobs: false,
                poll_interval: 20
            });
            taskManagers.push(taskManager);
            taskManager.db.setup();
            taskManager.use('disabled_startup_recovery', () => {
                resumedRuns += 1;
                return { resumed: true };
            });
            taskManager.start();

            coroutine.sleep(300);

            const oldWorker = bootstrapAdapter.getWorker('worker-old');
            const newWorker = bootstrapAdapter.getWorker('worker-new');
            const task = bootstrapAdapter.getTask(taskId);
            const recoveryEvents = bootstrapAdapter.getTaskEvents(taskId, { event_type: 'task_recovered' });

            assert.equal(oldWorker.status, 'superseded');
            assert.equal(newWorker.status, 'active');
            assert.equal(task.status, 'running');
            assert.equal(task.worker_id, 'worker-old');
            assert.equal(recoveryEvents.length, 0);
            assert.equal(resumedRuns, 0);
        });

        it('should not recover expired peer tasks when recovery is disabled', () => {
            let peerHandlerRuns = 0;

            const taskId = bootstrapAdapter.insertTask({
                name: 'disabled_peer_recovery',
                type: 'async'
            });

            bootstrapAdapter.claimTask(['disabled_peer_recovery'], 'worker-expired');
            bootstrapAdapter.registerWorker({
                worker_id: 'worker-expired',
                pod_id: 'pod-disabled-peer-expired',
                now: 100,
                ttl: 1,
                meta: { pid: 1 }
            });

            const taskManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-disabled-peer-live',
                worker_id: 'worker-peer',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                recover_running_jobs: false,
                poll_interval: 20
            });
            taskManagers.push(taskManager);
            taskManager.db.setup();
            taskManager.use('disabled_peer_recovery', () => {
                peerHandlerRuns += 1;
                return { rescued: true };
            });
            taskManager.start();

            coroutine.sleep(300);

            const expiredWorker = bootstrapAdapter.getWorker('worker-expired');
            const task = bootstrapAdapter.getTask(taskId);
            const recoveryEvents = bootstrapAdapter.getTaskEvents(taskId, { event_type: 'task_recovered' });

            assert.equal(expiredWorker.status, 'dead');
            assert.equal(task.status, 'running');
            assert.equal(task.worker_id, 'worker-expired');
            assert.equal(recoveryEvents.length, 0);
            assert.equal(peerHandlerRuns, 0);
        });

        it('should ignore stale completion writes from a superseded worker', () => {
            let oldHandlerStarted = false;
            let oldHandlerFinished = false;
            let newHandlerRuns = 0;

            const oldManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-race',
                worker_id: 'worker-old',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(oldManager);
            oldManager.db.setup();
            oldManager.use('stale_completion_task', () => {
                oldHandlerStarted = true;
                coroutine.sleep(700);
                oldHandlerFinished = true;
                return { owner: 'old-worker' };
            });
            oldManager.start();

            const taskId = oldManager.async('stale_completion_task');
            assert.ok(waitFor(() => oldManager.getTask(taskId).status === 'running' && oldHandlerStarted, 3000));

            const newManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-race',
                worker_id: 'worker-new',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(newManager);
            newManager.db.setup();
            newManager.use('stale_completion_task', () => {
                newHandlerRuns += 1;
                return { owner: 'new-worker' };
            });
            newManager.start();

            assert.ok(waitFor(() => {
                const task = newManager.getTask(taskId);
                return task && task.status === 'completed' && task.result && task.result.owner === 'new-worker';
            }, 5000));

            assert.ok(waitFor(() => oldHandlerFinished, 5000));

            const finalTask = newManager.getTask(taskId);
            const attempts = newManager.getTaskAttempts(taskId);
            const recoveryEvents = newManager.getTaskEvents(taskId, { event_type: 'task_recovered' });

            assert.equal(newHandlerRuns, 1);
            assert.equal(finalTask.status, 'completed');
            assert.deepEqual(finalTask.result, { owner: 'new-worker' });
            assert.equal(finalTask.worker_id, 'worker-new');
            assert.equal(attempts.length, 2);
            assert.equal(attempts[0].worker_id, 'worker-old');
            assert.equal(attempts[0].outcome, 'interrupted');
            assert.equal(attempts[1].worker_id, 'worker-new');
            assert.equal(attempts[1].outcome, 'completed');
            assert.equal(recoveryEvents.length, 1);
            assert.equal(recoveryEvents[0].metadata.recovered_from_worker_id, 'worker-old');
        });

        it('should prevent a superseded worker from claiming newly queued tasks', () => {
            let oldClaims = 0;
            let newClaims = 0;

            const oldManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-claim-race',
                worker_id: 'worker-old',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(oldManager);
            oldManager.db.setup();
            oldManager.use('claim_after_supersede', () => {
                oldClaims += 1;
                return { owner: 'old-worker' };
            });
            oldManager.start();

            const newManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-claim-race',
                worker_id: 'worker-new',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                poll_interval: 20
            });
            taskManagers.push(newManager);
            newManager.db.setup();
            newManager.use('claim_after_supersede', () => {
                newClaims += 1;
                return { owner: 'new-worker' };
            });
            newManager.start();
            newManager.pause();

            const oldWorker = bootstrapAdapter.getWorker('worker-old');
            assert.equal(oldWorker.status, 'superseded');

            const taskId = bootstrapAdapter.insertTask({
                name: 'claim_after_supersede',
                type: 'async'
            });

            coroutine.sleep(300);

            const task = bootstrapAdapter.getTask(taskId);
            assert.equal(oldClaims, 0);
            assert.equal(newClaims, 0);
            assert.equal(task.status, 'pending');
            assert.equal(task.worker_id, null);
        });

        it('should not recover running tasks from a worker that is still heartbeating', () => {
            let oldHandlerStarted = false;
            let oldHandlerFinished = false;
            let peerHandlerRuns = 0;

            const oldManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-live',
                worker_id: 'worker-live',
                worker_heartbeat_interval: 50,
                worker_ttl: 150,
                active_update_interval: 1000,
                poll_interval: 20
            });
            taskManagers.push(oldManager);
            oldManager.db.setup();
            oldManager.use('healthy_long_task', () => {
                oldHandlerStarted = true;
                coroutine.sleep(350);
                oldHandlerFinished = true;
                return { owner: 'worker-live' };
            });
            oldManager.start();

            const taskId = oldManager.async('healthy_long_task');
            assert.ok(waitFor(() => {
                const task = oldManager.getTask(taskId);
                return oldHandlerStarted && task && task.status === 'running' && task.worker_id === 'worker-live';
            }, 3000));

            const peerManager = new TaskManager({
                dbConnection: testDb.connection,
                pod_id: 'pod-peer-live',
                worker_id: 'worker-peer-live',
                worker_heartbeat_interval: 50,
                worker_ttl: 1000,
                active_update_interval: 1000,
                poll_interval: 20
            });
            taskManagers.push(peerManager);
            peerManager.db.setup();
            peerManager.use('healthy_long_task', () => {
                peerHandlerRuns += 1;
                return { owner: 'worker-peer-live' };
            });
            peerManager.start();

            coroutine.sleep(250);

            const midTask = bootstrapAdapter.getTask(taskId);
            const midWorker = bootstrapAdapter.getWorker('worker-live');
            const midRecoveryEvents = bootstrapAdapter.getTaskEvents(taskId, { event_type: 'task_recovered' });

            assert.equal(midWorker.status, 'active');
            assert.ok(['running', 'completed'].includes(midTask.status));
            assert.equal(midTask.worker_id, 'worker-live');
            assert.equal(midRecoveryEvents.length, 0);
            assert.equal(peerHandlerRuns, 0);

            assert.ok(waitFor(() => oldHandlerFinished && bootstrapAdapter.getTask(taskId).status === 'completed', 5000));

            const finalTask = bootstrapAdapter.getTask(taskId);
            const finalRecoveryEvents = bootstrapAdapter.getTaskEvents(taskId, { event_type: 'task_recovered' });
            const attempts = bootstrapAdapter.getTaskAttempts(taskId);

            assert.equal(finalTask.status, 'completed');
            assert.deepEqual(finalTask.result, { owner: 'worker-live' });
            assert.equal(finalTask.worker_id, 'worker-live');
            assert.equal(finalRecoveryEvents.length, 0);
            assert.equal(peerHandlerRuns, 0);
            assert.equal(attempts.length, 1);
            assert.equal(attempts[0].worker_id, 'worker-live');
            assert.equal(attempts[0].outcome, 'completed');
        });
    });
});