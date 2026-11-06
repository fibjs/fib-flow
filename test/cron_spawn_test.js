const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');
const coroutine = require('coroutine');

const { TaskManager } = require('..');
const config = require('./config.js');

function waitFor(predicate, timeoutMs = 8000, intervalMs = 50) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (predicate()) {
            return true;
        }
        coroutine.sleep(intervalMs);
    }

    return predicate();
}

/**
 * Cron spawn mode (cronMode: 'spawn').
 *
 * A schedule row must never execute user code: every round inserts an
 * independent async run row and advances the schedule in one transaction. These
 * tests pin the properties that make that worth doing — atomicity, suspension
 * isolation, bounded attempt/event growth, no stall under handler saturation, and
 * a spawn result that consumers can subscribe to.
 */
describe('Cron spawn mode', () => {
    let taskManager;

    const createManager = (options = {}) => new TaskManager({
        dbConnection: config.dbConnection,
        poll_interval: 100,
        cronMode: 'spawn',
        ...options
    });

    beforeEach(() => {
        taskManager = createManager();
        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
            taskManager = null;
        }
    });

    it('should reject an unknown cronMode', () => {
        assert.throws(
            () => new TaskManager({ dbConnection: config.dbConnection, cronMode: 'bogus' }),
            /Invalid cronMode/
        );
    });

    it('should reject an unknown cronMode before opening a connection pool', () => {
        // Validation must run before the adapter is created: throwing afterwards
        // would leak a pooled connection on every failed construction (observed as
        // lingering DbConnection objects in the test watchdog on PostgreSQL).
        let opened = 0;
        const spyPool = () => {
            opened++;
            throw new Error('connection pool must not be created for an invalid option');
        };

        assert.throws(
            () => new TaskManager({
                dbConnection: spyPool,
                dbType: 'sqlite',
                cronMode: 'not-a-mode'
            }),
            /Invalid cronMode/
        );
        assert.equal(opened, 0, 'an invalid option must not reach the adapter');
    });

    it('should default to inline mode', () => {
        const manager = new TaskManager({ dbConnection: config.dbConnection });
        try {
            assert.equal(manager.options.cronMode, 'inline');
        } finally {
            manager.db.close();
        }
    });

    it('should spawn one async run row per round and never execute the schedule row', () => {
        let handlerCalls = 0;
        taskManager.use('spawn_basic', () => {
            handlerCalls++;
            return { ok: true };
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_basic', '* * * * * *', { topic: 'k1' });
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasksByStatus('completed').length >= 1),
            'expected at least one completed run row'
        );

        const schedule = taskManager.getTask(scheduleId);
        assert.equal(schedule.type, 'cron');
        assert.equal(schedule.status, 'pending', 'schedule row must return to pending');
        assert.equal(schedule.result, null, 'schedule row result must stay untouched');

        const runs = taskManager.getTasks({ name: 'spawn_basic', type: 'async' });
        assert.ok(runs.length >= 1, 'expected spawned run rows');
        for (const run of runs) {
            assert.equal(run.type, 'async');
            assert.equal(run.tag, schedule.tag || null);
            assert.deepStrictEqual(run.payload, { topic: 'k1' });
        }

        // Every handler call belongs to an async run row — never to the schedule.
        assert.ok(handlerCalls >= 1);
        assert.equal(
            taskManager.getTask(runs[0].id).type,
            'async',
            'handler must run on the spawned row'
        );
    });

    it('should keep spawning while a run is suspended', () => {
        let calls = 0;
        taskManager.use('spawn_suspend', (task) => {
            calls++;
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return 'resumed';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_suspend', '* * * * * *', {});
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasksByStatus('suspended').length >= 1),
            'expected a suspended run row'
        );

        const suspendedBefore = taskManager.getTasksByStatus('suspended').length;
        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_suspend', type: 'async' }).length > suspendedBefore),
            'schedule must keep spawning while a run is suspended'
        );

        const schedule = taskManager.getTask(scheduleId);
        assert.notEqual(schedule.status, 'suspended', 'schedule row must not follow the run state');
        assert.equal(schedule.status, 'pending');
    });

    it('should not grow the schedule row attempt table', () => {
        taskManager.use('spawn_attempts', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_attempts', '* * * * * *', {});
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_attempts', type: 'async' }).length >= 3, 15000),
            'expected several spawned runs'
        );

        const schedule = taskManager.getTask(scheduleId);
        assert.equal(schedule.status, 'pending');

        const scheduleAttempts = taskManager.getTaskAttempts(scheduleId);
        assert.equal(
            scheduleAttempts.length,
            0,
            'a spawn round is not an execution: the schedule must not accumulate attempts'
        );

        // Run rows keep their normal attempt trail.
        const runs = taskManager.getTasks({ name: 'spawn_attempts', type: 'async' });
        const runAttempts = taskManager.getTaskAttempts(runs[0].id);
        assert.ok(runAttempts.length >= 1, 'spawned runs must keep their attempts');

        // The round is recorded as one spawn event, not a claim/start pair.
        const events = taskManager.getTaskEvents(scheduleId);
        const types = events.map(event => event.event_type);
        assert.ok(types.includes('task_cron_spawned'), 'expected a task_cron_spawned event');
        assert.ok(!types.includes('task_started'), 'spawn rounds must not emit task_started');
    });

    it('should expose the spawned run id on the schedule view', () => {
        taskManager.use('spawn_link', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_link', '* * * * * *', {});
        taskManager.start();

        assert.ok(
            waitFor(() => {
                const [schedule] = taskManager.getCronSchedules({ name: 'spawn_link' });
                return Boolean(schedule && schedule.last_spawned_task_id);
            }),
            'expected last_spawned_task_id to be populated'
        );

        const [schedule] = taskManager.getCronSchedules({ name: 'spawn_link' });
        assert.equal(schedule.id, scheduleId);
        const spawned = taskManager.getTask(schedule.last_spawned_task_id);
        assert.ok(spawned, 'last_spawned_task_id must point at a real row');
        assert.equal(spawned.type, 'async');
        assert.equal(spawned.name, 'spawn_link');
    });

    it('should spawn even when the handler concurrency limit is saturated', () => {
        // A long-running run holds the only slot; the schedule must still tick.
        taskManager.use('spawn_saturated', {
            handler: () => {
                coroutine.sleep(1200);
                return 'done';
            },
            max_concurrent_tasks: 1
        });

        const scheduleId = taskManager.cron('spawn_saturated', '* * * * * *', {});
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_saturated', type: 'async' }).length >= 2, 12000),
            'schedule must keep spawning while the handler is saturated'
        );

        const schedule = taskManager.getTask(scheduleId);
        assert.equal(schedule.status, 'pending');
    });

    it('should spawn immediately via runCronNow without moving the schedule', () => {
        taskManager.use('spawn_now', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_now', '0 0 1 1 *', {});

        const before = taskManager.getTask(scheduleId);
        const result = taskManager.runCronNow(scheduleId, { via: 'manual' });

        const after = taskManager.getTask(scheduleId);
        assert.equal(after.next_run_time, before.next_run_time, 'runCronNow must not move next_run_time');
        assert.equal(after.status, before.status);
        assert.equal(after.retry_count, before.retry_count);

        const run = taskManager.getTask(result.taskId);
        assert.ok(run, 'runCronNow must create a run row');
        assert.equal(run.type, 'async');
        assert.deepStrictEqual(run.payload, { via: 'manual' });
    });

    it('should reject runCronNow on unknown or non-cron ids', () => {
        taskManager.use('spawn_now_err', () => 'ok');
        taskManager.db.setup();

        assert.throws(() => taskManager.runCronNow(999999), /not found/);

        const asyncId = taskManager.async('spawn_now_err', {});
        assert.throws(() => taskManager.runCronNow(asyncId), /not a cron schedule/);
    });

    it('should keep the schedule firing after a spawn failure', () => {
        taskManager.use('spawn_fail', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_fail', '* * * * * *', {});

        // Inject a failure in the spawn transaction path only.
        const db = taskManager.db;
        const original = db.spawnCronRun.bind(db);
        let failures = 0;
        db.spawnCronRun = (params) => {
            if (failures === 0) {
                failures++;
                throw new Error('injected spawn failure');
            }
            return original(params);
        };

        taskManager.start();

        assert.ok(
            waitFor(() => {
                const schedule = taskManager.getTask(scheduleId);
                return schedule.status === 'pending' && /injected spawn failure/.test(schedule.error || '');
            }),
            'failed spawn must leave the schedule pending with the error recorded'
        );

        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_fail', type: 'async' }).length >= 1, 8000),
            'the next round must still spawn'
        );

        const events = taskManager.getTaskEvents(scheduleId);
        assert.ok(
            events.some(event => event.event_type === 'task_cron_spawn_failed'),
            'expected a task_cron_spawn_failed event'
        );
    });

    it('should satisfy catchUp=skip without spawning a stale window', () => {
        taskManager.use('spawn_skip', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_skip', '0 0 1 1 *', {}, { catchUp: 'skip' });

        // Force the schedule into the past by two full periods.
        taskManager.db.pool(conn => conn.execute(
            `UPDATE fib_flow_tasks SET next_run_time = ? WHERE id = ?`,
            Math.floor(Date.now() / 1000) - 2 * 365 * 24 * 3600,
            scheduleId
        ));

        taskManager.start();

        assert.ok(
            waitFor(() => {
                const events = taskManager.getTaskEvents(scheduleId);
                return events.some(event => event.event_type === 'task_cron_missed');
            }),
            'expected the missed window to be skipped'
        );

        const runs = taskManager.getTasks({ name: 'spawn_skip', type: 'async' });
        assert.equal(runs.length, 0, 'a skipped window must not spawn a run');
    });

    it('should inherit priority, timeout, retries and retention from the schedule', () => {
        taskManager.use('spawn_inherit', () => 'ok');
        taskManager.db.setup();

        taskManager.cron('spawn_inherit', '* * * * * *', {}, {
            priority: 7,
            timeout: 120,
            max_retries: 5,
            retry_interval: 9,
            retention_expire_time: 1234
        });
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_inherit', type: 'async' }).length >= 1),
            'expected a spawned run'
        );

        const [run] = taskManager.getTasks({ name: 'spawn_inherit', type: 'async' });
        assert.equal(run.priority, 7);
        assert.equal(run.timeout, 120);
        assert.equal(run.max_retries, 5);
        assert.equal(run.retry_interval, 9);
        assert.equal(run.retention_expire_time, 1234, 'a run inherits the schedule retention window');
    });

    it('should never treat the schedule row as one of its own runs', () => {
        taskManager.use('spawn_identity', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_identity', '* * * * * *', {});
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_identity', type: 'async' }).length >= 2),
            'expected at least two spawned runs'
        );

        const runs = taskManager.getTasks({ name: 'spawn_identity', type: 'async' });
        const runIds = new Set(runs.map(run => String(run.id)));
        assert.ok(!runIds.has(String(scheduleId)), 'the schedule row must never appear as a run');

        const [schedule] = taskManager.getCronSchedules({ name: 'spawn_identity' });
        assert.ok(runIds.has(String(schedule.last_spawned_task_id)));
    });

    it('should record the spawn event as the schedule last event', () => {
        taskManager.use('spawn_last_event', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_last_event', '* * * * * *', {});
        taskManager.start();

        assert.ok(
            waitFor(() => taskManager.getTasks({ name: 'spawn_last_event', type: 'async' }).length >= 1),
            'expected a spawned run'
        );

        const schedule = taskManager.getTask(scheduleId);
        assert.equal(schedule.last_event_type, 'task_cron_spawned');
        assert.ok(schedule.last_event_time > 0);
    });

    it('should not revive a cancelled schedule through runCronNow', () => {
        taskManager.use('spawn_no_revive', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_no_revive', '0 0 1 1 *', {});
        taskManager.cancelCron({ name: 'spawn_no_revive' });

        const before = taskManager.getTask(scheduleId);
        assert.equal(before.status, 'cancelled');

        const result = taskManager.runCronNow(scheduleId, { manual: true });

        const after = taskManager.getTask(scheduleId);
        assert.equal(after.status, 'cancelled', 'a manual trigger must not revive a cancelled schedule');
        assert.equal(after.next_run_time, before.next_run_time);

        const run = taskManager.getTask(result.taskId);
        assert.equal(run.status, 'pending');
        assert.deepStrictEqual(run.payload, { manual: true });
    });

    it('should keep a stuck running schedule recoverable', () => {
        // A crash between "claim" and "spawn" leaves the row `running`. Worker
        // recovery (or the timeout sweep) must bring it back, otherwise spawn mode
        // would trade one stall for another.
        const manager = createManager({
            task_heartbeat_interval: 200,
            task_heartbeat_timeout: 600
        });
        manager.db.setup();
        manager.db.clearTasks();
        manager.use('spawn_stuck', () => 'ok');

        const scheduleId = manager.cron('spawn_stuck', '* * * * * *', {});
        const now = Math.floor(Date.now() / 1000);
        manager.db.pool(conn => conn.execute(
            `UPDATE fib_flow_tasks
                SET status = 'running', worker_id = 'ghost-worker',
                    start_time = ?, last_active_time = ?, next_run_time = ?
              WHERE id = ?`,
            now - 10, now - 10, now - 5, scheduleId
        ));
        assert.equal(manager.getTask(scheduleId).status, 'running');

        manager.start();
        try {
            assert.ok(
                waitFor(() => manager.getTasks({ name: 'spawn_stuck', type: 'async' }).length >= 1, 15000),
                'a schedule stuck in running must be recovered and keep spawning'
            );
        } finally {
            manager.stop();
        }
    });

    it('should keep the per-round audit trail bounded', () => {
        // A schedule row is never collected by retention, so an uncapped
        // per-round event would grow forever (a one-minute schedule writes half a
        // million rows a year). The spawned run rows are the authoritative round
        // history; the schedule keeps a bounded tail.
        taskManager.use('spawn_bound', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_bound', '* * * * * *', {});

        // Drive rounds directly: claiming through the loop would take a minute.
        const db = taskManager.db;
        const original = db.spawnCronRun.bind(db);
        let rounds = 0;
        db.spawnCronRun = (params) => {
            rounds++;
            return original(params);
        };

        taskManager.start();

        assert.ok(
            waitFor(() => rounds >= 25, 60000, 100),
            `expected at least 25 rounds, got ${rounds}`
        );

        const attempts = taskManager.getTaskAttempts(scheduleId);
        assert.equal(attempts.length, 0, 'spawn rounds must not create attempt rows');

        const roundEvents = taskManager.getTaskEvents(scheduleId).filter(event =>
            ['task_cron_spawned', 'task_cron_spawn_failed', 'task_cron_missed'].includes(event.event_type)
        );
        assert.ok(roundEvents.length > 0, 'the newest round event must be kept');
        assert.ok(
            roundEvents.length <= 20,
            `round events must stay bounded, got ${roundEvents.length} after ${rounds} rounds`
        );

        // Pruning must not break the run lookup.
        const [schedule] = taskManager.getCronSchedules({ name: 'spawn_bound' });
        const lastSpawned = taskManager.getTask(schedule.last_spawned_task_id);
        assert.ok(lastSpawned, 'last_spawned_task_id must still resolve after pruning');
        assert.equal(lastSpawned.type, 'async');
    });

    it('should keep the round trail bounded for manual triggers too', () => {
        taskManager.use('spawn_manual_bound', () => 'ok');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_manual_bound', '0 0 1 1 *', {});

        for (let i = 0; i < 30; i++) {
            taskManager.runCronNow(scheduleId, { i });
        }

        const roundEvents = taskManager.getTaskEvents(scheduleId).filter(event =>
            ['task_cron_spawned', 'task_cron_spawn_failed', 'task_cron_missed'].includes(event.event_type)
        );
        assert.ok(
            roundEvents.length <= 20,
            `manual triggers must keep the trail bounded, got ${roundEvents.length}`
        );
        assert.equal(taskManager.getTaskAttempts(scheduleId).length, 0);

        const [schedule] = taskManager.getCronSchedules({ name: 'spawn_manual_bound' });
        assert.ok(
            taskManager.getTask(schedule.last_spawned_task_id),
            'last_spawned_task_id must still resolve after manual-trigger pruning'
        );
    });

    it('should never hand a schedule row to the user handler', () => {
        // Both types share one name in spawn mode, so an unfiltered execution
        // claim could pick up a cron row and run the handler inline — silently
        // reintroducing the freeze-on-suspend behaviour spawn mode removes.
        //
        // Deterministic by construction: the spawn probe is skipped (empty cached
        // name list), so only the execution probe could return the row. This
        // asserts the probe itself rather than racing the loop.
        let handlerSawCronRow = null;
        taskManager.use('spawn_type_guard', (task) => {
            if (task.type !== 'async') {
                handlerSawCronRow = { id: task.id, type: task.type };
            }
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_type_guard', '* * * * * *', {});

        // Make the schedule due now.
        taskManager.db.pool(conn => conn.execute(
            `UPDATE fib_flow_tasks SET next_run_time = ? WHERE id = ?`,
            Math.floor(Date.now() / 1000) - 1,
            scheduleId
        ));

        // Stale cache: the spawn probe will find no eligible schedule names.
        taskManager._scheduleNameCache = { at: Date.now(), eligible: [], known: new Set() };

        // The schedule row must never be returned as an executable claim.
        const claimed = taskManager._claimNextTask();
        assert.ok(
            !claimed.scheduleOnly,
            'the spawn probe was skipped, so this must be an execution claim'
        );
        assert.ok(
            !claimed.task || claimed.task.type === 'async',
            `the execution probe returned a ${claimed.task && claimed.task.type} row`
        );

        // Run one round of the loop for the handler-level assertion.
        taskManager.start();
        coroutine.sleep(800);

        assert.equal(handlerSawCronRow, null, 'the handler must never receive a schedule row');
        assert.equal(
            taskManager.getTaskAttempts(scheduleId).length,
            0,
            'a cron row must never be claimed as an execution'
        );
        const schedule = taskManager.getTask(scheduleId);
        assert.equal(schedule.status, 'pending', 'the schedule must still be untouched');
        assert.equal(schedule.result, null, 'the schedule result must stay untouched');
    });

    it('should not spawn runs for an orphan schedule', () => {
        // An orphan schedule (handler removed, row still `pending`) must not spawn:
        // the run rows would carry a name nobody registered, so they can never be
        // claimed, never reach a terminal state, and never become retention
        // candidates. Inline mode is immune because `claimTask` already filters on
        // the registered name list — this leak is specific to spawn.
        taskManager.use('spawn_orphan_job', () => 'orphan');
        taskManager.use('spawn_orphan_peer', () => 'peer');
        taskManager.db.setup();

        taskManager.cron('spawn_orphan_job', '* * * * * *', {});
        taskManager.cron('spawn_orphan_peer', '* * * * * *', {});

        // Simulate "definition deleted, cancelCron never ran" (crash in between, or
        // a partially failed definition load).
        taskManager.unuse('spawn_orphan_job');
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => taskManager.getTasks({ name: 'spawn_orphan_peer', type: 'async' }).length >= 2, 12000),
                'the registered schedule must keep spawning'
            );

            const [schedule] = taskManager.getCronSchedules({ name: 'spawn_orphan_job' });
            assert.equal(schedule.orphan, true, 'the schedule must be reported as orphaned');

            assert.equal(
                taskManager.getTasks({ name: 'spawn_orphan_job', type: 'async' }).length,
                0,
                'an orphan schedule must not create run rows that can never be claimed'
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should resume an orphan schedule once its handler is registered again', () => {
        // The guard must not turn "leak" into "permanently dead schedule": a
        // re-registered handler has to pick the schedule back up.
        taskManager.use('spawn_orphan_resume', () => 'resumed');
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_orphan_resume', '* * * * * *', {});
        taskManager.unuse('spawn_orphan_resume');
        taskManager.start();

        try {
            coroutine.sleep(2000);
            assert.equal(
                taskManager.getTasks({ name: 'spawn_orphan_resume', type: 'async' }).length,
                0,
                'no runs while orphaned'
            );
            assert.equal(taskManager.getTask(scheduleId).status, 'pending', 'the row stays pending, not cancelled');

            // Handler comes back.
            taskManager.use('spawn_orphan_resume', () => 'resumed');

            assert.ok(
                waitFor(() => {
                    const runs = taskManager.getTasks({ name: 'spawn_orphan_resume', type: 'async' });
                    return runs.some(run => run.status === 'completed');
                }, 12000),
                'the schedule must resume once a handler is registered again'
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should not overlap runs by default', () => {
        // Inline mode was mutually exclusive for free: its cron row was `running`
        // and therefore not claimable. Spawn removes that, so the equivalent cap
        // has to be explicit — otherwise a handler slower than its period stacks
        // runs forever, and the queued ones never reach a terminal state (so
        // retention never collects them either).
        let concurrent = 0;
        let peak = 0;
        taskManager.use('spawn_overlap', () => {
            concurrent++;
            peak = Math.max(peak, concurrent);
            coroutine.sleep(1500);
            concurrent--;
            return 'ok';
        });
        taskManager.db.setup();

        taskManager.cron('spawn_overlap', '* * * * * *', {});
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => taskManager.getTasks({ name: 'spawn_overlap', type: 'async' }).length >= 3, 15000),
                'expected several rounds'
            );

            assert.equal(peak, 1, 'rounds must not overlap by default');

            const [schedule] = taskManager.getCronSchedules({ name: 'spawn_overlap' });
            const skipEvents = taskManager.getTaskEvents(schedule.id)
                .filter(event => event.event_type === 'task_cron_overlap_skipped');
            assert.ok(skipEvents.length >= 1, 'an overlap skip must be recorded on the schedule');

            // No backlog of rows that can never finish.
            const inFlight = taskManager.getTasks({ name: 'spawn_overlap', type: 'async' })
                .filter(run => run.status === 'pending' || run.status === 'running');
            assert.ok(inFlight.length <= 1, `at most one run may be in flight, got ${inFlight.length}`);

            // A skipped round must not look like a failure.
            assert.equal(schedule.last_error, null, 'an overlap skip is not an error');
        } finally {
            taskManager.stop();
        }
    });

    it('should honour maxInstances to allow controlled overlap', () => {
        let concurrent = 0;
        let peak = 0;
        taskManager.use('spawn_overlap2', () => {
            concurrent++;
            peak = Math.max(peak, concurrent);
            coroutine.sleep(1500);
            concurrent--;
            return 'ok';
        });
        taskManager.db.setup();

        taskManager.cron('spawn_overlap2', '* * * * * *', {}, { maxInstances: 2 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => peak >= 2, 15000),
                `maxInstances: 2 must allow two concurrent runs, peak was ${peak}`
            );
            assert.ok(peak <= 2, `must never exceed the declared cap, peak was ${peak}`);
        } finally {
            taskManager.stop();
        }
    });

    it('should reject an invalid maxInstances', () => {
        taskManager.use('spawn_overlap_bad', () => 'ok');
        taskManager.db.setup();

        assert.throws(
            () => taskManager.cron('spawn_overlap_bad', '* * * * * *', {}, { maxInstances: -1 }),
            /maxInstances must be a non-negative integer/
        );
        assert.throws(
            () => taskManager.cron('spawn_overlap_bad', '* * * * * *', {}, { maxInstances: 1.5 }),
            /maxInstances must be a non-negative integer/
        );
    });

    it('should surface a failing round on the schedule row', () => {
        // Inline mode reported failures on the schedule row because the row was the
        // execution. Spawn moved execution to a run row, so without mirroring the
        // schedule would look healthy while every round fails.
        taskManager.use('spawn_fail_signal', () => {
            throw new Error('boom');
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_fail_signal', '* * * * * *', {}, { maxInstances: 0 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => {
                    const schedule = taskManager.getTask(scheduleId);
                    return /boom/.test(schedule.error || '');
                }, 12000),
                'a failing run must leave the reason on the schedule row'
            );

            const [schedule] = taskManager.getCronSchedules({ name: 'spawn_fail_signal' });
            assert.match(schedule.last_error, /boom/);
            // Still firing: the schedule did not stop.
            assert.equal(schedule.status, 'pending');
        } finally {
            taskManager.stop();
        }
    });

    it('should clear the schedule error once a round succeeds again', () => {
        let shouldFail = true;
        taskManager.use('spawn_fail_recover', () => {
            if (shouldFail) {
                throw new Error('boom');
            }
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_fail_recover', '* * * * * *', {}, { maxInstances: 0 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => /boom/.test(taskManager.getTask(scheduleId).error || ''), 12000),
                'expected the failure to be visible'
            );

            shouldFail = false;

            assert.ok(
                waitFor(() => taskManager.getTask(scheduleId).error === null, 12000),
                'a successful round must clear the stale error'
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should not regress the inline path', () => {
        // Same handler, same expression, default mode: the schedule row runs it and
        // no async row appears. This pins "8.1.0 is additive".
        const manager = createManager({ cronMode: 'inline' });
        manager.db.setup();
        manager.db.clearTasks();
        manager.use('spawn_inline_guard', () => 'inline-result');

        const scheduleId = manager.cron('spawn_inline_guard', '* * * * * *', {});
        manager.start();

        try {
            assert.ok(
                waitFor(() => manager.getTask(scheduleId).result === 'inline-result'),
                'inline mode must write the result on the schedule row'
            );
            assert.equal(
                manager.getTasks({ name: 'spawn_inline_guard', type: 'async' }).length,
                0,
                'inline mode must not spawn run rows'
            );
        } finally {
            manager.stop();
        }
    });
});
