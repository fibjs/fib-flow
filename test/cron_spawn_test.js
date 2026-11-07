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

        // `ignoreMaxInstances`: this asserts the event trail stays bounded, not the
        // overlap cap (which the following cases cover).
        for (let i = 0; i < 30; i++) {
            taskManager.runCronNow(scheduleId, { i }, { ignoreMaxInstances: true });
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

    it('should bound the run-outcome events too, not just spawn events', () => {
        // `task_cron_run_failed` / `task_cron_run_recovered` are written on the
        // schedule row, which retention never collects. They must be part of the
        // capped round-event family, otherwise the earlier fix just moved the leak
        // from one event name to another.
        taskManager.use('spawn_capped_outcome', () => {
            throw new Error('always');
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_capped_outcome', '* * * * * *', {}, { maxInstances: 0 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => taskManager.getTasks({ name: 'spawn_capped_outcome', type: 'async' }).length >= 8, 20000),
                'expected several rounds'
            );

            const roundEvents = taskManager.getTaskEvents(scheduleId).filter(event => [
                'task_cron_spawned',
                'task_cron_spawn_failed',
                'task_cron_missed',
                'task_cron_overlap_skipped',
                'task_cron_run_failed',
                'task_cron_run_recovered'
            ].includes(event.event_type));

            assert.ok(
                roundEvents.length <= 20,
                `all round events must stay bounded, got ${roundEvents.length}`
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should emit task_cron_run_recovered when a run succeeds again', () => {
        // The clearing signal must be observable, otherwise a consumer cannot tell
        // "recovered" from "never failed".
        let shouldFail = true;
        taskManager.use('spawn_outcome_recover', () => {
            if (shouldFail) {
                throw new Error('boom');
            }
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_outcome_recover', '* * * * * *', {}, { maxInstances: 0 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => /boom/.test(taskManager.getTask(scheduleId).error || ''), 12000),
                'expected the failure to be visible'
            );

            shouldFail = false;

            assert.ok(
                waitFor(() => taskManager.getTask(scheduleId).error === null, 12000),
                'expected the error to clear'
            );

            const types = taskManager.getTaskEvents(scheduleId).map(event => event.event_type);
            assert.ok(types.includes('task_cron_run_recovered'), 'the recovery must be recorded');
        } finally {
            taskManager.stop();
        }
    });

    it('should keep the failure signal visible while a slow run is in flight', () => {
        // Clearing `error` at spawn time made a failing schedule look healthy for
        // the whole duration of each run. With a handler slower than the period that
        // is most of the time.
        let fail = true;
        taskManager.use('spawn_slow_fail', () => {
            coroutine.sleep(1200);
            if (fail) {
                throw new Error('slow boom');
            }
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_slow_fail', '* * * * * *', {}, { maxInstances: 0 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => /slow boom/.test(taskManager.getTask(scheduleId).error || ''), 12000),
                'expected the failure to show up'
            );

            // Sample across a few periods: the signal must stay set.
            let emptySamples = 0;
            for (let i = 0; i < 10; i++) {
                coroutine.sleep(200);
                if (taskManager.getTask(scheduleId).error === null) {
                    emptySamples++;
                }
            }
            assert.equal(emptySamples, 0, 'a failing schedule must not report a blank error between rounds');
        } finally {
            fail = false;
            taskManager.stop();
        }
    });

    it('should not let a manual run block or taint its schedule', () => {
        // One handler name means a manual task shares `name` and `tag` with the
        // schedule's own runs. Counting or reporting by those would let a user
        // clicking "run now" starve the tick and overwrite the schedule's health.
        const tag = 'taskdef:shared';
        let cronRounds = 0;
        taskManager.use('spawn_manual_scope', (task) => {
            if (task.payload && task.payload.manual === 'fail') {
                throw new Error('manual boom');
            }
            if (task.payload && task.payload.manual === 'slow') {
                coroutine.sleep(4000);
                return 'slow done';
            }
            cronRounds++;
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_manual_scope', '* * * * * *', {}, { tag });
        taskManager.start();

        try {
            coroutine.sleep(800);

            // A long manual run must not stop the schedule.
            taskManager.async('spawn_manual_scope', { manual: 'slow' }, { tag });

            assert.ok(
                waitFor(() => cronRounds >= 3, 15000),
                `the schedule must keep firing during a manual run, saw ${cronRounds} rounds`
            );

            // A failing manual run must not report against the schedule.
            const beforeManualFailure = taskManager.getTask(scheduleId).error;
            taskManager.async('spawn_manual_scope', { manual: 'fail' }, { tag });

            assert.ok(
                waitFor(() => {
                    const runs = taskManager.getTasks({ name: 'spawn_manual_scope', type: 'async' });
                    return runs.some(run => run.status === 'failed');
                }, 12000),
                'expected the manual run to fail'
            );

            assert.equal(
                taskManager.getTask(scheduleId).error,
                beforeManualFailure,
                'a manual run must not write to the schedule health'
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should count in-flight runs per schedule, not per name', () => {
        // `(name, cronKey)` is the schedule identity. Two schedules can share name
        // and tag; counting by those would make them starve each other.
        taskManager.use('spawn_multi_key', () => 'ok');
        taskManager.db.setup();

        const first = taskManager.cron('spawn_multi_key', '* * * * * *', {}, { cronKey: 'a', tag: 'same' });
        const second = taskManager.cron('spawn_multi_key', '* * * * * *', {}, { cronKey: 'b', tag: 'same' });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => taskManager.getTasks({ name: 'spawn_multi_key', type: 'async' }).length >= 6, 20000),
                'both schedules must spawn independently'
            );

            const skippedFirst = taskManager.getTaskEvents(first)
                .filter(event => event.event_type === 'task_cron_overlap_skipped').length;
            const skippedSecond = taskManager.getTaskEvents(second)
                .filter(event => event.event_type === 'task_cron_overlap_skipped').length;

            // A few skips are fine (the default cap is 1 per schedule); what must not
            // happen is one schedule starving the other for the whole run.
            const runs = taskManager.getTasks({ name: 'spawn_multi_key', type: 'async' });
            assert.ok(runs.length >= 6, `expected both schedules to produce runs, got ${runs.length}`);
            assert.ok(
                skippedFirst < 20 && skippedSecond < 20,
                `schedules must not starve each other (skips: ${skippedFirst}/${skippedSecond})`
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should not rewrite the schedule signal when the outcome is unchanged', () => {
        // Every round of a permanently failing schedule would otherwise append a
        // round event on a row retention never collects. The schedule holds one
        // signal, not a per-round log — the run rows carry the history.
        taskManager.use('spawn_signal_dedupe', () => {
            throw new Error('always');
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_signal_dedupe', '* * * * * *', {}, { maxInstances: 0 });
        taskManager.start();

        try {
            assert.ok(
                waitFor(() => taskManager.getTasks({ name: 'spawn_signal_dedupe', type: 'async' }).length >= 6, 20000),
                'expected several failing rounds'
            );

            const failedEvents = taskManager.getTaskEvents(scheduleId)
                .filter(event => event.event_type === 'task_cron_run_failed');
            assert.ok(
                failedEvents.length <= 3,
                `the same failure must not be re-recorded every round, got ${failedEvents.length}`
            );

            // ...while the signal itself stays visible.
            assert.match(taskManager.getTask(scheduleId).error || '', /always/);
        } finally {
            taskManager.stop();
        }
    });

    it('should apply maxInstances to runCronNow too', () => {
        // A manual trigger creates a run of this schedule, so it must count toward
        // the cap. The alternative — exempt from the cap but still occupying it —
        // would be two answers to one question, and a user clicking "run now" would
        // silently skip the next scheduled round.
        taskManager.use('spawn_manual_cap', () => {
            coroutine.sleep(3000);
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_manual_cap', '0 0 1 1 *', {});

        taskManager.runCronNow(scheduleId, { n: 1 });

        assert.throws(
            () => taskManager.runCronNow(scheduleId, { n: 2 }),
            /already has 1 run\(s\) in flight/,
            'the second manual trigger must hit the same cap as a scheduled round'
        );

        // Forcing is possible, and explicit.
        const forced = taskManager.runCronNow(scheduleId, { n: 3 }, { ignoreMaxInstances: true });
        assert.ok(forced.taskId, 'ignoreMaxInstances must create the run');
        assert.equal(taskManager.getTasks({ name: 'spawn_manual_cap', type: 'async' }).length, 2);
    });

    it('should persist maxInstances across a restart', () => {
        // The cap used to live in memory only, so any tick that ran before the caller
        // re-registered the schedule silently reverted to 1.
        const conn = 'sqlite:/tmp/fibflow_maxinst_persist.sqlite';
        const first = new TaskManager({ dbConnection: conn, poll_interval: 100, cronMode: 'spawn' });
        first.db.setup();
        first.db.clearTasks();
        first.use('spawn_persist', () => 'ok');
        const scheduleId = first.cron('spawn_persist', '0 0 1 1 *', {}, { maxInstances: 3 });
        first.db.close();

        const second = new TaskManager({ dbConnection: conn, poll_interval: 100, cronMode: 'spawn' });
        second.use('spawn_persist', () => 'ok');
        try {
            const row = second.db.getTask(scheduleId);
            assert.equal(
                second._getScheduleMaxInstances(row),
                3,
                'the cap must be read back from storage, not defaulted'
            );

            const [schedule] = second.getCronSchedules({ name: 'spawn_persist' });
            assert.equal(schedule.max_instances, 3, 'the schedule view must report the effective cap');
        } finally {
            second.db.close();
        }
    });

    it('should not rewrite a cancelled schedule when a late run fails', () => {
        // A run can finish after its schedule was cancelled. Writing the failure back
        // would falsify the record and refresh `last_active_time`, postponing the
        // retention of a row that is meant to age out.
        taskManager.use('spawn_late_fail', (task) => {
            if (task.payload && task.payload.slowFail) {
                coroutine.sleep(1500);
                throw new Error('late boom');
            }
            return 'ok';
        });
        taskManager.db.setup();

        const scheduleId = taskManager.cron('spawn_late_fail', '0 0 1 1 *', {});
        taskManager.runCronNow(scheduleId, { slowFail: true }, { ignoreMaxInstances: true });

        // The run must have started before the schedule is cancelled, otherwise it
        // would simply be cancelled while still pending.
        taskManager.start();
        try {
            assert.ok(
                waitFor(() => {
                    const runs = taskManager.getTasks({ name: 'spawn_late_fail', type: 'async' });
                    return runs.some(run => run.status === 'running');
                }, 8000),
                'expected the run to start'
            );

            taskManager.cancelCron({ name: 'spawn_late_fail' });
            const before = taskManager.getTask(scheduleId);
            assert.equal(before.status, 'cancelled');

            assert.ok(
                waitFor(() => {
                    const runs = taskManager.getTasks({ name: 'spawn_late_fail', type: 'async' });
                    return runs.some(run => run.status === 'failed');
                }, 12000),
                'expected the late run to fail'
            );

            const after = taskManager.getTask(scheduleId);
            assert.equal(after.error, before.error, 'a cancelled schedule must keep its recorded reason');
            assert.equal(
                after.last_active_time,
                before.last_active_time,
                'a cancelled schedule must not have its retention clock refreshed'
            );
        } finally {
            taskManager.stop();
        }
    });

    it('should keep maxInstances when a later cron() omits it', () => {
        // `cron()` is an idempotent upsert: several call sites re-register the same
        // schedule without repeating every option. Treating an absent field as
        // "reset to default" silently wiped a non-default cap on every
        // re-registration — and, because the value is persisted, across restarts too.
        taskManager.use('spawn_omit_cap', () => 'ok');
        taskManager.db.setup();

        taskManager.cron('spawn_omit_cap', '0 0 1 1 *', {}, { maxInstances: 7 });
        const [first] = taskManager.getCronSchedules({ name: 'spawn_omit_cap' });
        assert.equal(first.max_instances, 7);

        // Re-register without the option, the way a definition sync would.
        taskManager.cron('spawn_omit_cap', '0 0 1 1 *', {});
        const [second] = taskManager.getCronSchedules({ name: 'spawn_omit_cap' });
        assert.equal(second.max_instances, 7, 'omitting maxInstances must keep the stored value');

        // Explicit reset.
        taskManager.cron('spawn_omit_cap', '0 0 1 1 *', {}, { maxInstances: null });
        const [third] = taskManager.getCronSchedules({ name: 'spawn_omit_cap' });
        assert.equal(third.max_instances, 1, 'an explicit null resets to the default');
    });

    it('should sweep stored maxInstances for schedules that no longer exist', () => {
        taskManager.use('spawn_meta_gc_a', () => 'ok');
        taskManager.use('spawn_meta_gc_b', () => 'ok');
        taskManager.db.setup();

        // Scope the assertions to this test's two schedules: a shared database may
        // hold policy rows from other runs, and the sweep legitimately removes
        // those too.
        const hasMeta = (name) => taskManager.db.pool(conn => conn.execute(
            `SELECT COUNT(*) AS total FROM fib_flow_meta WHERE meta_key LIKE ?`,
            `%${name}%`
        ))[0].total > 0;

        taskManager.cron('spawn_meta_gc_a', '0 0 1 1 *', {}, { maxInstances: 2 });
        taskManager.cron('spawn_meta_gc_b', '0 0 1 1 *', {}, { maxInstances: 3 });
        assert.ok(hasMeta('spawn_meta_gc_a'));
        assert.ok(hasMeta('spawn_meta_gc_b'));

        // Remove one schedule's row outright, as a deletion would.
        taskManager.db.pool(conn => conn.execute(
            `DELETE FROM fib_flow_tasks WHERE type = 'cron' AND name = 'spawn_meta_gc_a'`
        ));

        const pruned = taskManager.db.pruneScheduleMeta();
        assert.ok(pruned >= 1, 'the orphaned policy row must be removed');
        assert.equal(hasMeta('spawn_meta_gc_a'), false, 'the removed schedule loses its policy');
        assert.equal(hasMeta('spawn_meta_gc_b'), true, 'the live schedule keeps its policy');

        const [live] = taskManager.getCronSchedules({ name: 'spawn_meta_gc_b' });
        assert.equal(live.max_instances, 3);
    });

    it('should sweep schedule meta on the automatic retention path', () => {
        // `runRetention()` is optional for an application that relies on the default
        // retention window: most deployments never call it, and the sweep runs from
        // the active timer via `handleTimeoutTasks`. Wiring the meta sweep only into
        // the public entry point would leave it dead in production.
        taskManager.use('spawn_auto_gc', () => 'ok');
        taskManager.db.setup();

        const hasMeta = () => taskManager.db.pool(conn => conn.execute(
            `SELECT COUNT(*) AS total FROM fib_flow_meta WHERE meta_key LIKE ?`,
            '%spawn_auto_gc%'
        ))[0].total > 0;

        taskManager.cron('spawn_auto_gc', '0 0 1 1 *', {}, { maxInstances: 2 });
        assert.ok(hasMeta(), 'registration must persist the policy');

        taskManager.db.pool(conn => conn.execute(
            `DELETE FROM fib_flow_tasks WHERE type = 'cron' AND name = 'spawn_auto_gc'`
        ));

        // Walk the automatic path: consume the policy, then hand it to the sweep that
        // the active timer calls. The throttle is bypassed the same way a real tick
        // does once it expires.
        taskManager.options.retention = { expire_time: 1, statuses: ['completed', 'permanently_failed', 'cancelled'] };
        taskManager.nextRetentionSweepAt = 0;
        const policy = taskManager._consumeAutoRetentionPolicy(Date.now());
        assert.ok(policy, 'the sweep must be armed');

        taskManager.db.handleTimeoutTasks({
            task_heartbeat_interval: 1000,
            task_heartbeat_timeout: 30000
        }, policy);

        assert.equal(hasMeta(), false, 'the automatic sweep must prune metadata for a removed schedule');
    });

    it('should sweep schedule meta even with the global retention window disabled', () => {
        // `retention: { expire_time: null }` is a supported configuration (row-level
        // windows only). The sweep gate used to consider that configuration "no work
        // to do" unless some row carried its own window — which left the metadata
        // growing, reintroducing the very problem the sweep was added to solve.
        const manager = createManager({
            retention: { expire_time: null }
        });
        manager.db.setup();
        manager.db.clearTasks();
        manager.use('spawn_gate_gc', () => 'ok');

        const hasMeta = () => manager.db.pool(conn => conn.execute(
            `SELECT COUNT(*) AS total FROM fib_flow_meta WHERE meta_key LIKE ?`,
            '%spawn_gate_gc%'
        ))[0].total > 0;

        manager.cron('spawn_gate_gc', '0 0 1 1 *', {}, { maxInstances: 2 });
        assert.ok(hasMeta());

        manager.db.pool(conn => conn.execute(
            `DELETE FROM fib_flow_tasks WHERE type = 'cron' AND name = 'spawn_gate_gc'`
        ));

        try {
            assert.equal(
                manager._hasRetentionWork(),
                true,
                'outstanding schedule metadata counts as sweep work'
            );

            manager.nextRetentionSweepAt = 0;
            const policy = manager._consumeAutoRetentionPolicy(Date.now());
            assert.ok(policy, 'the sweep must be armed with the global window disabled');

            manager.db.handleTimeoutTasks({
                task_heartbeat_interval: 1000,
                task_heartbeat_timeout: 30000
            }, policy);

            assert.equal(hasMeta(), false, 'the pass must prune metadata without a global window');
        } finally {
            manager.stop();
        }
    });

    it('should drop an inline-era result when a schedule switches to spawn', () => {
        // Spawn never writes `result` on the schedule. A value is therefore always a
        // leftover from the round that ran the handler in place, and leaving it makes
        // the schedule report a previous execution while the run rows report the
        // current ones.
        const conn = 'sqlite:/tmp/fibflow_switch_modes.sqlite';
        const inline = new TaskManager({ dbConnection: conn, poll_interval: 100 });
        inline.db.setup();
        inline.db.clearTasks();
        inline.use('spawn_switch_job', () => 'INLINE_RESULT');
        const scheduleId = inline.cron('spawn_switch_job', '* * * * * *', {});

        inline.start();
        try {
            assert.ok(
                waitFor(() => inline.getTask(scheduleId).result === 'INLINE_RESULT', 12000),
                'the inline round must write its result on the schedule'
            );
        } finally {
            inline.stop();
        }

        const spawn = new TaskManager({ dbConnection: conn, poll_interval: 100, cronMode: 'spawn' });
        spawn.use('spawn_switch_job', () => 'SPAWN_RESULT');
        spawn.start();
        try {
            assert.ok(
                waitFor(() => spawn.getTasks({ name: 'spawn_switch_job', type: 'async' }).length >= 1, 12000),
                'expected a spawned run'
            );

            const schedule = spawn.getTask(scheduleId);
            assert.equal(
                schedule.result,
                null,
                'the inline-era result must not linger on a spawn-mode schedule'
            );

            const [run] = spawn.getTasks({ name: 'spawn_switch_job', type: 'async' });
            assert.equal(run.result, 'SPAWN_RESULT', 'the run row carries the result');
        } finally {
            spawn.stop();
        }
    });

    it('should leave no trace when a registration is rejected', () => {
        // A rejected `cron()` must not have written anything. Persisting the
        // `maxInstances` policy before validating the expression left an orphan
        // policy row behind, and an orphan row makes `_hasRetentionWork()` true —
        // so the following ticks ran a full retention pass for nothing until the
        // sweep collected it.
        taskManager.use('spawn_reject_trace', () => 'ok');
        taskManager.db.setup();

        const metaCount = () => taskManager.db.pool(conn => conn.execute(
            `SELECT COUNT(*) AS total FROM fib_flow_meta WHERE meta_key LIKE ?`,
            '%spawn_reject_trace%'
        ))[0].total;
        const cronRowCount = () => taskManager.db.pool(conn => conn.execute(
            `SELECT COUNT(*) AS total FROM fib_flow_tasks WHERE type = 'cron' AND name = ?`,
            'spawn_reject_trace'
        ))[0].total;

        const rejections = [
            ['invalid expression', () => taskManager.cron('spawn_reject_trace', 'not a cron', {})],
            ['negative maxInstances', () => taskManager.cron('spawn_reject_trace', '0 0 1 1 *', {}, { maxInstances: -1 })],
            ['blank cronKey', () => taskManager.cron('spawn_reject_trace', '0 0 1 1 *', {}, { cronKey: '   ' })],
            ['invalid timezone', () => taskManager.cron('spawn_reject_trace', '0 0 1 1 *', {}, { timezone: 'Mars/Olympus' })],
            ['bad catchUp', () => taskManager.cron('spawn_reject_trace', '0 0 1 1 *', {}, { catchUp: 'always' })]
        ];

        for (const [label, attempt] of rejections) {
            assert.throws(attempt, `the ${label} case must be rejected`);
            assert.equal(metaCount(), 0, `the ${label} case must not persist a policy row`);
            assert.equal(cronRowCount(), 0, `the ${label} case must not create a schedule row`);
        }

        // And a valid registration still works.
        taskManager.cron('spawn_reject_trace', '0 0 1 1 *', {}, { maxInstances: 2 });
        assert.equal(metaCount(), 1);
        assert.equal(cronRowCount(), 1);
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
