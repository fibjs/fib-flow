const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const coroutine = require('coroutine');

const { TaskManager } = require('..');
const config = require('./config.js');

const rootDir = path.join(__dirname, '..');

function waitFor(predicate, timeoutMs = 5000, intervalMs = 50) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (predicate()) {
            return true;
        }
        coroutine.sleep(intervalMs);
    }

    return predicate();
}

describe('Type declarations', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            dbConnection: config.dbConnection,
            poll_interval: 100
        });
        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
        }
    });

    it('should ship a declaration entry referenced by package.json', () => {
        const pkg = JSON.parse(fs.readFileSync(path.join(rootDir, 'package.json'), 'utf8'));
        assert.equal(pkg.types, 'index.d.ts');
        assert.ok(fs.existsSync(path.join(rootDir, pkg.types)), 'types entry must exist');
    });

    it('should declare every runtime export', () => {
        const runtimeExports = Object.keys(require('..'));
        const declarations = [
            fs.readFileSync(path.join(rootDir, 'index.d.ts'), 'utf8'),
            fs.readFileSync(path.join(rootDir, 'lib/types/core.d.ts'), 'utf8'),
            fs.readFileSync(path.join(rootDir, 'lib/types/task-manager.d.ts'), 'utf8')
        ].join('\n');

        // Runtime exports are TaskManager, Suspension plus the adapter factory.
        for (const name of runtimeExports) {
            assert.ok(
                new RegExp(`\\b${name}\\b`).test(declarations),
                `export "${name}" is missing from the type declarations`
            );
        }
    });

    it('should declare a TaskStatus union matching the runtime status list', () => {
        const declarations = fs.readFileSync(path.join(rootDir, 'lib/types/core.d.ts'), 'utf8');
        const expected = [
            'pending', 'running', 'completed', 'failed',
            'timeout', 'permanently_failed', 'paused', 'suspended', 'cancelled'
        ];

        for (const status of expected) {
            assert.ok(
                declarations.includes(`'${status}'`),
                `status "${status}" is missing from the TaskStatus declaration`
            );
        }
    });

    it('should return the updated task from resumeTask / pauseTask / cancelTask', () => {
        taskManager.use('decl_probe', (task) => task.suspend({ reason: 'awaiting_approval' }));
        taskManager.start();

        const taskId = taskManager.async('decl_probe', {});
        assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'suspended'));

        // Pause first: resumeTask() wakes the processing loop immediately, so the
        // returned snapshot would otherwise be the freshly claimed `running` row.
        taskManager.pause();

        // Declared as `TaskRecord | null` — the runtime must hand back the row,
        // not the adapter's internal update result.
        const resumed = taskManager.resumeTask(taskId, { resume_reason: 'approved' });
        assert.ok(resumed, 'resumeTask must return the updated task');
        assert.equal(resumed.id, taskId);
        assert.equal(resumed.status, 'pending');
        assert.equal(resumed.retry_count, 0);

        const paused = taskManager.pauseTask(taskId);
        assert.ok(paused, 'pauseTask must return the updated task');
        assert.equal(paused.status, 'paused');

        const cancelled = taskManager.cancelTask(taskId, { reason: 'withdrawn' });
        assert.ok(cancelled, 'cancelTask must return the updated task');
        assert.equal(cancelled.status, 'cancelled');
        assert.equal(cancelled.error, 'withdrawn');
    });

    it('should return the declared schedule and stats shapes', () => {
        taskManager.use('decl_cron', () => 'ok');
        taskManager.start();

        taskManager.cron('decl_cron', '0 9 * * *', {}, {
            cronKey: 'decl',
            timezone: 'Asia/Shanghai',
            catchUp: 'skip',
            retention_expire_time: 3600,
            tag: 'decl_tag'
        });

        const schedules = taskManager.getCronSchedules({ name: 'decl_cron' });
        assert.equal(schedules.length, 1);

        const schedule = schedules[0];
        assert.equal(schedule.name, 'decl_cron');
        assert.equal(schedule.cron_key, 'decl');
        assert.equal(schedule.cron_tz, 'Asia/Shanghai');
        assert.equal(schedule.cron_catch_up, 'skip');
        assert.equal(schedule.retention_expire_time, 3600);
        assert.equal(typeof schedule.next_run_time, 'number');
        assert.equal(typeof schedule.retry_count, 'number');
        assert.equal(schedule.orphan, false);

        const stats = taskManager.getCronScheduleStats();
        assert.equal(typeof stats.total, 'number');
        assert.equal(typeof stats.active, 'number');
        assert.equal(typeof stats.paused, 'number');
        assert.equal(typeof stats.cancelled, 'number');
        assert.equal(typeof stats.orphan, 'number');
        assert.equal(stats.total, 1);
    });
});
