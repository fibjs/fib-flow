const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');

const coroutine = require('coroutine');
const { TaskManager } = require('..');
const config = require('./config.js');

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

describe('Cron Tests', () => {
    let db;
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            dbConnection: config.dbConnection,
            poll_interval: 100,
            max_retries: 2,
            task_heartbeat_interval: 1000,
            max_concurrent_tasks: 3
        });

        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        taskManager.stop();
    });

    it('should validate cron expression', () => {
        taskManager.use('test', () => { });
        taskManager.start();

        assert.doesNotThrow(() => {
            taskManager.cron('test', '*/5 * * * *');
        });

        assert.throws(() => {
            taskManager.cron('test', 'invalid');
        }, /Invalid cron expression/);
    });

    it('should calculate next run time correctly', () => {
        taskManager.use('test', () => { });
        taskManager.start();

        const now = Math.floor(Date.now() / 1000);
        const taskId = taskManager.cron('test', '0 0 * * *'); 
        const task = taskManager.getTask(taskId);

        assert.equal(task.next_run_time > now, true);
        const nextMidnight = new Date();
        nextMidnight.setHours(24, 0, 0, 0);
        assert.equal(task.next_run_time, Math.floor(nextMidnight.getTime() / 1000));
    });

    it('should handle cron task execution and rescheduling', () => {
        let executionCount = 0;
        taskManager.use('test', (task) => {
            executionCount++;
            return 'success';
        });
        taskManager.start();

        const taskId = taskManager.cron('test', '* * * * * *', { data: 'test' });
        
        coroutine.sleep(2500); 

        assert.equal(executionCount >= 2, true);
        assert.equal(executionCount <= 4, true);

        taskManager.pause();
        const settleDeadline = Date.now() + 2000;
        while (Date.now() < settleDeadline) {
            const currentTask = taskManager.getTask(taskId);
            if (currentTask.status === 'pending') {
                break;
            }
            coroutine.sleep(20);
        }

        const task = taskManager.getTask(taskId);
        
        assert.equal(task.id, taskId);
        assert.equal(task.status, 'pending');  
        assert.equal(task.type, 'cron');
        assert.equal(typeof task.next_run_time, 'number');
        assert.equal(task.next_run_time >= Math.floor(Date.now() / 1000) - 1, true);  

        assert.equal(task.result, 'success');

        taskManager.stop();
    });

    it('should record completed attempts for each cron execution round', () => {
        let executionCount = 0;

        taskManager.use('cron_attempt_test', () => {
            executionCount++;
            if (executionCount === 2) {
                taskManager.pause();
            }
            return 'success';
        });

        taskManager.start();

        const taskId = taskManager.cron('cron_attempt_test', '* * * * * *');

        assert.ok(waitFor(() => executionCount >= 2, 5000, 100));

        assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'pending', 5000, 100));

        const taskAttempts = taskManager.getTaskAttempts(taskId);
        assert.equal(taskAttempts.length, 2);
        assert.equal(taskAttempts[0].attempt, 1);
        assert.equal(taskAttempts[0].outcome, 'completed');
        assert.ok(taskAttempts[0].ended_at >= taskAttempts[0].started_at);
        assert.equal(taskAttempts[1].attempt, 2);
        assert.equal(taskAttempts[1].outcome, 'completed');
        assert.ok(taskAttempts[1].ended_at >= taskAttempts[1].started_at);
    });

    it('should handle cron task failure and retry', () => {
        let attempts = 0;
        taskManager.use('test', () => {
            attempts++;
            throw new Error('test error');
        });
        taskManager.start();

        const taskId = taskManager.cron('test', '* * * * * *');
        
        assert.ok(waitFor(() => attempts >= taskManager.options.max_retries, 5000, 100));

        assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'paused', 5000, 100));

        const task = taskManager.getTask(taskId);
        assert.equal(task.retry_count > 0, true);
        assert.equal(task.status, 'paused');
        assert.equal(attempts, taskManager.options.max_retries); 
    });

    it('should respect task priority in cron tasks', () => {
        const executed = [];
        taskManager.use('test', (task) => {
            executed.push(task.payload.id);
        });
        taskManager.start();
        coroutine.sleep(1);

        taskManager.pause();
        // Distinct cronKey: schedules are identified by (name, cronKey), so three
        // same-name registrations must carry different keys to coexist.
        taskManager.cron('test', '* * * * * *', { id: 1 }, { priority: 1, cronKey: 'p1' });
        taskManager.cron('test', '* * * * * *', { id: 2 }, { priority: 2, cronKey: 'p2' });
        taskManager.cron('test', '* * * * * *', { id: 3 }, { priority: 1, cronKey: 'p3' });
        taskManager.resume();

        coroutine.sleep(1500);

        assert.equal(executed[0], 2);
        assert.equal(taskManager.getTasks({ type: 'cron', name: 'test' }).length, 3);
    });

    it('should handle task timeout in cron tasks', () => {
        let executionCount = 0;
        taskManager.use('test', (task) => {
            executionCount++;
            coroutine.sleep(2000); 
            task.checkTimeout();
        });
        
        taskManager.start();  
        
        const taskId = taskManager.cron('test', '* * * * * *', {}, { 
            timeout: 1,
            max_retries: 2,  
            retry_interval: 1 
        });

        let task;
        assert.ok(waitFor(() => {
            task = taskManager.getTask(taskId);
            return task.status === 'timeout';
        }, 5000, 100));
        assert.equal(executionCount, 1);

        assert.ok(waitFor(() => {
            task = taskManager.getTask(taskId);
            return task.retry_count === 1;
        }, 8000, 100));
        assert.equal(task.retry_count, 1);

        assert.ok(waitFor(() => {
            task = taskManager.getTask(taskId);
            return executionCount >= 2 && task.retry_count === 1 && ['timeout', 'paused'].includes(task.status);
        }, 8000, 100));
        assert.equal(executionCount >= 2, true);

        assert.ok(waitFor(() => {
            task = taskManager.getTask(taskId);
            return task.status === 'paused';
        }, 12000, 100));

        assert.equal(task.status, 'paused');
        assert.equal(task.retry_count, 1);
        assert.equal(executionCount, 2);
    });

    it('should resume paused cron task', () => {
        let executionCount = 0;
        taskManager.use('test', (task) => {
            executionCount++;
            coroutine.sleep(2000); 
            task.checkTimeout();
        });
        
        taskManager.start();
        
        const taskId = taskManager.cron('test', '* * * * * *', {}, { 
            timeout: 1,
            max_retries: 2,
            retry_interval: 1
        });

        let task;
        assert.ok(waitFor(() => {
            task = taskManager.getTask(taskId);
            return task.status === 'paused';
        }, 12000, 100));
        
        assert.equal(task.status, 'paused');
        assert.equal(executionCount, 2);

        // Pause first so the resumed task is not immediately claimed
        // (resumeTask now wakes the processing loop without delay).
        taskManager.pause();
        taskManager.resumeTask(taskId);
        const settleDeadline = Date.now() + 2000;
        while (Date.now() < settleDeadline) {
            task = taskManager.getTask(taskId);
            if (task.status === 'pending') break;
            coroutine.sleep(20);
        }
        task = taskManager.getTask(taskId);
        assert.equal(task.status, 'pending');
        assert.equal(task.retry_count, 0);
    });

    describe('idempotent registration', () => {
        it('should upsert by default cronKey and keep a single row', () => {
            taskManager.use('idem', () => 'ok');
            taskManager.start();

            const firstId = taskManager.cron('idem', '0 9 * * *');
            const secondId = taskManager.cron('idem', '0 10 * * *');

            assert.equal(firstId, secondId);

            const rows = taskManager.getTasks({ type: 'cron', name: 'idem' });
            assert.equal(rows.length, 1);
            assert.equal(rows[0].cron_expr, '0 10 * * *');
            assert.equal(rows[0].cron_key, 'default');
        });

        it('should keep same-name schedules with different cronKey independent', () => {
            taskManager.use('idem_multi', () => 'ok');
            taskManager.start();

            const a = taskManager.cron('idem_multi', '0 9 * * *', {}, { cronKey: 'a' });
            const b = taskManager.cron('idem_multi', '0 10 * * *', {}, { cronKey: 'b' });

            assert.notEqual(a, b);
            assert.equal(taskManager.getTasks({ type: 'cron', name: 'idem_multi' }).length, 2);
        });

        it('should not move next_run_time when the expression is unchanged', () => {
            taskManager.use('idem_stable', () => 'ok');
            taskManager.start();

            const taskId = taskManager.cron('idem_stable', '0 9 * * *');
            const first = taskManager.getTask(taskId).next_run_time;

            coroutine.sleep(1100);
            taskManager.cron('idem_stable', '0 9 * * *');
            const second = taskManager.getTask(taskId).next_run_time;

            assert.equal(second, first);
        });

        it('should recompute next_run_time when the expression changes', () => {
            taskManager.use('idem_recalc', () => 'ok');
            taskManager.start();

            const taskId = taskManager.cron('idem_recalc', '0 9 * * *');
            const before = taskManager.getTask(taskId).next_run_time;
            const updated = taskManager.cron('idem_recalc', '0 3 * * *');
            const after = taskManager.getTask(taskId).next_run_time;

            assert.equal(updated, taskId);
            assert.notEqual(after, before);

            const rescheduled = taskManager.getTaskEvents(taskId)
                .filter(event => event.event_type === 'task_cron_rescheduled');
            assert.equal(rescheduled.length, 1);
        });

        it('should revive a paused schedule on re-registration', () => {
            taskManager.use('idem_revive', () => 'ok');
            taskManager.start();

            const taskId = taskManager.cron('idem_revive', '0 9 * * *');
            taskManager.pause();
            taskManager.pauseTask(taskId);
            assert.equal(taskManager.getTask(taskId).status, 'paused');

            taskManager.cron('idem_revive', '0 9 * * *');
            const revived = taskManager.getTask(taskId);

            assert.equal(revived.status, 'pending');
            assert.equal(revived.retry_count, 0);
            assert.equal(revived.error, null);
        });

        it('should reject an empty cronKey', () => {
            taskManager.use('idem_bad_key', () => 'ok');
            taskManager.start();

            assert.throws(() => {
                taskManager.cron('idem_bad_key', '0 9 * * *', {}, { cronKey: '  ' });
            }, /cronKey must be a non-empty string/);
        });

        it('should allow same-name async tasks to coexist', () => {
            taskManager.use('idem_async', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const a = taskManager.async('idem_async', {});
            const b = taskManager.async('idem_async', {});

            assert.notEqual(a, b);
            assert.equal(taskManager.getTasks({ type: 'async', name: 'idem_async' }).length, 2);
        });
    });

    describe('cancellation', () => {
        it('should stop a cancelled schedule from firing', () => {
            let runs = 0;
            taskManager.use('cancel_cron', () => {
                runs += 1;
                return 'ok';
            });
            taskManager.start();

            const taskId = taskManager.cron('cancel_cron', '* * * * * *');
            const summary = taskManager.cancelCron({ name: 'cancel_cron' });

            assert.equal(summary.cancelled, 1);
            assert.deepEqual(summary.taskIds, [taskId]);
            assert.equal(taskManager.getTask(taskId).status, 'cancelled');

            coroutine.sleep(2000);
            assert.equal(runs, 0);
        });

        it('should cancel a pending async task', () => {
            taskManager.use('cancel_async', () => 'ok');
            taskManager.start();
            taskManager.pause();

            const taskId = taskManager.async('cancel_async', {});
            taskManager.cancelTask(taskId, { reason: 'no longer needed' });

            const task = taskManager.getTask(taskId);
            assert.equal(task.status, 'cancelled');
            assert.equal(task.error, 'no longer needed');
        });

        it('should report zero cancellations for an unknown schedule', () => {
            taskManager.use('cancel_none', () => 'ok');
            taskManager.start();

            const summary = taskManager.cancelCron({ name: 'does_not_exist' });
            assert.deepEqual(summary, { cancelled: 0, skipped: 0, taskIds: [] });
        });

        it('should throw for an unknown task id', () => {
            taskManager.use('cancel_missing', () => 'ok');
            taskManager.start();

            assert.throws(() => {
                taskManager.cancelTask(999999);
            }, /Task 999999 not found/);
        });

        it('should refuse to cancel a running task', () => {
            taskManager.use('cancel_running', () => {
                coroutine.sleep(1500);
                return 'ok';
            });
            taskManager.start();

            const taskId = taskManager.async('cancel_running', {});
            assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'running', 5000, 50));

            assert.throws(() => {
                taskManager.cancelTask(taskId);
            }, /Cannot cancel running task/);

            assert.equal(taskManager.getTask(taskId).status, 'running');
        });

        it('should not mark the parent workflow failed when a child is cancelled', () => {
            taskManager.use('cancel_parent', async (task, next) => {
                if (task.stage === 0) {
                    return next([{ name: 'cancel_child' }]);
                }
                return 'ok';
            });
            // The child suspends itself so it stays cancellable while the parent
            // keeps waiting for it.
            taskManager.use('cancel_child', (task) => task.suspend({ reason: 'awaiting_manual_step' }));
            taskManager.start();

            const parentId = taskManager.async('cancel_parent', {});
            assert.ok(waitFor(() => taskManager.getChildTasks(parentId).length === 1, 5000, 50));

            const child = taskManager.getChildTasks(parentId)[0];
            assert.ok(waitFor(() => taskManager.getTask(child.id).status === 'suspended', 5000, 50));

            taskManager.cancelTask(child.id, { reason: 'cancelled by operator' });

            coroutine.sleep(200);
            assert.equal(taskManager.getTask(child.id).status, 'cancelled');
            assert.equal(taskManager.getTask(parentId).status, 'suspended');
        });
    });

    describe('timezone', () => {
        function wallClock(unixSeconds, timeZone) {
            const parts = new Intl.DateTimeFormat('en-US', {
                timeZone,
                hour12: false,
                hour: '2-digit',
                minute: '2-digit'
            }).format(new Date(unixSeconds * 1000));
            return parts;
        }

        it('should evaluate the expression in the schedule time zone', () => {
            taskManager.use('tz_task', () => 'ok');
            taskManager.start();

            const shanghaiId = taskManager.cron('tz_task', '0 9 * * *', {}, { cronKey: 'sh', timezone: 'Asia/Shanghai' });
            const utcId = taskManager.cron('tz_task', '0 9 * * *', {}, { cronKey: 'utc', timezone: 'UTC' });

            const shanghai = taskManager.getTask(shanghaiId);
            const utc = taskManager.getTask(utcId);

            assert.equal(shanghai.cron_tz, 'Asia/Shanghai');
            assert.equal(utc.cron_tz, 'UTC');

            // Both must land on 09:00 wall-clock in their own zone, which is only
            // possible when the two instants are 8 hours apart.
            assert.equal(wallClock(shanghai.next_run_time, 'Asia/Shanghai'), '09:00');
            assert.equal(wallClock(utc.next_run_time, 'UTC'), '09:00');
            assert.equal(Math.abs(utc.next_run_time - shanghai.next_run_time) % 86400 % (8 * 3600), 0);
            assert.notEqual(utc.next_run_time, shanghai.next_run_time);
        });

        it('should reject an invalid time zone', () => {
            taskManager.use('tz_bad', () => 'ok');
            taskManager.start();

            assert.throws(() => {
                taskManager.cron('tz_bad', '0 9 * * *', {}, { timezone: 'Not/AZone' });
            }, /Invalid timezone/);
        });

        it('should keep local-time behaviour when no time zone is given', () => {
            taskManager.use('tz_local', () => 'ok');
            taskManager.start();

            const taskId = taskManager.cron('tz_local', '0 9 * * *');
            const task = taskManager.getTask(taskId);
            const localZone = new Intl.DateTimeFormat().resolvedOptions().timeZone;

            assert.equal(task.cron_tz, null);
            assert.equal(wallClock(task.next_run_time, localZone), '09:00');
            assert.ok(task.next_run_time > Math.floor(Date.now() / 1000));
        });
    });

    describe('catch-up policy', () => {
        it('should skip a missed window when catchUp is skip', () => {
            let runs = 0;
            taskManager.use('catchup_skip', () => {
                runs += 1;
                return 'ok';
            });
            taskManager.start();

            const taskId = taskManager.cron('catchup_skip', '0 9 * * *', {}, { catchUp: 'skip' });
            taskManager.pause();

            const now = Math.floor(Date.now() / 1000);
            taskManager.db.updateCronSchedule(taskId, { next_run_time: now - 3 * 86400 });

            taskManager.resume();
            assert.ok(waitFor(() => {
                const task = taskManager.getTask(taskId);
                return task.status === 'pending' && task.next_run_time > now;
            }, 5000, 50));

            coroutine.sleep(300);
            assert.equal(runs, 0);

            const missed = taskManager.getTaskEvents(taskId)
                .filter(event => event.event_type === 'task_cron_missed');
            assert.equal(missed.length, 1);
            assert.ok(missed[0].metadata.skipped_windows >= 1);
        });

        it('should still run when the delay is shorter than one period', () => {
            let runs = 0;
            taskManager.use('catchup_jitter', () => {
                runs += 1;
                return 'ok';
            });
            taskManager.start();

            const taskId = taskManager.cron('catchup_jitter', '0 9 * * *', {}, { catchUp: 'skip' });
            taskManager.pause();

            const now = Math.floor(Date.now() / 1000);
            // Two seconds late is well inside a daily period: not a missed window.
            taskManager.db.updateCronSchedule(taskId, { next_run_time: now - 2 });

            taskManager.resume();
            assert.ok(waitFor(() => runs >= 1, 5000, 50));
        });

        it('should run a missed window once when catchUp is once (default)', () => {
            let runs = 0;
            taskManager.use('catchup_once', () => {
                runs += 1;
                return 'ok';
            });
            taskManager.start();

            const taskId = taskManager.cron('catchup_once', '0 9 * * *');
            taskManager.pause();

            const now = Math.floor(Date.now() / 1000);
            taskManager.db.updateCronSchedule(taskId, { next_run_time: now - 3 * 86400 });

            taskManager.resume();
            assert.ok(waitFor(() => runs >= 1, 5000, 50));
            assert.equal(taskManager.getTask(taskId).cron_catch_up, 'once');
        });
    });

    describe('orphan schedules', () => {
        it('should flag schedules whose handler is gone', () => {
            taskManager.use('orphan_task', () => 'ok');
            taskManager.start();

            taskManager.cron('orphan_task', '0 9 * * *');
            taskManager.pause();
            taskManager.unuse('orphan_task');

            const schedules = taskManager.getCronSchedules({ name: 'orphan_task' });
            assert.equal(schedules.length, 1);
            assert.equal(schedules[0].orphan, true);

            const stats = taskManager.getCronScheduleStats();
            assert.equal(stats.total, 1);
            assert.equal(stats.orphan, 1);
        });

        it('should cancel orphan schedules and leave healthy ones alone', () => {
            taskManager.use('orphan_prune', () => 'ok');
            taskManager.use('orphan_keep', () => 'ok');
            taskManager.start();

            const orphanId = taskManager.cron('orphan_prune', '0 9 * * *');
            const keptId = taskManager.cron('orphan_keep', '0 9 * * *');
            taskManager.pause();
            taskManager.unuse('orphan_prune');

            const result = taskManager.pruneCronSchedules();

            assert.equal(result.cancelled, 1);
            assert.deepEqual(result.taskIds, [orphanId]);
            assert.equal(taskManager.getTask(orphanId).status, 'cancelled');
            assert.equal(taskManager.getTask(keptId).status, 'pending');
        });

        it('should not resurrect a pruned schedule on handler re-registration alone', () => {
            taskManager.use('orphan_revive', () => 'ok');
            taskManager.start();

            const taskId = taskManager.cron('orphan_revive', '0 9 * * *');
            taskManager.pause();
            taskManager.unuse('orphan_revive');

            taskManager.pruneCronSchedules();
            taskManager.use('orphan_revive', () => 'ok');
            taskManager.resume();

            coroutine.sleep(300);
            assert.equal(taskManager.getTask(taskId).status, 'cancelled');
        });
    });
});
