const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');

const coroutine = require('coroutine');
const { TaskManager } = require('..');

const TEST_DB_CONNECTION = 'sqlite::memory:';

function waitForStatus(taskManager, taskId, status, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const task = taskManager.getTask(taskId);
        if (task && task.status === status) {
            return task;
        }
        coroutine.sleep(50);
    }
    const current = taskManager.getTask(taskId);
    throw new Error(`Task ${taskId} did not reach status '${status}' within ${timeoutMs}ms (current: ${current ? current.status : 'missing'})`);
}

function waitForChildren(taskManager, parentId, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const children = taskManager.getChildTasks(parentId);
        if (children.length > 0) {
            return children;
        }
        coroutine.sleep(50);
    }
    throw new Error(`No children created for task ${parentId} within ${timeoutMs}ms`);
}

describe('Explicit Suspension (human-in-the-loop)', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            dbConnection: TEST_DB_CONNECTION,
            poll_interval: 50,
            max_concurrent_tasks: 3,
            task_heartbeat_interval: 50,
            task_heartbeat_timeout: 200
        });

        taskManager.db.setup();
        taskManager.start();
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
        }
    });

    it('should suspend task when handler returns task.suspend()', () => {
        let executions = 0;

        taskManager.use('approvalFlow', async (task) => {
            executions++;
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return { approved: true };
        });

        const taskId = taskManager.async('approvalFlow', { amount: 100 });
        waitForStatus(taskManager, taskId, 'suspended');

        assert.equal(executions, 1, 'handler should run exactly once before suspending');

        const events = taskManager.getTaskEvents(taskId);
        const suspendEvent = events.find(event => event.event_type === 'task_suspended');
        assert.ok(suspendEvent, 'task_suspended event should be recorded');
        assert.equal(suspendEvent.from_status, 'running');
        assert.equal(suspendEvent.to_status, 'suspended');
        assert.equal(suspendEvent.metadata.suspend_reason, 'awaiting_approval');

        const attempts = taskManager.db.getTaskAttempts(taskId);
        assert.equal(attempts[attempts.length - 1].outcome, 'suspended', 'attempt should close with suspended outcome');
    });

    it('should resume and re-run handler with advanced stage', () => {
        let executions = 0;

        taskManager.use('approvalFlow', async (task) => {
            executions++;
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return { continued: true };
        });

        const taskId = taskManager.async('approvalFlow', { amount: 100 });
        waitForStatus(taskManager, taskId, 'suspended');

        taskManager.resumeTask(taskId, { resume_reason: 'approved_by_ops' });

        const completed = waitForStatus(taskManager, taskId, 'completed');

        assert.equal(executions, 2, 'handler should re-run after resume');
        assert.equal(completed.result.continued, true, 'handler should continue on the resumed run');
        assert.equal(completed.stage, 1, 'stage should advance on resume instead of resetting');

        const events = taskManager.getTaskEvents(taskId);
        const resumeEvent = events.find(event => event.event_type === 'task_resumed');
        assert.ok(resumeEvent, 'task_resumed event should be recorded');
        assert.equal(resumeEvent.from_status, 'suspended');
        assert.equal(resumeEvent.to_status, 'pending');
        assert.equal(resumeEvent.metadata.resume_reason, 'approved_by_ops');
    });

    it('should cancel suspended task to permanently_failed', () => {
        taskManager.use('approvalFlow', async (task) => {
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return { approved: true };
        });

        const taskId = taskManager.async('approvalFlow', { amount: 100 });
        waitForStatus(taskManager, taskId, 'suspended');

        taskManager.cancelTask(taskId, { reason: 'request abandoned' });
        const cancelled = waitForStatus(taskManager, taskId, 'permanently_failed');

        assert.equal(cancelled.error, 'request abandoned');

        const events = taskManager.getTaskEvents(taskId);
        assert.ok(events.some(event =>
            event.event_type === 'task_permanently_failed' && event.from_status === 'suspended'
        ));
    });

    it('should list suspended tasks filtered by suspend_reason', () => {
        taskManager.use('approvalFlow', async (task) => {
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return { approved: true };
        });

        taskManager.use('inputFlow', async (task) => {
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_input' });
            }
            return { done: true };
        });

        const approvalId = taskManager.async('approvalFlow', {});
        const inputId = taskManager.async('inputFlow', {});
        waitForStatus(taskManager, approvalId, 'suspended');
        waitForStatus(taskManager, inputId, 'suspended');

        const approvals = taskManager.getTasksByStatus('suspended', { suspend_reason: 'awaiting_approval' });
        assert.equal(approvals.length, 1);
        assert.equal(approvals[0].id, approvalId);

        const all = taskManager.getTasksByStatus('suspended');
        assert.equal(all.length, 2);
    });

    it('should stay suspended until explicitly resumed (immune to heartbeat timeout)', () => {
        taskManager.use('approvalFlow', async (task) => {
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return { approved: true };
        });

        taskManager.use('sideTask', async () => ({ ok: true }));

        const taskId = taskManager.async('approvalFlow', {});
        waitForStatus(taskManager, taskId, 'suspended');

        // Complete unrelated tasks - explicit suspension must not be touched
        const sideId = taskManager.async('sideTask', {});
        waitForStatus(taskManager, sideId, 'completed');

        // Longer than the configured heartbeat timeout window
        coroutine.sleep(600);

        const task = taskManager.getTask(taskId);
        assert.equal(task.status, 'suspended', 'suspended task should not be timed out');
    });

    it('should persist binary context snapshot through suspension and resume', () => {
        const snapshot = Buffer.from([1, 2, 3, 4]);
        let resumedContext;

        taskManager.use('snapshotFlow', async (task) => {
            if (task.stage === 0) {
                return task.suspend({
                    reason: 'awaiting_approval',
                    context: snapshot
                });
            }
            resumedContext = task.context;
            return {
                restored: resumedContext && resumedContext.toString('hex') === snapshot.toString('hex')
            };
        });

        const taskId = taskManager.async('snapshotFlow', {});
        const suspended = waitForStatus(taskManager, taskId, 'suspended');
        assert.equal(
            suspended.context.toString('hex'),
            snapshot.toString('hex'),
            'context should be persisted when suspending'
        );

        taskManager.resumeTask(taskId);
        const completed = waitForStatus(taskManager, taskId, 'completed');

        assert.equal(completed.result.restored, true, 'handler should read the same context after resume');
        assert.equal(resumedContext.toString('hex'), snapshot.toString('hex'));
    });

    it('should fail task when suspend() is called without a reason', () => {
        taskManager.use('badFlow', async (task) => {
            return task.suspend({});
        });

        const taskId = taskManager.async('badFlow', {});
        const failed = waitForStatus(taskManager, taskId, 'failed');

        assert.ok(failed.error.includes('Suspension requires a non-empty reason'));
    });

    it('should integrate with parent-child workflows', () => {
        taskManager.use('parentFlow', (task, next) => {
            if (task.stage === 0) {
                return next([{ name: 'approvalChild', payload: {} }]);
            }
            return { parent_done: true };
        });

        taskManager.use('approvalChild', async (task) => {
            if (task.stage === 0) {
                return task.suspend({ reason: 'awaiting_approval' });
            }
            return { approved: true };
        });

        const parentId = taskManager.async('parentFlow', {});
        const children = waitForChildren(taskManager, parentId);
        const childId = children[0].id;

        waitForStatus(taskManager, childId, 'suspended');

        // Parent stays suspended while the child waits for human interaction
        assert.equal(taskManager.getTask(parentId).status, 'suspended');

        taskManager.resumeTask(childId);
        waitForStatus(taskManager, childId, 'completed');
        waitForStatus(taskManager, parentId, 'completed');

        assert.equal(taskManager.getTask(parentId).result.parent_done, true);
    });

    it('should support legacy pause and resume', () => {
        taskManager.use('pausable', async (task) => {
            coroutine.sleep(200);
            return { done: true };
        });

        const taskId = taskManager.async('pausable', {});
        waitForStatus(taskManager, taskId, 'running');

        taskManager.pauseTask(taskId);
        waitForStatus(taskManager, taskId, 'paused');

        taskManager.resumeTask(taskId);
        const completed = waitForStatus(taskManager, taskId, 'completed');
        assert.equal(completed.result.done, true);
    });
});
