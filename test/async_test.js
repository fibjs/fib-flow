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

describe('Async Tasks', () => {
    let db;
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            dbConnection: config.dbConnection,
            poll_interval: 100,
            max_retries: 2,
            max_concurrent_tasks: 3
        });

        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        taskManager.stop();
    });

    it('should handle async tasks with success', () => {
        let taskExecuted = false;
        let taskPayload = null;
        const payload = { data: 'test_data' };

        taskManager.use('test_task', (task) => {
            taskExecuted = true;
            taskPayload = task.payload;
            return { success: true };
        });

        taskManager.start();

        const taskId = taskManager.async('test_task', payload);
        assert.ok(taskId > 0);

        coroutine.sleep(1000);

        assert.ok(taskExecuted);
        assert.deepEqual(taskPayload, payload);

        const task = taskManager.getTask(taskId);
        assert.equal(task.status, 'completed');
        assert.deepEqual(task.result, { success: true });
    });

    it('should recover from a transient claimTask failure in the processing loop', () => {
        let taskExecuted = false;
        let shouldFailClaim = true;
        const originalClaimTask = taskManager.db.claimTask.bind(taskManager.db);

        taskManager.db.claimTask = function (...args) {
            if (shouldFailClaim) {
                shouldFailClaim = false;
                throw new Error('temporary claimTask failure');
            }

            return originalClaimTask(...args);
        };

        taskManager.use('claim_retry_task', () => {
            taskExecuted = true;
            return { success: true };
        });

        taskManager.start();

        const taskId = taskManager.async('claim_retry_task');
        assert.ok(waitFor(() => {
            const task = taskManager.getTask(taskId);
            return taskExecuted && task && task.status === 'completed';
        }, 5000));

        const task = taskManager.getTask(taskId);
        assert.equal(task.status, 'completed');
        assert.deepEqual(task.result, { success: true });
    });

    it('should handle async task failure and retries', () => {
        let attempts = 0;

        taskManager.use('failing_task', (task) => {
            attempts++;
            throw new Error('Task failed');
        });

        taskManager.start();

        const taskId = taskManager.async('failing_task', {}, { max_retries: 2 });

        coroutine.sleep(5000);

        const task = taskManager.getTask(taskId);
        assert.equal(task.status, 'permanently_failed');
        assert.equal(task.error.split('\n')[0], 'Error: Task failed');
        assert.equal(attempts, 2);
    });

    it('should handle task timeout through periodic check', () => {
        let taskStarted = false;
        taskManager.use('timeout_task', (task) => {
            taskStarted = true;
            coroutine.sleep(3000);
            return { success: true };
        });

        taskManager.start();

        const taskId = taskManager.async('timeout_task', {}, { timeout: 1, max_retries: 0 });
        assert.ok(taskId > 0);

        coroutine.sleep(5000);

        assert.ok(taskStarted);
        const task = taskManager.getTask(taskId);
        assert.equal(task.status, 'permanently_failed');
    });

    it('should record handler checkpoints and expose paged task audit views', () => {
        let handlerStatus;
        let handlerWorkerId;
        let handlerAttempt;

        taskManager.use('audited_task', (task) => {
            handlerStatus = task.status;
            handlerWorkerId = task.worker_id;
            handlerAttempt = task.attempt;

            const checkpointId1 = task.audit('payload_validated', {
                message: 'Payload validated',
                metadata: { step: 1 }
            });
            const checkpointId2 = task.audit({
                code: 'remote_call_started',
                message: 'Remote call started',
                metadata: { provider: 'demo' }
            });

            assert.ok(checkpointId1 > 0);
            assert.ok(checkpointId2 > 0);

            return { success: true };
        });

        taskManager.start();

        const taskId = taskManager.async('audited_task', { hello: 'world' });
        coroutine.sleep(1000);

        assert.equal(handlerStatus, 'running');
        assert.ok(handlerWorkerId);
        assert.equal(handlerAttempt, 1);

        const checkpointEvents = taskManager.getTaskEvents(taskId, {
            event_type: 'task_checkpoint'
        });
        assert.equal(checkpointEvents.length, 2);
        assert.equal(checkpointEvents[0].attempt, 1);
        assert.equal(checkpointEvents[0].metadata.code, 'payload_validated');
        assert.equal(checkpointEvents[1].metadata.code, 'remote_call_started');

        const eventPage = taskManager.queryTaskEvents(taskId, {
            event_type: 'task_checkpoint',
            limit: 1,
            offset: 0
        });
        assert.equal(eventPage.total, 2);
        assert.equal(eventPage.items.length, 1);
        assert.equal(eventPage.has_more, true);

        const attemptPage = taskManager.queryTaskAttempts(taskId, {
            limit: 5,
            offset: 0
        });
        assert.equal(attemptPage.total, 1);
        assert.equal(attemptPage.items.length, 1);
        assert.equal(attemptPage.items[0].outcome, 'completed');

        const taskAudit = taskManager.getTaskAudit(taskId, {
            events: {
                event_type: 'task_checkpoint',
                limit: 10
            },
            attempts: {
                limit: 10
            }
        });
        assert.equal(taskAudit.task.id, taskId);
        assert.equal(taskAudit.events.total, 2);
        assert.equal(taskAudit.attempts.total, 1);
    });

    it('should persist task progress snapshots and progress events', () => {
        taskManager.use('progress_task', (task) => {
            const progressId1 = task.progress('Downloading input', {
                stage_name: 'download',
                progress_percent: 20,
                metadata: { chunk: 1 }
            });

            const progressId2 = task.progress({
                stage_name: 'transform',
                progress_text: 'Transforming records',
                progress_percent: 75,
                message: 'Transform stage running',
                metadata: { transformed: 15 }
            });

            assert.ok(progressId1 > 0);
            assert.ok(progressId2 > 0);
            return { ok: true };
        });

        taskManager.start();

        const taskId = taskManager.async('progress_task', { value: 1 });
        coroutine.sleep(1000);

        const task = taskManager.getTask(taskId);
        assert.equal(task.current_stage_name, 'transform');
        assert.equal(task.progress_text, 'Transforming records');
        assert.equal(task.progress_percent, 75);
        assert.equal(task.last_event_type, 'task_completed');
        assert.ok(task.last_event_time >= task.created_at);

        const progressEvents = taskManager.getTaskEvents(taskId, {
            event_type: 'task_progress'
        });
        assert.equal(progressEvents.length, 2);
        assert.equal(progressEvents[0].metadata.stage_name, 'download');
        assert.equal(progressEvents[0].metadata.progress_percent, 20);
        assert.equal(progressEvents[1].metadata.stage_name, 'transform');
        assert.equal(progressEvents[1].metadata.transformed, 15);
    });

    it('should normalize and validate handler audit naming inputs', () => {
        taskManager.use('naming_task', (task) => {
            const checkpointId = task.audit('  payload_validated  ', {
                message: '  Payload validated  ',
                metadata: { step: 1 }
            });
            const progressId = task.progress('  Downloading input  ', {
                stage_name: '  download_phase  ',
                message: '  downloading  '
            });

            assert.ok(checkpointId > 0);
            assert.ok(progressId > 0);

            assert.throws(() => {
                task.audit('Invalid Code');
            }, /snake_case/);

            assert.throws(() => {
                task.progress({ stage_name: 'Invalid Stage' });
            }, /snake_case/);

            assert.throws(() => {
                task.progress({ progress_text: '   ' });
            }, /must not be empty/);

            return { ok: true };
        });

        taskManager.start();

        const taskId = taskManager.async('naming_task', { value: 1 });
        coroutine.sleep(1000);

        const checkpointEvents = taskManager.getTaskEvents(taskId, {
            event_type: 'task_checkpoint'
        });
        assert.equal(checkpointEvents[0].metadata.code, 'payload_validated');
        assert.equal(checkpointEvents[0].message, 'Payload validated');

        const progressEvents = taskManager.getTaskEvents(taskId, {
            event_type: 'task_progress'
        });
        assert.equal(progressEvents[0].metadata.stage_name, 'download_phase');
        assert.equal(progressEvents[0].metadata.progress_text, 'Downloading input');
        assert.equal(progressEvents[0].message, 'downloading');
    });

    it('should handle task resume', () => {
        let executionCount = 0;
        taskManager.use('resume_task', (task) => {
            executionCount++;
            if (executionCount === 1) {
                // First execution - pause the task
                taskManager.pauseTask(task.id);
                return;
            }
            // Second execution after resume
            return { success: true };
        });

        taskManager.start();

        const taskId = taskManager.async('resume_task');
        assert.ok(taskId > 0);

        coroutine.sleep(500);
        const pausedTask = taskManager.getTask(taskId);
        assert.equal(pausedTask.status, 'paused');
        assert.equal(executionCount, 1);

        // Resume the task
        taskManager.resumeTask(taskId);
        assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'completed', 5000, 50));

        const completedTask = taskManager.getTask(taskId);
        assert.equal(completedTask.status, 'completed');
        assert.equal(executionCount, 2);
        assert.deepEqual(completedTask.result, { success: true });

        const taskAttempts = taskManager.getTaskAttempts(taskId);
        assert.equal(taskAttempts.length, 2);
        assert.equal(taskAttempts[0].attempt, 1);
        assert.equal(taskAttempts[0].outcome, 'paused');
        assert.ok(taskAttempts[0].ended_at >= taskAttempts[0].started_at);
        assert.equal(taskAttempts[1].attempt, 2);
        assert.equal(taskAttempts[1].outcome, 'completed');
        assert.ok(taskAttempts[1].ended_at >= taskAttempts[1].started_at);
    });

    it('should use the latest handler after a paused task is resumed', () => {
        let executionCount = 0;

        taskManager.use('resume_with_update_task', (task) => {
            executionCount++;
            if (executionCount === 1) {
                taskManager.pauseTask(task.id);
                return;
            }

            return { version: 'old' };
        });

        taskManager.start();

        const taskId = taskManager.async('resume_with_update_task');
        coroutine.sleep(500);

        const pausedTask = taskManager.getTask(taskId);
        assert.equal(pausedTask.status, 'paused');
        assert.equal(executionCount, 1);

        taskManager.use('resume_with_update_task', () => {
            executionCount++;
            return { version: 'new' };
        });

        taskManager.resumeTask(taskId);
        assert.ok(waitFor(() => taskManager.getTask(taskId).status === 'completed', 5000, 50));

        const completedTask = taskManager.getTask(taskId);
        assert.equal(completedTask.status, 'completed');
        assert.equal(executionCount, 2);
        assert.deepEqual(completedTask.result, { version: 'new' });
    });

    it('should emit pause and generic resume audit events for manually resumed tasks', () => {
        let executionCount = 0;
        taskManager.use('resume_event_task', (task) => {
            executionCount++;
            if (executionCount === 1) {
                taskManager.pauseTask(task.id);
                return;
            }

            return { ok: true };
        });

        taskManager.start();

        const taskId = taskManager.async('resume_event_task');
        coroutine.sleep(500);

        taskManager.resumeTask(taskId);
        coroutine.sleep(1000);

        const events = taskManager.getTaskEvents(taskId);
        assert.ok(events.some(event =>
            event.event_type === 'task_paused'
            && event.from_status === 'running'
            && event.to_status === 'paused'
        ));
        assert.ok(events.some(event =>
            event.event_type === 'task_status_changed'
            && event.from_status === 'paused'
            && event.to_status === 'pending'
        ));
        assert.ok(!events.some(event => event.event_type === 'task_resumed'));
    });

    it('should reset retry count when resuming task', () => {
        let executionCount = 0;
        taskManager.use('retry_resume_task', (task) => {
            executionCount++;
            if (executionCount === 1) {
                // First execution - pause the task
                taskManager.db.updateTaskStatus(task.id, 'paused', { retry_count: 2 });
                return;
            }
            // Second execution after resume
            return { success: true };
        });

        taskManager.start();

        const taskId = taskManager.async('retry_resume_task', {}, { max_retries: 2 });
        assert.ok(taskId > 0);

        coroutine.sleep(1000);
        const pausedTask = taskManager.getTask(taskId);
        assert.equal(pausedTask.status, 'paused');
        assert.equal(pausedTask.retry_count, 2);

        // Resume the task - should reset retry count
        taskManager.resumeTask(taskId);
        coroutine.sleep(1000);

        const completedTask = taskManager.getTask(taskId);
        assert.equal(completedTask.status, 'completed');
        assert.equal(completedTask.retry_count, 0);
        assert.deepEqual(completedTask.result, { success: true });
    });

    it('should handle concurrent tasks', () => {
        const results = [];
        const maxTasks = 3;
        let completed = 0;

        taskManager.use('concurrent_task', (task) => {
            results.push(task.id);
            coroutine.sleep(500);
            completed++;
            return { taskId: task.id };
        });

        taskManager.start();
        coroutine.sleep(1);

        const taskIds = [];
        for (let i = 0; i < maxTasks; i++) {
            taskIds.push(taskManager.async('concurrent_task', { index: i }));
        }

        while (completed < maxTasks) {
            coroutine.sleep(100);
        }
        coroutine.sleep(500);

        assert.equal(results.length, maxTasks);

        for (const taskId of taskIds) {
            const task = taskManager.getTask(taskId);
            assert.equal(task.status, 'completed');
        }
    });

    it('should handle task priority', () => {
        const executionOrder = [];
        let completed = 0;

        taskManager.use('priority_task', (task) => {
            executionOrder.push(task.id);
            completed++;
            return { priority: task.priority };
        });

        taskManager.start();
        coroutine.sleep(100);

        taskManager.pause();
        const lowPriorityId = taskManager.async('priority_task', {}, { priority: 0, max_retries: 0 });
        const highPriorityId = taskManager.async('priority_task', {}, { priority: 10, max_retries: 0 });
        const mediumPriorityId = taskManager.async('priority_task', {}, { priority: 5, max_retries: 0 });
        taskManager.resume();

        while (completed < 3) {
            coroutine.sleep(100);
        }
        coroutine.sleep(1000);

        assert.equal(executionOrder[0], highPriorityId);
        assert.equal(executionOrder[1], mediumPriorityId);
        assert.equal(executionOrder[2], lowPriorityId);
    });

    it('should order tasks by delay and priority', () => {
        const executed = [];
        const now = Math.floor(Date.now() / 1000);

        taskManager.use('test', async (task) => {
            executed.push(task.payload.id);
        });

        taskManager.start();
        coroutine.sleep(100);

        taskManager.pause();
        taskManager.async('test', { id: 1 }, { priority: 1, delay: 5 });
        taskManager.async('test', { id: 2 }, { priority: 2, delay: 3 });
        taskManager.async('test', { id: 3 }, { priority: 1, delay: 3 });
        taskManager.resume();

        while (executed.length < 3) {
            coroutine.sleep(100);
        }

        assert.deepEqual(executed, [2, 3, 1]);
    });

    it('should retrieve a task by ID', () => {
        const payload = { data: 'test_data' };

        taskManager.use('test_task', (task) => { });

        taskManager.start();

        const taskId = taskManager.async('test_task', payload);

        const task = taskManager.getTask(taskId);

        assert.ok(task);
        assert.equal(task.id, taskId);
        assert.equal(task.payload.data, payload.data);
    });

    it('should retrieve tasks by name', () => {
        const payload1 = { data: 'test_data_1' };
        const payload2 = { data: 'test_data_2' };

        taskManager.use('test_task', (task) => { });

        taskManager.start();

        taskManager.async('test_task', payload1);
        taskManager.async('test_task', payload2);

        const tasks = taskManager.getTasksByName('test_task');

        assert.ok(tasks.length >= 2);
        assert.ok(tasks.some(task => task.payload.data === 'test_data_1'));
        assert.ok(tasks.some(task => task.payload.data === 'test_data_2'));
    });

    it('should retrieve tasks by status', () => {
        const payload1 = { data: 'test_data_1' };
        const payload2 = { data: 'test_data_2' };

        taskManager.use('test_task_1', (task) => { });
        taskManager.use('test_task_2', (task) => { });

        taskManager.start();

        taskManager.async('test_task_1', payload1);
        taskManager.async('test_task_2', payload2);

        coroutine.sleep(1000);

        const tasks = taskManager.getTasksByStatus('completed');

        assert.ok(tasks.length >= 2);
        assert.ok(tasks.some(task => task.payload.data === 'test_data_1'));
        assert.ok(tasks.some(task => task.payload.data === 'test_data_2'));
    });

    it('should retrieve tasks by name', () => {
        const payload1 = { data: 'test_data_1' };
        const payload2 = { data: 'test_data_2' };

        taskManager.use('test_task', (task) => { });

        taskManager.start();

        taskManager.async('test_task', payload1);
        taskManager.async('test_task', payload2);

        const tasks = taskManager.getTasksByName('test_task');

        assert.ok(tasks.length >= 2);
        assert.ok(tasks.some(task => task.payload.data === 'test_data_1'));
        assert.ok(tasks.some(task => task.payload.data === 'test_data_2'));
    });
});
