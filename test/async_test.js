const test = require('test');
test.setup();

const coroutine = require('coroutine');
const { TaskManager } = require('..');
const config = require('./config.js');

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
        assert.equal(task.error, 'Task failed');
        assert.equal(attempts, 3);
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

    it('should handle task resume', () => {
        let executionCount = 0;
        taskManager.use('resume_task', (task) => {
            executionCount++;
            if (executionCount === 1) {
                // First execution - pause the task
                taskManager.db.updateTaskStatus(task.id, 'paused');
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
        coroutine.sleep(500);

        const completedTask = taskManager.getTask(taskId);
        assert.equal(completedTask.status, 'completed');
        assert.equal(executionCount, 2);
        assert.deepEqual(completedTask.result, { success: true });
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
