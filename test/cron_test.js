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
        taskManager.cron('test', '* * * * * *', { id: 1 }, { priority: 1 });
        taskManager.cron('test', '* * * * * *', { id: 2 }, { priority: 2 });
        taskManager.cron('test', '* * * * * *', { id: 3 }, { priority: 1 });
        taskManager.resume();

        coroutine.sleep(1500);

        assert.equal(executed[0], 2); 
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

        taskManager.resumeTask(taskId);
        taskManager.pause();
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
});
