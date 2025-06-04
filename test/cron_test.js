const test = require('test');
test.setup();

const coroutine = require('coroutine');
const { TaskManager } = require('..');
const config = require('./config.js');

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

        const task = taskManager.getTask(taskId);
        
        assert.equal(task.id, taskId);
        assert.equal(task.status, 'pending');  
        assert.equal(task.type, 'cron');
        assert.equal(typeof task.next_run_time, 'number');
        assert.equal(task.next_run_time >= Date.now() / 1000, true);  

        assert.equal(task.result, 'success');

        taskManager.stop();
    });

    it('should handle cron task failure and retry', () => {
        let attempts = 0;
        taskManager.use('test', () => {
            attempts++;
            throw new Error('test error');
        });
        taskManager.start();

        const taskId = taskManager.cron('test', '* * * * * *');
        
        while(attempts < taskManager.options.max_retries) {
            coroutine.sleep(100);
        }

        while(taskManager.getTask(taskId).status !== 'paused') {
            coroutine.sleep(100);
        }

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

        while (true) {
            const task = taskManager.getTask(taskId);
            if (task.status === 'timeout') break;
            coroutine.sleep(100);
        }
        assert.equal(executionCount, 1);

        while (true) {
            task = taskManager.getTask(taskId);
            if (task.status === 'pending') break;
            coroutine.sleep(100);
        }
        assert.equal(task.retry_count, 1);

        while (true) {
            task = taskManager.getTask(taskId);
            if (task.status === 'timeout' && task.retry_count === 1) break;
            coroutine.sleep(100);
        }
        assert.equal(executionCount, 2);

        while (true) {
            task = taskManager.getTask(taskId);
            if (task.status === 'paused') break;
            coroutine.sleep(100);
        }

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

        while (true) {
            task = taskManager.getTask(taskId);
            if (task.status === 'paused') break;
            coroutine.sleep(100);
        }
        
        assert.equal(task.status, 'paused');
        assert.equal(executionCount, 2);

        taskManager.resumeTask(taskId);
        task = taskManager.getTask(taskId);
        assert.equal(task.status, 'pending');
        assert.equal(task.retry_count, 0);
    });
});
