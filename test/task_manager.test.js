const test = require('test');
const assert = require('assert');
const coroutine = require('coroutine');
const TaskManager = require('../lib/task.js');

test.setup();

describe('TaskManager Initialization', () => {
    it('should create TaskManager without dbConnection', () => {
        const taskManager = new TaskManager();
        
        // Verify that taskManager is created successfully
        assert.ok(taskManager, 'TaskManager should be created');
        
        // Optional: You might want to add more specific checks based on your implementation
        // For example, checking if the default database adapter is created
    });

    it('should create TaskManager with custom dbConnection', () => {
        const customDbConnection = 'sqlite:///path/to/custom/db.sqlite';
        const taskManager = new TaskManager({ 
            dbConnection: customDbConnection,
            dbType: 'sqlite'
        });
        
        // Verify that taskManager is created with custom connection
        assert.ok(taskManager, 'TaskManager should be created with custom connection');
        
        // Optional: Add checks to verify the custom connection is used
    });

    it('should use default options when no options are provided', () => {
        const taskManager = new TaskManager();
        
        // Verify default options
        assert.ok(taskManager, 'TaskManager should be created');
        
        // Check default configuration options
        assert.equal(taskManager.options.poll_interval, 1000, 'Default poll_interval should be 1000');
        assert.equal(taskManager.options.max_retries, 3, 'Default max_retries should be 3');
        assert.equal(taskManager.options.retry_interval, 0, 'Default retry_interval should be 0');
        assert.equal(taskManager.options.max_concurrent_tasks, 10, 'Default max_concurrent_tasks should be 10');
        assert.equal(taskManager.options.active_update_interval, 1000, 'Default active_update_interval should be 1000');
        
        // Verify worker_id is generated
        assert.ok(taskManager.options.worker_id, 'Worker ID should be generated');
        assert.ok(taskManager.options.worker_id.startsWith('worker-'), 'Worker ID should start with "worker-"');
    });

    it('should submit tasks before starting TaskManager', () => {
        const taskManager = new TaskManager();
        taskManager.db.setup();

        // Register handler first
        taskManager.use('testTask', task => {
            return { success: true };
        });

        // Submit async task before start
        const asyncTaskId = taskManager.async('testTask', { data: 'test' });
        assert.ok(asyncTaskId, 'Should create async task before start');

        // Submit cron task before start
        const cronTaskId = taskManager.cron('testTask', '*/5 * * * *', { data: 'test' });
        assert.ok(cronTaskId, 'Should create cron task before start');

        // Start TaskManager and verify tasks execute
        taskManager.start();
        coroutine.sleep(100);

        const asyncTask = taskManager.getTask(asyncTaskId);
        assert.equal(asyncTask.status, 'completed', 'Async task should complete after start');

        const cronTask = taskManager.getTask(cronTaskId);
        assert.equal(cronTask.status, 'pending', 'Cron task should be pending for next run');

        taskManager.stop();
    });

    it('should allow task management operations before start', () => {
        const taskManager = new TaskManager();
        taskManager.db.setup();

        // Register handler
        taskManager.use('managedTask', task => {
            return { success: true };
        });

        // Create initial tasks
        const taskId1 = taskManager.async('managedTask', { data: '1' });
        const taskId2 = taskManager.async('managedTask', { data: '2' });

        // Test pause/resume operations before start
        taskManager.pauseTask(taskId1);
        const pausedTask = taskManager.getTask(taskId1);
        assert.equal(pausedTask.status, 'paused', 'Should pause task before start');

        taskManager.resumeTask(taskId1);
        const resumedTask = taskManager.getTask(taskId1);
        assert.equal(resumedTask.status, 'pending', 'Should resume task before start');

        // Test delete operation before start
        const deletedCount = taskManager.deleteTasks({ name: 'managedTask' });
        assert.equal(deletedCount, 2, 'Should delete tasks before start');

        taskManager.stop();
    });

    it('should handle task operations in different manager states', () => {
        const taskManager = new TaskManager();
        taskManager.db.setup();

        // Register handler
        taskManager.use('stateTask', task => {
            return { success: true };
        });

        // Test operations in init state
        const initTaskId = taskManager.async('stateTask', { state: 'init' });
        assert.ok(initTaskId, 'Should create task in init state');

        // Start and immediately pause
        taskManager.start();
        taskManager.pause();

        // Test operations in paused state
        const pausedTaskId = taskManager.async('stateTask', { state: 'paused' });
        assert.ok(pausedTaskId, 'Should create task in paused state');

        // Resume and stop
        taskManager.resume();
        coroutine.sleep(100);
        taskManager.stop();
    });
});

describe('Task Tags', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager();
        taskManager.db.setup();
        
        // Register test handlers
        taskManager.use('tagged_task', task => {
            return { result: 'success' };
        });
        
        taskManager.start();
    });

    afterEach(() => {
        taskManager.stop();
    });

    it('should create async task with tag', () => {
        const taskId = taskManager.async('tagged_task', { data: 'test' }, { tag: 'test_tag' });
        const task = taskManager.getTask(taskId);
        assert.equal(task.tag, 'test_tag');
    });

    it('should create cron task with tag', () => {
        const taskId = taskManager.cron('tagged_task', '*/5 * * * *', { data: 'test' }, { tag: 'test_tag' });
        const task = taskManager.getTask(taskId);
        assert.equal(task.tag, 'test_tag');
    });

    it('should get tasks by tag', () => {
        // Create tasks with different tags
        taskManager.async('tagged_task', { data: '1' }, { tag: 'tag1' });
        taskManager.async('tagged_task', { data: '2' }, { tag: 'tag1' });
        taskManager.async('tagged_task', { data: '3' }, { tag: 'tag2' });

        const tag1Tasks = taskManager.getTasksByTag('tag1');
        assert.equal(tag1Tasks.length, 2);
        assert.ok(tag1Tasks.every(task => task.tag === 'tag1'));
    });

    it('should get task statistics by tag', () => {
        // Create tasks with different tags and wait for completion
        taskManager.async('tagged_task', { data: '1' }, { tag: 'stats_tag' });
        taskManager.async('tagged_task', { data: '2' }, { tag: 'stats_tag' });
        taskManager.async('tagged_task', { data: '3' }, { tag: 'other_tag' });

        coroutine.sleep(500); // Wait for tasks to complete

        const stats = taskManager.getTaskStatsByTag('stats_tag');
        assert.ok(stats.some(stat => 
            stat.tag === 'stats_tag' && 
            stat.name === 'tagged_task' && 
            stat.count === 2
        ));

        // Test with status filter
        const completedStats = taskManager.getTaskStatsByTag('stats_tag', 'completed');
        assert.ok(completedStats.some(stat =>
            stat.tag === 'stats_tag' &&
            stat.name === 'tagged_task' &&
            stat.status === 'completed' &&
            stat.count === 2
        ));
    });

    it('should support tasks without tags', () => {
        const taskId = taskManager.async('tagged_task', { data: 'test' });
        const task = taskManager.getTask(taskId);
        assert.equal(task.tag, null);
    });
});

describe('Task Concurrency', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({
            poll_interval: 100,
            max_retries: 2,
            max_concurrent_tasks: 5
        });

        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        taskManager.stop();
    });

    it('should respect task-level max_concurrent_tasks', () => {
        let running = 0;
        let maxRunning = 0;
        let completed = 0;

        taskManager.use({
            limitedTask: {
                handler: async (task) => {
                    running++;
                    maxRunning = Math.max(maxRunning, running);
                    coroutine.sleep(200);
                    running--;
                    completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 2
            }
        });

        taskManager.start();

        // Submit 5 tasks
        for (let i = 0; i < 5; i++) {
            taskManager.async('limitedTask', { index: i });
        }

        // Wait for all tasks to complete
        while (completed < 5) {
            coroutine.sleep(100);
        }

        assert.equal(maxRunning, 2, 'Should not exceed max_concurrent_tasks limit');
        assert.equal(completed, 5, 'All tasks should complete');
    });

    it('should handle multiple task types with different concurrency limits', () => {
        const stats = {
            task1: { running: 0, maxRunning: 0, completed: 0 },
            task2: { running: 0, maxRunning: 0, completed: 0 }
        };

        taskManager.use({
            task1: {
                handler: async (task) => {
                    stats.task1.running++;
                    stats.task1.maxRunning = Math.max(stats.task1.maxRunning, stats.task1.running);
                    coroutine.sleep(200);
                    stats.task1.running--;
                    stats.task1.completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 2
            },
            task2: {
                handler: async (task) => {
                    stats.task2.running++;
                    stats.task2.maxRunning = Math.max(stats.task2.maxRunning, stats.task2.running);
                    coroutine.sleep(200);
                    stats.task2.running--;
                    stats.task2.completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 3
            }
        });

        taskManager.start();

        // Submit tasks for both types
        for (let i = 0; i < 4; i++) {
            taskManager.async('task1', { index: i });
            taskManager.async('task2', { index: i });
        }

        // Wait for all tasks to complete
        while (stats.task1.completed < 4 || stats.task2.completed < 4) {
            coroutine.sleep(100);
        }

        assert.equal(stats.task1.maxRunning, 2, 'Task1 should not exceed its limit');
        assert.equal(stats.task2.maxRunning, 3, 'Task2 should not exceed its limit');
        assert.equal(stats.task1.completed, 4, 'All task1 should complete');
        assert.equal(stats.task2.completed, 4, 'All task2 should complete');
    });

    it('should handle tasks with and without concurrency limits', () => {
        const stats = {
            limited: { running: 0, maxRunning: 0, completed: 0 },
            unlimited: { running: 0, maxRunning: 0, completed: 0 }
        };

        taskManager.use({
            limitedTask: {
                handler: async (task) => {
                    stats.limited.running++;
                    stats.limited.maxRunning = Math.max(stats.limited.maxRunning, stats.limited.running);
                    coroutine.sleep(200);
                    stats.limited.running--;
                    stats.limited.completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 2
            },
            unlimitedTask: async (task) => {
                stats.unlimited.running++;
                stats.unlimited.maxRunning = Math.max(stats.unlimited.maxRunning, stats.unlimited.running);
                coroutine.sleep(200);
                stats.unlimited.running--;
                stats.unlimited.completed++;
                return { success: true };
            }
        });

        taskManager.start();

        // Submit tasks for both types
        for (let i = 0; i < 4; i++) {
            taskManager.async('limitedTask', { index: i });
            taskManager.async('unlimitedTask', { index: i });
        }

        // Wait for all tasks to complete
        while (stats.limited.completed < 4 || stats.unlimited.completed < 4) {
            coroutine.sleep(100);
        }

        assert.equal(stats.limited.maxRunning, 2, 'Limited tasks should not exceed limit');
        assert.ok(stats.unlimited.maxRunning >= 3, 'Unlimited tasks can run more concurrently');
        assert.equal(stats.limited.completed, 4, 'All limited tasks should complete');
        assert.equal(stats.unlimited.completed, 4, 'All unlimited tasks should complete');
    });

    it('should respect global max_concurrent_tasks', () => {
        let runningTasks = 0;
        let maxRunningTasks = 0;
        let completedTasks = 0;

        taskManager.use('globalTask', async (task) => {
            runningTasks++;
            maxRunningTasks = Math.max(maxRunningTasks, runningTasks);
            coroutine.sleep(100);
            runningTasks--;
            completedTasks++;
            return { success: true };
        });

        taskManager.start();

        // Submit more tasks than max_concurrent_tasks
        const totalTasks = 8; // Greater than max_concurrent_tasks (5)
        for (let i = 0; i < totalTasks; i++) {
            taskManager.async('globalTask', { index: i });
        }

        // Wait for all tasks to complete
        while (completedTasks < totalTasks) {
            coroutine.sleep(50);
        }

        assert.equal(maxRunningTasks, 5, 'Should not exceed global max_concurrent_tasks');
        assert.equal(completedTasks, totalTasks, 'All tasks should complete');
    });

    it('should handle task-level concurrency limit when tasks fail', () => {
        let runningTasks = 0;
        let maxRunningTasks = 0;
        let completedTasks = 0;
        let failedTasks = 0;

        taskManager.use({
            errorTask: {
                handler: async (task) => {
                    runningTasks++;
                    maxRunningTasks = Math.max(maxRunningTasks, runningTasks);
                    coroutine.sleep(100);
                    runningTasks--;
                    
                    if (task.payload.shouldFail) {
                        failedTasks++;
                        throw new Error('Task failed');
                    }
                    
                    completedTasks++;
                    return { success: true };
                },
                max_concurrent_tasks: 2
            }
        });

        taskManager.start();

        // Submit mix of failing and succeeding tasks
        for (let i = 0; i < 4; i++) {
            taskManager.async('errorTask', { 
                index: i,
                shouldFail: i % 2 === 0 
            });
        }

        // Wait for all tasks to process
        while ((completedTasks + failedTasks) < 4) {
            coroutine.sleep(50);
        }

        assert.equal(maxRunningTasks, 2, 'Should maintain concurrency limit even with failures');
        assert.equal(completedTasks, 2, 'Half of tasks should complete');
        assert.equal(failedTasks, 2, 'Half of tasks should fail');
    });

    it('should maintain separate concurrency limits for different task types', () => {
        const stats = {
            fastTask: { running: 0, maxRunning: 0, completed: 0 },
            slowTask: { running: 0, maxRunning: 0, completed: 0 }
        };

        taskManager.use({
            fastTask: {
                handler: async (task) => {
                    stats.fastTask.running++;
                    stats.fastTask.maxRunning = Math.max(stats.fastTask.maxRunning, stats.fastTask.running);
                    coroutine.sleep(50);
                    stats.fastTask.running--;
                    stats.fastTask.completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 3
            },
            slowTask: {
                handler: async (task) => {
                    stats.slowTask.running++;
                    stats.slowTask.maxRunning = Math.max(stats.slowTask.maxRunning, stats.slowTask.running);
                    coroutine.sleep(150);
                    stats.slowTask.running--;
                    stats.slowTask.completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 2
            }
        });

        taskManager.start();

        // Submit mixed tasks
        for (let i = 0; i < 4; i++) {
            taskManager.async('fastTask', { index: i });
            taskManager.async('slowTask', { index: i });
        }

        // Wait for completion
        while (stats.fastTask.completed < 4 || stats.slowTask.completed < 4) {
            coroutine.sleep(50);
        }

        assert.equal(stats.fastTask.maxRunning, 3, 'Fast tasks should respect their concurrency limit');
        assert.equal(stats.slowTask.maxRunning, 2, 'Slow tasks should respect their concurrency limit');
        assert.equal(stats.fastTask.completed + stats.slowTask.completed, 8, 'All tasks should complete');
    });

    it('should handle combined task-level and global concurrency limits', () => {
        let running = 0;
        let maxRunning = 0;
        let completed = 0;

        taskManager.use({
            limitedTask: {
                handler: async (task) => {
                    running++;
                    maxRunning = Math.max(maxRunning, running);
                    coroutine.sleep(100);
                    running--;
                    completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 3
            }
        });

        taskManager.start();

        // Submit 6 tasks (more than both limits)
        for (let i = 0; i < 6; i++) {
            taskManager.async('limitedTask', { index: i });
        }

        // Wait for all tasks to complete
        while (completed < 6) {
            coroutine.sleep(50);
        }

        // Should respect the lower of the two limits
        assert.equal(maxRunning, 3, 'Should respect task-level concurrency limit');
        assert.equal(completed, 6, 'All tasks should complete');
    });

    it('should handle dynamic task concurrency changes', () => {
        let running = 0;
        let maxRunning = 0;
        let completed = 0;

        const handler = {
            limitedTask: {
                handler: async (task) => {
                    running++;
                    maxRunning = Math.max(maxRunning, running);
                    coroutine.sleep(100);
                    running--;
                    completed++;
                    return { success: true };
                },
                max_concurrent_tasks: 2
            }
        };

        taskManager.use(handler);
        taskManager.start();

        // Submit first batch with lower concurrency
        for (let i = 0; i < 3; i++) {
            taskManager.async('limitedTask', { batch: 1, index: i });
        }

        coroutine.sleep(150);
        
        // Update concurrency limit
        handler.limitedTask.max_concurrent_tasks = 3;
        
        // Submit second batch with higher concurrency
        for (let i = 0; i < 3; i++) {
            taskManager.async('limitedTask', { batch: 2, index: i });
        }

        // Wait for all tasks to complete
        while (completed < 6) {
            coroutine.sleep(50);
        }

        assert.ok(maxRunning <= 3, 'Should not exceed updated concurrency limit');
        assert.equal(completed, 6, 'All tasks should complete');
    });
});
