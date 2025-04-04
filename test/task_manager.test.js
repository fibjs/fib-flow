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
        assert.equal(taskManager.options.retry_interval, 300, 'Default retry_interval should be 300');
        assert.equal(taskManager.options.max_concurrent_tasks, 10, 'Default max_concurrent_tasks should be 10');
        assert.equal(taskManager.options.active_update_interval, 1000, 'Default active_update_interval should be 1000');
        
        // Verify worker_id is generated
        assert.ok(taskManager.options.worker_id, 'Worker ID should be generated');
        assert.ok(taskManager.options.worker_id.startsWith('worker-'), 'Worker ID should start with "worker-"');
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
