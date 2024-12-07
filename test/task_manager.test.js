const test = require('test');
const assert = require('assert');
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
