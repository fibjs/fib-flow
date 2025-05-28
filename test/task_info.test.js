const test = require('test');
const assert = require('assert');
const coroutine = require('coroutine');
const TaskManager = require('../lib/task.js');

test.setup();

describe('Task Information API', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager();
        taskManager.db.setup();
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
        }
    });

    it('should get task info for simple task', () => {
        // Register a simple task
        taskManager.use('simpleTask', async (task) => {
            return { success: true };
        });

        // Get task info
        const taskInfo = taskManager.getTaskInfo('simpleTask');
        
        assert.equal(taskInfo.name, 'simpleTask', 'Task info should have correct name');
        assert.equal(taskInfo.description, '', 'Simple task should have empty description');
        assert.deepStrictEqual(taskInfo.parameters, {}, 'Simple task should have empty parameters schema');
        assert.deepStrictEqual(taskInfo.output, {}, 'Simple task should have empty output schema');
    });

    it('should get task info with complete metadata', () => {
        // Register a task with complete metadata
        taskManager.use('completeTask', {
            handler: async (task) => {
                return { success: true, data: task.payload.data };
            },
            description: 'A complete task with full metadata',
            parameters: {
                type: 'object',
                properties: {
                    data: { type: 'string' }
                }
            },
            output: {
                type: 'object',
                properties: {
                    success: { type: 'boolean' },
                    data: { type: 'string' }
                }
            }
        });

        // Get task info
        const taskInfo = taskManager.getTaskInfo('completeTask');
        
        assert.equal(taskInfo.name, 'completeTask', 'Task info should have correct name');
        assert.equal(taskInfo.description, 'A complete task with full metadata', 'Task should have correct description');
        
        assert.equal(taskInfo.parameters.type, 'object', 'Parameters schema should be preserved');
        assert.ok(taskInfo.parameters.properties.data, 'Parameters schema properties should be preserved');
        
        assert.equal(taskInfo.output.type, 'object', 'Output schema should be preserved');
        assert.ok(taskInfo.output.properties.success, 'Output schema properties should be preserved');
        assert.ok(taskInfo.output.properties.data, 'Output schema properties should be preserved');
    });

    it('should throw error for unregistered task', () => {
        // Try to get info for non-existent task
        assert.throws(() => {
            taskManager.getTaskInfo('nonExistentTask');
        }, /No task registered with name/, 'Should throw error for unregistered task');
    });

    it('should get task info for bulk registered tasks', () => {
        // Register multiple tasks at once
        taskManager.use({
            bulkTask1: {
                handler: async (task) => {
                    return { success: true };
                },
                description: 'First bulk task'
            },
            bulkTask2: {
                handler: async (task) => {
                    return { success: true };
                },
                description: 'Second bulk task'
            }
        });

        // Get task info for both tasks
        const taskInfo1 = taskManager.getTaskInfo('bulkTask1');
        const taskInfo2 = taskManager.getTaskInfo('bulkTask2');
        
        assert.equal(taskInfo1.description, 'First bulk task', 'First bulk task should have correct description');
        assert.equal(taskInfo2.description, 'Second bulk task', 'Second bulk task should have correct description');
    });

    it('should get task info with partial metadata', () => {
        // Register a task with only some metadata fields
        taskManager.use('partialTask', {
            handler: async (task) => {
                return { success: true };
            },
            description: 'A task with partial metadata',
            // No parameters schema
            output: {
                type: 'object',
                properties: {
                    success: { type: 'boolean' }
                }
            }
        });

        // Get task info
        const taskInfo = taskManager.getTaskInfo('partialTask');
        
        assert.equal(taskInfo.description, 'A task with partial metadata', 'Task should have correct description');
        assert.deepStrictEqual(taskInfo.parameters, {}, 'Parameters schema should be empty object');
        assert.equal(taskInfo.output.type, 'object', 'Output schema should be preserved');
    });
});
