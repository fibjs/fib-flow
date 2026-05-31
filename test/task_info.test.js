const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');
const TaskManager = require('../lib/task.js');

const TEST_DB_CONNECTION = 'sqlite::memory:';

describe('Task Information API', () => {
    let taskManager;

    beforeEach(() => {
        taskManager = new TaskManager({ dbConnection: TEST_DB_CONNECTION });
        taskManager.db.setup();
    });

    afterEach(() => {
        if (taskManager) {
            taskManager.stop();
        }
    });

    it('should get task info for simple task', () => {
        taskManager.use('simpleTask', async (task) => {
            return { success: true };
        });

        const taskInfo = taskManager.getTaskInfo('simpleTask');

        assert.equal(taskInfo.name, 'simpleTask', 'Task info should have correct name');
        assert.equal(taskInfo.description, '', 'Simple task should have empty description');
    });

    it('should get task info with supported metadata', () => {
        taskManager.use('completeTask', {
            handler: async (task) => {
                return { success: true, data: task.payload.data };
            },
            description: 'A complete task with supported metadata'
        });

        const taskInfo = taskManager.getTaskInfo('completeTask');

        assert.equal(taskInfo.name, 'completeTask', 'Task info should have correct name');
        assert.equal(taskInfo.description, 'A complete task with supported metadata', 'Task should have correct description');
    });

    it('should throw error for unregistered task', () => {
        assert.throws(() => {
            taskManager.getTaskInfo('nonExistentTask');
        }, /No task registered with name/, 'Should throw error for unregistered task');
    });

    it('should get task info for bulk registered tasks', () => {
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

        const taskInfo1 = taskManager.getTaskInfo('bulkTask1');
        const taskInfo2 = taskManager.getTaskInfo('bulkTask2');

        assert.equal(taskInfo1.description, 'First bulk task', 'First bulk task should have correct description');
        assert.equal(taskInfo2.description, 'Second bulk task', 'Second bulk task should have correct description');
    });

    it('should not expose removed schema metadata fields', () => {
        taskManager.use('simpleTask', async (task) => {
            return { success: true };
        });

        const taskInfo = taskManager.getTaskInfo('simpleTask');

        assert.equal(Object.prototype.hasOwnProperty.call(taskInfo, 'parameters'), false, 'Task info should not include parameters');
        assert.equal(Object.prototype.hasOwnProperty.call(taskInfo, 'output'), false, 'Task info should not include output');
    });
});
