const { describe, it, beforeEach, afterEach } = require('test');
const assert = require('assert');
const TaskManager = require('../lib/task.js');

const TEST_DB_CONNECTION = 'sqlite::memory:';

describe('Task Schema Removal', () => {
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

    it('should reject handler parameters schema configuration', () => {
        assert.throws(() => {
            taskManager.use('schemaTask', {
                handler: async (task) => {
                    return { success: true };
                },
                parameters: {
                    type: 'object'
                }
            });
        }, /Handler schema is no longer supported/, 'Should reject parameters schema configuration');
    });

    it('should reject handler output schema configuration', () => {
        assert.throws(() => {
            taskManager.use('schemaTask', {
                handler: async (task) => {
                    return { success: true };
                },
                output: {
                    type: 'object'
                }
            });
        }, /Handler schema is no longer supported/, 'Should reject output schema configuration');
    });

    it('should reject schema configuration in bulk registration', () => {
        assert.throws(() => {
            taskManager.use({
                bulkSchemaTask: {
                    handler: async (task) => {
                        return { success: true };
                    },
                    parameters: {
                        type: 'object'
                    }
                }
            });
        }, /Handler schema is no longer supported/, 'Should reject schema configuration during bulk registration');
    });

    it('should still accept arbitrary payloads when no schema is specified', () => {
        taskManager.use('noSchemaTask', async (task) => {
            return { success: true };
        });

        const taskId = taskManager.async('noSchemaTask', {
            anyField: 'any value',
            anotherField: 123
        });
        assert.ok(taskId, 'Should accept payload when no schema is specified');
    });
});
