const test = require('test');
const assert = require('assert');
const coroutine = require('coroutine');
const TaskManager = require('../lib/task.js');

test.setup();

describe('Task Schema Validation', () => {
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

    it('should validate task payload against schema', () => {
        // Register a task with schema definition
        taskManager.use('validatedTask', {
            handler: async (task) => {
                return { success: true };
            },
            parameters: {
                type: 'object',
                required: ['name', 'age'],
                properties: {
                    name: { type: 'string' },
                    age: { type: 'integer', minimum: 18 }
                }
            }
        });

        // Valid payload should pass validation
        const validTaskId = taskManager.async('validatedTask', { 
            name: 'John',
            age: 25
        });
        assert.ok(validTaskId, 'Should accept valid payload');

        // Invalid payload should throw error
        assert.throws(() => {
            taskManager.async('validatedTask', { 
                name: 'John',
                age: 17  // Less than minimum
            });
        }, /Invalid task payload/, 'Should reject payload with invalid age');

        assert.throws(() => {
            taskManager.async('validatedTask', { 
                name: 'John'
                // Missing required age
            });
        }, /Invalid task payload/, 'Should reject payload with missing required field');
    });

    it('should validate complex schema with formats', () => {
        // Register a task with complex schema including formats
        taskManager.use('complexValidatedTask', {
            handler: async (task) => {
                return { success: true };
            },
            parameters: {
                type: 'object',
                required: ['email', 'date'],
                properties: {
                    email: { type: 'string', format: 'email' },
                    date: { type: 'string', format: 'date' },
                    tags: { 
                        type: 'array',
                        items: { type: 'string' }
                    }
                }
            }
        });

        // Valid payload should pass validation
        const validTaskId = taskManager.async('complexValidatedTask', { 
            email: 'test@example.com',
            date: '2025-01-01',
            tags: ['tag1', 'tag2']
        });
        assert.ok(validTaskId, 'Should accept valid complex payload');

        // Invalid email format should throw error
        assert.throws(() => {
            taskManager.async('complexValidatedTask', { 
                email: 'not-an-email',
                date: '2025-01-01'
            });
        }, /Invalid task payload/, 'Should reject payload with invalid email format');

        // Invalid date format should throw error
        assert.throws(() => {
            taskManager.async('complexValidatedTask', { 
                email: 'test@example.com',
                date: 'not-a-date'
            });
        }, /Invalid task payload/, 'Should reject payload with invalid date format');

        // Invalid array type should throw error
        assert.throws(() => {
            taskManager.async('complexValidatedTask', { 
                email: 'test@example.com',
                date: '2025-01-01',
                tags: 'not-an-array'
            });
        }, /Invalid task payload/, 'Should reject payload with invalid array type');
    });

    it('should handle invalid schema definitions', () => {
        // Register a task with invalid schema should throw error
        assert.throws(() => {
            taskManager.use('invalidSchemaTask', {
                handler: async (task) => {
                    return { success: true };
                },
                parameters: {
                    type: 'invalid-type',  // Invalid type
                    properties: {
                        name: { type: 'string' }
                    }
                }
            });
        }, /Invalid schema/, 'Should reject invalid schema type');
    });

    it('should pass validation when no schema is specified', () => {
        // Register a task without schema
        taskManager.use('noSchemaTask', async (task) => {
            return { success: true };
        });

        // Any payload should be accepted
        const taskId = taskManager.async('noSchemaTask', { 
            anyField: 'any value',
            anotherField: 123
        });
        assert.ok(taskId, 'Should accept any payload when no schema is specified');
    });
});
