/**
 * Main entry point for the fib-flow library
 * Exports TaskManager and database adapters for distributed task scheduling
 * @module fib-flow
 */

const db = require('./db/index.js');
const TaskManager = require('./task.js');

module.exports = {
    TaskManager,
    ...db
};
