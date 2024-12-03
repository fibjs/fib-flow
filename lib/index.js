/**
 * Main entry point for the fib-dcron library
 * Exports TaskManager and database adapters for distributed task scheduling
 * @module fib-dcron
 */

const db = require('./db/index.js');
const TaskManager = require('./task.js');

module.exports = {
    TaskManager,
    ...db
};
