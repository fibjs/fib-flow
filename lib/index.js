const db = require('./db/index.js');
const TaskManager = require('./task.js');

module.exports = {
    TaskManager,
    ...db
};
