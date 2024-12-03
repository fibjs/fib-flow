/**
 * Database adapter factory and exports
 * Provides SQLite and MySQL adapters for task persistence
 */

const SQLiteAdapter = require('./sqlite');
const MySQLAdapter = require('./mysql');

/**
 * Creates database adapter based on configuration
 * @param {string|object} config - Connection string (e.g. 'sqlite:./db.sqlite') or config object
 * @param {string} [type] - Optional database type, inferred from config if not provided
 * @returns {BaseDBAdapter} Database adapter instance
 * @throws {Error} If database type is unsupported or config is invalid
 */
function createAdapter(config, type) {
    // Handle string input (connection string)
    if (typeof config === 'string') {
        if (config.startsWith('mysql:')) {
            type = "mysql";
        } else if (config.startsWith('sqlite:')) {
            type = "sqlite";
        } else {
            throw new Error(`Unsupported database type: ${config}`);
        }
    } else if (typeof config === 'object') {
        type = config.type.toLowerCase();
    } else if (typeof config !== "function") {
        throw new Error('Invalid configuration. Must be a connection string or configuration object');
    }

    switch (type) {
        case 'sqlite':
            return new SQLiteAdapter(config, 5);
        case 'mysql':
            return new MySQLAdapter(config, 5);
        default:
            throw new Error(`Unsupported database type: ${type}`);
    }
}

module.exports = {
    createAdapter,
    SQLiteAdapter,
    MySQLAdapter
};
