/**
 * Database Adapter Factory
 * 
 * Provides a centralized mechanism for creating database adapters
 * Supports multiple database types with a uniform interface
 * Handles connection configuration and adapter instantiation
 */

const SQLiteAdapter = require('./sqlite');
const MySQLAdapter = require('./mysql');

/**
 * Creates a database adapter based on the provided configuration
 * 
 * @param {string|object} config - Connection configuration
 *        - For SQLite: 'sqlite:path/to/db' or 'sqlite::memory:'
 *        - For MySQL: 'mysql://user:pass@host/database'
 * @param {string} [type] - Optional database type, inferred from config if not provided
 * @returns {BaseDBAdapter} Configured database adapter instance
 * @throws {Error} If database type is unsupported or configuration is invalid
 */
function createAdapter(config, type) {
    // Infer database type from connection string if not explicitly provided
    if (typeof config === 'string') {
        if (config.startsWith('mysql:')) {
            type = "mysql";
        } else if (config.startsWith('sqlite:')) {
            type = "sqlite";
        } else {
            // Reject unsupported connection string formats
            throw new Error(`Unsupported database type: ${config}`);
        }
    } else if (typeof config === 'object') {
        // Use type from configuration object
        type = config.type.toLowerCase();
    } else if (typeof config !== "function") {
        // Ensure configuration is either a string, object, or function
        throw new Error('Invalid configuration. Must be a connection string or configuration object');
    }

    // Select and instantiate appropriate database adapter
    switch (type) {
        case 'sqlite':
            // Default pool size of 5 for standard database connections
            return new SQLiteAdapter(config, 5);
        case 'mysql':
            // Default pool size of 5 for MySQL connections
            return new MySQLAdapter(config, 5);
        default:
            // Catch any unhandled database types
            throw new Error(`Unsupported database type: ${type}`);
    }
}

module.exports = {
    createAdapter,
    SQLiteAdapter,
    MySQLAdapter
};
