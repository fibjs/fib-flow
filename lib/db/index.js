/**
 * Database Adapter Factory
 * 
 * Provides a centralized mechanism for creating database adapters
 * Supports multiple database types with a uniform interface
 * Handles connection configuration and adapter instantiation
 */

const SQLiteAdapter = require('./sqlite');
const MySQLAdapter = require('./mysql');
const util = require('util');

// Create logger for database factory operations
const logger = util.debuglog('fib-flow');

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
    logger.notice(`[DBFactory] Creating database adapter with config:`, config);

    // Infer database type from connection string if not explicitly provided
    if (typeof config === 'string') {
        if (config.startsWith('mysql:')) {
            type = "mysql";
        } else if (config.startsWith('sqlite:')) {
            type = "sqlite";
        } else {
            // Reject unsupported connection string formats
            logger.error(`[DBFactory] Unsupported database type in connection string: ${config}`);
            throw new Error(`Unsupported database type: ${config}`);
        }
    } else if (typeof config === 'object') {
        // Use type from configuration object
        type = config.type.toLowerCase();
    } else if (typeof config !== "function") {
        // Ensure configuration is either a string, object, or function
        logger.error(`[DBFactory] Invalid configuration type: ${typeof config}`);
        throw new Error('Invalid configuration. Must be a connection string or configuration object');
    }

    logger.debug(`[DBFactory] Using database type: ${type}`);

    // Select and instantiate appropriate database adapter
    try {
        let adapter;
        switch (type) {
            case 'sqlite':
                // Default pool size of 5 for standard database connections
                logger.debug(`[DBFactory] Creating SQLite adapter`);
                adapter = new SQLiteAdapter(config, 5);
                break;
            case 'mysql':
                // Default pool size of 5 for MySQL connections
                logger.debug(`[DBFactory] Creating MySQL adapter`);
                adapter = new MySQLAdapter(config, 5);
                break;
            default:
                // Catch any unhandled database types
                logger.error(`[DBFactory] Unsupported database type: ${type}`);
                throw new Error(`Unsupported database type: ${type}`);
        }
        logger.notice(`[DBFactory] Database adapter created successfully`);
        return adapter;
    } catch (error) {
        logger.error(`[DBFactory] Failed to create database adapter:`, error);
        throw error;
    }
}

module.exports = {
    createAdapter,
    SQLiteAdapter,
    MySQLAdapter
};
