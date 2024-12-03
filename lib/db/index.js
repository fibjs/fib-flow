const SQLiteAdapter = require('./sqlite');
const MySQLAdapter = require('./mysql');

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
