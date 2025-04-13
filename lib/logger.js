const util = require('util');

/**
 * Creates a no-op logger with all standard logging methods
 * @returns {Object} Logger object with empty function implementations
 */
function createNoOpLogger() {
    return {
        debug: () => {},
        info: () => {},
        notice: () => {},
        warning: () => {},
        error: () => {}
    };
}

/**
 * Creates a logger that wraps util.debuglog if available,
 * otherwise returns a no-op logger
 * @param {string} namespace - Debug namespace
 * @returns {Object} Logger object
 */
function createLogger(namespace) {
    // Check if util.debuglog is available
    if (typeof util.debuglog === 'function')
        return util.debuglog(namespace);
    
    return createNoOpLogger();
}

module.exports = createLogger;
