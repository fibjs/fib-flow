
if (process.argv.includes('--memory')) {
    console.notice("Using in-memory database for testing");
    module.exports = {
        "dbConnection": "memory"
    };
} else {
    console.notice("Using persistent database for testing");
    module.exports = {
        // "dbConnection": "mysql://root:******@127.0.0.1:3306/test"
        // "dbConnection": "psql://postgres:******@127.0.0.1/test"
        // "dbConnection": "sqlite:test.db"
        "dbConnection": "sqlite::memory:"
    };
}
