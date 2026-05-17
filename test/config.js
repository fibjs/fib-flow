
console.notice("Using persistent database for testing");

const defaultConnection = "sqlite::memory:";
const dbConnection = process.env.FIB_FLOW_TEST_DB || defaultConnection;

module.exports = {
    // export FIB_FLOW_TEST_DB="mysql://root:******@127.0.0.1:3306/test"
    // export FIB_FLOW_TEST_DB="psql://postgres:******@127.0.0.1/test"
    // export FIB_FLOW_TEST_DB="sqlite:test.db"
    "dbConnection": dbConnection
};
