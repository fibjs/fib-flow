# Database Configuration

This document provides comprehensive guidance on configuring database connections in fib-flow. It covers various connection methods, supported database types, and transaction management features. Whether you're using SQLite for simple deployments or MySQL for production environments, you'll find detailed information about setup options and best practices.

## Table of Contents
- [Connection Methods](#connection-methods)
  - [Connection String](#connection-string)
  - [DB Connection Object](#db-connection-object)
  - [Connection Pool](#connection-pool)
- [Database Types](#database-types)
  - [SQLite](#sqlite)
  - [MySQL](#mysql)
  - [Automatic Initialization](#automatic-initialization)
- [Transaction Support](#transaction-support)

## Connection Methods

fib-flow supports both SQLite and MySQL databases. You can specify the database connection in three ways:

### Connection String
```javascript
// SQLite
const taskManager = new TaskManager({
    dbConnection: 'sqlite:tasks.db'
});
```

### DB Connection Object
```javascript
const dbConn = db.open('sqlite:tasks.db');
const taskManager = new TaskManager({
    dbConnection: dbConn
});
```

### Connection Pool
```javascript
// When using a connection pool, you must specify the database type
const pool = Pool({
    create: () => db.open('sqlite:tasks.db'),
    destroy: conn => conn.close(),
    timeout: 30000,
    retry: 1,
    maxsize: 5
});

const taskManager = new TaskManager({
    dbConnection: pool,
    dbType: 'sqlite'    // Required when using connection pool
});
```

## Database Types

### SQLite
- Supports file-based and in-memory databases
- Automatic schema initialization
- Ideal for single-instance deployments
- No configuration required for in-memory mode
- Supports both relative and absolute paths for database files
- Thread-safe operations for concurrent access
- Built-in WAL (Write-Ahead Logging) support
- In-memory database example:
```javascript
const taskManager = new TaskManager({
    dbConnection: 'sqlite::memory:'
});
```

### MySQL
- Supports connection pooling for better performance
- Automatic reconnection handling
- Built-in connection timeout management
- Compatible with MySQL 5.7 and later versions
- Handles connection string parsing automatically
```javascript
const mysqlPool = Pool({
    create: () => db.open('mysql://user:password@localhost:3306/dbname'),
    destroy: conn => conn.close(),
    timeout: 30000,
    retry: 1,
    maxsize: 5
});

const taskManager = new TaskManager({
    dbConnection: mysqlPool,
    dbType: 'mysql'    // Required when using connection pool
});
```

Note: The `dbType` parameter is only required when using a connection pool. When using a connection string, the database type is automatically inferred from the connection string prefix ('sqlite:' or 'mysql:').

## Automatic Initialization

You can now create a `TaskManager` without explicitly providing a database connection. In such cases, an in-memory SQLite database will be automatically created:

```javascript
const taskManager = new TaskManager(); // No database connection specified
taskManager.db.setup(); // Initialize database schema
```

### Single-Instance Use Cases

This feature is particularly beneficial for single-instance, in-process scenarios where:
- Distributed task management is not required
- High fault tolerance is not critical
- Simple, lightweight task orchestration is needed
- Tasks are executed within a single process or application

Benefits in single-instance scenarios:
- Zero configuration overhead
- Minimal performance impact
- Simplified task management for local, non-distributed workloads
- Ideal for microservices, background processing, and event-driven architectures

Note: For production environments with high reliability requirements, it's recommended to use a persistent database configuration.

## Transaction Support

fib-flow automatically handles transactions for task state changes and workflow operations. All database operations related to task status updates, child task creation, and result storage are wrapped in transactions to ensure data consistency.

### Transaction Behaviors

The transaction system provides the following guarantees:

- Atomic task state transitions
- Consistent parent-child task relationships
- Isolated concurrent task operations
- Durable task status updates
- Automatic rollback on errors
- Nested transaction support for complex workflows

### Transaction Examples
```javascript
// Automatic transaction handling for workflow operations
taskManager.use('parent_task', async (task, next) => {
    // All child task creation and state changes are atomic
    return next([
        { name: 'child1' },
        { name: 'child2' }
    ]);
});
```

### Best Practices

When working with transactions:

1. Avoid long-running operations within transactions
2. Use connection pools for better performance
3. Set appropriate timeout values
4. Monitor transaction duration
5. Handle transaction failures gracefully

Note: The transaction system is designed to be transparent to the user while ensuring data consistency across all database operations.
