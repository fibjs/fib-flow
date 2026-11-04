/**
 * Type declarations for fib-flow.
 *
 * Runtime entry point is `lib/index.js`; this file only describes the public
 * surface so TypeScript consumers get completion and structural checks.
 */

export * from './lib/types/core';
export * from './lib/types/task-manager';

export { TaskManager, Suspension } from './lib/types/task-manager';
export {
    createAdapter,
    DatabaseAdapter,
    SQLiteAdapter,
    MySQLAdapter,
    PSQLAdapter
} from './lib/types/task-manager';
