const { describe, it, xit, beforeEach, afterEach } = require('test');
const assert = require('assert');

const db = require('db');
const coroutine = require('coroutine');
const { createAdapter, TaskManager } = require('..');
const Pool = require('fib-pool');
const config = require('./config.js');

const TEST_TIMEOUT_CONFIG = {
    task_heartbeat_interval: 1000,
    task_heartbeat_timeout: 5000
};

function createDBConn() {
    if (typeof config.dbConnection === 'string') {
        return db.open(config.dbConnection);
    }
}

const is_postgres = config.dbConnection.startsWith("psql://");

// Helper function to directly modify task properties for testing
function directUpdateTaskProperty(adapter, taskId, property, value) {
    if (adapter.tasks) {
        const task = adapter.tasks.get(taskId);
        if (task) {
            // Store old task state for index update
            const oldTask = { ...task };

            // Update the property
            task[property] = value;

            // Update indexes if we changed a property that affects indexing
            if (['status', 'name', 'tag', 'parent_id', 'next_run_time', 'worker_id'].includes(property)) {
                adapter._updateIndexes(oldTask, task);
            }
        }
    } else {
        // SQL adapter - use SQL update
        adapter.pool(conn => {
            const sql = `UPDATE fib_flow_tasks SET ${property} = ? WHERE id = ?`;
            conn.execute(sql, value, taskId);
        });
    }
}

describe("TaskManager DB Connection", () => {
    describe("TaskManager DB Connection Options", () => {
        it("should initialize and get task with a connection string", () => {
            const taskManager = new TaskManager({ dbConnection: config.dbConnection });
            taskManager.db.setup();
            taskManager.db.close();
        });

        it("should initialize and get task with a DB connection object", () => {
            const dbConn = createDBConn();
            const taskManager = new TaskManager({ dbConnection: dbConn });
            taskManager.db.setup();
            taskManager.db.close();
        });

        it("should initialize and get task with a Pool object", () => {
            const pool = Pool({
                create: createDBConn,
                destroy: conn => conn.close(),
                maxsize: 5
            });
            const taskManager = new TaskManager({
                dbConnection: pool,
                dbType: config.dbConnection.split(":")[0]
            });
            taskManager.db.setup();
            taskManager.db.close();
        });
    });

    describe("Task Management", () => {
        let adapter;

        beforeEach(() => {
            adapter = createAdapter(config.dbConnection);
            adapter.setup();
            adapter.clearTasks();
        });

        afterEach(() => {
            adapter.close();
        });

        describe("Basic Task Operations", () => {
            it("should create and get task", () => {
                const taskId = adapter.insertTask({
                    name: "test_task",
                    type: "async",
                    payload: { data: "test" }
                });

                assert.ok(taskId > 0);

                const task = adapter.getTask(taskId);
                assert.equal(task.name, "test_task");
                assert.equal(task.type, "async");
                assert.equal(task.status, "pending");
                assert.deepEqual(task.payload, { data: "test" });
            });

            it("should throw error for invalid task", () => {
                assert.throws(() => {
                    adapter.insertTask({});
                }, /Task name is required/);

                assert.throws(() => {
                    adapter.insertTask({ name: "" });
                }, /Task name is required/);

                assert.throws(() => {
                    adapter.insertTask({ name: "test", type: "invalid" });
                }, /Task type must be either "async" or "cron"/);
            });

            it("should handle invalid parameters", () => {
                assert.throws(() => {
                    adapter.insertTask({});
                }, /name.*required/);

                assert.throws(() => {
                    adapter.insertTask({ name: "test" });
                }, /type.*required/);

                assert.throws(() => {
                    adapter.insertTask({
                        name: "test",
                        type: "invalid_type"
                    });
                }, /type.*either/);

                // Create a valid task first, then try invalid status update
                const taskId = adapter.insertTask({
                    name: "test_task",
                    type: "async"
                });

                assert.throws(() => {
                    adapter.updateTaskStatus(1, "invalid_status");
                }, /Invalid.*value/);
            });
        });

        describe("Task Claiming and Status", () => {
            it("should claim task", () => {
                const taskId = adapter.insertTask({
                    name: "claim_test",
                    type: "async",
                    payload: { data: "claim" }
                });

                const claimed = adapter.claimTask(["claim_test"], "test-worker");
                assert.ok(claimed);
                assert.equal(claimed.id, taskId);
                assert.equal(claimed.name, "claim_test");
                assert.deepEqual(claimed.payload, { data: "claim" });
            });

            it("should handle invalid task names for claim", () => {
                assert.throws(() => {
                    adapter.claimTask(["test"], "");
                }, /Worker ID is required/);
            });

            it("should update task status", () => {
                const taskId = adapter.insertTask({
                    name: "status_test",
                    type: "async",
                    payload: { data: "status" }
                });

                adapter.claimTask(["status_test"], "test-worker");
                adapter.updateTaskStatus(taskId, "completed");

                const task = adapter.getTask(taskId);
                assert.equal(task.status, "completed");
            });

            it("should emit lifecycle audit events for create claim and status update", () => {
                const taskId = adapter.insertTask({
                    name: "audit_lifecycle_test",
                    type: "async"
                });

                adapter.claimTask(["audit_lifecycle_test"], "test-worker");
                adapter.updateTaskStatus(taskId, "completed");

                const events = adapter.getTaskEvents(taskId);
                assert.equal(events.length, 4);
                assert.equal(events[0].event_type, "task_created");
                assert.equal(events[0].to_status, "pending");
                assert.equal(events[1].event_type, "task_claimed");
                assert.equal(events[1].from_status, "pending");
                assert.equal(events[1].to_status, "running");
                assert.equal(events[1].worker_id, "test-worker");
                assert.equal(events[2].event_type, "task_started");
                assert.equal(events[2].attempt, 1);
                assert.equal(events[2].worker_id, "test-worker");
                assert.equal(events[3].event_type, "task_completed");
                assert.equal(events[3].from_status, "running");
                assert.equal(events[3].to_status, "completed");
            });

            it("should emit retry started audit events when retrying a task", () => {
                const taskId = adapter.insertTask({
                    name: "audit_retry_start_test",
                    type: "async",
                    max_retries: 3,
                    retry_interval: 0
                });

                adapter.claimTask(["audit_retry_start_test"], "worker-1");
                adapter.updateTaskStatus(taskId, "failed", { error: "first failure" });
                coroutine.sleep(1000);
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);
                adapter.claimTask(["audit_retry_start_test"], "worker-2");

                const events = adapter.getTaskEvents(taskId);
                assert.ok(events.some(event =>
                    event.event_type === "task_started" &&
                    event.attempt === 1 &&
                    event.worker_id === "worker-1"
                ));
                assert.ok(events.some(event =>
                    event.event_type === "task_started" &&
                    event.attempt === 2 &&
                    event.worker_id === "worker-2"
                ));
                assert.ok(events.some(event =>
                    event.event_type === "task_retry_started" &&
                    event.attempt === 2 &&
                    event.worker_id === "worker-2"
                ));

                const attempts = adapter.getTaskAttempts(taskId);
                assert.equal(attempts.length, 2);
                assert.equal(attempts[0].attempt, 1);
                assert.equal(attempts[0].worker_id, "worker-1");
                assert.equal(attempts[0].outcome, "failed");
                assert.ok(attempts[0].ended_at >= attempts[0].started_at);
                assert.equal(attempts[1].attempt, 2);
                assert.equal(attempts[1].worker_id, "worker-2");
                assert.equal(attempts[1].ended_at, null);
                assert.equal(attempts[1].outcome, null);
            });

            it("should store completed attempt records for finished tasks", () => {
                const taskId = adapter.insertTask({
                    name: "attempt_complete_test",
                    type: "async"
                });

                adapter.claimTask(["attempt_complete_test"], "attempt-worker");
                adapter.updateTaskStatus(taskId, "completed", { result: { ok: true } });

                const attempts = adapter.getTaskAttempts(taskId);
                assert.equal(attempts.length, 1);
                assert.equal(attempts[0].attempt, 1);
                assert.equal(attempts[0].worker_id, "attempt-worker");
                assert.equal(attempts[0].outcome, "completed");
                assert.ok(attempts[0].ended_at >= attempts[0].started_at);
                assert.equal(attempts[0].timeout_flag, 0);
            });

            it("should finish parent attempt as suspended when creating child tasks", () => {
                const parentTaskId = adapter.insertTask({
                    name: "attempt_parent_test",
                    type: "async"
                });

                adapter.claimTask(["attempt_parent_test"], "parent-worker");
                adapter.insertTask({
                    name: "attempt_child_test",
                    type: "async"
                }, {
                    root_id: parentTaskId,
                    parent_id: parentTaskId
                });

                const attempts = adapter.getTaskAttempts(parentTaskId);
                assert.equal(attempts.length, 1);
                assert.equal(attempts[0].attempt, 1);
                assert.equal(attempts[0].worker_id, "parent-worker");
                assert.equal(attempts[0].outcome, "suspended");
                assert.ok(attempts[0].ended_at >= attempts[0].started_at);
            });

            it("should handle task failure", () => {
                const taskId = adapter.insertTask({
                    name: "failure_test",
                    type: "async",
                    payload: { data: "failure" }
                });

                adapter.claimTask(["failure_test"], "test-worker");
                adapter.updateTaskStatus(taskId, "failed", { error: "error message" });

                const task = adapter.getTask(taskId);
                assert.equal(task.status, "failed");
                assert.equal(task.error, "error message");
            });

            it("should get running tasks", () => {
                const taskId = adapter.insertTask({
                    name: "running_test",
                    type: "async"
                });

                adapter.claimTask(["running_test"], "test-worker");

                const runningTasks = adapter.getRunningTasks();
                assert.ok(runningTasks.some(task => task.id === taskId));
            });

            it("should update task active time", () => {
                const taskId = adapter.insertTask({
                    name: "active_time_test",
                    type: "async"
                });

                adapter.claimTask(["active_time_test"], "test-worker");
                adapter.updateTaskActiveTime(taskId);

                const task = adapter.getTask(taskId);
                assert.ok(task.last_active_time > 0);
            });

            it("should handle empty task names array for claim", () => {
                // Should return null for empty task names array
                const result = adapter.claimTask([], "test-worker");
                assert.equal(result, null);

                // Verify database state hasn't changed
                const pendingTasks = adapter.getTasksByStatus("pending");
                const runningTasks = adapter.getTasksByStatus("running");
                const initialPendingCount = pendingTasks.length;
                const initialRunningCount = runningTasks.length;

                // Try claim with empty array again
                adapter.claimTask([], "test-worker");

                // Verify counts remain the same
                assert.equal(adapter.getTasksByStatus("pending").length, initialPendingCount);
                assert.equal(adapter.getTasksByStatus("running").length, initialRunningCount);
            });

            it("should insert and query task audit events", () => {
                const rootTaskId = adapter.insertTask({
                    name: "audit_root_task",
                    type: "async"
                });
                const childTaskId = adapter.insertTask({
                    name: "audit_child_task",
                    type: "async"
                }, {
                    root_id: rootTaskId
                });

                const now = Math.floor(Date.now() / 1000);
                const insertedIds = adapter.insertTaskEvents([
                    {
                        task_id: rootTaskId,
                        root_id: rootTaskId,
                        event_type: 'task_created',
                        event_time: now - 2,
                        metadata: { source: 'root' }
                    },
                    {
                        task_id: childTaskId,
                        root_id: rootTaskId,
                        event_type: 'task_started',
                        event_time: now - 1,
                        worker_id: 'test-worker',
                        metadata: { source: 'child' }
                    }
                ]);

                assert.equal(insertedIds.length, 2);

                const rootTaskEvents = adapter.getTaskEvents(rootTaskId);
                assert.equal(rootTaskEvents.length, 2);
                assert.ok(rootTaskEvents.every(event => event.event_type === 'task_created'));
                assert.ok(rootTaskEvents.some(event => event.metadata && event.metadata.source === 'root'));
                assert.ok(rootTaskEvents.some(event => event.metadata && event.metadata.name === 'audit_root_task'));

                const workflowEvents = adapter.getWorkflowEvents(rootTaskId);
                assert.equal(workflowEvents.length, 4);
                assert.equal(workflowEvents.filter(event => event.task_id === rootTaskId).length, 2);
                assert.equal(workflowEvents.filter(event => event.task_id === childTaskId).length, 2);
                assert.ok(workflowEvents.some(event => event.worker_id === 'test-worker'));

                const filteredEvents = adapter.getWorkflowEvents(rootTaskId, {
                    event_type: 'task_started'
                });
                assert.equal(filteredEvents.length, 1);
                assert.equal(filteredEvents[0].task_id, childTaskId);
            });

            it("should handle tasks timeout with last_active_time", () => {
                const taskId = adapter.insertTask({
                    name: "active_time_test",
                    type: "async",
                    timeout: 60,
                    max_retries: 2
                });

                // Claim the task
                adapter.claimTask(["active_time_test"], "test-worker");

                // Set last_active_time to a past time using helper function
                const task = adapter.getTask(taskId);
                const now = Math.floor(Date.now() / 1000);
                directUpdateTaskProperty(adapter, taskId, 'last_active_time', now - 300); // 5 minutes ago

                // First call to handleTimeoutTasks - should mark task as timeout
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);

                // Verify task is marked as timeout first
                let updatedTask = adapter.getTask(taskId);
                assert.equal(updatedTask.status, "timeout");
                assert.ok(updatedTask.error.includes("Task heartbeat timed out"));
                let events = adapter.getTaskEvents(taskId);
                assert.ok(events.some(event => event.event_type === 'task_timed_out' && event.metadata.timeout_type === 'heartbeat'));

                coroutine.sleep(1000); // Wait for a second

                // Second call to handleTimeoutTasks - should trigger retry
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);

                // Now verify task becomes pending with incremented retry count
                updatedTask = adapter.getTask(taskId);
                assert.equal(updatedTask.status, "pending");
                assert.equal(updatedTask.retry_count, 1);
                events = adapter.getTaskEvents(taskId);
                assert.ok(events.some(event => event.event_type === 'task_retry_scheduled' && event.to_status === 'pending'));
            });

            it("should handle task timeout scenarios", () => {
                const now = Math.floor(Date.now() / 1000);

                // 1. Test total timeout (task running too long)
                const totalTimeoutTaskId = adapter.insertTask({
                    name: "total_timeout_test",
                    type: "async",
                    timeout: 60,
                    max_retries: 2
                });

                // Claim the task and set start_time to simulate long-running task
                adapter.claimTask(["total_timeout_test"], "test-worker");
                directUpdateTaskProperty(adapter, totalTimeoutTaskId, 'start_time', now - 120); // 2 minutes ago

                // 2. Test heartbeat timeout (inactive task)
                const heartbeatTimeoutTaskId = adapter.insertTask({
                    name: "heartbeat_timeout_test",
                    type: "async",
                    timeout: 60,
                    max_retries: 2
                });

                adapter.claimTask(["heartbeat_timeout_test"], "test-worker");
                directUpdateTaskProperty(adapter, heartbeatTimeoutTaskId, 'last_active_time', now - 300); // 5 minutes ago

                // 3. Test retry mechanism for async task
                const retryTaskId = adapter.insertTask({
                    name: "retry_test",
                    type: "async",
                    max_retries: 2,
                    timeout: 60
                });

                adapter.claimTask(["retry_test"], "test-worker");
                directUpdateTaskProperty(adapter, retryTaskId, 'last_active_time', now - 300);

                // 4. Test cron task final state
                const cronTaskId = adapter.insertTask({
                    name: "cron_timeout_test",
                    type: "cron",
                    cron_expr: "*/5 * * * *",
                    max_retries: 1,
                    timeout: 60
                });

                adapter.claimTask(["cron_timeout_test"], "test-worker");
                directUpdateTaskProperty(adapter, cronTaskId, 'last_active_time', now - 300);
                directUpdateTaskProperty(adapter, cronTaskId, 'retry_count', 1);

                // First call to handleTimeoutTasks - should mark tasks as timeout
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);

                // Verify total timeout task is marked as timeout
                let task = adapter.getTask(totalTimeoutTaskId);
                assert.equal(task.status, "timeout");
                assert.ok(task.error.includes("Task exceeded total timeout limit"));

                // Verify heartbeat timeout task is marked as timeout
                task = adapter.getTask(heartbeatTimeoutTaskId);
                assert.equal(task.status, "timeout");
                assert.ok(task.error.includes("Task heartbeat timed out"));

                // Verify retry task is marked as timeout first
                task = adapter.getTask(retryTaskId);
                assert.equal(task.status, "timeout");

                coroutine.sleep(1000); // Wait for a second

                // Second call to handleTimeoutTasks - should trigger retries
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);

                // Verify tasks are now pending with incremented retry count
                task = adapter.getTask(totalTimeoutTaskId);
                assert.equal(task.status, "pending");
                assert.equal(task.retry_count, 1);
                assert.ok(task.next_run_time > now);

                task = adapter.getTask(heartbeatTimeoutTaskId);
                assert.equal(task.status, "pending");
                assert.equal(task.retry_count, 1);
                assert.ok(task.next_run_time > now);

                task = adapter.getTask(retryTaskId);
                assert.equal(task.status, "pending");
                assert.equal(task.retry_count, 1);
                assert.ok(task.next_run_time > now);

                // Set retry count to max to test retry exhaustion
                directUpdateTaskProperty(adapter, retryTaskId, 'status', 'timeout');
                directUpdateTaskProperty(adapter, retryTaskId, 'last_active_time', Math.floor(Date.now() / 1000) - 300);
                directUpdateTaskProperty(adapter, retryTaskId, 'retry_count', 2);

                // Wait for retry_interval to pass
                coroutine.sleep(1200);

                // Third call to handleTimeoutTasks - should handle retry exhaustion
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);

                // Verify retry task final state (should be permanently failed)
                task = adapter.getTask(retryTaskId);
                assert.equal(task.status, "permanently_failed");
                const retryEvents = adapter.getTaskEvents(retryTaskId);
                assert.ok(retryEvents.some(event => event.event_type === 'task_permanently_failed'));

                // Verify cron task final state (should be paused)
                task = adapter.getTask(cronTaskId);
                assert.equal(task.status, "paused");
                const cronEvents = adapter.getTaskEvents(cronTaskId);
                assert.ok(cronEvents.some(event => event.event_type === 'task_paused'));
            });

            it("should repair parent state for already permanently failed child tasks", () => {
                const parentTaskId = adapter.insertTask({
                    name: "timeout_parent_repair",
                    type: "async"
                });

                adapter.claimTask(["timeout_parent_repair"], "parent-worker");

                const childTaskId = adapter.insertTask({
                    name: "timeout_child_repair",
                    type: "async",
                    max_retries: 1
                }, {
                    root_id: parentTaskId,
                    parent_id: parentTaskId
                });

                adapter.claimTask(["timeout_child_repair"], "child-worker");
                adapter.updateTaskStatus(childTaskId, "failed", { error: "repair me" });

                directUpdateTaskProperty(adapter, childTaskId, 'status', 'permanently_failed');
                directUpdateTaskProperty(adapter, parentTaskId, 'completed_children', 0);
                directUpdateTaskProperty(adapter, parentTaskId, 'result', null);

                const firstResult = adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);
                assert.equal(firstResult.permanently_failed, 0);

                let parentTask = adapter.getTask(parentTaskId);
                assert.equal(parentTask.status, 'pending');
                assert.equal(parentTask.completed_children, 1);
                assert.ok(Array.isArray(parentTask.result));
                assert.equal(parentTask.result.length, 1);
                assert.equal(parentTask.result[0].task_id, childTaskId);
                assert.equal(parentTask.result[0].error, 'repair me');

                const secondResult = adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG);
                assert.equal(secondResult.permanently_failed, 0);

                parentTask = adapter.getTask(parentTaskId);
                assert.equal(parentTask.completed_children, 1);
                assert.equal(parentTask.result.length, 1);
            });

            it("should delete expired completed and failed tasks", () => {
                const now = Math.floor(Date.now() / 1000);

                // Create test tasks
                const completedTaskId = adapter.insertTask({
                    name: "completed_task",
                    type: "async"
                });

                const failedTaskId = adapter.insertTask({
                    name: "failed_task",
                    type: "async"
                });

                // Mark tasks as completed/failed and set last_active_time using helper functions
                // First claim and complete the tasks properly
                adapter.claimTask(["completed_task"], "test-worker");
                adapter.updateTaskStatus(completedTaskId, 'completed');
                directUpdateTaskProperty(adapter, completedTaskId, 'last_active_time', now - 3600); // 1 hour ago

                adapter.claimTask(["failed_task"], "test-worker");
                adapter.updateTaskStatus(failedTaskId, 'failed');
                adapter.updateTaskStatus(failedTaskId, 'permanently_failed');
                directUpdateTaskProperty(adapter, failedTaskId, 'last_active_time', now - 3600);

                // Call handleTimeoutTasks with 30 minute expiry
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG, 1800); // 30 minutes

                // Verify tasks were deleted
                assert.equal(adapter.getTask(completedTaskId), null);
                assert.equal(adapter.getTask(failedTaskId), null);
                assert.equal(adapter.getTaskEvents(completedTaskId).length, 0);
                assert.equal(adapter.getTaskAttempts(completedTaskId).length, 0);
                assert.equal(adapter.getTaskEvents(failedTaskId).length, 0);
                assert.equal(adapter.getTaskAttempts(failedTaskId).length, 0);

                // Create tasks that should not expire
                const activeCompletedId = adapter.insertTask({
                    name: "active_completed",
                    type: "async"
                });

                // Update task status and time using proper methods
                adapter.claimTask(["active_completed"], "test-worker");
                adapter.updateTaskStatus(activeCompletedId, 'completed');
                directUpdateTaskProperty(adapter, activeCompletedId, 'last_active_time', now - 900); // 15 minutes ago

                // Verify recent tasks not deleted
                adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG, 1800);
                const retrievedTask = adapter.getTask(activeCompletedId);
                assert.ok(retrievedTask);
                assert.equal(retrievedTask.status, 'completed');
            });

            it("should expose explicit retention cleanup with configurable statuses", () => {
                const now = Math.floor(Date.now() / 1000);

                const completedTaskId = adapter.insertTask({
                    name: "retention_completed",
                    type: "async"
                });
                adapter.claimTask(["retention_completed"], "retention-worker");
                adapter.updateTaskStatus(completedTaskId, 'completed');
                directUpdateTaskProperty(adapter, completedTaskId, 'last_active_time', now - 3600);

                const pausedTaskId = adapter.insertTask({
                    name: "retention_paused",
                    type: "cron",
                    status: "paused"
                });
                directUpdateTaskProperty(adapter, pausedTaskId, 'last_active_time', now - 3600);

                const defaultCleanup = adapter.cleanupExpiredTasks({ expire_time: 1800, now });
                assert.equal(defaultCleanup.tasks_deleted, 1);
                assert.equal(adapter.getTask(completedTaskId), null);
                assert.ok(adapter.getTask(pausedTaskId));

                const pausedCleanup = adapter.cleanupExpiredTasks({
                    expire_time: 1800,
                    statuses: ['paused'],
                    now
                });
                assert.equal(pausedCleanup.tasks_deleted, 1);
                assert.equal(adapter.getTask(pausedTaskId), null);
            });

            it("should clean expired tasks in small batches while draining explicit retention", () => {
                const now = Math.floor(Date.now() / 1000);
                const taskIds = [];

                for (let index = 0; index < 101; index += 1) {
                    const taskName = `retention_batch_${index}`;
                    const taskId = adapter.insertTask({
                        name: taskName,
                        type: 'async'
                    });
                    adapter.claimTask([taskName], 'retention-worker');
                    adapter.updateTaskStatus(taskId, 'completed');
                    directUpdateTaskProperty(adapter, taskId, 'last_active_time', now - (3600 + index));
                    taskIds.push(taskId);
                }

                const cleanupResult = adapter.cleanupExpiredTasks({
                    expire_time: 1800,
                    statuses: ['completed'],
                    now
                });

                assert.equal(cleanupResult.tasks_deleted, 101);
                taskIds.forEach(taskId => {
                    assert.equal(adapter.getTask(taskId), null);
                });
            });

            it("should limit automatic retention cleanup to one batch per sweep", () => {
                const now = Math.floor(Date.now() / 1000);
                const taskIds = [];

                for (let index = 0; index < 101; index += 1) {
                    const taskName = `retention_auto_batch_${index}`;
                    const taskId = adapter.insertTask({
                        name: taskName,
                        type: 'async'
                    });
                    adapter.claimTask([taskName], 'retention-worker');
                    adapter.updateTaskStatus(taskId, 'completed');
                    directUpdateTaskProperty(adapter, taskId, 'last_active_time', now - (3600 + index));
                    taskIds.push(taskId);
                }

                const firstSweep = adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG, {
                    expire_time: 1800,
                    statuses: ['completed'],
                    now
                });
                assert.equal(firstSweep.tasks_deleted, 100);
                assert.equal(taskIds.filter(taskId => adapter.getTask(taskId)).length, 1);

                const secondSweep = adapter.handleTimeoutTasks(TEST_TIMEOUT_CONFIG, {
                    expire_time: 1800,
                    statuses: ['completed'],
                    now
                });
                assert.equal(secondSweep.tasks_deleted, 1);
            });

            it("should keep audit queries consistent after retention deletes workflow rows", () => {
                const now = Math.floor(Date.now() / 1000);

                const rootTaskId = adapter.insertTask({
                    name: "retention_workflow_root",
                    type: "async"
                });
                adapter.claimTask(["retention_workflow_root"], "retention-worker");

                const childTaskId = adapter.insertTask({
                    name: "retention_workflow_child",
                    type: "async"
                }, {
                    root_id: rootTaskId,
                    parent_id: rootTaskId
                });

                adapter.claimTask(["retention_workflow_child"], "retention-worker");
                adapter.updateTaskStatus(childTaskId, 'completed', { parent_id: rootTaskId });
                adapter.claimTask(["retention_workflow_root"], "retention-worker");
                adapter.updateTaskStatus(rootTaskId, 'completed');

                directUpdateTaskProperty(adapter, rootTaskId, 'last_active_time', now - 3600);
                directUpdateTaskProperty(adapter, childTaskId, 'last_active_time', now - 3600);

                const cleanup = adapter.cleanupExpiredTasks({ expire_time: 1800, now });
                assert.equal(cleanup.tasks_deleted, 2);

                const workflowAudit = adapter.getWorkflowAudit(rootTaskId);
                assert.equal(workflowAudit.root_task, null);
                assert.equal(workflowAudit.tasks.total, 0);
                assert.equal(workflowAudit.tasks.items.length, 0);
                assert.equal(workflowAudit.events.total, 0);
                assert.equal(workflowAudit.events.items.length, 0);

                const taskAudit = adapter.getTaskAudit(rootTaskId);
                assert.equal(taskAudit.task, null);
                assert.equal(taskAudit.events.total, 0);
                assert.equal(taskAudit.attempts.total, 0);
            });

            it('should validate direct checkpoint and progress payloads', () => {
                const taskId = adapter.insertTask({
                    name: 'validation_task',
                    type: 'async'
                });

                assert.throws(() => {
                    adapter.recordTaskCheckpoint(taskId, { code: 'Bad Code' });
                }, /snake_case/);

                assert.throws(() => {
                    adapter.recordTaskProgress(taskId, { stage_name: 'Bad Stage' });
                }, /snake_case/);

                assert.throws(() => {
                    adapter.recordTaskProgress(taskId, { progress_text: '   ' });
                }, /must not be empty/);
            });
        });

        describe("Worker Management", () => {
            it("should set start_time and worker_id when claiming task", () => {
                const taskId = adapter.insertTask({
                    name: "claim_fields_test",
                    type: "async"
                });

                const beforeClaim = adapter.getTask(taskId);
                assert.equal(beforeClaim.start_time, null);
                assert.equal(beforeClaim.worker_id, null);

                const claimed = adapter.claimTask(["claim_fields_test"], "test-worker");
                const afterClaim = adapter.getTask(taskId);

                assert.ok(afterClaim.start_time > 0);
                assert.equal(afterClaim.worker_id, "test-worker");
                assert.ok(afterClaim.last_active_time > 0);
            });

            it("should preserve worker_id when task completes", () => {
                const taskId = adapter.insertTask({
                    name: "complete_worker_test",
                    type: "async"
                });

                adapter.claimTask(["complete_worker_test"], "test-worker");
                adapter.updateTaskStatus(taskId, "completed");

                const task = adapter.getTask(taskId);
                assert.equal(task.worker_id, "test-worker");
                assert.equal(task.status, "completed");
            });

            it("should preserve worker_id when task fails", () => {
                const taskId = adapter.insertTask({
                    name: "fail_worker_test",
                    type: "async"
                });

                adapter.claimTask(["fail_worker_test"], "test-worker");
                adapter.updateTaskStatus(taskId, "failed");

                const task = adapter.getTask(taskId);
                assert.equal(task.worker_id, "test-worker");
                assert.equal(task.status, "failed");
            });
        });

        describe("Task Querying", () => {
            it("should get tasks by status", () => {
                const taskIds = [
                    adapter.insertTask({
                        name: "pending_task",
                        type: "async",
                        status: "pending"
                    }),
                    adapter.insertTask({
                        name: "running_task",
                        type: "async",
                        status: "pending"
                    }),
                    adapter.insertTask({
                        name: "completed_task",
                        type: "async",
                        status: "pending"
                    })
                ];

                adapter.claimTask(["running_task"], "test-worker");

                adapter.claimTask(["completed_task"], "test-worker");
                adapter.updateTaskStatus(taskIds[2], "completed");

                const pendingTasks = adapter.getTasksByStatus("pending");
                assert.equal(pendingTasks.length, 1);
                assert.equal(pendingTasks[0].name, "pending_task");

                const runningTasks = adapter.getTasksByStatus("running");
                assert.equal(runningTasks.length, 1);
                assert.equal(runningTasks[0].name, "running_task");

                const completedTasks = adapter.getTasksByStatus("completed");
                assert.equal(completedTasks.length, 1);
                assert.equal(completedTasks[0].name, "completed_task");
            });

            it("should get tasks by name", () => {
                const taskName = "test_get_by_name";
                adapter.insertTask({
                    name: taskName,
                    type: "async",
                    payload: { data: "test1" }
                });
                adapter.insertTask({
                    name: taskName,
                    type: "async",
                    payload: { data: "test2" }
                });

                const tasks = adapter.getTasksByName(taskName);
                assert.ok(tasks.length >= 2);
                assert.ok(tasks.some(task => task.payload.data === "test1"));
                assert.ok(tasks.some(task => task.payload.data === "test2"));
            });

            it("should return empty array for non-existent task name", () => {
                const tasks = adapter.getTasksByName("non_existent_task");
                assert.ok(Array.isArray(tasks));
                assert.equal(tasks.length, 0);
            });

            it("should throw error for invalid status in getTasksByStatus", () => {
                assert.throws(() => {
                    adapter.getTasksByStatus("invalid_status");
                }, /Invalid status value/);
            });

            it("should throw error for missing name in getTasksByName", () => {
                assert.throws(() => {
                    adapter.getTasksByName();
                }, /Task name is required/);
            });

            it("should filter tasks with multiple conditions", () => {
                // Create test tasks with various combinations
                adapter.insertTask({
                    name: "filter_task1",
                    type: "async",
                    tag: "filter_tag",
                    status: "pending",
                    payload: { data: "test1" }
                });

                adapter.insertTask({
                    name: "filter_task1",
                    type: "async",
                    tag: "filter_tag",
                    status: "completed",
                    payload: { data: "test2" }
                });

                adapter.insertTask({
                    name: "filter_task2",
                    type: "async",
                    tag: "other_tag",
                    status: "pending",
                    payload: { data: "test3" }
                });

                // Test single filter
                let tasks = adapter.getTasks({ tag: "filter_tag" });
                assert.equal(tasks.length, 2);
                assert.ok(tasks.every(task => task.tag === "filter_tag"));

                // Test multiple filters
                tasks = adapter.getTasks({
                    tag: "filter_tag",
                    status: "pending",
                    name: "filter_task1"
                });
                assert.equal(tasks.length, 1);
                assert.equal(tasks[0].payload.data, "test1");

                // Test no matches
                tasks = adapter.getTasks({
                    tag: "non_existent_tag",
                    status: "pending"
                });
                assert.equal(tasks.length, 0);

                // Test partial matches
                tasks = adapter.getTasks({ name: "filter_task1" });
                assert.equal(tasks.length, 2);
                assert.ok(tasks.every(task => task.name === "filter_task1"));
            });

            it("should handle invalid filter parameters gracefully", () => {
                // Test with empty filters object
                const tasks = adapter.getTasks({});
                assert.ok(Array.isArray(tasks));

                // Test with invalid status
                assert.throws(() => {
                    adapter.getTasks({ status: "invalid_status" });
                }, /Invalid status value/);

                if (!is_postgres) {
                    // Test with non-string values
                    const result = adapter.getTasks({
                        tag: 123,
                        name: true
                    });
                    assert.ok(Array.isArray(result));
                }
            });

            it("should delete tasks with filter conditions", () => {
                // Create test tasks with various combinations
                adapter.insertTask({
                    name: "delete_task1",
                    type: "async",
                    tag: "delete_tag",
                    status: "pending",
                    payload: { data: "test1" }
                });

                adapter.insertTask({
                    name: "delete_task1",
                    type: "async",
                    tag: "delete_tag",
                    status: "completed",
                    payload: { data: "test2" }
                });

                adapter.insertTask({
                    name: "delete_task2",
                    type: "async",
                    tag: "other_tag",
                    status: "pending",
                    payload: { data: "test3" }
                });

                // Test delete by tag
                let deletedCount = adapter.deleteTasks({ tag: "delete_tag" });
                assert.equal(deletedCount, 2);
                let remainingTasks = adapter.getTasks({ tag: "delete_tag" });
                assert.equal(remainingTasks.length, 0);

                // Test delete by status
                adapter.insertTask({
                    name: "delete_task3",
                    type: "async",
                    status: "completed",
                    payload: { data: "test4" }
                });
                adapter.insertTask({
                    name: "delete_task4",
                    type: "async",
                    status: "completed",
                    payload: { data: "test5" }
                });

                deletedCount = adapter.deleteTasks({ status: "completed" });
                assert.equal(deletedCount, 2);
                remainingTasks = adapter.getTasks({ status: "completed" });
                assert.equal(remainingTasks.length, 0);

                // Test delete by name
                adapter.insertTask({
                    name: "delete_task5",
                    type: "async",
                    payload: { data: "test6" }
                });
                adapter.insertTask({
                    name: "delete_task5",
                    type: "async",
                    payload: { data: "test7" }
                });

                deletedCount = adapter.deleteTasks({ name: "delete_task5" });
                assert.equal(deletedCount, 2);
                remainingTasks = adapter.getTasks({ name: "delete_task5" });
                assert.equal(remainingTasks.length, 0);

                const auditedTaskId = adapter.insertTask({
                    name: "delete_audit_task",
                    type: "async",
                    tag: "delete_audit_tag"
                });
                adapter.claimTask(["delete_audit_task"], "audit-delete-worker");
                adapter.updateTaskStatus(auditedTaskId, "completed");

                assert.ok(adapter.getTaskEvents(auditedTaskId).length > 0);
                assert.equal(adapter.getTaskAttempts(auditedTaskId).length, 1);

                deletedCount = adapter.deleteTasks({ name: "delete_audit_task" });
                assert.equal(deletedCount, 1);
                assert.equal(adapter.getTaskEvents(auditedTaskId).length, 0);
                assert.equal(adapter.getTaskAttempts(auditedTaskId).length, 0);

                // Test delete with multiple conditions
                adapter.insertTask({
                    name: "multi_delete",
                    type: "async",
                    tag: "multi_tag",
                    status: "pending",
                    payload: { data: "test8" }
                });
                adapter.insertTask({
                    name: "multi_delete",
                    type: "async",
                    tag: "multi_tag",
                    status: "completed",
                    payload: { data: "test9" }
                });

                deletedCount = adapter.deleteTasks({
                    name: "multi_delete",
                    tag: "multi_tag",
                    status: "pending"
                });
                assert.equal(deletedCount, 1);
                remainingTasks = adapter.getTasks({ name: "multi_delete" });
                assert.equal(remainingTasks.length, 1);
                assert.equal(remainingTasks[0].status, "completed");
            });

            it("should handle invalid delete parameters gracefully", () => {
                // Test with empty filters object
                const deletedCount = adapter.deleteTasks({});
                assert.ok(typeof deletedCount === 'number');

                // Test with invalid status
                assert.throws(() => {
                    adapter.deleteTasks({ status: "invalid_status" });
                }, /Invalid status value/);

                if (!is_postgres) {
                    // Test with non-string values
                    const result = adapter.deleteTasks({
                        tag: 123,
                        name: true
                    });
                    assert.ok(typeof result === 'number');
                }
            });

            it("should clear attempts together with tasks and events", () => {
                const taskId = adapter.insertTask({
                    name: "clear_audit_task",
                    type: "async"
                });

                adapter.claimTask(["clear_audit_task"], "clear-worker");
                adapter.updateTaskStatus(taskId, "completed");

                assert.ok(adapter.getTaskEvents(taskId).length > 0);
                assert.equal(adapter.getTaskAttempts(taskId).length, 1);

                adapter.clearTasks();

                assert.equal(adapter.getTasks({}).length, 0);
                assert.equal(adapter.getTaskEvents(taskId).length, 0);
                assert.equal(adapter.getTaskAttempts(taskId).length, 0);
            });
        });

        describe("Priority and Scheduling", () => {
            it("should respect task priority", () => {
                adapter.insertTask({
                    name: "high_priority",
                    type: "async",
                    priority: 10
                });

                adapter.insertTask({
                    name: "low_priority",
                    type: "async",
                    priority: 1
                });

                const claimed = adapter.claimTask(["high_priority", "low_priority"], "test-worker");
                assert.equal(claimed.name, "high_priority");
            });

            it("should handle cron task", () => {
                const now = Math.floor(Date.now() / 1000);
                const taskId = adapter.insertTask({
                    name: "cron_test",
                    type: "cron",
                    cron_expr: "*/5 * * * *",
                    payload: { data: "cron" },
                    next_run_time: now + 300
                });

                const task = adapter.getTask(taskId);
                assert.equal(task.type, "cron");
                assert.equal(task.cron_expr, "*/5 * * * *");
                assert.equal(task.next_run_time, now + 300);
            });

            it("should prioritize tasks by priority and execution time", () => {
                const now = Math.floor(Date.now() / 1000);
                adapter.insertTask({
                    name: "early_low_priority",
                    type: "async",
                    priority: 1,
                    next_run_time: now - 100
                });
                adapter.insertTask({
                    name: "high_priority_on_time",
                    type: "async",
                    priority: 10,
                    next_run_time: now
                });
                adapter.insertTask({
                    name: "medium_priority_late",
                    type: "async",
                    priority: 5,
                    next_run_time: now + 100
                });
                const claimed = adapter.claimTask([
                    "early_low_priority",
                    "high_priority_on_time",
                    "medium_priority_late"
                ], "test-worker");
                assert.equal(claimed.name, "high_priority_on_time");
            });
        });

        describe("Error Handling", () => {
            xit("should handle invalid JSON payload", () => {
                adapter.pool(conn => {
                    conn.execute(
                        `INSERT INTO fib_flow_tasks (name, type, status, next_run_time, payload) 
                    VALUES ('invalid_json', 'async', 'pending', 0, '{invalid}')`
                    );
                });

                const tasks = adapter.getTasksByName("invalid_json");
                assert.ok(tasks.length > 0);
                assert.equal(tasks[0].payload, '{invalid}');
            });

            it("should handle connection pool timeout", () => {
                const smallPoolAdapter = createAdapter(config.dbConnection, 1);
                smallPoolAdapter.setup();
                smallPoolAdapter.clearTasks();

                const fibers = [];
                for (let i = 0; i < 5; i++) {
                    fibers.push(coroutine.start(() => {
                        try {
                            smallPoolAdapter.insertTask({
                                name: `concurrent_test_${i}`,
                                type: "async"
                            });
                            coroutine.sleep(100); // 
                        } catch (e) {
                            assert.ok(e.message.includes('timeout') || e.message.includes('busy'));
                        }
                    }));
                }

                fibers.forEach(fiber => fiber.join());
                smallPoolAdapter.close();
            });
        });

        describe("Task Tags", () => {
            it("should create task with tag", () => {
                const taskId = adapter.insertTask({
                    name: "tagged_task",
                    type: "async",
                    tag: "test_tag",
                    payload: { data: "test" }
                });

                const task = adapter.getTask(taskId);
                assert.equal(task.tag, "test_tag");
            });

            it("should get tasks by tag", () => {
                // Create multiple tasks with same tag
                adapter.insertTask({
                    name: "task1",
                    type: "async",
                    tag: "common_tag",
                    payload: { data: "test1" }
                });

                adapter.insertTask({
                    name: "task2",
                    type: "async",
                    tag: "common_tag",
                    payload: { data: "test2" }
                });

                adapter.insertTask({
                    name: "task3",
                    type: "async",
                    tag: "other_tag",
                    payload: { data: "test3" }
                });

                const taggedTasks = adapter.getTasksByTag("common_tag");
                assert.equal(taggedTasks.length, 2);
                assert.ok(taggedTasks.every(task => task.tag === "common_tag"));
                assert.ok(taggedTasks.some(task => task.name === "task1"));
                assert.ok(taggedTasks.some(task => task.name === "task2"));
            });

            it("should get task statistics by tag", () => {
                // Create tasks with different tags and statuses
                adapter.insertTask({
                    name: "stats_task1",
                    type: "async",
                    tag: "stats_tag",
                    status: "pending",
                    payload: { data: "test1" }
                });

                adapter.insertTask({
                    name: "stats_task1",
                    type: "async",
                    tag: "stats_tag",
                    status: "pending",
                    payload: { data: "test2" }
                });

                adapter.insertTask({
                    name: "stats_task2",
                    type: "async",
                    tag: "stats_tag",
                    status: "completed",
                    payload: { data: "test3" }
                });

                // Get stats for specific tag
                const tagStats = adapter.getTaskStatsByTag("stats_tag");
                assert.ok(Array.isArray(tagStats));
                assert.ok(tagStats.length > 0);

                // Verify stats structure and counts
                const pendingStats = tagStats.find(stat =>
                    stat.tag === "stats_tag" &&
                    stat.name === "stats_task1" &&
                    stat.status === "pending"
                );
                assert.ok(pendingStats);
                assert.equal(pendingStats.count, 2);

                const completedStats = tagStats.find(stat =>
                    stat.tag === "stats_tag" &&
                    stat.name === "stats_task2" &&
                    stat.status === "completed"
                );
                assert.ok(completedStats);
                assert.equal(completedStats.count, 1);

                // Get stats with status filter
                const pendingTagStats = adapter.getTaskStatsByTag("stats_tag", "pending");
                assert.ok(Array.isArray(pendingTagStats));
                assert.equal(pendingTagStats.length, 1);
                assert.equal(pendingTagStats[0].count, 2);
            });

            it("should throw error for missing tag in getTasksByTag", () => {
                assert.throws(() => {
                    adapter.getTasksByTag();
                }, /Tag is required/);
            });
        });
    });
});