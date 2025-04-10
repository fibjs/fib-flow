const test = require('test');
test.setup();

const db = require('db');
const coroutine = require('coroutine');
const { createAdapter, TaskManager } = require('..');
const Pool = require('fib-pool');
const config = require('./config.js');

function createDBConn() {
    return db.open(config.dbConnection);
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