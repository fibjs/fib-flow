const test = require('test');
test.setup();

const coroutine = require('coroutine');
const { TaskManager } = require('..');
const config = require('./config.js');

describe("Workflow Tests", () => {
    let taskManager;

    beforeEach(() => {
        // 创建任务管理器实例
        taskManager = new TaskManager({
            dbConnection: config.dbConnection,
            poll_interval: 100,
            max_retries: 2,
            max_concurrent_tasks: 3
        });

        taskManager.db.setup();
        taskManager.db.clearTasks();
    });

    afterEach(() => {
        taskManager.stop();
    });

    it("should handle simple workflow", () => {
        let parentTaskId;

        // 注册父任务处理器
        taskManager.use('parent_task', (task, next) => {
            console.log('Parent task started:', task.id);

            // 第一次执行时创建子任务
            if (!task.completed_children) {
                return next([
                    {
                        name: 'child_task1',
                        payload: { data: 'child1_data' }
                    },
                    {
                        name: 'child_task2',
                        payload: { data: 'child2_data' }
                    }
                ]);
            }

            // 子任务完成后检查结果
            console.log('All child tasks completed');
            return { result: 'parent_done' };
        });

        // 注册子任务处理器
        taskManager.use('child_task1', task => {
            console.log('Child task 1 executing:', task.id);
            return { result: 'child1_result' };
        });

        taskManager.use('child_task2', task => {
            console.log('Child task 2 executing:', task.id);
            return { result: 'child2_result' };
        });

        // 启动任务管理器
        taskManager.start();

        // 提交父任务
        parentTaskId = taskManager.async('parent_task', { data: 'parent_data' });
        console.log('Created parent task:', parentTaskId);

        // 等待任务完成
        while (taskManager.getTask(parentTaskId).status !== 'completed') {
            coroutine.sleep(100);
        }

        // 验证父任务状态和结果
        const parentTask = taskManager.getTask(parentTaskId);
        assert.equal(parentTask.status, 'completed');
        assert.equal(parentTask.result.result, 'parent_done');

        // 验证子任务状态和结果
        const children = taskManager.getChildTasks(parentTaskId);
        assert.equal(children.length, 2);
        assert.equal(children[0].status, 'completed');
        assert.equal(children[0].result.result, 'child1_result');
        assert.equal(children[1].status, 'completed');
        assert.equal(children[1].result.result, 'child2_result');
    });

    it("should handle task failure", () => {
        let parentTaskId;

        // 注册会失败的任务处理器
        taskManager.use('failing_parent', (task, next) => {
            console.log('Failing parent task started:', task.id);

            if (!task.completed_children) {
                return next([
                    {
                        name: 'failing_child',
                        payload: { data: 'will_fail' }
                    }
                ]);
            }

            return { result: 'parent_done' };
        });

        taskManager.use('failing_child', task => {
            console.log('Failing child task executing:', task.id);
            throw new Error('Intentional failure');
        });

        // 启动任务管理器
        taskManager.start();

        // 提交会失败的任务
        parentTaskId = taskManager.async('failing_parent', { data: 'parent_data' });
        console.log('Created failing parent task:', parentTaskId);

        // 等待任务完成
        while (taskManager.getTask(parentTaskId).status !== 'permanently_failed') {
            coroutine.sleep(100);
        }

        // 验证父任务状态
        const parentTask = taskManager.getTask(parentTaskId);
        assert.equal(parentTask.status, 'permanently_failed');

        // 验证子任务失败状态
        const children = taskManager.getChildTasks(parentTaskId);
        assert.equal(children.length, 1);
        assert.equal(children[0].status, 'permanently_failed');
        assert.equal(children[0].error, 'Intentional failure');
    });

    it("should handle nested workflows", () => {
        let rootTaskId;

        // 注册多层嵌套的任务处理器
        taskManager.use('root_task', (task, next) => {
            console.log('Root task started:', task.id);

            if (!task.completed_children) {
                return next([
                    {
                        name: 'middle_task',
                        payload: { level: 1 }
                    }
                ]);
            }

            return { result: 'root_done' };
        });

        taskManager.use('middle_task', (task, next) => {
            console.log('Middle task started:', task.id);

            if (!task.completed_children) {
                return next([
                    {
                        name: 'leaf_task',
                        payload: { level: 2 }
                    }
                ]);
            }

            return { result: 'middle_done' };
        });

        taskManager.use('leaf_task', task => {
            console.log('Leaf task executing:', task.id);
            return { result: 'leaf_done' };
        });

        // 启动任务管理器
        taskManager.start();

        // 提交根任务
        rootTaskId = taskManager.async('root_task', { data: 'root_data' });
        console.log('Created root task:', rootTaskId);

        // 等待任务完成
        while (taskManager.getTask(rootTaskId).status !== 'completed') {
            coroutine.sleep(100);
        }

        // 验证整个工作流的状态和结果
        const rootTask = taskManager.getTask(rootTaskId);
        assert.equal(rootTask.status, 'completed');
        assert.equal(rootTask.result.result, 'root_done');

        const middleTasks = taskManager.getChildTasks(rootTaskId);
        assert.equal(middleTasks.length, 1);
        assert.equal(middleTasks[0].status, 'completed');
        assert.equal(middleTasks[0].result.result, 'middle_done');

        const leafTasks = taskManager.getChildTasks(middleTasks[0].id);
        assert.equal(leafTasks.length, 1);
        assert.equal(leafTasks[0].status, 'completed');
        assert.equal(leafTasks[0].result.result, 'leaf_done');
    });
});
