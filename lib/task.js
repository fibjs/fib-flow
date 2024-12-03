const coroutine = require('coroutine');
const parser = require('cron-parser');
const { createAdapter } = require('./db/index.js');

class TaskManager {
    constructor(options = {}) {
        // Initialize the database adapter using options
        this.db = createAdapter(options.dbConnection, options.dbType);

        this.options = {
            poll_interval: options.poll_interval || 1000,
            max_retries: options.max_retries || 3,
            retry_interval: options.retry_interval || 300,
            worker_id: options.worker_id || `worker-${Math.random().toString(36).slice(2)}`,
            max_concurrent_tasks: options.max_concurrent_tasks || 10,
            active_update_interval: options.active_update_interval || 1000,
            ...options
        };
        this.handlers = new Map();
        this.state = 'init';  // 初始状态
        this.currentFiber = null;
        this.runningTasks = new Set();
        // 初始化信号量控制并发
        this.semaphore = new coroutine.Semaphore(this.options.max_concurrent_tasks);
        this.event = new coroutine.Event(true);
        this.activeTimer = null;
    }

    // 注册任务处理器
    use(taskName, handler) {
        if (this.state !== 'init') {
            throw new Error('Can only register handler when TaskManager is in init state');
        }
        this.handlers.set(taskName, handler);
    }

    // 提交异步任务
    async(taskName, payload = {}, options = {}) {
        if (this.state !== 'running') {
            throw new Error('Can only submit task when TaskManager is running');
        }
        if (!this.handlers.has(taskName)) {
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        const now = Math.floor(Date.now() / 1000);
        const delay = options.delay || 0;  // Default delay is 0
        const nextRunTime = now + delay;

        return this.db.insertTask({
            name: taskName,
            type: 'async',
            payload,
            priority: options.priority,
            max_retries: options.max_retries !== undefined ? options.max_retries : this.options.max_retries,
            retry_interval: options.retry_interval,
            timeout: options.timeout,
            next_run_time: nextRunTime
        });
    }

    // 提交定时任务
    cron(taskName, cron_expr, payload = {}, options = {}) {
        if (this.state !== 'running') {
            throw new Error('Can only submit task when TaskManager is running');
        }
        if (!this.handlers.has(taskName)) {
            throw new Error(`No handler registered for task: ${taskName}`);
        }

        // 验证 cron 表达式
        try {
            parser.parseExpression(cron_expr);
        } catch (e) {
            throw new Error(`Invalid cron expression: ${cron_expr}`);
        }

        const nextRunTime = this._getNextRunTime(cron_expr);
        return this.db.insertTask({
            name: taskName,
            type: 'cron',
            cron_expr: cron_expr,
            payload,
            priority: options.priority,
            max_retries: options.max_retries !== undefined ? options.max_retries : this.options.max_retries,
            retry_interval: options.retry_interval,
            timeout: options.timeout,
            next_run_time: nextRunTime
        });
    }

    resumeTask(taskId) {
        if (this.state !== 'running') {
            throw new Error('Can only resume task when TaskManager is running');
        }

        const task = this.db.getTask(taskId);
        if (!task) {
            throw new Error('Task not found');
        }

        if (task.status !== 'paused') {
            throw new Error('Can only resume paused tasks');
        }

        // 重置重试次数，重新计算下次运行时间
        const now = Math.floor(Date.now() / 1000);
        return this.db.updateTaskStatus(taskId, 'pending', {
            retry_count: 0,
            next_run_time: now
        });
    }

    // 计算下次运行时间
    _getNextRunTime(cron_expr) {
        const interval = parser.parseExpression(cron_expr);
        return Math.floor(interval.next().getTime() / 1000);
    }

    // 执行任务
    _executeTask(task) {
        // 将任务添加到运行中的任务集合
        this.runningTasks.add(task.id);

        console.log(`Executing task: ${task.name}(${task.id}), Payload:`, task.payload);

        // 记录任务开始时间
        const startTime = Date.now();

        // 绑定检查超时方法到任务对象
        task.checkTimeout = function () {
            if (this.timeout && (Date.now() - startTime) >= (this.timeout * 1000)) {
                throw new Error('Task execution timeout');
            }
        };

        // 创建任务 fiber
        coroutine.start(async () => {
            try {
                // 执行任务处理器
                const result = await this.handlers.get(task.name)(task);
                if (this.state !== 'running') {
                    return;
                }

                // 更新任务状态为完成
                if (task.type === 'cron')
                    this.db.updateTaskStatus(task.id, 'pending', { result, next_run_time: this._getNextRunTime(task.cron_expr) });
                else
                    this.db.updateTaskStatus(task.id, 'completed', { result });
            } catch (error) {
                console.error('Error executing task:', error);
                // 更新任务状态为失败
                const status = error.message.includes('timeout') ? 'timeout' : 'failed';
                this.db.updateTaskStatus(task.id, status, { error: error.message || String(error) });
            } finally {
                // 释放信号量
                this.semaphore.release();
                // 从运行中的任务集合中移除
                this.runningTasks.delete(task.id);
            }
        });
    }

    // 启动任务处理循环
    start() {
        if (this.state === 'running') {
            return;
        }
        if (this.state === 'stopped') {
            throw new Error('Cannot restart a stopped TaskManager');
        }

        this.state = 'running';
        this.runningTasks = new Set();

        // 创建全局活动时间更新定时器
        this.activeTimer = setInterval(() => {
            // 更新所有运行中任务的活动时间
            if (this.runningTasks.size > 0) {
                this.db.updateTaskActiveTime(Array.from(this.runningTasks));
            }
            this.db.handleTimeoutTasks();
        }, this.options.active_update_interval);

        coroutine.start(() => {
            while (this.state === 'running') {
                // 先获取信号量，确保有可用的 worker
                this.semaphore.acquire();
                if (this.state !== 'running')
                    break;

                this.event.wait();

                // 认领任务，只获取已注册 handler 的任务
                const task = this.db.claimTask(Array.from(this.handlers.keys()));
                if (!task) {
                    // 如果没有任务，释放信号量并返回
                    this.semaphore.release();
                    coroutine.sleep(this.options.poll_interval);
                    continue;
                }

                // 执行任务
                this._executeTask(task);
            }
        });
    }

    pause() {
        if (this.state !== 'running') {
            return;
        }
        this.event.clear();
    }

    resume() {
        if (this.state !== 'running') {
            return;
        }
        this.event.set();
    }

    // 停止任务处理
    stop() {
        if (this.state !== 'running') {
            return;
        }
        this.state = 'stopped';

        // 等待所有正在运行的任务完成
        while (this.runningTasks.size > 0) {
            coroutine.sleep(100);
        }

        // 清理定时器
        if (this.activeTimer) {
            clearInterval(this.activeTimer);
            this.activeTimer = null;
        }

        if (this.db) {
            this.db.close();
            this.db = null;
        }
    }

    getTask(taskId) {
        return this.db.getTask(taskId);
    }

    getTasksByName(name) {
        return this.db.getTasksByName(name);
    }

    getTasksByStatus(status) {
        return this.db.getTasksByStatus(status);
    }
}

module.exports = TaskManager;
