/**
 * Memory-based database adapter for high-performance task processing
 * Provides in-memory storage with optimized indexing for fast queries
 * Fully compatible with BaseDBAdapter interface
 */

const BaseDBAdapter = require('./base');
const createLogger = require('../logger');
const { parseTask } = require('./util');

// Create logger for memory adapter operations
const logger = createLogger('fib-flow');

/**
 * Memory-based database adapter with performance optimizations
 * Uses native JavaScript data structures and indexing for fast operations
 */
class MemoryAdapter extends BaseDBAdapter {
    /**
     * Initialize memory adapter with optimized data structures
     * @param {*} conn - Connection parameter (ignored for memory adapter)
     * @param {number} [poolSize=5] - Pool size (ignored for memory adapter)
     */
    constructor(conn, poolSize = 5) {
        // Create a mock pool function for compatibility
        const mockPool = (callback) => callback(this);
        mockPool.clear = () => { };

        // Initialize with mock connection
        super(mockPool, poolSize);

        logger.info(`[MemoryAdapter] Initializing memory-based storage`);

        // Primary storage for tasks
        this.tasks = new Map();
        this.nextId = 1;

        // Performance optimization indexes
        this.indexes = {
            // Fast status lookups
            byStatus: new Map(),
            // Fast name lookups  
            byName: new Map(),
            // Fast tag lookups
            byTag: new Map(),
            // Fast parent-child relationships
            byParent: new Map(),
            // Time-based indexes for scheduling
            byNextRunTime: new Map(),
            // Worker assignment tracking
            byWorker: new Map()
        };

        logger.info(`[MemoryAdapter] Memory storage initialized with optimized indexes`);
    }

    /**
     * Setup method for compatibility - memory storage is ready immediately
     */
    setup() {
        logger.info(`[MemoryAdapter] Memory storage setup complete`);
        // No setup needed for memory storage
    }

    /**
     * Add task to indexes for fast lookups
     * @private
     * @param {object} task - Task to index
     */
    _addToIndexes(task) {
        const { id, status, name, tag, parent_id, next_run_time, worker_id } = task;

        // Status index
        if (!this.indexes.byStatus.has(status)) {
            this.indexes.byStatus.set(status, new Set());
        }
        this.indexes.byStatus.get(status).add(id);

        // Name index
        if (!this.indexes.byName.has(name)) {
            this.indexes.byName.set(name, new Set());
        }
        this.indexes.byName.get(name).add(id);

        // Tag index
        if (tag) {
            if (!this.indexes.byTag.has(tag)) {
                this.indexes.byTag.set(tag, new Set());
            }
            this.indexes.byTag.get(tag).add(id);
        }

        // Parent index
        if (parent_id) {
            if (!this.indexes.byParent.has(parent_id)) {
                this.indexes.byParent.set(parent_id, new Set());
            }
            this.indexes.byParent.get(parent_id).add(id);
        }

        // Time index for scheduling
        if (next_run_time) {
            if (!this.indexes.byNextRunTime.has(next_run_time)) {
                this.indexes.byNextRunTime.set(next_run_time, new Set());
            }
            this.indexes.byNextRunTime.get(next_run_time).add(id);
        }

        // Worker index
        if (worker_id) {
            if (!this.indexes.byWorker.has(worker_id)) {
                this.indexes.byWorker.set(worker_id, new Set());
            }
            this.indexes.byWorker.get(worker_id).add(id);
        }
    }

    /**
     * Remove task from indexes
     * @private
     * @param {object} oldTask - Task to remove from indexes
     */
    _removeFromIndexes(oldTask) {
        const { id, status, name, tag, parent_id, next_run_time, worker_id } = oldTask;

        // Remove from status index
        if (this.indexes.byStatus.has(status)) {
            this.indexes.byStatus.get(status).delete(id);
            if (this.indexes.byStatus.get(status).size === 0) {
                this.indexes.byStatus.delete(status);
            }
        }

        // Remove from name index
        if (this.indexes.byName.has(name)) {
            this.indexes.byName.get(name).delete(id);
            if (this.indexes.byName.get(name).size === 0) {
                this.indexes.byName.delete(name);
            }
        }

        // Remove from tag index
        if (tag && this.indexes.byTag.has(tag)) {
            this.indexes.byTag.get(tag).delete(id);
            if (this.indexes.byTag.get(tag).size === 0) {
                this.indexes.byTag.delete(tag);
            }
        }

        // Remove from parent index
        if (parent_id && this.indexes.byParent.has(parent_id)) {
            this.indexes.byParent.get(parent_id).delete(id);
            if (this.indexes.byParent.get(parent_id).size === 0) {
                this.indexes.byParent.delete(parent_id);
            }
        }

        // Remove from time index
        if (next_run_time && this.indexes.byNextRunTime.has(next_run_time)) {
            this.indexes.byNextRunTime.get(next_run_time).delete(id);
            if (this.indexes.byNextRunTime.get(next_run_time).size === 0) {
                this.indexes.byNextRunTime.delete(next_run_time);
            }
        }

        // Remove from worker index
        if (worker_id && this.indexes.byWorker.has(worker_id)) {
            this.indexes.byWorker.get(worker_id).delete(id);
            if (this.indexes.byWorker.get(worker_id).size === 0) {
                this.indexes.byWorker.delete(worker_id);
            }
        }
    }

    /**
     * Update indexes when task properties change
     * @private
     * @param {object} oldTask - Task before update
     * @param {object} newTask - Task after update
     */
    _updateIndexes(oldTask, newTask) {
        this._removeFromIndexes(oldTask);
        this._addToIndexes(newTask);
    }

    /**
     * Override the getLastInsertedId method for memory adapter
     * @protected
     * @param {*} conn - Connection (unused)
     * @param {*} rs - Result set (unused)
     * @returns {number} The ID of the last inserted task
     */
    _getLastInsertedId(conn, rs) {
        return this.nextId - 1;
    }

    /**
     * Insert task(s) into memory storage with transaction-like behavior
     * @param {object|Array<object>} tasks - Task(s) to insert
     * @param {object} [options] - Insert options
     * @returns {number|Array<number>} Inserted task ID(s)
     */
    insertTask(tasks, options = {}) {
        logger.info(`[MemoryAdapter] Inserting tasks with options:`, options);

        const isArray = Array.isArray(tasks);
        const taskArray = isArray ? tasks : [tasks];
        const taskIds = [];
        const now = Math.floor(Date.now() / 1000);

        try {
            // Update parent task if specified
            if (options.parent_id) {
                logger.info(`[MemoryAdapter] Updating parent task ${options.parent_id}`);

                const parentTask = this.tasks.get(options.parent_id);
                if (!parentTask || parentTask.status !== 'running') {
                    throw new Error(`Parent task ${options.parent_id} is not in running state`);
                }

                const oldParent = { ...parentTask };
                parentTask.total_children += taskArray.length;
                parentTask.status = 'suspended';
                parentTask.result = null;

                if (options.context !== undefined) {
                    parentTask.context = options.context;
                }

                this._updateIndexes(oldParent, parentTask);
            }

            // Insert all tasks
            for (const task of taskArray) {
                // Validate task
                if (!task) {
                    throw new Error('Task object is required');
                }
                if (!task.name) {
                    throw new Error('Task name is required');
                }
                if (!task.type) {
                    throw new Error('Task type is required');
                }
                if (options.parent_id && task.type !== 'async') {
                    throw new Error('Parent tasks can only be of type "async"');
                }
                if (!['async', 'cron'].includes(task.type)) {
                    throw new Error('Task type must be either "async" or "cron"');
                }

                const taskId = this.nextId++;
                const newTask = {
                    id: taskId,
                    name: task.name,
                    type: task.type,
                    status: task.status || 'pending',
                    priority: task.priority || 0,
                    payload: task.payload ? JSON.stringify(task.payload) : null,
                    cron_expr: task.cron_expr || null,
                    max_retries: task.max_retries !== undefined ? task.max_retries : 3,
                    retry_interval: task.retry_interval || 0,
                    retry_count: 0,
                    next_run_time: task.next_run_time || now,
                    timeout: task.timeout || 60,
                    created_at: now,
                    last_active_time: null,
                    start_time: null,
                    worker_id: null,
                    root_id: options.root_id || null,
                    parent_id: options.parent_id || null,
                    total_children: 0,
                    completed_children: 0,
                    tag: task.tag || null,
                    result: null,
                    error: null,
                    context: null,
                    stage: 0
                };

                this.tasks.set(taskId, newTask);
                this._addToIndexes(newTask);
                taskIds.push(taskId);

                logger.info(`[MemoryAdapter] Inserted task ${taskId} with name: ${task.name}`);
            }

            return isArray ? taskIds : taskIds[0];

        } catch (error) {
            logger.error(`[MemoryAdapter] Error inserting tasks:`, error);
            throw error;
        }
    }

    /**
     * Claim next available task with optimized lookup
     * @param {Array<string>} taskNames - Eligible task names
     * @param {string} workerId - Worker ID claiming the task
     * @returns {object|null} Claimed task or null
     */
    claimTask(taskNames, workerId) {
        logger.info(`[MemoryAdapter] Attempting to claim task for worker ${workerId}, names:`, taskNames);

        // Validate worker ID
        if (!workerId || workerId.trim() === '') {
            throw new Error('Worker ID is required');
        }

        if (!Array.isArray(taskNames) || taskNames.length === 0) {
            return null;
        }

        const now = Math.floor(Date.now() / 1000);

        // Get pending task IDs using index
        const pendingIds = this.indexes.byStatus.get('pending') || new Set();

        // Filter by task names and run time, then sort by priority and time
        const eligibleTasks = [];

        for (const taskId of pendingIds) {
            const task = this.tasks.get(taskId);
            if (task &&
                taskNames.includes(task.name) &&
                task.next_run_time <= now) {
                eligibleTasks.push(task);
            }
        }

        // Sort by priority (DESC) and next_run_time (ASC)
        eligibleTasks.sort((a, b) => {
            if (a.priority !== b.priority) {
                return b.priority - a.priority;
            }
            return a.next_run_time - b.next_run_time;
        });

        if (eligibleTasks.length === 0) {
            logger.debug(`[MemoryAdapter] No eligible tasks found`);
            return null;
        }

        const task = eligibleTasks[0];
        const oldTask = { ...task };

        // Update task status
        task.status = 'running';
        task.last_active_time = now;
        task.worker_id = workerId;
        task.start_time = now;

        this._updateIndexes(oldTask, task);

        logger.info(`[MemoryAdapter] Successfully claimed task ${task.id}`);

        // Parse task using the utility function for complete processing
        const clonedTask = { ...task };
        return parseTask(clonedTask);
    }

    /**
     * Update task status with optimized index updates
     * @param {string|number} taskId - Task ID
     * @param {string} status - New status
     * @param {object} extra - Additional fields to update
     */
    updateTaskStatus(taskId, status, extra = {}) {
        logger.info(`[MemoryAdapter] Updating task ${taskId} to status '${status}'`);

        const task = this.tasks.get(taskId);
        if (!task) {
            throw new Error(`Task ${taskId} not found`);
        }

        // Validate status transition
        const allowedPreviousStatuses = {
            'running': ['pending'],
            'completed': ['running'],
            'failed': ['running'],
            'timeout': ['running'],
            'pending': ['running', 'failed', 'timeout', 'paused', 'suspended'],
            'permanently_failed': ['failed', 'timeout'],
            'paused': ['running', 'pending', 'failed', 'timeout'],
            'suspended': ['running']
        };

        if (!allowedPreviousStatuses[status]) {
            throw new Error('Invalid status value');
        }

        if (!allowedPreviousStatuses[status].includes(task.status)) {
            throw new Error(`Invalid status transition from ${task.status} to ${status}`);
        }

        const oldTask = { ...task };
        const now = Math.floor(Date.now() / 1000);

        // Update task properties
        task.status = status;
        task.last_active_time = now;

        if ('result' in extra) {
            task.result = extra.result ? JSON.stringify(extra.result) : null;
        }
        if ('error' in extra) {
            task.error = extra.error;
        }
        if (status === 'pending') {
            task.stage = 0;
        }
        if ('next_run_time' in extra) {
            task.next_run_time = extra.next_run_time;
        }
        if ('retry_count' in extra) {
            task.retry_count = extra.retry_count;
        }

        this._updateIndexes(oldTask, task);

        // Handle parent task updates
        if (extra.parent_id && status === 'completed') {
            const parentTask = this.tasks.get(extra.parent_id);
            if (parentTask) {
                const oldParent = { ...parentTask };
                parentTask.completed_children += 1;

                // Update parent result
                const resultJson = task.result || 'null';
                const resultEntry = `${taskId}:${resultJson}\n`;
                parentTask.result = (parentTask.result || '') + resultEntry;

                // Check if all children completed
                if (parentTask.completed_children === parentTask.total_children) {
                    parentTask.status = 'pending';
                    parentTask.stage += 1;
                }

                this._updateIndexes(oldParent, parentTask);
            }
        }

        logger.info(`[MemoryAdapter] Successfully updated task ${taskId}`);
    }

    /**
     * Update active time for multiple tasks efficiently
     * @param {Array<string|number>} taskIds - Task IDs to update
     */
    updateTaskActiveTime(taskIds) {
        if (!Array.isArray(taskIds) || taskIds.length === 0) {
            return;
        }

        const now = Math.floor(Date.now() / 1000);

        for (const taskId of taskIds) {
            const task = this.tasks.get(taskId);
            if (task) {
                task.last_active_time = now;
            }
        }

        logger.debug(`[MemoryAdapter] Updated active time for ${taskIds.length} tasks`);
    }

    /**
     * Handle timeout tasks with efficient processing
     * @param {number} active_update_interval - Active update interval in ms
     * @param {number} [expireTime] - Expire time in seconds
     */
    handleTimeoutTasks(active_update_interval, expireTime = null) {
        logger.info(`[MemoryAdapter] Handling timeout tasks`);

        const now = Math.floor(Date.now() / 1000);
        const heartbeatThreshold = now - Math.floor(active_update_interval * 5 / 1000);

        let totalTimeoutCount = 0;
        let heartbeatTimeoutCount = 0;
        let retryCount = 0;
        let pausedCount = 0;
        let permanentlyFailedCount = 0;
        let expiredCount = 0;

        // Process running tasks for timeouts
        const runningIds = this.indexes.byStatus.get('running') || new Set();

        for (const taskId of runningIds) {
            const task = this.tasks.get(taskId);
            if (!task) continue;

            const oldTask = { ...task };
            let updated = false;

            // Check total timeout
            if (task.start_time && task.start_time + task.timeout < now) {
                task.status = 'timeout';
                task.error = 'Task exceeded total timeout limit';
                task.last_active_time = now;
                updated = true;
                totalTimeoutCount++;
            }
            // Check heartbeat timeout
            else if (task.last_active_time && task.last_active_time < heartbeatThreshold) {
                task.status = 'timeout';
                task.error = 'Task heartbeat lost - worker may be dead';
                task.last_active_time = now;
                updated = true;
                heartbeatTimeoutCount++;
            }

            if (updated) {
                this._updateIndexes(oldTask, task);
            }
        }

        // Handle retry logic for failed/timeout tasks
        const failedIds = new Set([
            ...(this.indexes.byStatus.get('timeout') || []),
            ...(this.indexes.byStatus.get('failed') || [])
        ]);

        for (const taskId of failedIds) {
            const task = this.tasks.get(taskId);
            if (!task) continue;

            const oldTask = { ...task };
            let updated = false;

            if (task.last_active_time + task.retry_interval < now) {
                if (task.retry_count + 1 < task.max_retries) {
                    // Retry the task
                    task.status = 'pending';
                    task.stage = 0;
                    task.result = null;
                    task.context = null;
                    task.retry_count += 1;
                    task.last_active_time = now;
                    task.next_run_time = now + task.retry_interval;
                    updated = true;
                    retryCount++;
                }
            }

            if (updated) {
                this._updateIndexes(oldTask, task);
            }
        }

        const cronFailedIds = new Set([
            ...Array.from(this.indexes.byStatus.get('timeout') || []),
            ...Array.from(this.indexes.byStatus.get('failed') || [])
        ]);

        for (const taskId of cronFailedIds) {
            const task = this.tasks.get(taskId);
            if (!task || task.retry_count + 1 < task.max_retries || task.type !== 'cron') continue;

            const oldTask = { ...task };
            task.status = 'paused';
            task.last_active_time = now;
            this._updateIndexes(oldTask, task);
            pausedCount++;
        }

        const exhaustedRetryTaskIds = new Set([
            ...(this.indexes.byStatus.get('timeout') || []),
            ...(this.indexes.byStatus.get('failed') || [])
        ]);

        // Handle async tasks with no retries left - set to permanently_failed and update parent
        const asyncFailedTasks = [];

        // Collect failed async tasks
        for (const taskId of exhaustedRetryTaskIds) {
            const task = this.tasks.get(taskId);
            if (!task || task.retry_count + 1 < task.max_retries || task.type !== 'async') continue;
            asyncFailedTasks.push(task);
        }

        // Process each failed async task
        for (const task of asyncFailedTasks) {
            const oldTask = { ...task };
            task.status = 'permanently_failed';
            task.last_active_time = now;
            this._updateIndexes(oldTask, task);
            permanentlyFailedCount++;

            // Handle parent task update
            if (task.parent_id) {
                const parentTask = this.tasks.get(task.parent_id);
                if (parentTask && parentTask.status === 'suspended') {
                    const oldParent = { ...parentTask };
                    
                    // Update parent completed children count
                    parentTask.completed_children += 1;
                    
                    // Append error result to parent result
                    const errorEntry = `${task.id}!${task.error || 'Task permanently failed'}\n`;
                    parentTask.result = (parentTask.result || '') + errorEntry;
                    
                    // Check if all children completed and if so, wake up parent task
                    if (parentTask.completed_children === parentTask.total_children) {
                        parentTask.status = 'pending';
                        parentTask.stage += 1;
                    }
                    
                    this._updateIndexes(oldParent, parentTask);
                }
            }
        }

        // Clean up expired tasks
        if (expireTime) {
            const expireThreshold = now - expireTime;
            const toDelete = [];

            for (const [taskId, task] of this.tasks) {
                if ((task.status === 'completed' || task.status === 'permanently_failed') &&
                    task.last_active_time < expireThreshold) {
                    toDelete.push(taskId);
                }
            }

            for (const taskId of toDelete) {
                const task = this.tasks.get(taskId);
                if (task) {
                    this._removeFromIndexes(task);
                    this.tasks.delete(taskId);
                    expiredCount++;
                }
            }
        }

        if (totalTimeoutCount || heartbeatTimeoutCount || retryCount ||
            pausedCount || permanentlyFailedCount || expiredCount) {
            logger.info(`[MemoryAdapter] Timeout handling complete: ` +
                `total_timeout=${totalTimeoutCount}, heartbeat_timeout=${heartbeatTimeoutCount}, ` +
                `retry=${retryCount}, paused=${pausedCount}, permanently_failed=${permanentlyFailedCount}, ` +
                `expired=${expiredCount}`);
        }
    }

    /**
     * Get task by ID with optimized lookup
     * @param {string|number} taskId - Task ID
     * @returns {object|null} Task object or null
     */
    getTask(taskId) {
        logger.debug(`[MemoryAdapter] Getting task: ${taskId}`);
        const task = this.tasks.get(taskId);
        return task ? parseTask({ ...task }) : null;
    }

    /**
     * Get tasks by name using index for fast lookup
     * @param {string} name - Task name
     * @returns {Array<object>} Array of tasks
     */
    getTasksByName(name) {
        logger.debug(`[MemoryAdapter] Getting tasks by name: ${name}`);

        if (!name) {
            throw new Error('Task name is required');
        }

        const taskIds = this.indexes.byName.get(name) || new Set();
        const tasks = [];

        for (const taskId of taskIds) {
            const task = this.tasks.get(taskId);
            if (task) {
                tasks.push(parseTask({ ...task }));
            }
        }

        return tasks;
    }

    /**
     * Get tasks by status using index for fast lookup
     * @param {string} status - Status to filter by
     * @returns {Array<object>} Array of tasks
     */
    getTasksByStatus(status) {
        logger.debug(`[MemoryAdapter] Getting tasks by status: ${status}`);

        if (!status) {
            throw new Error('Status is required');
        }

        const validStatuses = ['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'];
        if (!validStatuses.includes(status)) {
            throw new Error('Invalid status value');
        }

        const taskIds = this.indexes.byStatus.get(status) || new Set();
        const tasks = [];

        for (const taskId of taskIds) {
            const task = this.tasks.get(taskId);
            if (task) {
                tasks.push(parseTask({ ...task }));
            }
        }

        return tasks;
    }

    /**
     * Get tasks by tag using index for fast lookup
     * @param {string} tag - Tag to filter by
     * @returns {Array<object>} Array of tasks
     */
    getTasksByTag(tag) {
        logger.debug(`[MemoryAdapter] Getting tasks by tag: ${tag}`);

        if (!tag) {
            throw new Error('Tag is required');
        }

        const taskIds = this.indexes.byTag.get(tag) || new Set();
        const tasks = [];

        for (const taskId of taskIds) {
            const task = this.tasks.get(taskId);
            if (task) {
                tasks.push(parseTask({ ...task }));
            }
        }

        return tasks;
    }

    /**
     * Get task statistics by tag with optimized aggregation
     * @param {string} tag - Tag filter (optional)
     * @param {string} status - Status filter (optional)
     * @returns {Array<object>} Array of statistics
     */
    getTaskStatsByTag(tag, status) {
        logger.debug(`[MemoryAdapter] Getting task stats by tag: ${tag}, status: ${status}`);

        const stats = new Map();

        for (const [taskId, task] of this.tasks) {
            // Apply filters
            if (tag && task.tag !== tag) continue;
            if (status && task.status !== status) continue;

            const key = `${task.tag || 'null'}:${task.name}:${task.status}`;

            if (!stats.has(key)) {
                stats.set(key, {
                    tag: task.tag,
                    name: task.name,
                    status: task.status,
                    count: 0
                });
            }

            stats.get(key).count++;
        }

        return Array.from(stats.values()).sort((a, b) => {
            if (a.tag !== b.tag) return (a.tag || '').localeCompare(b.tag || '');
            if (a.name !== b.name) return a.name.localeCompare(b.name);
            return a.status.localeCompare(b.status);
        });
    }

    /**
     * Get tasks with multiple filters and optimized performance
     * @param {object} filters - Filter conditions
     * @returns {Array<object>} Array of filtered tasks
     */
    getTasks(filters = {}) {
        logger.debug(`[MemoryAdapter] Getting tasks with filters:`, filters);

        // Validate status filter
        if (filters.status) {
            const validStatuses = ['pending', 'running', 'completed', 'failed', 'timeout', 'permanently_failed', 'paused', 'suspended'];
            if (!validStatuses.includes(filters.status)) {
                throw new Error('Invalid status value');
            }
        }

        let candidateIds = new Set();
        let firstFilter = true;

        // Use indexes to find candidate sets efficiently
        if (filters.status) {
            candidateIds = new Set(this.indexes.byStatus.get(filters.status) || []);
            firstFilter = false;
        }

        if (filters.name) {
            const nameIds = this.indexes.byName.get(filters.name) || new Set();
            if (firstFilter) {
                candidateIds = new Set(nameIds);
                firstFilter = false;
            } else {
                candidateIds = new Set([...candidateIds].filter(id => nameIds.has(id)));
            }
        }

        if (filters.tag) {
            const tagIds = this.indexes.byTag.get(filters.tag) || new Set();
            if (firstFilter) {
                candidateIds = new Set(tagIds);
                firstFilter = false;
            } else {
                candidateIds = new Set([...candidateIds].filter(id => tagIds.has(id)));
            }
        }

        // If no filters applied, get all tasks
        if (firstFilter) {
            candidateIds = new Set(this.tasks.keys());
        }

        // Convert to array and sort by creation time (newest first)
        const tasks = [];
        for (const taskId of candidateIds) {
            const task = this.tasks.get(taskId);
            if (task) {
                tasks.push(parseTask({ ...task }));
            }
        }

        return tasks.sort((a, b) => b.created_at - a.created_at);
    }

    /**
     * Delete tasks with filters
     * @param {object} filters - Filter conditions
     * @returns {number} Number of deleted tasks
     */
    deleteTasks(filters = {}) {
        logger.info(`[MemoryAdapter] Deleting tasks with filters:`, filters);

        const tasksToDelete = this.getTasks(filters);
        let deletedCount = 0;

        for (const task of tasksToDelete) {
            if (this.tasks.has(task.id)) {
                this._removeFromIndexes(task);
                this.tasks.delete(task.id);
                deletedCount++;
            }
        }

        logger.info(`[MemoryAdapter] Deleted ${deletedCount} tasks`);
        return deletedCount;
    }

    /**
     * Get child tasks using parent index for fast lookup
     * @param {string|number} parentId - Parent task ID
     * @returns {Array<object>} Array of child tasks
     */
    getChildTasks(parentId) {
        logger.debug(`[MemoryAdapter] Getting child tasks for parent: ${parentId}`);

        if (!parentId) {
            throw new Error('Parent task ID is required');
        }

        const childIds = this.indexes.byParent.get(parentId) || new Set();
        const tasks = [];

        for (const childId of childIds) {
            const task = this.tasks.get(childId);
            if (task) {
                tasks.push(parseTask({ ...task }));
            }
        }

        return tasks;
    }

    /**
     * Get running tasks efficiently
     * @returns {Array<object>} Array of running tasks
     */
    getRunningTasks() {
        logger.debug(`[MemoryAdapter] Getting running tasks`);
        return this.getTasksByStatus('running');
    }

    /**
     * Clear all tasks and reset storage
     * @returns {number} Number of cleared tasks
     */
    clearTasks() {
        logger.info(`[MemoryAdapter] Clearing all tasks`);

        const count = this.tasks.size;
        this.tasks.clear();

        // Reset all indexes
        for (const index of Object.values(this.indexes)) {
            index.clear();
        }

        this.nextId = 1;

        logger.info(`[MemoryAdapter] Cleared ${count} tasks`);
        return count;
    }

    /**
     * Close memory adapter (cleanup)
     */
    close() {
        logger.info(`[MemoryAdapter] Closing memory adapter`);
        this.clearTasks();
    }
}

module.exports = MemoryAdapter;
