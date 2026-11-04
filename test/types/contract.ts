/**
 * Compile-time contract test for the published type declarations.
 *
 * This file is never executed — it is type-checked by `fibjs --check` (the
 * project's only type gate). Each block pins a documented API promise so a drift
 * between `lib/task.js` and `index.d.ts` fails the build instead of surfacing in
 * a consumer's editor.
 *
 * Negative assertions use `@ts-expect-error`, which fails the check when the
 * error disappears.
 */

import {
    TaskManager,
    Suspension,
    createAdapter,
    type TaskManagerOptions,
    type TaskOptions,
    type CronOptions,
    type TaskRecord,
    type TaskStatus,
    type TaskEventRecord,
    type TaskAttemptRecord,
    type CronSchedule,
    type CronScheduleStats,
    type CancelCronResult,
    type PruneCronResult,
    type PagedResult,
    type WorkflowAuditSummary,
    type TaskRetentionResult,
    type TaskHandlerTask,
    type SubTaskFactory,
    type WorkerRecord,
    type TaskTagStat
} from '../..';

// --- constructor & options ------------------------------------------------

const options: TaskManagerOptions = {
    dbConnection: 'sqlite::memory:',
    pod_id: 'scheduler-a',
    poll_interval: 100,
    max_retries: 3,
    retention: {
        expire_time: 86400,
        statuses: ['completed', 'permanently_failed', 'cancelled']
    }
};

const taskManager = new TaskManager(options);
taskManager.db.setup();

// @ts-expect-error — dbConnection is required
new TaskManager({});

// @ts-expect-error — unknown option is rejected
new TaskManager({ dbConnection: 'sqlite::memory:', nope: true });

// --- handler registration -------------------------------------------------

taskManager.use('plain', () => 'ok');

taskManager.use('typed', async (task) => {
    // Payload is typed by the handler's generic.
    return { id: task.id, status: task.status };
});

taskManager.use<{ userId: string }>('typed_payload', (task) => {
    const userId: string = task.payload.userId;
    return userId;
});

taskManager.use('configured', {
    handler: async (task, next) => {
        // `next` returns a Suspension marker.
        const suspension: Suspension = next([{ name: 'child', payload: { a: 1 } }]);
        return suspension;
    },
    timeout: 120,
    max_retries: 2,
    retry_interval: 30,
    priority: 5,
    max_concurrent_tasks: 3,
    description: 'demo'
});

taskManager.use({
    bulk_a: () => 'ok',
    bulk_b: { handler: () => 'ok', timeout: 10 }
});

// @ts-expect-error — a handler is required for a named registration
taskManager.use('missing_handler');

// @ts-expect-error — handler options do not accept arbitrary keys
taskManager.use('bad_option', { handler: () => 'ok', nope: 1 });

const removed: number = taskManager.unuse(['bulk_a', 'bulk_b']);
const info: { name: string; description: string } = taskManager.getTaskInfo('plain');

// --- task creation --------------------------------------------------------

const asyncId: number | string = taskManager.async('plain', { a: 1 }, { tag: 'x', retention_expire_time: 3600 });

const cronId = taskManager.cron('plain', '0 9 * * *', { a: 1 }, {
    cronKey: 'eu',
    timezone: 'Asia/Shanghai',
    catchUp: 'skip',
    priority: 1,
    retention_expire_time: 90 * 86400
});

// @ts-expect-error — catchUp only accepts 'once' | 'skip'
taskManager.cron('plain', '0 9 * * *', {}, { catchUp: 'all' });

const plainTaskOptions: TaskOptions = { delay: 5, timeout: 30 };
const plainCronOptions: CronOptions = { cronKey: 'default' };
// @ts-expect-error — cron options have no `delay`
const invalidCronOptions: CronOptions = { delay: 5 };

// --- lifecycle & control --------------------------------------------------

const resumed: TaskRecord | null = taskManager.resumeTask(asyncId, { resume_reason: 'approved' });
const cancelled: TaskRecord | null = taskManager.cancelTask(asyncId, { reason: 'withdrawn' });
const paused: TaskRecord | null = taskManager.pauseTask(asyncId);

const cancelSummary: CancelCronResult = taskManager.cancelCron({ name: 'plain' }, { reason: 'retired' });
const cancelledCount: number = cancelSummary.cancelled;
const skippedCount: number = cancelSummary.skipped;

const retention: TaskRetentionResult = taskManager.runRetention({ expire_time: 3600 });
const byRow: number | undefined = retention.tasks_deleted_by_row_policy;

// @ts-expect-error — cancel filters require at least a property, not a string
taskManager.cancelCron('plain');

// --- queries --------------------------------------------------------------

const task: TaskRecord | null = taskManager.getTask(asyncId);
if (task) {
    const status: TaskStatus = task.status;
    const cronKey: string | null | undefined = task.cron_key;
    const catchUp: 'once' | 'skip' | undefined = task.cron_catch_up;
    void status; void cronKey; void catchUp;
}

const all: TaskRecord[] = taskManager.getTasks({ type: 'cron', status: 'cancelled' });
// @ts-expect-error — unknown status is rejected
taskManager.getTasks({ status: 'done' });

const byStatus: TaskRecord[] = taskManager.getTasksByStatus('suspended', { suspend_reason: 'awaiting_approval' });
const tagStats: TaskTagStat[] = taskManager.getTaskStatsByTag();
const statCount: number | undefined = tagStats[0]?.count;

const page: PagedResult<TaskRecord> = taskManager.queryTasks({ limit: 10, order: 'desc' });
const pageHasMore: boolean = page.has_more;

const schedules: CronSchedule[] = taskManager.getCronSchedules({ status: 'pending' });
const isOrphan: boolean | undefined = schedules[0]?.orphan;
const stats: CronScheduleStats = taskManager.getCronScheduleStats();
const orphanCount: number = stats.orphan;
const pruned: PruneCronResult = taskManager.pruneCronSchedules();

// --- audit ----------------------------------------------------------------

const events: TaskEventRecord[] = taskManager.getTaskEvents(asyncId, { event_types: ['task_cancelled'] });
const attempts: TaskAttemptRecord[] = taskManager.getTaskAttempts(asyncId);
const eventPage: PagedResult<TaskEventRecord> = taskManager.queryTaskEvents(asyncId, { limit: 5 });
const audit = taskManager.getTaskAudit(asyncId, { events: { limit: 20 } });
const auditTask: TaskRecord | null = audit.task;

const summary: WorkflowAuditSummary = taskManager.getWorkflowAuditSummary(asyncId);
const cancelledTasksCount: number = summary.cancelled_tasks.length;
const failedTasksCount: number = summary.failed_tasks.length;

// @ts-expect-error — attempt outcome is a closed union
taskManager.queryTaskAttempts(asyncId, { outcome: 'unknown' });

// --- adapter surface ------------------------------------------------------

const adapter = createAdapter('sqlite::memory:');
adapter.setup();
const worker: WorkerRecord | null = adapter.getWorker('worker-1');
const recovered: number = adapter.recoverTasksForWorkers(['worker-1'], { pod_id: 'pod-a' });
const deleted: number = adapter.deleteTasks({ name: 'plain' });
const cleared: number = adapter.clearTasks();

// Note: `deleteTasks({})` is rejected at runtime, not at compile time. The
// runtime guard is covered by test/db_test.js ('should handle invalid delete
// parameters gracefully'); a type-level rejection would forbid expressing
// "filters built dynamically".

// --- handler-side helpers -------------------------------------------------

const handler: (task: TaskHandlerTask<{ userId: string }>, next: SubTaskFactory) => unknown =
    (task, next) => {
        task.checkTimeout();
        task.audit('payload_validated', { message: 'ok', metadata: { source: 'test' } });
        task.progress({ stage_name: 'upload', progress_percent: 40 });
        const userId: string = task.payload.userId;
        if (userId === 'pause') {
            return task.suspend({ reason: 'awaiting_approval', context: new Uint8Array([1, 2]) });
        }
        return next({ name: 'child' });
    };

taskManager.use('typed_handler', handler);

// @ts-expect-error — suspend requires a reason
const badSuspension = new Suspension({});

// --- keep exports obviously used -----------------------------------------

export const __contract = {
    taskManager, asyncId, cronId, plainTaskOptions, plainCronOptions,
    invalidCronOptions, resumed, cancelled, paused, cancelledCount, skippedCount,
    byRow, all, byStatus, statCount, pageHasMore, isOrphan, orphanCount, pruned,
    events, attempts, eventPage, auditTask, cancelledTasksCount, failedTasksCount,
    worker, recovered, deleted, cleared, badSuspension, removed, info
};
