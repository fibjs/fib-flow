/**
 * TaskManager surface: constructor options, handler contract, and the public
 * scheduling / query / audit methods, plus the persistence adapter surface.
 *
 * Every member here mirrors `lib/task.js` and `lib/db/*.js`. The compile-time
 * contract test in `test/types/contract.ts` keeps this file honest — if a runtime
 * signature drifts, `fibjs --check` fails.
 */

import type {
    CancelCronFilters,
    CancelCronResult,
    CronCatchUp,
    CronMode,
    CronSchedule,
    CronScheduleFilters,
    CronScheduleInput,
    CronScheduleState,
    CronScheduleStats,
    CronScheduleUpdate,
    CronScheduleUpsertResult,
    CronOverlapSkipInput,
    CronRunOutcomeInput,
    CronSkipDetails,
    CronSpawnFailureInput,
    CronSpawnInput,
    CronSpawnResult,
    InsertTaskOptions,
    PagedResult,
    PruneCronResult,
    SubTaskDefinition,
    TaskAttemptQueryFilters,
    TaskAttemptRecord,
    TaskAuditCheckpoint,
    TaskAuditQueryOptions,
    TaskAuditView,
    TaskDeleteFilters,
    TaskEventInput,
    TaskEventQueryFilters,
    TaskEventRecord,
    TaskIdentifier,
    TaskInfo,
    TaskListQueryFilters,
    TaskProgressSnapshot,
    TaskQueryFilters,
    TaskRecord,
    TaskRecordInput,
    TaskRetentionPolicy,
    TaskRetentionResult,
    TaskStatus,
    TaskStatusUpdate,
    TaskTagStat,
    TaskType,
    WorkerRecord,
    WorkflowAuditQueryOptions,
    WorkflowAuditSummary,
    WorkflowAuditView
} from './core';

/**
 * Accepted `dbConnection` values: a connection string (`sqlite:...`,
 * `mysql://...`, `psql://...`), a live fibjs database handle, or a pool wrapper
 * function taking an executor callback.
 */
export type DbConnectionInput = string | object | ((callback: (conn: unknown) => unknown) => unknown);

export type DatabaseType = 'sqlite' | 'mysql' | 'psql';

/** TaskManager configuration. `dbConnection` is the only required field. */
export interface TaskManagerOptions {
    dbConnection: DbConnectionInput;
    /** Explicit backend selection; inferred from the connection string when omitted. */
    dbType?: DatabaseType | string;
    /** Poll interval in milliseconds. Default `1000`. */
    poll_interval?: number;
    /** Default total attempts per task, including the first one. Default `3`. */
    max_retries?: number;
    /** Default delay between retries in seconds. Default `0`. */
    retry_interval?: number;
    /** Default execution timeout in seconds. Default `60`. */
    timeout?: number;
    /** Worker identity; auto-generated when omitted. */
    worker_id?: string;
    /** Stable logical node identity enabling worker recovery across restarts. */
    pod_id?: string | null;
    /** Maximum number of tasks executing simultaneously. Default `10`. */
    max_concurrent_tasks?: number;
    /** Running-task heartbeat interval in milliseconds. Default `5000`. */
    task_heartbeat_interval?: number;
    /** Window before a running task is considered to have lost its heartbeat. Default `30000`. */
    task_heartbeat_timeout?: number;
    /** Worker registry heartbeat interval in milliseconds. Default `5000`. */
    worker_heartbeat_interval?: number;
    /** Worker liveness window in milliseconds. Default `30000`. */
    worker_heartbeat_timeout?: number;
    /** Reclaim `running` jobs from dead or superseded workers. Default `true`. */
    recover_running_jobs?: boolean;
    /**
     * How cron schedules execute.
     *
     * - `'inline'` (default): the cron row runs the handler itself and is reset
     *   after every round. A handler that suspends freezes the whole schedule.
     * - `'spawn'`: the cron row never runs business code; each round inserts an
     *   independent `async` run row and advances the schedule atomically.
     */
    cronMode?: CronMode;
    /** Backward-compatible shortcut for `retention.expire_time`. */
    expire_time?: number;
    /** Retention policy; row-level `retention_expire_time` overrides it per task. */
    retention?: TaskRetentionPolicy;
}

/** Per-task options accepted by `async()` and by handler registration. */
export interface TaskOptions {
    /** Delay in seconds before the first attempt (async only). */
    delay?: number;
    priority?: number;
    max_retries?: number;
    retry_interval?: number;
    timeout?: number;
    tag?: string;
    /** Per-row retention window in seconds; `null`/omitted uses the global policy. */
    retention_expire_time?: number;
}

/** Options accepted by `cron()`. */
export interface CronOptions extends Omit<TaskOptions, 'delay'> {
    /** Schedule identity; `(name, cronKey)` is unique. Default `'default'`. */
    cronKey?: string;
    /** IANA time zone used to evaluate the expression, e.g. `'Asia/Shanghai'`. */
    timezone?: string;
    /** Missed-window policy. Default `'once'`. */
    catchUp?: CronCatchUp;
    /**
     * Maximum number of runs of this schedule that may be in flight at once.
     * Only meaningful in `cronMode: 'spawn'`.
     *
     * Default `1`, which reproduces inline mode's mutual exclusion. `0` removes the
     * cap (rounds may overlap freely). A round that would exceed the cap is skipped:
     * `next_run_time` still advances and a `task_cron_overlap_skipped` event is
     * recorded, but no run row is created and `error` is left alone.
     *
     * **Omitting this option on a later `cron()` keeps the stored value** — `cron()`
     * is an idempotent upsert, so an absent field means "leave it alone". Pass
     * `null` to reset to the default. `runCronNow()` counts toward the same cap.
     */
    maxInstances?: number | null;
}

export interface SuspensionOptions {
    /** Required non-empty suspend reason, e.g. `'awaiting_approval'`. */
    reason: string;
    /** Optional structured metadata describing the suspension. */
    metadata?: Record<string, unknown>;
    /** Binary snapshot persisted to the task's `context` column. */
    context?: Uint8Array | null;
}

/**
 * Marker returned from a handler to suspend its task.
 * Return `task.suspend(...)` rather than constructing this directly.
 */
export declare class Suspension {
    readonly reason: string;
    readonly metadata?: Record<string, unknown>;
    readonly context: Uint8Array | null;
    constructor(options: SuspensionOptions);
}

/**
 * Creates subtasks and suspends the parent until they all finish.
 * This is the second handler argument (`next`).
 */
export type SubTaskFactory = (
    tasks: SubTaskDefinition | SubTaskDefinition[],
    context?: Uint8Array | null
) => Suspension;

/**
 * Task object handed to a handler: the persisted row plus the helpers the
 * runtime decorates onto it at execution time.
 */
export interface TaskHandlerTask<T = unknown> extends Omit<TaskRecord, 'payload'> {
    /** Parsed payload as stored when the task was created. */
    payload: T;
    /** Throws when the task exceeded its configured timeout. */
    checkTimeout(): void;
    /** Writes a `task_checkpoint` audit event and returns its id. */
    audit(checkpoint: TaskAuditCheckpoint | string, details?: Partial<TaskAuditCheckpoint>): number;
    /** Writes a `task_progress` event, updates the snapshot cache, returns the event id. */
    progress(snapshot: TaskProgressSnapshot | string, details?: Partial<TaskProgressSnapshot>): number;
    /** Suspends the task; `context` is persisted for the resumed run. */
    suspend(options: SuspensionOptions): Suspension;
}

export type TaskHandler<T = unknown> = (
    task: TaskHandlerTask<T>,
    next: SubTaskFactory
) => unknown;

/** Handler registration object form. */
export interface TaskHandlerConfig<T = unknown> extends TaskOptions {
    handler: TaskHandler<T>;
    /** Cap on simultaneously executing tasks of this type. */
    max_concurrent_tasks?: number;
    /** Free-form description reported by `getTaskInfo()`. */
    description?: string;
}

/** Options accepted by `resumeTask`. */
export interface ResumeTaskOptions {
    resume_reason?: string;
}

/** Options accepted by `cancelTask` / `cancelCron`. */
export interface CancelTaskOptions {
    /** Stored as the task `error` and recorded in the `task_cancelled` event. */
    reason?: string;
}

/** Options accepted by `runCronNow`. */
export interface RunCronNowOptions {
    /** Recorded on the `task_cron_spawned` event metadata. Default `'manual'`. */
    trigger?: string;
    /**
     * Bypass `maxInstances`. A manual trigger counts toward the cap like any other
     * run of the schedule, so this is the explicit way to force one anyway.
     */
    ignoreMaxInstances?: boolean;
}

/** Options accepted by `adapter.claimTask()`. */
export interface ClaimTaskOptions {
    /** Do not open an attempt row (spawn-mode schedule rounds). */
    skipAttempt?: boolean;
    /** Do not write `task_claimed` / `task_started` events. */
    skipEvents?: boolean;
    /** Restrict the claim to one task type. */
    typeFilter?: TaskType;
}

/** Options accepted by `adapter.getCronSchedules()`. */
export interface CronScheduleQueryOptions {
    /**
     * Also resolve `last_spawned_task_id` from the audit trail. Costs an extra
     * lookup, so hot-path callers leave it off.
     */
    include_spawned?: boolean;
}

/** Filters accepted by `getTasksByStatus`. */
export interface StatusQueryFilters {
    /** Keep only tasks whose latest `task_suspended` event carries this reason. */
    suspend_reason?: string;
}

/** Result of `handleTimeoutTasks` on the adapter. */
export interface TimeoutSweepResult {
    timed_out_total: number;
    timed_out_heartbeat: number;
    retried: number;
    paused: number;
    permanently_failed: number;
    tasks_deleted: number;
    events_deleted: number;
    attempts_deleted: number;
}

/** Heartbeat configuration consumed by `handleTimeoutTasks`. */
export interface TimeoutSweepConfig {
    task_heartbeat_interval: number;
    task_heartbeat_timeout: number;
}

/**
 * Distributed task scheduler and executor.
 *
 * Cron registration is idempotent per `(name, cronKey)`, cancellation produces a
 * distinct `cancelled` outcome, and each schedule may carry its own time zone,
 * catch-up policy and retention window.
 */
export declare class TaskManager {
    /** Marker class re-exported for `instanceof` checks against handler results. */
    static readonly Suspension: typeof Suspension;

    /** Effective configuration after defaults are applied. */
    readonly options: TaskManagerOptions & {
        poll_interval: number;
        max_retries: number;
        retry_interval: number;
        timeout: number;
        worker_id: string;
        max_concurrent_tasks: number;
        task_heartbeat_interval: number;
        task_heartbeat_timeout: number;
        worker_heartbeat_interval: number;
        worker_heartbeat_timeout: number;
        recover_running_jobs: boolean;
    };

    /** `init` before `start()`, `running` while processing, `stopped` after `stop()`. */
    readonly state: 'init' | 'running' | 'stopped';

    /** Underlying adapter; call `db.setup()` once to create the schema. */
    db: DatabaseAdapter;

    constructor(options: TaskManagerOptions);

    // --- handler registry -------------------------------------------------
    /** Register one handler (function form). */
    use<T = unknown>(taskName: string, handler: TaskHandler<T>): void;
    /** Register one handler with per-type options (object form). */
    use<T = unknown>(taskName: string, config: TaskHandlerConfig<T>): void;
    /** Register several handlers at once. */
    use(handlers: Record<string, TaskHandler | TaskHandlerConfig>): void;
    /** Unregister handlers; running attempts keep their captured snapshot. */
    unuse(taskName: string | string[]): number;
    /** Registered handler metadata. */
    getTaskInfo(taskName: string): TaskInfo;

    // --- task creation ----------------------------------------------------
    /** Enqueue a one-shot task and return its id. */
    async<T = unknown>(taskName: string, payload?: T, options?: TaskOptions): TaskIdentifier;
    /**
     * Create or update a cron schedule and return the task id.
     * Repeated calls with the same `(taskName, cronKey)` converge on one row.
     */
    cron<T = unknown>(taskName: string, cronExpr: string, payload?: T, options?: CronOptions): TaskIdentifier;

    // --- lifecycle --------------------------------------------------------
    /** Start the processing loop. */
    start(): void;
    /** Stop claiming new tasks without tearing the manager down. */
    pause(): void;
    /** Resume claiming after `pause()`. */
    resume(): void;
    /** Stop the manager, wait for in-flight tasks, and close the database. */
    stop(): void;

    // --- task control -----------------------------------------------------
    /** Return a `suspended` / `paused` task to `pending` and reset its retry budget. */
    resumeTask(taskId: TaskIdentifier, options?: ResumeTaskOptions): TaskRecord | null;
    /** Cancel a task. Running tasks are rejected; returns the updated row. */
    cancelTask(taskId: TaskIdentifier, options?: CancelTaskOptions): TaskRecord | null;
    /** Cancel cron schedules by identity filter. */
    cancelCron(filters?: CancelCronFilters, options?: CancelTaskOptions): CancelCronResult;
    /**
     * Spawn one cron run immediately, without waiting for its window.
     *
     * Only a run row is created: the schedule row is untouched, so a cancelled or
     * paused schedule is not revived and `next_run_time` does not move.
     */
    runCronNow<T = unknown>(scheduleId: TaskIdentifier, payload?: T, options?: RunCronNowOptions): CronSpawnResult;
    /** Transition a running or pending task to `paused`. */
    pauseTask(taskId: TaskIdentifier): TaskRecord | null;
    /** Run retention cleanup now, optionally overriding the policy. */
    runRetention(policy?: TaskRetentionPolicy | number | null): TaskRetentionResult;

    // --- task queries -----------------------------------------------------
    getTask(taskId: TaskIdentifier): TaskRecord | null;
    getTasks(filters?: TaskQueryFilters): TaskRecord[];
    getTasksByName(name: string): TaskRecord[];
    getTasksByStatus(status: TaskStatus, filters?: StatusQueryFilters): TaskRecord[];
    getTasksByTag(tag: string): TaskRecord[];
    getChildTasks(taskId: TaskIdentifier): TaskRecord[];
    getTaskStatsByTag(tag?: string, status?: TaskStatus): TaskTagStat[];
    queryTasks(filters?: TaskListQueryFilters): PagedResult<TaskRecord>;

    // --- cron observability -----------------------------------------------
    /** List schedules; `orphan` marks rows whose handler is no longer registered. */
    getCronSchedules(filters?: CronScheduleFilters): CronSchedule[];
    /** Cancel pending schedules whose handler is gone. */
    pruneCronSchedules(): PruneCronResult;
    getCronScheduleStats(): CronScheduleStats;

    // --- audit ------------------------------------------------------------
    getTaskEvents(taskId: TaskIdentifier, filters?: TaskEventQueryFilters): TaskEventRecord[];
    getWorkflowEvents(rootId: TaskIdentifier, filters?: TaskEventQueryFilters): TaskEventRecord[];
    getTaskAttempts(taskId: TaskIdentifier): TaskAttemptRecord[];
    queryTaskEvents(taskId: TaskIdentifier, filters?: TaskEventQueryFilters): PagedResult<TaskEventRecord>;
    queryWorkflowEvents(rootId: TaskIdentifier, filters?: TaskEventQueryFilters): PagedResult<TaskEventRecord>;
    queryTaskAttempts(taskId: TaskIdentifier, filters?: TaskAttemptQueryFilters): PagedResult<TaskAttemptRecord>;
    queryWorkflowAttempts(rootId: TaskIdentifier, filters?: TaskAttemptQueryFilters): PagedResult<TaskAttemptRecord>;
    getTaskAudit(taskId: TaskIdentifier, options?: TaskAuditQueryOptions): TaskAuditView;
    getWorkflowAudit(rootId: TaskIdentifier, options?: WorkflowAuditQueryOptions): WorkflowAuditView;
    getWorkflowAuditSummary(rootId: TaskIdentifier): WorkflowAuditSummary;

    // --- maintenance ------------------------------------------------------
    /** Delete tasks matching the filters. An empty filter object throws. */
    deleteTasks(filters: TaskDeleteFilters): number;
}

/** Minimal surface of a fibjs database connection. */
export interface DbConnection {
    execute(sql: string, ...params: unknown[]): unknown[];
    trans<T>(callback: () => T): T;
    close(): void;
}

/** Pool wrapper: runs `callback` with a connection and returns its result. */
export type PoolExecutor = <T>(callback: (conn: DbConnection) => T) => T;

/** Registration payload for the worker registry. */
export interface WorkerRegistration {
    worker_id: string;
    pod_id: string;
    /** Unix seconds; defaults to now. */
    now?: number;
    /** Lease duration in seconds. */
    ttl: number;
    meta?: Record<string, unknown>;
}

/** Recovery context passed to `recoverTasksForWorkers`. */
export interface WorkerRecoveryContext {
    worker_id?: string | null;
    pod_id?: string | null;
    now?: number;
}

/**
 * Low-level persistence adapter, reachable through `taskManager.db`.
 * The concrete subclasses differ only in DDL and dialect details.
 */
export declare class DatabaseAdapter {
    constructor(conn: DbConnectionInput, poolSize?: number);
    /** Execute work against a pooled connection. */
    readonly pool: PoolExecutor;

    /** Create the schema (v2) and enforce the schema version gate. */
    setup(): void;
    /** Close the underlying pool. */
    close(): void;

    insertTask(tasks: TaskRecordInput | TaskRecordInput[], options?: InsertTaskOptions): number | number[];
    insertTaskEvent(event: TaskEventInput): number;
    insertTaskEvents(events: TaskEventInput[]): number[];

    /** Create or update a cron schedule identified by `(name, cron_key)`. */
    upsertCronTask(schedule: CronScheduleInput, options?: InsertTaskOptions): CronScheduleUpsertResult;
    /** Update only the scheduling fields of an existing cron row. */
    updateCronSchedule(taskId: TaskIdentifier, updates: CronScheduleUpdate): TaskRecord | null;
    /** Read the persisted scheduling fields (used by the completion path). */
    getCronScheduleState(taskId: TaskIdentifier): CronScheduleState | null;

    claimTask(taskNames: string[], workerId: string, options?: ClaimTaskOptions): TaskRecord | null;
    /**
     * Spawn one run row for a cron schedule and advance the schedule, atomically.
     *
     * Both statements share one transaction, so a crash in between rolls the whole
     * round back instead of producing a second run on the next tick.
     */
    spawnCronRun(params: CronSpawnInput): CronSpawnResult;
    /**
     * Advance a schedule past a failed spawn without creating a run row, so the
     * schedule keeps firing and the error stays visible via `last_error`.
     */
    failCronSpawn(params: CronSpawnFailureInput): { next_run_time: number };
    /**
     * Count runs of a schedule that are still `pending` or `running`.
     * Only runs that schedule spawned are counted (see `schedule_task_id`),
     * so a manually enqueued task under the same handler name does not count.
     */
    countScheduleRunsInFlight(scheduleId: TaskIdentifier): number;
    /**
     * Advance a schedule past a round skipped because too many runs are in flight.
     * Writes no `error`: an overlap skip is expected, not a failure.
     */
    skipCronRoundForOverlap(params: CronOverlapSkipInput): { next_run_time: number };
    /**
     * Mirror a finished run's outcome onto the schedule that spawned it, so a
     * failing schedule stays visible through `getCronSchedules()[].last_error`.
     * Pass `error: null` on success to clear a previous failure.
     */
    recordScheduleRunOutcome(params: CronRunOutcomeInput): number;
    /** Persist the per-schedule `maxInstances` policy (stored in `fib_flow_meta`). */
    setScheduleMaxInstances(scheduleName: string, cronKey: string, maxInstances: number | null): void;
    /** Read a persisted `maxInstances` policy; `undefined` when none is stored. */
    getScheduleMaxInstances(scheduleName: string, cronKey: string): number | null | undefined;
    /** Delete per-schedule `fib_flow_meta` rows whose schedule row no longer exists. */
    pruneScheduleMeta(): number;
    updateTaskStatus(taskId: TaskIdentifier, status: TaskStatus, extra?: TaskStatusUpdate): void;
    recordCronWindowSkipped(taskId: TaskIdentifier, details: CronSkipDetails): boolean;
    recordTaskProgress(taskId: TaskIdentifier, progress: TaskProgressSnapshot, workerId?: string | null): number;
    recordTaskCheckpoint(taskId: TaskIdentifier, checkpoint: TaskAuditCheckpoint, workerId?: string | null): number;
    updateTaskActiveTime(taskIds: TaskIdentifier[], workerId?: string | null): unknown;

    getTask(taskId: TaskIdentifier): TaskRecord | null;
    getTasks(filters?: TaskQueryFilters): TaskRecord[];
    getTasksByName(name: string): TaskRecord[];
    getTasksByStatus(status: TaskStatus, filters?: StatusQueryFilters): TaskRecord[];
    getTasksByTag(tag: string): TaskRecord[];
    getChildTasks(parentId: TaskIdentifier): TaskRecord[];
    getTaskStatsByTag(tag?: string, status?: TaskStatus): TaskTagStat[];
    queryTasks(filters?: TaskListQueryFilters): PagedResult<TaskRecord>;
    getRunningTasks(): TaskRecord[];
    deleteTasks(filters: TaskDeleteFilters): number;
    clearTasks(): number;

    getTaskEvents(taskId: TaskIdentifier, filters?: TaskEventQueryFilters): TaskEventRecord[];
    getWorkflowEvents(rootId: TaskIdentifier, filters?: TaskEventQueryFilters): TaskEventRecord[];
    getTaskAttempts(taskId: TaskIdentifier, filters?: TaskAttemptQueryFilters): TaskAttemptRecord[];
    queryTaskEvents(taskId: TaskIdentifier, filters?: TaskEventQueryFilters): PagedResult<TaskEventRecord>;
    queryWorkflowEvents(rootId: TaskIdentifier, filters?: TaskEventQueryFilters): PagedResult<TaskEventRecord>;
    queryTaskAttempts(taskId: TaskIdentifier, filters?: TaskAttemptQueryFilters): PagedResult<TaskAttemptRecord>;
    queryWorkflowAttempts(rootId: TaskIdentifier, filters?: TaskAttemptQueryFilters): PagedResult<TaskAttemptRecord>;
    getTaskAudit(taskId: TaskIdentifier, options?: TaskAuditQueryOptions): TaskAuditView;
    getWorkflowAudit(rootId: TaskIdentifier, options?: WorkflowAuditQueryOptions): WorkflowAuditView;
    getWorkflowAuditSummary(rootId: TaskIdentifier): WorkflowAuditSummary;

    getCronSchedules(filters?: CronScheduleFilters, registeredNames?: string[] | null, options?: CronScheduleQueryOptions): CronSchedule[];
    pruneCronSchedules(registeredNames: string[], reason?: string): PruneCronResult;
    getCronScheduleStats(registeredNames?: string[] | null): CronScheduleStats;

    handleTimeoutTasks(config: TimeoutSweepConfig, retention?: TaskRetentionPolicy | number | null): TimeoutSweepResult;
    cleanupExpiredTasks(policy?: TaskRetentionPolicy | number): TaskRetentionResult;

    registerWorker(worker: WorkerRegistration): WorkerRecord | null;
    heartbeatWorker(workerId: string, now?: number, ttl?: number): unknown;
    getWorker(workerId: string): WorkerRecord | null;
    listWorkers(filters?: { pod_id?: string; status?: WorkerRecord['status'] }): WorkerRecord[];
    supersedeOlderWorkers(podId: string, currentWorkerId: string, now?: number): string[];
    reapExpiredWorkers(now?: number): string[];
    markWorkerDead(workerId: string, now?: number): unknown;
    recoverTasksForWorkers(workerIds: string[], recoveredBy?: WorkerRecoveryContext): number;
}

export declare class SQLiteAdapter extends DatabaseAdapter {
    /** SQLite has no `FOR UPDATE SKIP LOCKED`; the lock clause is empty. */
    readonly lockClause: string;
}

export declare class MySQLAdapter extends DatabaseAdapter {}

export declare class PSQLAdapter extends DatabaseAdapter {}

/** Build an adapter for the given connection value. */
export declare function createAdapter(config: DbConnectionInput, type?: DatabaseType): DatabaseAdapter;
