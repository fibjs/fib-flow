/**
 * Data-level types for fib-flow.
 *
 * These describe the persisted shapes returned by the query APIs. They are
 * shipped as `.d.ts` so consumers never compile library sources; the project's
 * own `fibjs --check` gate validates them with `skipLibCheck: false`.
 */

export type TaskStatus =
    | 'pending'
    | 'running'
    | 'completed'
    | 'failed'
    | 'timeout'
    | 'permanently_failed'
    | 'paused'
    | 'suspended'
    | 'cancelled';

export type TaskType = 'async' | 'cron';

/**
 * How cron schedules execute.
 *
 * - `inline`: the cron row itself runs the handler (8.0.x behaviour).
 * - `spawn`: the cron row only spawns an independent `async` run row per round.
 */
export type CronMode = 'inline' | 'spawn';

/** Result of a spawn operation (`runCronNow`). */
export interface CronSpawnResult {
    /** Id of the created run row. */
    taskId: TaskIdentifier;
    /** Schedule instant after the round; `null` when the schedule was not advanced. */
    next_run_time: number | null;
}

/** Statuses a task can never leave. */
export type TerminalTaskStatus = 'completed' | 'permanently_failed' | 'cancelled';

/** Statuses accepted by the retention `statuses` policy option. */
export type RetentionTaskStatus = 'completed' | 'permanently_failed' | 'paused' | 'cancelled';

export type TaskEventType =
    | 'task_created'
    | 'task_claimed'
    | 'task_started'
    | 'task_checkpoint'
    | 'task_progress'
    | 'task_status_changed'
    | 'task_subtasks_created'
    | 'task_retry_scheduled'
    | 'task_retry_started'
    | 'task_failed'
    | 'task_timed_out'
    | 'task_completed'
    | 'task_paused'
    | 'task_suspended'
    | 'task_resumed'
    | 'task_cancelled'
    | 'task_cron_missed'
    | 'task_cron_rescheduled'
    | 'task_cron_spawned'
    | 'task_cron_spawn_failed'
    | 'task_cron_overlap_skipped'
    | 'task_cron_run_failed'
    | 'task_cron_run_recovered'
    | 'task_permanently_failed'
    | 'task_recovered';

/** Outcome recorded on a single execution round (attempt). */
export type AttemptOutcome =
    | 'completed'
    | 'failed'
    | 'timeout'
    | 'suspended'
    | 'interrupted'
    | 'skipped'
    /** Spawn-mode cron round: a run row was created, no handler executed. */
    | 'spawned'
    /** Spawn-mode cron round: creating the run row failed. */
    | 'spawn_failed';

export type TaskIdentifier = number | string;

export type TaskAuditCode = string;
export type TaskStageName = string;

export interface TaskAuditMetadata {
    [key: string]: unknown;
}

export interface TaskProgressMetadata {
    [key: string]: unknown;
}

export interface TaskResultEntry {
    task_id: number;
    result?: unknown;
    error?: unknown;
}

export interface TaskRecord {
    id: TaskIdentifier;
    name: string;
    type: TaskType;
    status: TaskStatus;
    stage: number;
    priority?: number;
    payload?: unknown;
    result?: unknown;
    error?: string | null;
    /** Cron expression as persisted; `null` for async tasks. */
    cron_expr?: string | null;
    /** Schedule identity key; `null` for async tasks. */
    cron_key?: string | null;
    /** IANA time zone used to evaluate `cron_expr`; `null` means the process zone. */
    cron_tz?: string | null;
    cron_catch_up?: CronCatchUp;
    /** Per-row retention window in seconds; `null` falls back to the global policy. */
    retention_expire_time?: number | null;
    max_retries?: number;
    retry_count?: number;
    retry_interval?: number;
    timeout?: number;
    next_run_time?: number | null;
    created_at?: number;
    start_time?: number | null;
    last_active_time?: number | null;
    worker_id?: string | null;
    tag?: string | null;
    root_id?: TaskIdentifier | null;
    parent_id?: TaskIdentifier | null;
    last_event_time?: number | null;
    last_event_type?: TaskEventType | null;
    current_stage_name?: string | null;
    progress_text?: string | null;
    progress_percent?: number | null;
    total_children?: number;
    completed_children?: number;
    context?: Uint8Array | null;
}

/** Missed-window policy for cron schedules. */
export type CronCatchUp = 'once' | 'skip';

export interface TaskEventRecord {
    id?: TaskIdentifier;
    task_id: TaskIdentifier;
    root_id?: TaskIdentifier | null;
    parent_id?: TaskIdentifier | null;
    event_type: TaskEventType;
    from_status?: TaskStatus | null;
    to_status?: TaskStatus | null;
    stage?: number | null;
    worker_id?: string | null;
    attempt?: number | null;
    event_time: number;
    message?: string | null;
    metadata?: Record<string, unknown> | null;
}

export interface TaskAttemptRecord {
    id?: TaskIdentifier;
    task_id: TaskIdentifier;
    attempt: number;
    worker_id?: string | null;
    started_at: number;
    ended_at?: number | null;
    outcome?: AttemptOutcome | null;
    error?: string | null;
    timeout_flag?: boolean;
}

export interface WorkerRecord {
    worker_id: string;
    pod_id: string;
    status: 'active' | 'superseded' | 'dead' | 'stopped';
    registered_at: number;
    last_seen_at: number;
    expires_at: number;
    superseded_at?: number | null;
    dead_at?: number | null;
    meta?: string | null;
}

export interface TaskAuditCheckpoint {
    /**
     * Runtime accepts any non-empty string. Lowercase snake_case such as
     * payload_validated is recommended for consistency.
     */
    code: TaskAuditCode;
    message?: string;
    metadata?: TaskAuditMetadata;
}

export interface TaskProgressSnapshot {
    /**
     * Runtime accepts any non-empty string. Lowercase snake_case such as
     * download_phase is recommended for consistency.
     */
    stage_name?: TaskStageName;
    progress_text?: string;
    progress_percent?: number;
    message?: string;
    metadata?: TaskProgressMetadata;
}

export interface TaskRetentionPolicy {
    expire_time?: number | null;
    statuses?: RetentionTaskStatus[];
}

export interface TaskRetentionResult {
    tasks_deleted: number;
    events_deleted: number;
    attempts_deleted: number;
    tasks_deleted_by_row_policy?: number;
    tasks_deleted_by_global_policy?: number;
}

export interface SubTaskDefinition {
    name: string;
    payload?: unknown;
    priority?: number;
    tag?: string;
    max_retries?: number;
    retry_interval?: number;
    timeout?: number;
    retention_expire_time?: number;
}

/**
 * Filter shape shared by task queries. `getTasks` / `queryTasks` /
 * `deleteTasks` accept the task-related subset; event and attempt queries
 * accept their own subsets.
 */
export interface TaskQueryFilters {
    tag?: string;
    status?: TaskStatus;
    /**
     * Match any of these statuses. Alternative to `status`; supplying both is
     * rejected rather than resolved, because "this one" and "any of these" cannot
     * both hold.
     */
    statuses?: TaskStatus[];
    name?: string;
    type?: TaskType;
    worker_id?: string;
    root_id?: TaskIdentifier;
    workflow_root_id?: TaskIdentifier;
    parent_id?: TaskIdentifier;
    event_type?: TaskEventType;
    started_after?: number;
    started_before?: number;
}

export interface PaginationQuery {
    limit?: number;
    offset?: number;
    order?: 'asc' | 'desc';
}

export interface TaskEventQueryFilters extends PaginationQuery {
    task_id?: TaskIdentifier;
    root_id?: TaskIdentifier;
    parent_id?: TaskIdentifier;
    worker_id?: string;
    event_type?: TaskEventType;
    event_types?: TaskEventType[];
    attempt?: number;
    stage?: number;
    started_after?: number;
    started_before?: number;
}

export interface TaskAttemptQueryFilters extends PaginationQuery {
    task_id?: TaskIdentifier;
    workflow_root_id?: TaskIdentifier;
    worker_id?: string;
    outcome?: AttemptOutcome;
    started_after?: number;
    started_before?: number;
    ended_after?: number;
    ended_before?: number;
    open_only?: boolean;
}

export interface TaskListQueryFilters extends TaskQueryFilters, PaginationQuery {}

export interface TaskDeleteFilters {
    tag?: string;
    status?: TaskStatus;
    name?: string;
    type?: TaskType;
}

export interface TaskAuditQueryOptions {
    events?: TaskEventQueryFilters;
    attempts?: TaskAttemptQueryFilters;
}

export interface WorkflowAuditQueryOptions {
    tasks?: TaskListQueryFilters;
    events?: TaskEventQueryFilters;
}

export interface PagedResult<T> {
    items: T[];
    total: number;
    limit: number | null;
    offset: number;
    has_more: boolean;
}

export interface TaskAuditView {
    task: TaskRecord | null;
    events: PagedResult<TaskEventRecord>;
    attempts: PagedResult<TaskAttemptRecord>;
}

export interface WorkflowAuditView {
    root_task: TaskRecord | null;
    tasks: PagedResult<TaskRecord>;
    events: PagedResult<TaskEventRecord>;
}

export interface WorkflowTimingSummary {
    created_at: number | null;
    first_started_at: number | null;
    last_ended_at: number | null;
    last_event_time: number | null;
    workflow_duration_seconds: number | null;
}

export interface WorkflowStageTimingSummary {
    stage: number;
    attempt: number;
    started_at: number;
    ended_at: number | null;
    duration_seconds: number | null;
    outcome: AttemptOutcome | null;
    worker_id: string | null;
}

export interface WorkflowFailedTaskSummary {
    task_id: TaskIdentifier;
    name: string;
    status: TaskStatus;
    error?: string | null;
}

export interface WorkflowCancelledTaskSummary {
    task_id: TaskIdentifier;
    name: string;
    error?: string | null;
}

export interface WorkflowCriticalPathNode {
    task_id: TaskIdentifier;
    name: string;
    status: TaskStatus;
    stage: number;
    attempt: number | null;
    outcome: AttemptOutcome | null;
    duration_seconds: number | null;
    started_at: number | null;
    ended_at: number | null;
}

export interface WorkflowCriticalPathSummary {
    total_duration_seconds: number;
    nodes: WorkflowCriticalPathNode[];
}

export interface WorkflowAuditSummary {
    root_task: TaskRecord | null;
    totals: {
        tasks: number;
        attempts: number;
        events: number;
        checkpoints: number;
        max_attempt: number;
    };
    statuses: Record<string, number>;
    attempt_outcomes: Record<string, number>;
    task_names: Record<string, number>;
    stages: Record<string, number>;
    workers: string[];
    timing: WorkflowTimingSummary;
    stage_timings: WorkflowStageTimingSummary[];
    failed_tasks: WorkflowFailedTaskSummary[];
    cancelled_tasks: WorkflowCancelledTaskSummary[];
    critical_path: WorkflowCriticalPathSummary;
    slowest_attempts: Array<TaskAttemptRecord & {
        duration_seconds: number | null;
    }>;
}

/** Row returned by `getTaskStatsByTag`. */
export interface TaskTagStat {
    tag: string | null;
    name: string;
    status: TaskStatus;
    count: number;
}

/** Serialized schedule view returned by `getCronSchedules`. */
export interface CronSchedule {
    id: TaskIdentifier;
    name: string;
    tag: string | null;
    cron_key: string | null;
    cron_expr: string | null;
    cron_tz: string | null;
    cron_catch_up: CronCatchUp;
    retention_expire_time: number | null;
    /**
     * Effective overlap cap in spawn mode, read from the schedule row. `0` means
     * unlimited; when the schedule configures no cap the manager default of `1` is
     * reported here, so callers never have to re-derive it.
     */
    max_instances: number;
    next_run_time: number;
    status: TaskStatus;
    retry_count: number;
    last_error: string | null;
    /**
     * Id of the most recently spawned run (spawn mode only).
     *
     * Derived from the `task_cron_spawned` audit event rather than stored in a
     * column, so it costs no schema change. Omitted by the adapter unless
     * `getCronSchedules(filters, names, { include_spawned: true })` is used;
     * `TaskManager#getCronSchedules()` always resolves it.
     */
    last_spawned_task_id?: TaskIdentifier | null;
    /** `true` when no handler is registered for `name`; omitted when unknown. */
    orphan?: boolean;
}

export interface CronScheduleStats {
    total: number;
    active: number;
    paused: number;
    cancelled: number;
    orphan: number;
}

export interface CronScheduleFilters {
    name?: string;
    /**
     * Schedule identity key. Together with `name` this is the schedule's unique
     * identity — with several schedules per name, filtering by `name` alone
     * returns an arbitrary row rather than the one asked for.
     */
    cronKey?: string;
    tag?: string;
    status?: TaskStatus;
    /** Match any of these statuses; alternative to `status`, not a supplement. */
    statuses?: TaskStatus[];
}

export interface CancelCronFilters {
    name?: string;
    cronKey?: string;
    tag?: string;
}

export interface CancelCronResult {
    cancelled: number;
    skipped: number;
    taskIds: Array<TaskIdentifier>;
}

export interface PruneCronResult {
    cancelled: number;
    taskIds: Array<TaskIdentifier>;
}

export interface TaskInfo {
    name: string;
    description: string;
}

/** Row shape accepted by `adapter.insertTask()`. */
export interface TaskRecordInput {
    name: string;
    type: TaskType;
    status?: TaskStatus;
    priority?: number;
    payload?: unknown;
    cron_expr?: string | null;
    /** Required for cron rows; ignored (stored NULL) for async rows. */
    cron_key?: string | null;
    cron_tz?: string | null;
    cron_catch_up?: CronCatchUp | null;
    retention_expire_time?: number | null;
    max_retries?: number;
    retry_interval?: number;
    next_run_time?: number;
    timeout?: number;
    tag?: string | null;
}

export interface InsertTaskOptions {
    root_id?: TaskIdentifier | null;
    parent_id?: TaskIdentifier | null;
    worker_id?: string | null;
    /** Binary snapshot written to the parent task when creating subtasks. */
    context?: Uint8Array | null;
}

/** Input accepted by `adapter.spawnCronRun()`. */
export interface CronSpawnInput {
    /** Claimed schedule row. */
    schedule: TaskRecord;
    /** Next instant for the schedule; required unless `advance` is `false`. */
    next_run_time?: number;
    /** Run payload; defaults to the schedule payload. */
    payload?: unknown;
    /** Recorded on the spawned event metadata. Default `'cron'`. */
    trigger?: string;
    /**
     * Advance the schedule row. Manual triggers pass `false`: they must not move
     * `next_run_time` or revive a cancelled/paused schedule.
     */
    advance?: boolean;
}

/** Input accepted by `adapter.failCronSpawn()`. */
export interface CronSpawnFailureInput {
    /** Claimed schedule row. */
    schedule: TaskRecord;
    /** Next instant for the schedule. */
    next_run_time: number;
    /** Failure text recorded on the row and in the audit event. */
    error: string;
}

/** Input accepted by `adapter.skipCronRoundForOverlap()`. */
export interface CronOverlapSkipInput {
    /** Claimed schedule row. */
    schedule: TaskRecord;
    /** Next instant for the schedule. */
    next_run_time: number;
    /** Number of runs that were still in flight when the round was skipped. */
    in_flight: number;
}

/** Input accepted by `adapter.recordScheduleRunOutcome()`. */
export interface CronRunOutcomeInput {
    /** Finished run row id. Its `task_created` event carries the schedule link. */
    runTaskId: TaskIdentifier;
    /** Failure text, or `null` to clear a previous failure. */
    error: string | null;
}

export interface TaskEventInput {
    task_id: TaskIdentifier;
    event_type: TaskEventType;
    root_id?: TaskIdentifier | null;
    parent_id?: TaskIdentifier | null;
    from_status?: TaskStatus | null;
    to_status?: TaskStatus | null;
    stage?: number | null;
    worker_id?: string | null;
    attempt?: number | null;
    event_time?: number;
    message?: string | null;
    metadata?: Record<string, unknown> | string | null;
}

/** Schedule definition accepted by `adapter.upsertCronTask()`. */
export interface CronScheduleInput {
    name: string;
    cron_expr: string;
    cron_key?: string;
    cron_tz?: string | null;
    cron_catch_up?: CronCatchUp;
    retention_expire_time?: number | null;
    payload?: unknown;
    priority?: number;
    tag?: string | null;
    max_retries?: number;
    retry_interval?: number;
    timeout?: number;
    next_run_time?: number;
}

/** Result of an upsert; `outcome` reports what the call actually did. */
export interface CronScheduleUpsertResult extends TaskRecord {
    outcome: 'created' | 'updated' | 'revived';
}

/** Scheduling fields re-read by the cron completion path. */
export interface CronScheduleState {
    cron_expr: string | null;
    cron_tz: string | null;
    cron_catch_up: CronCatchUp;
    retry_count: number;
}

export interface CronScheduleUpdate {
    cron_expr?: string;
    cron_tz?: string | null;
    cron_catch_up?: CronCatchUp;
    next_run_time?: number;
    retention_expire_time?: number | null;
}

/** Details recorded when `catchUp: 'skip'` drops a missed window. */
export interface CronSkipDetails {
    worker_id?: string | null;
    next_run_time: number;
    skipped_windows?: number;
    scheduled_run_time?: number | null;
    now?: number;
}

/** Extra fields accepted by `adapter.updateTaskStatus()`. */
export interface TaskStatusUpdate {
    result?: unknown;
    error?: string | null;
    next_run_time?: number;
    retry_count?: number;
    context?: Uint8Array | null;
    worker_id?: string | null;
    parent_id?: TaskIdentifier | null;
    suspend_reason?: string;
    resume_reason?: string;
    reason?: string;
}
