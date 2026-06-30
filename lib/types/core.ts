export type TaskStatus =
    | 'pending'
    | 'running'
    | 'completed'
    | 'failed'
    | 'timeout'
    | 'permanently_failed'
    | 'paused'
    | 'suspended';

export type TaskType = 'async' | 'cron';

export type TaskEventType =
    | 'task_created'
    | 'task_claimed'
    | 'task_started'
    | 'task_checkpoint'
    | 'task_progress'
    | 'task_status_changed'
    | 'task_heartbeat'
    | 'task_subtasks_created'
    | 'task_retry_scheduled'
    | 'task_retry_started'
    | 'task_failed'
    | 'task_timed_out'
    | 'task_completed'
    | 'task_paused'
    | 'task_resumed'
    | 'task_permanently_failed'
    | 'task_expired_deleted';

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
    cron_expr?: string | null;
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
    outcome?: TaskStatus | null;
    error?: string | null;
    timeout_flag?: boolean;
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
    statuses?: Array<'completed' | 'permanently_failed' | 'paused'>;
}

export interface TaskRetentionResult {
    tasks_deleted: number;
    events_deleted: number;
    attempts_deleted: number;
}

export interface SubTaskDefinition {
    name: string;
    payload?: unknown;
    priority?: number;
    tag?: string;
    max_retries?: number;
    retry_interval?: number;
    timeout?: number;
}

export interface TaskQueryFilters {
    tag?: string;
    status?: TaskStatus;
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

export interface TaskEventQueryFilters extends TaskQueryFilters, PaginationQuery {
    event_types?: TaskEventType[];
    attempt?: number;
    stage?: number;
}

export interface TaskAttemptQueryFilters extends PaginationQuery {
    worker_id?: string;
    outcome?: TaskStatus;
    started_after?: number;
    started_before?: number;
    ended_after?: number;
    ended_before?: number;
    open_only?: boolean;
}

export interface TaskListQueryFilters extends TaskQueryFilters, PaginationQuery {}

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
    limit?: number | null;
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
    outcome: TaskStatus | null;
    worker_id: string | null;
}

export interface WorkflowFailedTaskSummary {
    task_id: TaskIdentifier;
    name: string;
    status: TaskStatus;
    error?: string | null;
}

export interface WorkflowCriticalPathNode {
    task_id: TaskIdentifier;
    name: string;
    status: TaskStatus;
    stage: number;
    attempt: number | null;
    outcome: TaskStatus | null;
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
    critical_path: WorkflowCriticalPathSummary;
    slowest_attempts: Array<TaskAttemptRecord & {
        duration_seconds: number | null;
    }>;
}
