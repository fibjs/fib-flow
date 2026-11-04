# TypeScript Support

fib-flow ships its own type declarations. There is no separate `@types` package,
and nothing needs to be generated before publishing.

## Entry point

`package.json` points at the declaration entry:

```json
{
    "main": "lib/index.js",
    "types": "index.d.ts"
}
```

`index.d.ts` re-exports two declaration modules:

| File | Contents |
| --- | --- |
| `lib/types/core.d.ts` | Data shapes: `TaskStatus`, `TaskType`, `TaskEventType`, `AttemptOutcome`, `TaskRecord`, `TaskEventRecord`, `TaskAttemptRecord`, `WorkerRecord`, query filters, paged results, audit views, cron schedule views, retention shapes |
| `lib/types/task-manager.d.ts` | Behavioural surface: `TaskManager`, `Suspension`, handler contract, option shapes, `DatabaseAdapter` and `createAdapter()` |

Declarations are hand-written against the runtime; they are never generated, so
every member here is intentional and reviewable.

## TaskManager

```typescript
import { TaskManager } from 'fib-flow';

const taskManager = new TaskManager({
    dbConnection: 'psql://user:pass@localhost:5432/tasks',
    pod_id: 'scheduler-a',
    retention: { expire_time: 7 * 86400, statuses: ['completed', 'cancelled'] }
});

taskManager.db.setup();
```

`dbConnection` is required; omitting it is a compile error. Every other option is
optional and documented inline in the editor.

Return types follow the runtime exactly:

| Method | Returns |
| --- | --- |
| `async()` / `cron()` | the task id (`number`) |
| `resumeTask()` / `pauseTask()` / `cancelTask()` | the updated `TaskRecord` (or `null` when the row is gone) |
| `cancelCron()` | `CancelCronResult` — `{ cancelled, skipped, taskIds }` |
| `pruneCronSchedules()` | `PruneCronResult` — `{ cancelled, taskIds }` |
| `getCronScheduleStats()` | `CronScheduleStats` — `{ total, active, paused, cancelled, orphan }` |
| `runRetention()` | `TaskRetentionResult` |
| `queryTasks()` / `queryTaskEvents()` / `queryTaskAttempts()` | `PagedResult<T>` |
| `getTaskAudit()` / `getWorkflowAudit()` | `TaskAuditView` / `WorkflowAuditView` |
| `getWorkflowAuditSummary()` | `WorkflowAuditSummary` |

## Typed payloads

The handler generic types `task.payload`, so payload typos are compile errors:

```typescript
interface EmailPayload {
    to: string;
    subject: string;
    body: string;
}

taskManager.use<EmailPayload>('sendEmail', async (task) => {
    const { to, subject, body } = task.payload; // all typed as string
    task.audit('payload_validated', { metadata: { recipient: to } });
    task.progress({ stage_name: 'delivery', progress_percent: 80 });
    return { sent: true };
});

taskManager.async<EmailPayload>('sendEmail', {
    to: 'user@example.com',
    subject: 'Hello',
    body: 'World'
});
```

The handler's second argument is the sub-task factory:

```typescript
taskManager.use('fan_out', (task, next) => next([
    { name: 'step_a', payload: { shard: 0 } },
    { name: 'step_b', payload: { shard: 1 } }
]));
```

## Cron options

```typescript
import type { CronOptions } from 'fib-flow';

const options: CronOptions = {
    cronKey: 'eu',                 // schedule identity; (name, cronKey) is unique
    timezone: 'Asia/Shanghai',     // IANA name, validated at registration
    catchUp: 'skip',               // 'once' (default) | 'skip'
    retention_expire_time: 90 * 86400,
    priority: 1,
    tag: 'reporting'
};

taskManager.cron('nightly_report', '0 3 * * *', { region: 'eu' }, options);

// @ts-expect-error — catchUp only accepts 'once' | 'skip'
taskManager.cron('nightly_report', '0 3 * * *', {}, { catchUp: 'all' });
```

## Status unions

`TaskStatus` is a closed union, so exhaustive switches are checked and stale
status names fail fast:

```typescript
import type { TaskStatus } from 'fib-flow';

function describe(status: TaskStatus): string {
    switch (status) {
        case 'cancelled':
            return 'cancelled by an operator';
        case 'permanently_failed':
            return 'retries exhausted';
        // ... remaining members
        default:
            return status;
    }
}
```

`AttemptOutcome` is likewise closed: `completed`, `failed`, `timeout`,
`suspended`, `interrupted`, `skipped`.

## How the declarations stay accurate

`fibjs --check` is the single type gate for this repository. It type-checks:

1. `index.d.ts` and `lib/types/**/*.d.ts` with `skipLibCheck: false`, so errors
   *inside* the declaration files are reported (not only at use sites);
2. `test/types/contract.ts`, a compile-time contract test that pins the public
   surface with positive assertions and `@ts-expect-error` negative assertions.

A drift between runtime behaviour and declaration fails the gate:

```
$ fibjs --check
lib/types/...: error TS2322: Type 'void' is not assignable to type 'TaskRecord | null'.
$ echo $?
2
```

Because `@ts-expect-error` must be *used*, removing a restriction also fails the
gate — the contract test cannot silently rot into a no-op.

## Runtime complements

Two things cannot be expressed in types and are covered by runtime tests instead:

- `test/types.test.js` asserts that `resumeTask` / `pauseTask` / `cancelTask`
  really return the updated row, that `package.json#types` points at a real file,
  and that every runtime export has a declaration.
- `deleteTasks({})` is rejected at runtime (empty filters would wipe the table),
  not at compile time — a type-level ban would forbid building filters
  dynamically.
