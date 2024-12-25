# Workflow Guide

fib-flow provides comprehensive support for complex task workflows, enabling you to create sophisticated task hierarchies and manage dependencies effectively.

## Table of Contents
- [Core Workflow Concepts](#core-workflow-concepts)
  - [Parent-Child Relationships](#parent-child-relationships)
  - [Task Stage Management](#task-stage-management)
  - [State Management](#state-management)
  - [Task Monitoring](#task-monitoring)
- [Workflow Examples](#workflow-examples)
  - [Basic Parent-Child Workflow](#basic-parent-child-workflow)
  - [Nested Workflows](#nested-workflows)
  - [Error Handling in Workflows](#error-handling-in-workflows)

## Core Workflow Concepts

### Parent-Child Relationships
- Parent tasks can create multiple child tasks
- Parent task enters `suspended` state while waiting for children
- Parent task resumes only when all children complete successfully

### Task Stage Management
- Each task has an internal `stage` attribute
- `stage` starts at `0` and increments automatically
- Enables multi-phase task processing and workflow control
- Allows conditional task creation and execution based on current stage

```javascript
taskManager.use('complex_workflow', (task, next) => {
    switch (task.stage) {
        case 0:
            // Initial validation or preparation
            return next([{ name: 'prepare_task' }]);
        case 1:
            // Main processing
            return next([{ name: 'process_task' }]);
        case 2:
            // Finalization
            return { completed: true };
    }
});
```

### State Management
For detailed information about task states and transitions, see [Task States and Transitions](core-concepts.md#task-states-and-transitions).

Key workflow-specific state behaviors:
- Parent tasks automatically transition to `suspended` when creating children
- Child task failures automatically propagate to parent:
  * Async parent tasks → `permanently_failed`
  * Cron parent tasks → `paused`
- No parent task callback on child failure - state changes are automatic

State transition rules in workflows:
- Child tasks inherit retry settings from parent unless explicitly overridden
- Parent tasks remain `suspended` until all children reach terminal states
- Cancelling a parent task automatically cancels all pending children
- Resume operations trigger re-execution of failed children only

### Task Monitoring
- Track entire workflow progress through task states
- Access child task results and errors
- Query tasks by parent-child relationships

Monitor workflow execution through:
- Real-time state tracking via event listeners
- Hierarchical task relationship queries
- Aggregate progress reporting for complex workflows
- Child task completion statistics and metrics

## Workflow Examples

### Basic Parent-Child Workflow
```javascript
// Parent task handler - creates and manages child tasks
taskManager.use('parent_task', (task, next) => {
    // First execution - create child tasks
    if (task.stage == 0) {
        console.log('Starting first phase');
        // Create child tasks or perform initial processing
        return next([
            {
                name: 'child_task1',
                payload: { phase: 'initialization' }
            },
            {
                name: 'child_task2',
                payload: { phase: 'processing' }
            }
        ]);
    }

    // Called only when all children complete successfully
    return { result: 'workflow_complete' };
});

// Child task handlers
taskManager.use('child_task1', task => {
    return { result: 'child1_result' };
});

taskManager.use('child_task2', task => {
    return { result: 'child2_result' };
});

// Start the workflow
const parentId = taskManager.async('parent_task', { data: 'parent_data' });
```

### Nested Workflows
```javascript
// Root task creates middle-level tasks
taskManager.use('root_task', (task, next) => {
    if (task.stage == 0) {
        // Initial validation or preparation
        return next([{
            name: 'middle_task',
            payload: { level: 1 }
        }]);
    }
    return { result: 'root_done' };
});

// Middle task creates leaf tasks
taskManager.use('middle_task', (task, next) => {
    if (task.stage == 0) {
        // Main processing
        return next([{
            name: 'leaf_task',
            payload: { level: 2 }
        }]);
    }
    return { result: 'middle_done' };
});

// Leaf task performs actual work
taskManager.use('leaf_task', task => {
    return { result: 'leaf_done' };
});
```

### Error Handling in Workflows
For basic error handling patterns, see [Error Handling](core-concepts.md#error-handling).

Workflow-specific error handling features:
- Customizable retry strategies per task level
- Automatic error propagation through task hierarchy
- Selective child task retry capabilities
- Error isolation and containment options

Workflow-specific error handling example:
```javascript
taskManager.use('parent_task', (task, next) => {
    // Only show workflow-specific error handling
    return next([{
        name: 'risky_task',
        max_retries: 3,
        retry_interval: 60
    }]);
});
```

Advanced error handling strategies:
- Configure different retry policies for different workflow branches
- Implement custom error recovery logic at any level
- Define fallback tasks for handling persistent failures
- Set up error notification and alerting per workflow