# Usage Examples

This section provides practical examples of using fib-flow in different scenarios.

## Table of Contents
- [Async Task Examples](#async-task-examples)
  - [Basic Tasks](#basic-tasks)
  - [Delayed Tasks](#delayed-tasks)
  - [Priority Tasks](#priority-tasks)
- [Cron Task Examples](#cron-task-examples)
  - [Daily Tasks](#daily-tasks)
  - [Weekly Tasks](#weekly-tasks)
  - [Monthly Tasks](#monthly-tasks)
- [Workflow Examples](#workflow-examples)
  - [Simple Workflow](#simple-workflow)
  - [Error Handling and Retry](#error-handling-and-retry)
  - [Task Control](#task-control)
  - [Nested Tasks](#nested-tasks)

## Async Task Examples

Async tasks are one-time operations that can be scheduled to run immediately or with a delay. These tasks are perfect for handling background operations that don't require periodic execution.

### Basic Task
The simplest form of async task execution. Use this when you need to process a task immediately in the background.

```javascript
taskManager.async('processOrder', {
    orderId: '12345',
    userId: 'user789'
});
```
This example demonstrates processing an order asynchronously, which is ideal for tasks that shouldn't block the main execution flow.

### Delayed Task
Delayed tasks are useful when you need to schedule a task to run after a specific time interval.

```javascript
taskManager.async('sendReminder', {
    userId: 'user123',
    message: 'Don\'t forget to complete your profile!'
}, {
    delay: 3600  // Send reminder after 1 hour
});
```
Perfect for scenarios like sending follow-up reminders or scheduling delayed notifications.

### Priority Task
When certain tasks need to be processed before others, you can assign them a priority level.

```javascript
taskManager.async('sendNotification', {
    userId: 'user456',
    type: 'urgent',
    message: 'System alert!'
}, {
    priority: 10  // Higher priority task
});
```
Higher priority values ensure the task gets processed sooner in the queue.

## Cron Task Examples

Cron tasks are recurring operations that follow a specified schedule. They're ideal for maintenance tasks and periodic operations.

### Daily Task
Tasks that need to run once every day at a specific time.

```javascript
taskManager.cron('dailyReport', '0 0 * * *', {
    reportType: 'daily',
    recipients: ['admin@example.com']
});
```
This example shows how to schedule a daily report generation at midnight.

### Weekly Task
For operations that need to run once a week.

```javascript
taskManager.cron('weeklyBackup', '0 0 * * 0', {
    backupType: 'full',
    destination: '/backups'
}, {
    timeout: 3600  // Allow up to 1 hour for backup
});
```
This task runs every Sunday at midnight and includes a timeout configuration for longer-running operations.

### Monthly Task
Tasks that execute once per month.

```javascript
taskManager.cron('monthlyCleanup', '0 0 1 * *', {
    older_than: '30d',
    target_dir: '/tmp'
}, {
    max_retries: 5,
    retry_interval: 600
});
```
This example demonstrates a monthly cleanup task with retry logic for improved reliability.

## Workflow Examples

### Simple Workflow
A simple workflow executes tasks in a sequential manner:

```javascript
// Define a simple linear workflow
taskManager.use('orderProcessing', (task, next) => {
    if (task.stage === 0) {
        // First validate the order
        return next({
            name: 'validateOrder',
            payload: { orderId: task.payload.orderId }
        });
    } else if (task.stage === 1) {
        // Then process payment
        return next({
            name: 'processPayment',
            payload: { orderId: task.payload.orderId }
        });
    }
    
    // Finally return the result
    return { status: 'completed' };
});

// Execute the workflow
taskManager.async('orderProcessing', { orderId: '123' });
```
This example shows a linear workflow where tasks are executed one after another.

### Error Handling and Retry
Handling errors and retrying tasks:

```javascript
taskManager.async('taskWithRetry', {
    taskData: { /* ... */ }
}, {
    retries: 3,
    retryDelay: 5000  // Retry every 5 seconds
});
```
This example demonstrates how to handle errors and retry tasks.

### Task Control
Controlling task execution:

```javascript
const taskId = taskManager.async('longRunningTask', { /* ... */ });

// Pause the task
taskManager.pauseTask(taskId);

// Resume the task
taskManager.resumeTask(taskId);
```
This example shows how to pause and resume tasks.

### Nested Tasks
Nested tasks allow you to create complex hierarchical task structures where parent tasks can spawn and manage multiple child tasks:

```javascript
// Define child tasks
taskManager.use('validateInventory', task => {
    return { result: 'inventory_validated' };
});

taskManager.use('processShipment', task => {
    return { result: 'shipment_processed' };
});

taskManager.use('sendNotifications', (task, next) => {
    if (task.stage === 0) {
        // Spawn multiple notification tasks in parallel
        return next([
            {
                name: 'sendSingleNotification',
                payload: { type: 'customer', orderId: task.payload.orderId }
            },
            {
                name: 'sendSingleNotification',
                payload: { type: 'warehouse', orderId: task.payload.orderId }
            }
        ]);
    }
    return { result: 'notifications_sent' };
});

// Define parent task that coordinates multiple child tasks
taskManager.use('fulfillOrder', (task, next) => {
    if (task.stage === 0) {
        // Run inventory check first
        return next({ 
            name: 'validateInventory', 
            payload: { items: task.payload.items } 
        });
    } else if (task.stage === 1) {
        // Then process shipment and notifications in parallel
        return next([
            { 
                name: 'processShipment', 
                payload: { 
                    items: task.payload.items,
                    address: task.payload.shippingAddress 
                } 
            },
            { 
                name: 'sendNotifications', 
                payload: { orderId: task.payload.orderId } 
            }
        ]);
    }
    
    // Finally return the result
    return { result: 'order_fulfilled' };
});
```
This example shows how nested tasks can create complex workflows with parallel execution and task hierarchies.
