# Task Handler System

The task handler system in fib-flow provides a flexible way to define and manage task execution logic across different workers and environments.

## Table of Contents
- [Handler Registration](#handler-registration)
- [Worker Specialization](#worker-specialization)
- [Handler Selection](#handler-selection)
- [Example Configuration](#example-configuration)
- [Usage Scenarios](#usage-scenarios)
- [Task Lifecycle](#task-lifecycle)
- [Best Practices](#best-practices)

## Handler Registration

Task handlers can be registered in two forms:

1. **Function Form** - Direct handler registration
   - Simple and straightforward
   - Uses global TaskManager defaults
   - Best for basic task processing

2. **Object Form** - Handler with configuration
   - Provides task-type specific defaults
   - Overrides global configuration
   - Supports advanced task control

Example:
```javascript
// Function form - uses global defaults
taskManager.use('simpleTask', async (task) => {
    return { result: 'done' };
});

// Object form - with task-specific defaults
taskManager.use('complexTask', {
    handler: async (task) => {
        return { result: 'done' };
    },
    timeout: 120,       // 2 minutes timeout
    max_retries: 2,     // Maximum 2 retries
    retry_interval: 30, // Retry every 30 seconds
    priority: 5         // Higher priority for this task type
});
```

### Configuration Options

When using object form registration, the following options are available:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| handler | Function | Required | Task implementation function |
| timeout | Number | 60 | Task execution timeout in seconds |
| max_retries | Number | 3 | Maximum total attempts (including initial attempt) |
| retry_interval | Number | 0 | Delay between retries in seconds |
| priority | Number | - | Default priority level for tasks |

Notes:
- Handler options take precedence over global TaskManager options
- Options can be overridden per task instance
- Function form handlers use global TaskManager defaults
- Invalid handler registration will throw an error

## Worker Specialization

### Load Balancing
- Multiple workers can register common handlers
- Tasks are distributed across available workers
- Automatic failover if a worker becomes unavailable
- Improved system throughput and reliability

### Specialized Processing
- GPU-dependent tasks can be routed to GPU-enabled workers
- Memory-intensive tasks can be directed to high-memory workers
- Special hardware requirements (e.g., TPU, FPGA) can be accommodated

### Advanced Worker Configuration
```javascript
// Configure worker with specific capabilities
taskManager.configureWorker({
    maxConcurrency: 5,
    capabilities: ['GPU', 'high-memory'],
    retryStrategy: {
        maxAttempts: 3,
        backoff: 'exponential'
    }
});
```

## Handler Selection
- Tasks are automatically routed to workers with matching handlers
- If multiple workers are available, load is balanced automatically
- Tasks requiring specific resources wait for appropriate workers
- Ensures optimal resource utilization across the system

## Example Configuration

```javascript
// Handler for data processing tasks
taskManager.use('data_processing', async (task) => {
    const { data } = task.payload;
    return { result: await processData(data) };
});

// Handler for GPU-intensive tasks
taskManager.use('gpu_task', async (task) => {
    if (!hasGPU) {
        throw new Error('GPU not available');
    }
    return { result: await gpuProcess(task.payload) };
});

// Handler for memory-intensive tasks
taskManager.use('memory_task', async (task) => {
    if (getAvailableMemory() < requiredMemory) {
        throw new Error('Insufficient memory');
    }
    return { result: await largeDataProcess(task.payload) };
});
```

## Usage Scenarios

### Data Processing Pipeline
```javascript
// Process large datasets across multiple workers
taskManager.use('process_dataset', async (task) => {
    const { dataset_id, chunk_size } = task.payload;
    
    // Split dataset into chunks for parallel processing
    const chunks = await splitDataset(dataset_id, chunk_size);
    
    // Create child tasks for each chunk
    return next(chunks.map(chunk => ({
        name: 'process_chunk',
        payload: { chunk_id: chunk.id }
    })));
});
```

### Resource-Specific Tasks
```javascript
// GPU-specific task handler
taskManager.use('train_model', async (task) => {
    if (!hasGPU()) {
        throw new Error('GPU required for this task');
    }
    // ... training code ...
});
```

## Task Lifecycle
- Task Creation: Tasks are created with specific type and payload
- Handler Selection: System matches task with registered handlers
- Execution: Selected worker processes the task
- Result Handling: Results are collected and processed
- Error Management: Failed tasks are retried or marked as failed

## Best Practices
- Register handlers early in application lifecycle
- Implement proper error handling and retries
- Monitor worker health and task completion rates
- Use appropriate task priorities for critical operations
- Implement graceful shutdown handling
