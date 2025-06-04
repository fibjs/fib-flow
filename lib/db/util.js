/**
 * Parse task payload and result based on task status
 * For completed tasks, parse result as JSON
 * For other statuses, handle subtask returns
 * @param {object} task - Task object to parse
 * @returns {object|null} Parsed task object
 */
function parseTask(task) {
    if (!task) return null;

    // Parse the payload if it's stored as string
    if (typeof task.payload === 'string') {
        try {
            task.payload = JSON.parse(task.payload);
        } catch (e) {
            // Keep original payload if parsing fails
        }
    }

    // Parse the result based on task status
    if (task.status === 'completed' || task.stage === 0) {
        // For completed tasks, try to parse result as JSON
        if (typeof task.result === 'string') {
            try {
                task.result = JSON.parse(task.result);
            } catch (e) {
                // Keep original result if parsing fails
            }
        }
    } else {
        // For other statuses, process results as task returns
        if (typeof task.result === 'string' && task.result !== '') {
            try {
                const results = task.result.split('\n');
                task.result = [];
                
                for (let i = 0; i < results.length; i++) {
                    const matchs = results[i].match(/^(\d+)([:!])(.*)$/);
                    if (!matchs) continue;
                    // Extract task_id and result from the string
                    if (matchs[2] === '!') {
                        // If the result starts with '!', it's an error
                        task.result.push({
                            task_id: parseInt(matchs[1], 10),
                            error: JSON.parse(matchs[3])
                        });
                    } else if (matchs[2] === ':') {
                        // If the result starts with ':', it's a normal result
                        task.result.push({
                            task_id: parseInt(matchs[1], 10),
                            result: JSON.parse(matchs[3])
                        });
                    }
                }
                
                // Sort results by task_id for consistent ordering
                task.result.sort((a, b) => a.task_id - b.task_id);
            } catch (e) {
                // Keep original result if parsing fails
            }
        }
    }

    return task;
}

module.exports = {
    parseTask
};