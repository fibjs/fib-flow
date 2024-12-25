# Cron Syntax Guide

This guide provides a comprehensive overview of cron syntax used in fib-flow for scheduling tasks. Whether you're new to cron expressions or need a quick reference, you'll find detailed explanations of the syntax, fields, special characters, and practical examples to help you schedule your tasks effectively.

## Table of Contents
- [Syntax Overview](#syntax-overview)
- [Field Descriptions](#field-descriptions)
- [Special Characters](#special-characters)
- [Examples](#examples)

## Syntax Overview

Cron expressions are used to define the schedule for recurring tasks. The syntax consists of six fields separated by spaces:

```
*    *    *    *    *    *
┬    ┬    ┬    ┬    ┬    ┬
│    │    │    │    │    |
│    │    │    │    │    └ day of week (0 - 7, 1L - 7L) (0 or 7 is Sun)
│    │    │    │    └───── month (1 - 12)
│    │    │    └────────── day of month (1 - 31, L)
│    │    └─────────────── hour (0 - 23)
│    └──────────────────── minute (0 - 59)
└───────────────────────── second (0 - 59, optional)
```

## Field Descriptions

1. **Second**: (optional) Specifies the exact second when the task should run. Valid values are 0-59.
   - Example: `30 * * * * *` runs at the 30th second of every minute
2. **Minute**: Specifies the exact minute when the task should run. Valid values are 0-59.
   - Example: `0 15 * * * *` runs at the 15th minute of every hour
3. **Hour**: Specifies the exact hour when the task should run. Valid values are 0-23.
   - Example: `0 0 8 * * *` runs at 8:00 AM every day
4. **Day of Month**: Specifies the day of the month when the task should run. Valid values are 1-31. The character `L` can be used to specify the last day of the month.
   - Example: `0 0 0 15 * *` runs at midnight on the 15th of every month
5. **Month**: Specifies the month when the task should run. Valid values are 1-12 or JAN-DEC.
   - Using numbers: `0 0 0 1 6 *` runs on June 1st at midnight
   - Using names: `0 0 0 1 JUN *` (same as above)
6. **Day of Week**: Specifies the day of the week when the task should run. Valid values are 0-7 (where 0 and 7 are both Sunday) or SUN-SAT. The character `L` can be used to specify the last day of the week.
   - Using numbers: `0 0 0 * * 1` runs every Monday at midnight
   - Using names: `0 0 0 * * MON` (same as above)
   - Last occurrence: `0 0 0 * * 5L` runs on the last Friday of every month

## Special Characters

- `*` - Matches any value
- `,` - Separates items in a list (e.g., `MON,WED,FRI`)
- `-` - Specifies a range (e.g., `1-5`)
- `/` - Specifies increments (e.g., `*/15` for every 15 minutes)
- `L` - Last day of the month or week (e.g., `L` in the day-of-month field means the last day of the month)

Note: The `W` character (nearest weekday) is not supported.

## Examples

Here are some common use cases with explanations:

- `* * * * * *` - Every second
  - Most frequent possible execution
- `0 */5 * * * *` - Every 5 minutes
  - Useful for frequent background tasks
- `0 0 0 * * *` - Every day at midnight
  - Common for daily maintenance tasks
- `0 0 9 * * 1-5` - Every weekday at 9 AM
  - Typical for business hour operations
- `0 0 12 1 * *` - At noon on the first day of every month
  - Suitable for monthly reports or billing tasks
- `0 0 0 L * *` - At midnight on the last day of every month
  - Perfect for end-of-month processing
- `0 0 9-17 * * 1-5` - Every hour from 9 AM to 5 PM on weekdays
  - Business hours operations
- `0 30 9 1,15 * *` - At 9:30 AM on the 1st and 15th of every month
  - Bi-monthly scheduled tasks

For more complex scheduling scenarios, consult the [cron-parser documentation](https://github.com/harrisiirak/cron-parser).