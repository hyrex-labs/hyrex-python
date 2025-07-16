# Hyrex Python SDK

Hyrex is a modern, open-source task orchestration framework built on PostgreSQL. It provides powerful features for distributed task processing, workflow management, and asynchronous job execution.

## Features

- **Task Orchestration**: Define and execute distributed tasks with automatic retries, timeouts, and error handling
- **Workflow Support**: Build complex DAG-based workflows with dependencies
- **Queue Management**: Route tasks to different queues with separate worker pools
- **Key-Value Store**: Built-in distributed key-value storage for task coordination
- **Task Context**: Rich execution context with task metadata and hierarchy tracking
- **Cron Scheduling**: Schedule recurring tasks with cron expressions
- **Hyrex Studio**: Web-based UI for monitoring and debugging (available at https://local.hyrex.studio)
- **Type Safety**: Full type hints and Pydantic model validation

## Installation

```bash
pip install hyrex
```

## Quick Start

### 1. Initialize a New Project

Use the interactive `hyrex init` command to set up a new project:

```bash
hyrex init
```

This will guide you through:
- Choosing a project name
- Selecting between PostgreSQL (self-hosted) or Hyrex Cloud
- Creating project files (`.env`, `hyrex_app.py`, `tasks.py`, `requirements.txt`, `Dockerfile`)

For manual database initialization (if needed):

```bash
export HYREX_DATABASE_URL="postgresql://user:password@localhost/dbname"
hyrex init-db
```

### 2. Project Structure

After running `hyrex init`, you'll have:

```
your-project/
├── .env                # Environment configuration
├── hyrex_app.py       # Hyrex app configuration
├── tasks.py           # Task definitions
├── requirements.txt   # Python dependencies
└── Dockerfile         # Container configuration
```

Example `tasks.py`:

```python
from hyrex import HyrexRegistry
from pydantic import BaseModel

hy = HyrexRegistry()

class EmailContext(BaseModel):
    to: str
    subject: str
    body: str

@hy.task
def send_email(context: EmailContext):
    print(f"Sending email to {context.to}")
    # Your email logic here
    return {"sent": True}
```

### 3. Send Tasks

Queue tasks for execution:

```python
# Send a task to the default queue
send_email.send(EmailContext(
    to="user@example.com",
    subject="Welcome!",
    body="Thanks for signing up"
))

# Send with custom configuration
send_email.with_config(
    queue="high-priority",
    max_retries=3,
    timeout_seconds=30
).send(EmailContext(...))
```

### 4. Run Workers

Start workers to process tasks:

```bash
hyrex run-worker hyrex_app:app
```

## Core Features

### Task Decorator

The `@task` decorator transforms functions into distributed tasks:

```python
@hy.task(
    queue="processing",           # Target queue (str or HyrexQueue object)
    max_retries=3,               # Maximum retry attempts (default: 0)
    timeout_seconds=300,         # Task timeout in seconds
    priority=5,                  # Priority 1-10 (higher = more important)
    on_error=error_handler,      # Error callback function
    retry_backoff=lambda n: n*10 # Backoff strategy function
)
def process_data(context: ProcessContext):
    # Task implementation
    pass
```

### Task Context

Access rich execution context within tasks:

```python
from hyrex import get_hyrex_context

@hy.task
def contextual_task():
    context = get_hyrex_context()

    print(f"Task ID: {context.task_id}")
    print(f"Task Name: {context.task_name}")
    print(f"Attempt: {context.attempt_number}/{context.max_retries}")
    print(f"Queue: {context.queue}")
    print(f"Parent Task: {context.parent_id}")
    print(f"Root Task: {context.root_id}")
```

### Key-Value Store

Use HyrexKV for distributed state management:

```python
from hyrex import HyrexKV

@hy.task
def process_with_state(user_id: str):
    # Store state
    HyrexKV.set(f"user:{user_id}:status", "processing")

    # Retrieve state
    status = HyrexKV.get(f"user:{user_id}:status")

    # Delete state
    HyrexKV.delete(f"user:{user_id}:status")
```

**Note**: HyrexKV stores string values up to 1MB in size.

### Workflows

Build complex DAG-based workflows:

```python
@hy.task
def extract_data():
    return {"data": "extracted"}

@hy.task
def transform_data():
    return {"data": "transformed"}

@hy.task
def load_data():
    return {"data": "loaded"}

class ETLWorkflowArgs(BaseModel):
    source: str
    destination: str

@hy.workflow(
    queue="etl",
    timeout_seconds=3600,
    workflow_arg_schema=ETLWorkflowArgs
)
def etl_workflow():
    # Define workflow DAG
    extract_data >> transform_data >> load_data

    # Parallel execution
    extract_data >> [transform_data, validate_data] >> load_data

    # With custom config
    extract_data >> transform_data.with_config(queue="cpu-intensive") >> load_data
```

Send workflows:

```python
etl_workflow.send(ETLWorkflowArgs(
    source="s3://input",
    destination="s3://output"
))
```

Access workflow context:

```python
from hyrex import get_hyrex_workflow_context

@hy.task
def workflow_task():
    wf_context = get_hyrex_workflow_context()

    # Access workflow arguments
    args = wf_context.workflow_args

    # Access other task results
    extract_result = wf_context.durable_runs.get("extract_data")
    if extract_result:
        extract_result.refresh()  # Get latest status
```

### Dynamic Task Configuration

Use `with_config()` to modify task behavior at runtime:

```python
# Define base task
@hy.task(queue="default", max_retries=1)
def flexible_task(data: str):
    return process(data)

# Override configuration when sending
flexible_task.with_config(
    queue="high-priority",
    max_retries=5,
    timeout_seconds=60,
    priority=10
).send("important-data")
```

### Cron Scheduling

Schedule recurring tasks:

```python
@hy.task(cron="0 2 * * *")  # Daily at 2 AM
def daily_cleanup():
    # Cleanup logic
    pass

# Tasks with default arguments can also be scheduled
@hy.task(cron="0 0 * * 0")  # Weekly on Sunday
def weekly_backup(retention_days: int = 30):
    # Backup logic with configurable retention
    pass

@hy.workflow(cron="0 0 * * 0")  # Weekly on Sunday
def weekly_report():
    generate_report >> send_report
```

**Note**: Cron-scheduled tasks must have no arguments or all arguments must have default values.

### Error Handling

Implement custom error handlers:

```python
def handle_task_error(error: Exception):
    # Log error, send alerts, etc.
    print(f"Task failed: {error}")

@hy.task(
    on_error=handle_task_error,
    max_retries=3,
    retry_backoff=lambda attempt: 2 ** attempt  # Exponential backoff
)
def risky_task():
    # Task that might fail
    pass
```

## CLI Commands

- `hyrex init` - Interactive project initialization wizard
- `hyrex init-db` - Initialize the database schema
- `hyrex run-worker <module:instance>` - Start a worker process
- `hyrex studio` - Start Hyrex Studio server

## Monitoring with Hyrex Studio

Hyrex Studio provides a web interface for monitoring your tasks and workflows:

1. Start the studio server:

   ```bash
   hyrex studio
   ```

2. Open https://local.hyrex.studio in your browser

3. Monitor task execution, view logs, and inspect your data

## Configuration

Hyrex uses environment variables for configuration:

- `HYREX_DATABASE_URL` - PostgreSQL connection string (required)
- `STUDIO_PORT` - Port for Hyrex Studio (default: 1337)
- `STUDIO_VERBOSE` - Enable verbose logging for Studio (default: false)

## Advanced Usage

### Registry Inheritance

Share task definitions across modules:

```python
# common_tasks.py
common_registry = HyrexRegistry()

@common_registry.task
def shared_task():
    pass

# main.py
from common_tasks import common_registry

hy = HyrexRegistry()
hy.add_registry(common_registry)  # Include all tasks from common_registry
```

### Task Composition

Build complex task hierarchies:

```python
@hy.task
def parent_task(count: int):
    # Spawn child tasks
    for i in range(count):
        child_task.send(index=i)

    # Tasks maintain parent-child relationships
    # visible in context.parent_id and context.root_id
```

### Idempotency

Ensure tasks run only once:

```python
@hy.task
def idempotent_task(user_id: str):
    # Process user
    pass

# Using idempotency key
idempotent_task.with_config(
    idempotency_key=f"process-user-{user_id}"
).send(user_id="123")
```

## Requirements

- Python 3.11+
- PostgreSQL 12+
- Required Python packages are automatically installed with pip

## License

Apache License 2.0

## Links

- [GitHub Repository](https://github.com/hyrex-labs/hyrex-python)
- [Documentation](https://github.com/hyrex-labs/hyrex-python)
- [Hyrex Studio](https://local.hyrex.studio)
