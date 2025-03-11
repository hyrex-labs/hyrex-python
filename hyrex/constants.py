# Default queue for enqueued tasks
DEFAULT_QUEUE = "default"
# Default task priority
DEFAULT_PRIORITY = 5
# Default for executors pulling tasks
ANY_QUEUE = "*"
# Default # of executors when running worker via CLI
DEFAULT_EXECUTOR_PROCESSES = 8

WORKER_ADMIN_PROCESS_TIMEOUT = 5.0
WORKER_EXECUTOR_PROCESS_TIMEOUT = 5.0
WORKER_CRON_SCHEDULER_PROCESS_TIMEOUT = 5.0
WORKER_HEARTBEAT_FREQUENCY = 30.0

# How often to refresh queues for round-robin processing
WORKER_EXECUTOR_QUEUE_REFRESH_SECONDS = 300
