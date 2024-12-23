import os
from dataclasses import dataclass, field
from datetime import timedelta

from typing_extensions import Literal

from superq.executors import executor_base


@dataclass(slots=True)
class Config:  # type: ignore [misc]
    # Delete completed tasks from the backend after this long (set to 0 to keep tasks indefinitely)
    backend_task_ttl: timedelta = timedelta(days=14)
    # Path to database file used by SQLite (if using SQLite)
    backend_sqlite_path: str = ''
    # Connection string for Redis (if using Redis)
    backend_redis_url: str = ''
    # Prefix for Redis keys (if using Redis)
    backend_redis_key_prefix: str = 'task'
    # Connection string for MongoDB (if using MongoDB)
    backend_mongo_url: str = ''
    # Database name, if using MongoDB
    backend_mongo_database: str = 'db'
    # Collection name, if using MongoDB
    backend_mongo_collection: str = 'tasks'
    # Enable to use an in-memory backend
    backend_in_memory: bool = False
    # Set the default priority assigned to new tasks (tasks with lower-numbered priority run first)
    task_priority: int = 1
    # Automatically abort (and optionally reschedule) any task that runs longer than this
    task_timeout: timedelta = timedelta(hours=1)
    # Schedule tasks this far in the future if they fail and get retried
    task_retry_delay: timedelta = timedelta(minutes=1)
    # Number of times to retry a task that fails due to an exception (-1 = infinite retries)
    task_retries_for_error: int = 0
    # Number of times to retry a task that fails because of a signal (-1 = infinite retries)
    task_retries_for_signal: int = 1
    # Number of times to retry a task that fails because of a timeout (-1 = infinite retries)
    task_retries_for_timeout: int = 0
    # Number of times to retry a task that is delayed because of concurrency limits (-1 = infinite retries)
    task_retries_for_concurrency: int = -1
    # Force tasks to run synchronously, bypassing the async worker entirely (useful for testing and debugging)
    task_run_sync: bool = False
    # Log level to use for the worker process
    worker_log_level: Literal['', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] = ''
    # If True, the worker will not start a pool to handle process-type tasks
    worker_disable_process_pool: bool = False
    # If True, the worker will not start a pool to handle threaded tasks
    worker_disable_threads_pool: bool = False
    # If True, the worker will not start a pool to handle asyncio tasks
    worker_disable_asyncio_pool: bool = False
    # Whether to use processes or threads (this can also be set per-task)
    worker_default_type: 'executor_base.ChildWorkerTypeSync' = 'process'
    # Time to keep idle worker processes alive and ready to accept new tasks
    worker_idle_process_ttl: timedelta = timedelta(seconds=10)
    # Maximum concurrent processes managed by the worker
    worker_max_processes: int = field(default_factory=lambda: max((os.cpu_count() or 0), 4))
    # Maximum number of process tasks to complete before starting a fresh process
    worker_max_process_tasks_per_restart: int = 0
    # Maximum number of thread pools, each runs in its own process
    worker_max_thread_processes: int = field(default_factory=lambda: max((os.cpu_count() or 0), 4))
    # Maximum concurrent threads managed by the worker
    worker_max_threads_per_process: int = 16
    # Maximum number of thread tasks to complete before starting a fresh process
    worker_max_thread_tasks_per_restart: int = 64
    # Maximum number of asyncio event loops to run concurrently (in processes)
    worker_max_event_loops: int = field(default_factory=lambda: os.cpu_count() or 1)
    # Maximum number of coroutines to run concurrently in each event loop (asyncio only)
    worker_max_coroutines_per_event_loop: int = 16
    # Maximum number of coroutine tasks to complete before starting a fresh process
    worker_max_coroutine_tasks_per_restart: int = 64
    # Extra time to allow for tasks to finish before sending SIGTERM and shutting down
    worker_grace_period: timedelta = timedelta(seconds=90)
    # How frequently the worker checks for new tasks
    worker_poll_interval: timedelta = timedelta(seconds=5)
    # The max percent of an executor's total capacity that can be filled in a single pass
    worker_max_fill_ratio_per_loop: float = 0.25
    # How often the worker checks whether there are new periodic tasks to schedule
    worker_scheduler_interval: timedelta = timedelta(seconds=10)
    # How often the worker deletes tasks older than the `backend_task_ttl`
    worker_backend_task_ttl_interval: timedelta = timedelta(hours=1)
    # Name of the server running the worker (if not set, defaults to HOSTNAME env var)
    worker_hostname: str = field(default_factory=lambda: os.environ.get('HOSTNAME', ''))
    # How frequently an async function result polls for completion
    result_poll_interval: timedelta = timedelta(seconds=1)
