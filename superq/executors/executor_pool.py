import logging
import random
from datetime import timedelta
from typing import ClassVar, TypeVar

from superq import callbacks, tasks
from superq.executors import executor_asyncio, executor_base, executor_process, executor_thread

TaskExecutorProcessPoolType = TypeVar('TaskExecutorProcessPoolType', bound='TaskExecutorProcessPool')

log = logging.getLogger(__name__)


class TaskExecutorProcessPool(executor_base.BaseTaskExecutor):
    """
    A higher-level task executor that manages a pool of child process executors.
    """

    EXECUTOR: ClassVar[type['executor_process.ProcessTaskExecutor']] = executor_process.ProcessTaskExecutor  # type: ignore [type-abstract]
    TYPE: ClassVar[executor_base.ChildWorkerType] = 'process'

    max_processes: int
    max_tasks: int
    tasks_per_restart: int
    idle_process_ttl: timedelta
    callbacks: 'callbacks.CallbackRegistry'
    worker_name: str | None
    worker_host: str | None
    procs: list['executor_process.ProcessTaskExecutor']

    __slots__ = (
        'max_processes',
        'max_tasks',
        'tasks_per_restart',
        'idle_process_ttl',
        'callbacks',
        'worker_name',
        'worker_host',
        'procs',
    )

    def __init__(
        self,
        max_processes: int,
        max_tasks: int,
        tasks_per_restart: int,
        idle_process_ttl: timedelta,
        callbacks: 'callbacks.CallbackRegistry',  # type: ignore [name-defined]
        worker_name: str | None = None,
        worker_host: str | None = None,
    ) -> None:
        self.procs = []
        self.max_processes = max_processes
        self.max_tasks = max_tasks
        self.tasks_per_restart = tasks_per_restart
        self.callbacks = callbacks
        self.idle_process_ttl = idle_process_ttl
        self.worker_name = worker_name
        self.worker_host = worker_host

    @property
    def max_tasks_per_process(self) -> int:
        return self.max_tasks // self.max_processes

    @property
    def capacity(self) -> int:
        """
        Return the number of additional tasks that may be submitted across all event loops.
        """
        capacity = 0
        for i in range(self.max_processes):
            if i < len(self.procs):
                capacity += max(self.procs[i].capacity, 0)
            else:
                capacity += max(self.max_tasks_per_process, 0)
        return capacity

    @property
    def active(self) -> int:
        """
        Return the number of incomplete (pending or running) tasks assigned to this executor.
        """
        return max(sum(proc.active for proc in self.procs), 0)

    def submit_task(self: TaskExecutorProcessPoolType, task: 'tasks.Task') -> 'TaskExecutorProcessPoolType':
        """
        Add a task that runs in the event loop with the most capacity.
        It is the responsibility of the caller to ensure that `capacity` is greater than 0.
        """
        # Start the first child process if none yet exist
        if not self.procs:
            executor = self.EXECUTOR(
                max_concurrency=self.max_tasks_per_process,
                tasks_per_restart=self.tasks_per_restart,
                idle_ttl=self.idle_process_ttl,
                callbacks=self.callbacks,
                worker_name=self.worker_name,
                worker_host=self.worker_host,
            )
            self.procs.append(executor)
            executor.submit_task(task)
            return self

        # Find the first inactive child process with capacity (if exists)
        next_empty_idx = next((i for i, p in enumerate(self.procs) if p.capacity and not p.active), None)
        if next_empty_idx is not None:
            self.procs[next_empty_idx].submit_task(task)
            return self

        # Create a new child processor if there's room and all others are active
        if len(self.procs) < self.max_processes:
            executor = self.EXECUTOR(
                max_concurrency=self.max_tasks_per_process,
                tasks_per_restart=self.tasks_per_restart,
                idle_ttl=self.idle_process_ttl,
                callbacks=self.callbacks,
                worker_name=self.worker_name,
                worker_host=self.worker_host,
            )
            self.procs.append(executor)
            executor.submit_task(task)
            return self

        max_child_capacity = 0
        next_child_idx = 0

        # Iterate to find the child with the most capacity
        for child_idx, proc in enumerate(self.procs):
            if proc.capacity > max_child_capacity:
                next_child_idx = child_idx
                max_child_capacity = proc.capacity

        # Choose a random child if all children have full capacity (this should not happen)
        if max_child_capacity <= 0:
            log.warning(f'Task {task.id} submitted to task executor with no capacity: running anyway')
            next_child_idx = random.randint(0, len(self.procs) - 1)

        # Submit this task to the event loop with the most capacity
        log.debug(f'Submitting task {task.id} to {self.TYPE} pool at index {next_child_idx}')
        self.procs[next_child_idx].submit_task(task)
        return self

    def kill(self, graceful: bool) -> None:
        """
        Propagate kill signal to all child processes.
        """
        for proc in self.procs:
            proc.kill(graceful=graceful)


class EventLoopTaskExecutorProcessPool(TaskExecutorProcessPool):  # type: ignore [misc]
    """
    A higher-level task executor that manages a process pool of child event loop executors.
    """

    TYPE = 'asyncio'
    EXECUTOR = executor_asyncio.AsyncioTaskExecutor


class ThreadTaskExecutorProcessPool(TaskExecutorProcessPool):  # type: ignore [misc]
    """
    A higher-level task executor that manages a process pool of child thread executors.
    """

    TYPE = 'thread'
    EXECUTOR = executor_thread.ThreadTaskExecutor
