import logging
import random
from datetime import timedelta
from typing import ClassVar, TypeVar

from superq import callbacks, config, tasks
from superq.executors import executor_asyncio, executor_base, executor_process, executor_thread

TaskExecutorProcessPoolType = TypeVar('TaskExecutorProcessPoolType', bound='TaskExecutorProcessPool')

log = logging.getLogger(__name__)


class TaskExecutorProcessPool(executor_base.BaseTaskExecutor):
    """
    A higher-level task executor that manages a pool of child process executors.
    """

    EXECUTOR: ClassVar[type['executor_process.ProcessTaskExecutor']] = executor_process.ProcessTaskExecutor  # type: ignore [type-abstract]
    TYPE: ClassVar[executor_base.ChildWorkerType] = 'process'

    cfg: 'config.Config'
    max_processes: int
    max_concurrency: int
    tasks_per_restart: int
    idle_process_ttl: timedelta
    callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn']
    worker_name: str | None
    worker_host: str | None
    procs: list['executor_process.ProcessTaskExecutor']

    __slots__ = (
        'cfg',
        'max_processes',
        'max_concurrency',
        'tasks_per_restart',
        'idle_process_ttl',
        'callbacks',
        'worker_name',
        'worker_host',
        'procs',
    )

    def __init__(
        self,
        cfg: 'config.Config',
        callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn'],  # type: ignore [name-defined]
    ) -> None:
        self.cfg = cfg
        self.procs = []
        self.max_processes = cfg.worker_max_processes
        self.max_concurrency = cfg.worker_max_processes
        self.idle_process_ttl = cfg.worker_idle_process_ttl
        self.tasks_per_restart = cfg.worker_max_process_tasks_per_restart
        self.callbacks = callbacks
        self.worker_name = None
        self.worker_host = None

    @property
    def max_concurrency_per_process(self) -> int:
        return self.max_concurrency // self.max_processes

    @property
    def capacity(self) -> int:
        """
        Return the number of additional tasks that may be submitted across all event loops.
        """
        capacity = 0
        for i in range(self.max_processes):
            executor = self.procs[i] if i < len(self.procs) else None
            if executor and executor.alive:
                capacity += max(executor.capacity, 0)
            else:
                capacity += max(self.max_concurrency_per_process, 0)
        return max(capacity, -1)

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
            log.debug(f'Initializing new {self.TYPE} pool for task {task.fn.path} ({task.id})')
            executor = self.EXECUTOR(
                cfg=self.cfg,
                max_concurrency=self.max_concurrency_per_process,
                tasks_per_restart=self.tasks_per_restart,
                idle_ttl=self.idle_process_ttl,
                callbacks=self.callbacks,
                worker_name=f'{self.worker_name}-0',
                worker_host=self.worker_host,
            )
            self.procs.append(executor)
            executor.submit_task(task)
            return self

        # Revive any dead processes
        next_dead_idx = next((i for i, p in enumerate(self.procs) if not p.alive), None)
        if next_dead_idx is not None:
            log.debug(f'Reviving dead {self.TYPE} pool for task {task.fn.path} ({task.id}) at index {next_dead_idx}')
            executor = self.EXECUTOR(
                cfg=self.cfg,
                max_concurrency=self.max_concurrency_per_process,
                tasks_per_restart=self.tasks_per_restart,
                idle_ttl=self.idle_process_ttl,
                callbacks=self.callbacks,
                worker_name=f'{self.worker_name}-{next_dead_idx}',
                worker_host=self.worker_host,
            )
            self.procs[next_dead_idx] = executor
            executor.submit_task(task)
            return self

        # Find the first inactive child process with capacity (if exists)
        next_empty_idx = next((i for i, p in enumerate(self.procs) if p.capacity and not p.active), None)
        if next_empty_idx is not None:
            log.debug(
                f'Submitting to idle {self.TYPE} pool for task {task.fn.path} ({task.id}) at index {next_empty_idx}'
            )
            self.procs[next_empty_idx].submit_task(task)
            return self

        # Create a new child processor if there's room and all others are active
        if len(self.procs) < self.max_processes:
            log.debug(
                f'Initializing new process in {self.TYPE} pool for task '
                f'{task.fn.path} ({task.id}) at index {len(self.procs)}'
            )
            executor = self.EXECUTOR(
                cfg=self.cfg,
                max_concurrency=self.max_concurrency_per_process,
                tasks_per_restart=self.tasks_per_restart,
                idle_ttl=self.idle_process_ttl,
                callbacks=self.callbacks,
                worker_name=f'{self.worker_name}-{len(self.procs)}',
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
            next_child_idx = random.randint(0, len(self.procs) - 1)
            log.warning(
                f'Task {task.fn.path} ({task.id}) submitted to {self.TYPE} pool '
                f'with no capacity: running anyway at index {next_child_idx}'
            )

        # Submit this task to the event loop with the most capacity
        log.debug(
            f'Submitting to {self.TYPE} pool with {max_child_capacity} capacity '
            f'at index {next_child_idx} for task {task.fn.path} ({task.id})'
        )
        self.procs[next_child_idx].submit_task(task)
        return self

    def kill(self, graceful: bool) -> None:
        """
        Propagate kill signal to all child processes.
        """
        log.debug(f'Shutting down {len(self.procs)} processes in {self.TYPE} pool (graceful={graceful})')
        for proc in self.procs:
            proc.kill(graceful=graceful)


class AsyncioTaskExecutorProcessPool(TaskExecutorProcessPool):  # type: ignore [misc]
    """
    A higher-level task executor that manages a process pool of child event loop executors.
    """

    TYPE = 'asyncio'
    EXECUTOR = executor_asyncio.AsyncioTaskExecutor

    def __init__(
        self,
        cfg: 'config.Config',
        callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn'],
    ) -> None:
        self.cfg = cfg
        self.procs = []
        self.max_processes = cfg.worker_max_event_loops
        self.max_concurrency = cfg.worker_max_event_loops * cfg.worker_max_coroutines_per_event_loop
        self.idle_process_ttl = cfg.worker_idle_process_ttl
        self.tasks_per_restart = cfg.worker_max_coroutine_tasks_per_restart
        self.callbacks = callbacks
        self.worker_name = None
        self.worker_host = None


class ThreadTaskExecutorProcessPool(TaskExecutorProcessPool):  # type: ignore [misc]
    """
    A higher-level task executor that manages a process pool of child thread executors.
    """

    TYPE = 'thread'
    EXECUTOR = executor_thread.ThreadTaskExecutor

    def __init__(
        self,
        cfg: 'config.Config',
        callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn'],
    ) -> None:
        self.cfg = cfg
        self.procs = []
        self.max_processes = cfg.worker_max_thread_processes
        self.max_concurrency = cfg.worker_max_thread_processes * cfg.worker_max_threads_per_process
        self.idle_process_ttl = cfg.worker_idle_process_ttl
        self.tasks_per_restart = cfg.worker_max_thread_tasks_per_restart
        self.callbacks = callbacks
        self.worker_name = None
        self.worker_host = None
