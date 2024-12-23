import functools
import logging
import multiprocessing as mp
import os
import signal
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event, Lock
from typing import Any, ClassVar, Literal, NamedTuple, Optional, TypeVar

from superq import callbacks, config, tasks, workers
from superq.bson import ObjectId
from superq.executors import executor_base

log = logging.getLogger(__name__)

ProcessTaskExecutorType = TypeVar('ProcessTaskExecutorType', bound='ProcessTaskExecutor')


class ProcessTaskExecutor(executor_base.BaseTaskExecutor):
    """
    Wraps a child process that executes one task at a time.
    """

    TYPE: ClassVar[executor_base.ChildWorkerType] = 'process'
    proc: mp.Process | None
    info: 'ProcessTransceiver'
    cfg: 'config.Config'
    max_concurrency: int
    worker_host: str | None
    worker_name: str | None

    __slots__ = ('proc', 'info', 'cfg', 'max_concurrency', 'worker_host', 'worker_name')

    def __init__(
        self,
        cfg: 'config.Config',
        callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn'],
        max_concurrency: int,
        tasks_per_restart: int,
        idle_ttl: timedelta,
        worker_name: str | None = None,
        worker_host: str | None = None,
    ) -> None:
        self.proc = None
        self.cfg = cfg
        self.worker_host = worker_host
        self.worker_name = worker_name
        self.max_concurrency = max_concurrency
        self.info = ProcessTransceiver(
            idle_ttl=idle_ttl,
            max_concurrency=max_concurrency,
            tasks_per_restart=tasks_per_restart,
            callbacks=callbacks,
            worker_name=worker_name,
            worker_host=worker_host,
        )

    def submit_task(self: ProcessTaskExecutorType, task: 'tasks.Task') -> ProcessTaskExecutorType:
        """
        Submit a new task for execution. The caller is responsible for first checking `capacity`.
        """
        if self.info.is_shutting_down:
            log.warning(f'Submitted task {task.fn.path} ({task.id}) to {self.TYPE} process that is shutting down')

        self.info.submit_task(task)

        if not self.proc or not self.proc.is_alive():  # Start a new process if necessary
            log.debug(f'Launching a new {self.TYPE} child process for task {task.fn.path} ({task.id})')
            self.info.is_shutting_down = False
            self.proc = mp.Process(target=self.run, args=(self.info, self.cfg), name=self.worker_name)
            self.proc.start()
        return self

    @property
    def alive(self) -> bool:
        """
        Return True if the child process is alive.
        """
        return bool(self.proc and self.proc.is_alive())

    @property
    def capacity(self) -> int:
        """
        Return the number of additional tasks that may be submitted to this process.
        """
        return self.info.capacity

    @property
    def active(self) -> int:
        """
        Return the number of tasks currently executing in this process.
        """
        return self.info.active

    @staticmethod
    def init_logging(info: 'ProcessTransceiver', cfg: 'config.Config') -> None:
        if cfg.worker_log_level:
            logging.getLogger('superq').setLevel(cfg.worker_log_level)
        info.callbacks['on_child_logconfig'](info.worker_name)

    def kill(self, graceful: bool) -> None:
        """
        If `graceful=True`, sends SIGINT to the child process. Otherwise sends SIGTERM.
        """
        if self.proc and self.proc.pid and self.proc.is_alive():
            if graceful:
                log.debug(f'Gracefully shutting down {self.TYPE} executor with pid {self.proc.pid}')
                os.kill(self.proc.pid, workers.SIGNALS_SOFT_SHUTDOWN[0])
            else:
                log.debug(f'Forcefully shutting down {self.TYPE} executor with pid {self.proc.pid}')
                os.kill(self.proc.pid, workers.SIGNALS_HARD_SHUTDOWN[0])
        else:
            log.debug(f'Ignoring shutdown request for {self.TYPE} executor: no active process')

    @staticmethod
    def exit(task_registry: 'ProcessTaskRegistry', exit_code: Literal[0, 1], immediate=False) -> None:
        """
        Called from inside the child process to fail all expired tasks.
        """
        now = datetime.now()

        if immediate:
            log.debug('Forcing immediate shutdown of task executor')
            sys.exit(exit_code)

        # Fail and conditionally reschedule all tasks
        for task, task_expires_at in task_registry.iter():
            is_timeout = now >= task_expires_at
            error_type: tasks.TaskFailureType = 'TIMEOUT' if is_timeout else 'SIGNAL'
            error = f'Task {task.id} timed out' if is_timeout else f'Task {task.id} received shutdown signal'

            if is_timeout and task.can_retry_for_timeout:
                log.debug(f'Rescheduling task {task.fn.path} ({task.id}) for timeout: executor is shutting down')
                task.reschedule(error, error_type, incr_num_timeouts=True, run_sync=False)
            elif task.can_retry_for_signal:
                log.debug(f'Rescheduling task {task.fn.path} ({task.id}) for signal: executor is shutting down')
                task.reschedule(error, error_type, incr_num_recovers=True, run_sync=False)
            else:
                log.debug(f'Failing task {task.fn.path} ({task.id}) for signal: executor is shutting down')
                task.set_failed(error, error_type, incr_num_recovers=True)
                task.fn.cb.fn[task.fn.path]['on_failure'](task)
                task.fn.cb.task['on_task_failure'](task)

        log.debug(f'Task executor exiting with code {exit_code} after graceful shutdown')
        sys.exit(exit_code)

    def timeout(self) -> None:
        """
        Signal to the child process that it has timed out and should stop immediately.
        """
        self.info.is_shutting_down = True
        if self.proc and self.proc.is_alive() and self.proc.pid:
            log.debug(f'Killing {self.TYPE} executor process {self.proc.pid} for timeout')
            os.kill(self.proc.pid, workers.SIGNALS_TIMEOUT[0])
        else:
            log.debug(f'Shutting down inactive {self.TYPE} executor process for timeout')

    @classmethod
    def register_signal_handlers(cls, task_registry: 'ProcessTaskRegistry', info: 'ProcessTransceiver') -> None:
        """
        Setup signal handlers in a child process.
        """

        def force_shutdown(sig: int, *args: Any, is_timeout=False, **kwargs: Any) -> None:
            if info.is_shutting_down:  # Shutdown immediately on second signal
                log.warning(f'Child worker process received second signal {sig}: exiting immediately')
                cls.exit(task_registry, exit_code=1, immediate=True)

            info.is_shutting_down = True
            log.warning(
                f'Child worker process received {"timeout signal" if is_timeout else "signal"} {sig}: '
                'shutting down forcefully'
            )
            cls.exit(task_registry, exit_code=1)

        def graceful_shutdown(sig: int, *args: Any, **kwargs: Any) -> None:
            if info.is_shutting_down:  # Force shutdown on second signal
                return force_shutdown(sig, *args, **kwargs)

            info.is_shutting_down = True
            log.warning(f'Child worker process received signal {sig}: shutting down gracefully')

        # Fail with a timeout error
        def timeout_shutdown(sig: int, *args: Any, **kwargs: Any) -> None:
            return force_shutdown(sig, *args, is_timeout=True, **kwargs)

        # Handle all signals
        for sig in workers.SIGNALS_SOFT_SHUTDOWN:
            signal.signal(sig, graceful_shutdown)
        for sig in workers.SIGNALS_HARD_SHUTDOWN:
            signal.signal(sig, force_shutdown)
        for sig in workers.SIGNALS_TIMEOUT:
            signal.signal(sig, timeout_shutdown)

    @classmethod
    def run(cls, info: 'ProcessTransceiver', cfg: 'config.Config') -> None:
        """
        Run tasks in this child process continuously until shutdown.
        """
        task_registry = ProcessTaskRegistry()
        cls.register_signal_handlers(task_registry, info)
        cls.init_logging(info, cfg)
        info.set_started()

        def monitor_timeout(task: tasks.Task) -> None:
            """Automatically kill this process if the task times out."""
            time.sleep(task.fn.timeout.total_seconds())
            if task_registry.get(task.id):
                pid = os.getpid()
                log.warning(f'Task {task.fn.path} ({task.id}) timed out in process executor: killing process {pid}')
                os.kill(pid, workers.SIGNALS_TIMEOUT[0])

        while True:
            task = info.pop_task(task_registry)

            if task:
                log.debug(f'Starting task {task.fn.path} ({task.id}) in process executor')
                threading.Thread(target=functools.partial(monitor_timeout, task), daemon=True).start()
                try:
                    task.run(worker_name=info.worker_name, worker_host=info.worker_host, run_sync=False)
                finally:
                    info.on_task_complete(task, task_registry)
                log.debug(f'Finished task {task.fn.path} ({task.id}) in process executor')

            elif info.is_idle(task_registry):
                if info.is_shutting_down or info.is_idle_ttl_expired:
                    log.debug('Gracefully shutting down idle process executor')
                    cls.exit(task_registry, exit_code=0)

            if task:
                continue  # Continue immediately to the next task
            time.sleep(1)


@dataclass(slots=True)
class ProcessTaskRegistry:
    """
    Tracks currently-executing tasks in a child process (never shared with the main process).
    """

    class ProcessTaskRegistryItem(NamedTuple):
        task: 'tasks.Task'
        expires_at: datetime

    _tasks_by_id: dict[ObjectId, ProcessTaskRegistryItem] = field(init=False, default_factory=dict)
    _threadlock: threading.Lock = field(init=False, default_factory=threading.Lock)

    def add(self, task: 'tasks.Task', expires_at: datetime) -> None:
        """
        Add a task to the registry.
        """
        with self._thread_safe_lock():
            self._tasks_by_id[task.id] = self.ProcessTaskRegistryItem(task, expires_at)

    def get(self, task_id: ObjectId) -> Optional[ProcessTaskRegistryItem]:
        """
        Get a task from the registry.
        """
        with self._thread_safe_lock():
            item = self._tasks_by_id.get(task_id)
            if item:
                return self.ProcessTaskRegistryItem(*item)  # Return a copy
            return None

    def pop(self, task_id: ObjectId) -> Optional[ProcessTaskRegistryItem]:
        """
        Remove a task from the registry.
        """
        with self._thread_safe_lock():
            return self._tasks_by_id.pop(task_id, None)

    def iter(self) -> Iterator[ProcessTaskRegistryItem]:
        """
        Return an iterator over all items in the registry.
        """
        with self._thread_safe_lock():
            for item in self._tasks_by_id.values():
                yield self.ProcessTaskRegistryItem(*item)

    def iter_expired(self, now: datetime | None = None) -> Iterator[ProcessTaskRegistryItem]:
        """
        Return an iterator over all expired items in the registry.
        """
        now = now if now is not None else datetime.now()
        for item in self.iter():
            if item.expires_at <= now:
                yield item

    def count_expired(self, now: datetime | None = None) -> int:
        """
        Return a count of all expired items in the registry.
        """
        return sum(1 for _ in self.iter_expired(now))

    @contextmanager
    def _thread_safe_lock(self, timeout: timedelta | int = 1) -> Iterator[None]:
        """
        Context manager for locking across threads (NOT processes). Always swallows exceptions.
        """
        timeout_seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        acquired = False
        try:
            acquired = self._threadlock.acquire(blocking=True, timeout=timeout_seconds)
            yield
            self._threadlock.release()
        except Exception as e:
            log.exception(f'Failed to acquire thread lock: {e}')
        if not acquired:
            log.error(f'Failed to acquire thread lock within {timeout_seconds} seconds')


@dataclass(slots=True)
class ProcessTransceiver:  # type: ignore [misc]
    """
    2-way data exchange between the main process and a child process using multiprocessing synchronization primitives.
    """

    idle_ttl: timedelta
    max_concurrency: int
    tasks_per_restart: int
    callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn']
    worker_name: str | None = None
    worker_host: str | None = None

    _pending_task_queue: mp.Queue = field(init=False, default_factory=mp.Queue)
    _num_tasks_pending: Synchronized = field(init=False, default_factory=lambda: mp.Value('i', 0))
    _num_tasks_executing: Synchronized = field(init=False, default_factory=lambda: mp.Value('i', 0))
    _num_tasks_completed: Synchronized = field(init=False, default_factory=lambda: mp.Value('i', 0))
    _num_tasks_til_restart: Synchronized = field(init=False, default_factory=lambda: mp.Value('i', 1))
    _last_task_completed_at_seconds: Synchronized = field(init=False, default_factory=lambda: mp.Value('i', 0))
    _is_shutting_down: Synchronized = field(init=False, default_factory=lambda: mp.Value('b', False))
    _started: Event = field(init=False, default_factory=mp.Event)
    _threadlock: Lock = field(init=False, default_factory=mp.Lock)

    def __post_init__(self) -> None:
        self._num_tasks_til_restart.value = self.tasks_per_restart
        if self.tasks_per_restart <= 0:  # We can't use 0 to mean "never restart" so we use -1 instead
            self._num_tasks_til_restart.value = -1

    def submit_task(self, task: 'tasks.Task') -> None:
        """
        Submit a task to the pending queue.
        """
        # NOTE: `_pending_task_queue.qsize()` is broken on MacOSX so we use `_num_tasks_pending` instead
        with self._num_tasks_pending.get_lock():
            self._num_tasks_pending.value += 1
        self._pending_task_queue.put(task)

    def pop_task(self, task_registry: 'ProcessTaskRegistry') -> Optional['tasks.Task']:
        """
        Pop a task from the pending queue, increment `_num_tasks_executing`, and add task to registry (if exists).
        """
        if self._pending_task_queue.empty():
            return None

        task: tasks.Task = self._pending_task_queue.get_nowait()  # type: ignore [no-any-return]
        task_registry.add(task, expires_at=datetime.now() + task.fn.timeout)
        with self._num_tasks_pending.get_lock():
            self._num_tasks_pending.value -= 1
        with self._num_tasks_executing.get_lock():
            self._num_tasks_executing.value += 1  # Assume the task will be executed immediately

        return task

    def set_started(self) -> None:
        """
        Set the event that the child process has started.
        """
        self._started.set()

    def wait_until_started(self, timeout=timedelta(seconds=1)) -> None:
        """
        Block until the executor sends the "started" event, waiting no longer than `timeout` seconds.
        """
        self._started.wait(timeout=timeout.total_seconds())

    def on_task_complete(self, task: 'tasks.Task', task_registry: 'ProcessTaskRegistry') -> None:
        """
        Increment the number of completed tasks and decrement the number currently-executing.
        """
        task_registry.pop(task.id)
        self._last_task_completed_at_seconds.value = int(datetime.now().timestamp())

        # See docs for why `get_lock()` is required:
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Value
        with self._num_tasks_executing.get_lock():
            self._num_tasks_executing.value -= 1

        with self._num_tasks_completed.get_lock():
            self._num_tasks_completed.value += 1

        with self._num_tasks_til_restart.get_lock():
            tasks_til_restart = self._num_tasks_til_restart.value - 1
            self._num_tasks_til_restart.value = tasks_til_restart

            # Initiate graceful shutdown if we've hit `tasks_per_restart`
            if tasks_til_restart > 0:
                log.debug(
                    f'Executor finished task {task.fn.path} ({task.id}): '
                    f'{self.tasks_per_restart - tasks_til_restart} remaining until executor restart'
                )
            elif tasks_til_restart == 0:  # Match exactly 0 so users can configure this to never restart
                log.debug(
                    f'Executor finished final task {task.fn.path} ({task.id}) '
                    f'of {self.tasks_per_restart} per restart: starting executor shutdown'
                )
                self.is_shutting_down = True

    @property
    def capacity(self) -> int:
        """
        Remaining number of tasks that may be added to this process.
        If the process is shutting down, capacity is always 0.
        """
        # Executors have no capacity while shutting down
        if self.is_shutting_down:
            return 0

        # Max concurrency <= 0 is treated as unlimited, so we return the value of `_num_tasks_til_restart`
        if self.max_concurrency <= 0:
            return self._num_tasks_til_restart.value  # type: ignore [no-any-return]

        capacity = self.max_concurrency - self._num_tasks_executing.value

        # If _num_tasks_til_restart is negative, we treat it as "unlimited" and return the remaining capacity
        if self._num_tasks_til_restart.value < 0:
            return capacity  # type: ignore [no-any-return]

        # If both values are non-negative, take the smaller
        return min(capacity, self._num_tasks_til_restart.value)  # type: ignore [no-any-return]

    @property
    def active(self) -> int:
        """
        Number of pending + currently-executing tasks.
        """
        return self._num_tasks_executing.value + self._num_tasks_pending.value  # type: ignore [no-any-return]

    @property
    def is_shutting_down(self) -> bool:
        """
        True if the process is shutting down.
        """
        return bool(self._is_shutting_down.value)  # type: ignore [no-any-return]

    @is_shutting_down.setter
    def is_shutting_down(self, value: bool) -> None:
        self._is_shutting_down.value = value

    def is_idle(self, task_registry: 'ProcessTaskRegistry') -> bool:
        """
        This process is "idle" if no (non-expired) tasks are executing or scheduled.
        """
        if not self._pending_task_queue.empty():  # The process cannot be idle if there are scheduled tasks
            return False
        return self._num_tasks_executing.value <= task_registry.count_expired()  # type: ignore [no-any-return]

    @property
    def is_idle_ttl_expired(self) -> bool:
        """
        This process is "idle" if no tasks are executing or scheduled.
        """
        now_seconds = datetime.now().timestamp()
        ttl_seconds = self.idle_ttl.total_seconds()
        is_expired = now_seconds - self._last_task_completed_at_seconds.value > ttl_seconds
        if is_expired:
            return True
        else:
            return False
