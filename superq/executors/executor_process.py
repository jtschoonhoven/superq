import functools
import logging
import multiprocessing as mp
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from multiprocessing.sharedctypes import Synchronized
from typing import Any, ClassVar, TypeVar

from superq import callbacks, tasks
from superq.bson import ObjectId
from superq.exceptions import WorkerError
from superq.executors import executor_base

log = logging.getLogger(__name__)

ProcessTaskExecutorType = TypeVar('ProcessTaskExecutorType', bound='ProcessTaskExecutor')


@dataclass(slots=True)
class ProcessTaskExecutor(executor_base.BaseTaskExecutor):  # type: ignore [misc]
    """
    Wraps a child process that executes one task at a time.
    Tasks are moved from the pending queue to the running queue when started, and removed when finished.
    """

    TYPE: ClassVar[executor_base.ChildWorkerType] = 'process'

    max_tasks: int  # Max concurrent tasks per-process (should always be 1 for ProcessTaskExecutor)
    tasks_per_restart: int  # Number of tasks to execute before restarting the process
    idle_process_ttl: timedelta  # Time to wait before shutting down an idle process
    callbacks: 'callbacks.CallbackRegistry'

    proc: mp.Process | None = field(init=False, default=None)
    tasks: list['tasks.Task'] = field(
        init=False, default_factory=list
    )  # List of tasks currently running in this process
    pending_queue: mp.Queue = field(init=False, default_factory=mp.Queue)  # Pending Task instances to add
    running_queue: mp.Queue = field(init=False, default_factory=mp.Queue)  # Stores `True` for each running task`
    finished_queue: mp.Queue = field(init=False, default_factory=mp.Queue)  # Finished and expired Task instances
    is_shutting_down: Synchronized = field(init=False, default_factory=lambda: mp.Value('b', False))  # type: ignore [arg-type, return-value]

    @property
    def capacity(self) -> int:
        """
        Remaining number of tasks that may be added to this event loop.
        """
        # Executors have no capacity while shutting down
        if self.is_shutting_down.value is True:
            return 0

        # Stopped executors can be restarted at full capacity
        if not self.proc or not self.proc.is_alive():
            return min(self.max_tasks, self.tasks_til_restart)

        # Else capacity equals max tasks minus active tasks OR tasks til restart (whichever is smaller)
        capacity = max(self.max_tasks - self.active, 0)
        return min(capacity, self.tasks_til_restart)

    @property
    def active(self) -> int:
        """
        Return the number of incomplete (pending or running) tasks assigned to this executor.
        """
        return self.pending_queue.qsize() + self.running_queue.qsize()

    @property
    def tasks_til_restart(self) -> int:
        if self.tasks_per_restart <= 0:
            return sys.maxsize  # Arbitrary large number
        return max(self.tasks_per_restart - self.finished_queue.qsize(), 0)

    def submit_task(
        self: ProcessTaskExecutorType,
        task: 'tasks.Task',  # type: ignore [name-defined]
        worker_name: str | None = None,
        host_name: str | None = None,
    ) -> ProcessTaskExecutorType:
        """
        Submit a new task for execution. The caller is responsible for first checking `capacity`.
        """
        self.pending_queue.put(task)
        if not self.proc or not self.proc.is_alive():  # Start a new process if necessary
            self.is_shutting_down.value = False
            self.proc = mp.Process(
                target=self._run,  # `self._run(...)` wraps this method's `self.run(...)`
                args=(
                    self.pending_queue,
                    self.running_queue,
                    self.finished_queue,
                    self.is_shutting_down,
                    self.idle_process_ttl,
                    self.tasks_per_restart,
                    self.callbacks,
                ),
                kwargs={'worker_name': worker_name, 'worker_host': host_name},
            )
            self.proc.start()
        return self

    def kill(self, graceful: bool) -> None:
        """
        If `graceful=True`, sends SIGINT to the child process. Otherwise sends SIGTERM.
        """
        self.is_shutting_down.value = True
        if self.proc and self.proc.pid and self.proc.is_alive():
            if graceful:
                os.kill(self.proc.pid, executor_base.SIG_SOFT_SHUTDOWN)
            else:
                os.kill(self.proc.pid, executor_base.SIG_HARD_SHUTDOWN)

    def timeout(self) -> None:
        """
        Signal to the child process that it has timed out and should stop immediately.
        """
        self.is_shutting_down.value = True
        if self.proc and self.proc.is_alive() and self.proc.pid:
            os.kill(self.proc.pid, executor_base.SIG_TIMEOUT)

    @staticmethod
    def register_signal_handlers(
        tasks_by_id: dict[ObjectId, tuple['tasks.Task', Any, datetime]],  # type: ignore [name-defined]
        is_shutting_down: Synchronized,
    ) -> None:
        is_graceful_shutdown = False
        is_force_shutdown = False

        def force_shutdown(sig: int, *args: Any, is_timeout=False, **kwargs: Any) -> None:
            nonlocal is_force_shutdown

            if is_force_shutdown:
                log.warning(f'Child worker process received second signal {sig}: exiting immediately')
                sys.exit(1)

            is_force_shutdown = True
            is_shutting_down.value = True
            log.warning(
                f'Child worker process received {"timeout signal" if is_timeout else "signal"} {sig}: '
                'shutting down forcefully'
            )

            # Fail and optionally reschedule tasks
            for task_id in list(tasks_by_id.keys()):
                task, _, _ = tasks_by_id.pop(task_id, (None, None, None))

                if task:
                    error_type: tasks.TaskFailureType = 'TIMEOUT' if is_timeout else 'SIGNAL'
                    error = f'Task {task.id} timed out' if is_timeout else f'Task {task.id} received signal {sig}'

                    if task.can_retry_for_signal:
                        task.reschedule(error, error_type, run_sync=False, incr_num_recovers=True)
                    else:
                        task.set_failed(error, error_type, incr_num_recovers=True)
                        task.fn.cb.fn[task.fn.path]['on_failure'](task)
                        task.fn.cb.task['on_task_failure'](task)
            sys.exit(1)

        def graceful_shutdown(sig: int, *args: Any, **kwargs: Any) -> None:
            nonlocal is_graceful_shutdown

            if is_graceful_shutdown:  # Force shutdown on second signal
                return force_shutdown(sig, *args, **kwargs)

            is_graceful_shutdown = True
            is_shutting_down.value = True
            log.warning(f'Child worker process received signal {sig}: shutting down gracefully')

        # Fail with a timeout error
        def timeout_shutdown(sig: int, *args: Any, **kwargs: Any) -> None:
            return force_shutdown(sig, *args, is_timeout=True, **kwargs)

        # Handle all signals
        signal.signal(executor_base.SIG_SOFT_SHUTDOWN, graceful_shutdown)
        signal.signal(executor_base.SIG_SOFT_SHUTDOWN_ALT, graceful_shutdown)
        signal.signal(executor_base.SIG_TIMEOUT, timeout_shutdown)
        signal.signal(executor_base.SIG_HARD_SHUTDOWN, force_shutdown)

    @classmethod
    def _run(
        cls,
        pending_queue: mp.Queue,
        running_queue: mp.Queue,
        finished_queue: mp.Queue,
        is_shutting_down: Synchronized,
        idle_process_ttl: timedelta,
        tasks_per_restart: int,
        callbacks: 'callbacks.CallbackRegistry',  # type: ignore [name-defined]
        worker_name: str | None,
        worker_host: str | None,
    ) -> None:
        """
        Call the `on_child_logconfig` callback then execute the `run(...)` method.
        """
        callbacks.child['on_child_logconfig'](worker_name)
        return cls.run(
            pending_queue,
            running_queue,
            finished_queue,
            is_shutting_down,
            idle_process_ttl,
            tasks_per_restart,
            worker_name=worker_name,
            worker_host=worker_host,
        )

    @classmethod
    def run(  # noqa: C901
        cls,
        pending_queue: mp.Queue,
        running_queue: mp.Queue,
        finished_queue: mp.Queue,
        is_shutting_down: Synchronized,
        idle_process_ttl: timedelta,
        tasks_per_restart: int,
        worker_name: str | None,
        worker_host: str | None,
    ) -> None:
        """
        Execute tasks in a child process until both queues are empty.
        """
        tasks_til_restart = tasks_per_restart
        tasks_by_id: dict[ObjectId, tuple[tasks.Task, None, datetime]] = {}
        idle_process_expiration: datetime | None = None
        exit_on_next_loop = False
        cls.register_signal_handlers(tasks_by_id, is_shutting_down)

        # Kill the current process if the task is still active after its timeout
        def monitor_timeout(task: tasks.Task) -> None:
            if task.id in tasks_by_id:
                os.kill(os.getpid(), executor_base.SIG_TIMEOUT)

        while True:
            now = datetime.now()

            if tasks_til_restart <= 0:
                is_shutting_down.value = True

            # Start the next pending task if one is available
            if not pending_queue.empty():
                idle_process_expiration = None  # Reset process idle timeout
                exit_on_next_loop = False

                task = pending_queue.get_nowait()  # Get the next task from the pending queue
                if not isinstance(task, tasks.Task):
                    raise WorkerError(f'Expected Task instance, got {type(task)}')

                task.fn.cb.child['on_child_logconfig'](worker_name)
                running_queue.put_nowait(True)  # Register a new running task in the running queue
                tasks_by_id[task.id] = task, None, now + task.fn.timeout

                # Start a thread that kills the current process on timeout
                threading.Thread(target=functools.partial(monitor_timeout, task), daemon=True)

                # Run this task synchronously
                try:
                    task.run(worker_name=worker_name, worker_host=worker_host, run_sync=False)
                finally:
                    tasks_by_id.pop(task.id, None)
                    running_queue.get_nowait()  # Pop from `running_queue` to signal the task is done
                    finished_queue.put_nowait(True)  # Register this task in the finished queue
                    tasks_til_restart -= 1

            # If both queues are empty, track idle time until shutdown
            elif running_queue.empty():
                if exit_on_next_loop:
                    return None

                time.sleep(1)

                if not idle_process_expiration:
                    idle_process_expiration = now + idle_process_ttl
                    continue
                # Do one final loop before exiting just in case of race conditions in `pending_queue`
                if is_shutting_down.value is True:
                    exit_on_next_loop = True
                    continue
                if now > idle_process_expiration:
                    exit_on_next_loop = True
                    continue
