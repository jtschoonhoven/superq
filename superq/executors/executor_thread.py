import multiprocessing as mp
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from multiprocessing.sharedctypes import Synchronized

from superq import tasks
from superq.bson import ObjectId
from superq.exceptions import WorkerError
from superq.executors import executor_process


@dataclass(slots=True)
class ThreadTaskExecutor(executor_process.ProcessTaskExecutor):  # type: ignore [misc]
    """
    Wraps a child process that manages a pool of threads.
    Tasks are moved from the pending queue to the running queue when started, and removed when finished.
    """

    TYPE = 'thread'

    @classmethod
    def run(  # noqa: C901
        cls,
        pending_queue: mp.Queue,
        running_queue: mp.Queue,
        finished_queue: mp.Queue,
        is_shutting_down: Synchronized,
        idle_process_ttl: timedelta,
        tasks_per_restart: int,
        worker_name: str | None = None,
        worker_host: str | None = None,
    ) -> None:
        """
        Start a thread pool that runs until both queues are empty.
        """
        tasks_til_restart = tasks_per_restart
        tasks_by_id: dict[ObjectId, tuple[tasks.Task, threading.Thread, datetime]] = {}
        idle_process_expiration: datetime | None = None
        exit_on_next_loop = False
        cls.register_signal_handlers(tasks_by_id, is_shutting_down)

        def run_task_in_thread(task: 'tasks.Task') -> None:
            nonlocal tasks_til_restart
            try:
                task.run(worker_name=worker_name, worker_host=worker_host, run_sync=False)
            finally:
                # Mark this task complete in the queues if it hasn't already been removed (e.g. due to expiration)
                if tasks_by_id.pop(task.id, None):
                    running_queue.get_nowait()  # Pop from `running_queue` to signal the task is done
                    finished_queue.put_nowait(True)
                tasks_til_restart -= 1

        while True:
            now = datetime.now()

            # Check for expired tasks
            for task, _, expires_at in list(tasks_by_id.values()):
                if now >= expires_at:
                    if tasks_by_id.pop(task.id, None):
                        error = f'Task timed out after {int(task.fn.timeout.total_seconds())} seconds'
                        if task.can_retry_for_timeout:
                            task.reschedule(error, 'TIMEOUT', run_sync=False, incr_num_timeouts=True)
                        else:
                            task.set_failed(error, 'TIMEOUT', incr_num_timeouts=True)
                        # Handle timeouts as failures and mark the task complete
                        running_queue.get_nowait()
                        finished_queue.put_nowait(True)
                        tasks_til_restart -= 1

            # Start the next pending task if one is available
            if not pending_queue.empty():
                idle_process_expiration = None
                exit_on_next_loop = False

                task = pending_queue.get_nowait()  # Get the next task from the pending queue
                if not isinstance(task, tasks.Task):
                    raise WorkerError(f'Expected Task instance, got {type(task)}')

                # Run the task in a new daemon thread (child threads are always daemon threads)
                thread = threading.Thread(target=run_task_in_thread, args=(task,), daemon=True)

                # Register this task as running then start the thread
                running_queue.put_nowait(True)  # Register a new running task in the running queue
                tasks_by_id[task.id] = task, thread, now + task.fn.timeout
                thread.start()

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
