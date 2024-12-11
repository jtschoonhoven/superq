import asyncio
import functools
import inspect
import multiprocessing as mp
from dataclasses import dataclass
from datetime import datetime, timedelta
from multiprocessing.sharedctypes import Synchronized
from typing import ClassVar

from superq import tasks
from superq.bson import ObjectId
from superq.exceptions import WorkerError
from superq.executors import executor_base, executor_process


@dataclass(slots=True)
class AsyncioTaskExecutor(executor_process.ProcessTaskExecutor):  # type: ignore [misc]
    """
    Wraps a child process that runs an asyncio event loop.
    Tasks are moved from the pending queue to the running queue when started, and removed when finished.
    """

    TYPE: ClassVar['executor_base.ChildWorkerType'] = 'asyncio'

    @classmethod
    def run(
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
        Start a new event loop that runs until there both queues are empty.
        WARNING: this function can only be called in a child process, at most once per process.
        """
        tasks_by_id: dict[ObjectId, tuple[tasks.Task, asyncio.Task, datetime]] = {}
        cls.register_signal_handlers(tasks_by_id, is_shutting_down)

        asyncio.run(
            AsyncioTaskExecutor._run_aio(
                pending_queue,
                running_queue,
                finished_queue,
                is_shutting_down,
                idle_process_ttl,
                tasks_per_restart,
                tasks_by_id,
                worker_name=worker_name,
                worker_host=worker_host,
            )
        )

    @classmethod
    async def _run_aio(  # noqa: C901
        cls,
        pending_queue: mp.Queue,
        running_queue: mp.Queue,
        finished_queue: mp.Queue,
        is_shutting_down: Synchronized,
        idle_process_ttl: timedelta,
        tasks_per_restart: int,
        tasks_by_id: dict[ObjectId, tuple['tasks.Task', asyncio.Task, datetime]],
        worker_name: str | None,
        worker_host: str | None,
    ) -> None:
        """
        Process all tasks assigned to this event loop until both queues are empty.
        The "running queue" only stores the value `True` for each task that is currently running.
        """
        tasks_til_restart = tasks_per_restart
        idle_process_expiration: datetime | None = None
        exit_on_next_loop = False
        asyncio_tasks: list[asyncio.Task] = []

        while True:
            now = datetime.now()

            if tasks_til_restart <= 0:
                is_shutting_down.value = True

            # Check for expired tasks
            # TODO: Simplify
            for task, asyncio_task, expires_at in list(tasks_by_id.values()):
                if now >= expires_at:
                    if tasks_by_id.pop(task.id, None):
                        error = f'Task timed out after {int(task.fn.timeout.total_seconds())} seconds'
                        if task.can_retry_for_timeout:
                            task.reschedule(error, 'TIMEOUT', run_sync=False, incr_num_timeouts=True)
                        else:
                            task.set_failed(error, 'TIMEOUT', incr_num_timeouts=True)
                        running_queue.get_nowait()
                        finished_queue.put_nowait(True)
                        tasks_til_restart -= 1

                        # Schedule this task to exit the next time it context switches:
                        # If the task succeeds without ever awaiting, it might still transition to 'SUCCESS' later
                        asyncio_task.cancel()
                        await asyncio.sleep(0)  # Yield control to the event loop

            # Start the next pending task if one is available
            if not pending_queue.empty():
                idle_process_expiration = None
                exit_on_next_loop = False

                task = pending_queue.get_nowait()  # Get the next task from the pending queue
                if not isinstance(task, tasks.Task):
                    raise WorkerError(f'Expected Task instance, got {type(task)}')

                # Ensure this task returns a coroutine
                coro = task.run_aio(worker_name=worker_name, worker_host=worker_host, run_sync=False)
                if not inspect.iscoroutine(coro):
                    finished_queue.put_nowait(True)
                    raise WorkerError(f'Expected coroutine, got {type(coro)}')

                # Run this task in the event loop in the background
                t = asyncio.create_task(coro)
                asyncio_tasks.append(t)

                # Track this task run
                tasks_by_id[task.id] = task, t, now + task.fn.timeout
                running_queue.put_nowait(True)  # Register a new running task in the running queue

                # Callback function to pop from `running_queue` and push task to `finished_queue`
                def on_done(
                    task: tasks.Task,
                    tasks_by_id: dict[ObjectId, tuple[tasks.Task, asyncio.Task, datetime]],
                    _: asyncio.Future,
                ) -> None:
                    nonlocal tasks_til_restart
                    if tasks_by_id.pop(task.id, None):
                        running_queue.get_nowait()
                        finished_queue.put_nowait(True)
                    tasks_til_restart -= 1

                # Automatically move from running to finished queue when done
                t.add_done_callback(functools.partial(on_done, task, tasks_by_id))

            # If both queues are empty, track idle time until shutdown
            elif running_queue.empty():
                if exit_on_next_loop:
                    await asyncio.gather(*asyncio_tasks)
                    return None

                await asyncio.sleep(1)  # Yield control to the event loop

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

            # Continue looping until no pending or running tasks are left
            await asyncio.sleep(1)
