import asyncio
import functools
from datetime import datetime, timedelta
from typing import ClassVar

from superq import tasks
from superq.bson import ObjectId
from superq.executors import executor_base, executor_process


class AsyncioTaskExecutor(executor_process.ProcessTaskExecutor):
    """
    Wraps a child process that runs an asyncio event loop.
    """

    TYPE: ClassVar['executor_base.ChildWorkerType'] = 'asyncio'

    __slots__ = ('proc', 'info')

    @classmethod
    def run(cls, info: 'executor_process.ProcessTransceiver') -> None:
        """
        Run tasks in this child process continuously until shutdown.
        """
        info.callbacks.child['on_child_logconfig'](info.worker_name)
        task_registry = executor_process.ProcessTaskRegistry()
        cls.register_signal_handlers(task_registry, info)
        asyncio.run(cls._run_aio(info, task_registry))

    @classmethod
    async def _run_aio(
        cls,
        info: 'executor_process.ProcessTransceiver',
        task_registry: 'executor_process.ProcessTaskRegistry',
    ) -> None:
        """
        Process all tasks assigned to this event loop until both queues are empty.
        The "running queue" only stores the value `True` for each task that is currently running.
        """
        info.set_started()
        asyncio_tasks_by_task_id: dict[ObjectId, asyncio.Task] = {}

        # Callback function to pop from `running_queue` and push task to `finished_queue`
        def on_task_complete(task: tasks.Task, _: asyncio.Future) -> None:
            info.on_task_complete(task, task_registry)
            asyncio_tasks_by_task_id.pop(task.id, None)

        while True:
            now = datetime.now()
            task = info.pop_task(task_registry)

            if task:
                coro = task.run_aio(worker_name=info.worker_name, worker_host=info.worker_host, run_sync=False)
                asyncio_task = asyncio.create_task(coro)
                asyncio_task.add_done_callback(functools.partial(on_task_complete, task))
                asyncio_tasks_by_task_id[task.id] = asyncio_task

            elif info.is_idle(task_registry):
                if info.is_shutting_down or info.is_idle_ttl_expired:
                    cls.exit(task_registry, exit_code=0)

            # Attempt to cancel expired tasks
            for expired_task, expired_at in task_registry.iter_expired():
                error = f'Task timed out after {int(expired_task.fn.timeout.total_seconds())} seconds'

                if expired_task.can_retry_for_timeout:
                    expired_task.reschedule(error, 'TIMEOUT', run_sync=False, incr_num_timeouts=True)
                else:
                    expired_task.set_failed(error, 'TIMEOUT', incr_num_timeouts=True)

                expired_asyncio_task = asyncio_tasks_by_task_id.pop(expired_task.id, None)
                if expired_asyncio_task:
                    expired_asyncio_task.cancel()  # This will still call `on_task_conplete`
                    await asyncio.sleep(0)

                # If an async task is truly stuck, our only option might be to shut down the process
                if expired_task.error_type == 'TIMEOUT' and now - expired_at > timedelta(seconds=60):
                    info.is_shutting_down = True

            # Continue looping until no pending or running tasks are left
            await asyncio.sleep(1)
