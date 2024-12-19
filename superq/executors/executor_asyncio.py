import asyncio
import functools
import logging
from datetime import datetime, timedelta
from typing import ClassVar

from superq import config, tasks
from superq.bson import ObjectId
from superq.executors import executor_base, executor_process

log = logging.getLogger(__name__)


class AsyncioTaskExecutor(executor_process.ProcessTaskExecutor):
    """
    Wraps a child process that runs an asyncio event loop.
    """

    TYPE: ClassVar['executor_base.ChildWorkerType'] = 'asyncio'

    __slots__ = ('proc', 'info')

    @classmethod
    def run(cls, info: 'executor_process.ProcessTransceiver', cfg: 'config.Config') -> None:
        """
        Run tasks in this child process continuously until shutdown.
        """
        task_registry = executor_process.ProcessTaskRegistry()
        cls.register_signal_handlers(task_registry, info)
        cls.init_logging(info, cfg)
        log.debug('Starting new asyncio task executor')
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
            log.debug(f'Asyncio executor finished task {task.fn.path} ({task.id})')
            info.on_task_complete(task, task_registry)
            asyncio_tasks_by_task_id.pop(task.id, None)

        while True:
            now = datetime.now()
            task = info.pop_task(task_registry)

            if task:
                log.debug(f'Asyncio executor starting task {task.fn.path} ({task.id})')
                coro = task.run_aio(worker_name=info.worker_name, worker_host=info.worker_host, run_sync=False)
                asyncio_task = asyncio.create_task(coro)
                asyncio_task.add_done_callback(functools.partial(on_task_complete, task))
                asyncio_tasks_by_task_id[task.id] = asyncio_task

            elif info.is_idle(task_registry):
                if info.is_shutting_down or info.is_idle_ttl_expired:
                    log.debug('Gracefully shutting down idle asyncio executor')
                    cls.exit(task_registry, exit_code=0)

            # Attempt to cancel expired tasks
            for expired_task, expired_at in task_registry.iter_expired():
                error = f'Task timed out after {int(expired_task.fn.timeout.total_seconds())} seconds'

                if expired_task.can_retry_for_timeout:
                    log.debug(f'Asyncio executor rescheduling expired task {expired_task.fn.path} ({expired_task.id})')
                    expired_task.reschedule(error, 'TIMEOUT', run_sync=False, incr_num_timeouts=True)
                else:
                    log.debug(f'Asyncio executor failing expired task {expired_task.fn.path} ({expired_task.id})')
                    expired_task.set_failed(error, 'TIMEOUT', incr_num_timeouts=True)

                expired_asyncio_task = asyncio_tasks_by_task_id.pop(expired_task.id, None)
                if expired_asyncio_task:
                    expired_asyncio_task.cancel()  # This will still call `on_task_conplete`
                    await asyncio.sleep(0)

                # If an async task is truly stuck, our only option might be to shut down the process
                if expired_task.error_type == 'TIMEOUT' and now - expired_at > timedelta(seconds=60):
                    info.is_shutting_down = True

            if task:
                await asyncio.sleep(0)  # Continue immediately to the next task
            else:
                await asyncio.sleep(1)
