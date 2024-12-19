import logging
import threading
import time

from superq import config, tasks
from superq.executors import executor_process

log = logging.getLogger(__name__)


class ThreadTaskExecutor(executor_process.ProcessTaskExecutor):
    """
    Wraps a child process that manages a pool of threads.
    """

    TYPE = 'thread'

    __slots__ = ('proc', 'info')

    @classmethod
    def run(cls, info: 'executor_process.ProcessTransceiver', cfg: 'config.Config') -> None:
        """
        Run tasks in this child process continuously until shutdown.
        """
        task_registry = executor_process.ProcessTaskRegistry()
        cls.register_signal_handlers(task_registry, info)
        cls.init_logging(info, cfg)
        info.set_started()
        log.debug('Starting new thread task executor')

        def run_task_in_thread(task: 'tasks.Task') -> None:
            try:
                task.run(worker_name=info.worker_name, worker_host=info.worker_host, run_sync=False)
            finally:
                log.debug(f'Thread executor finished task {task.fn.path} ({task.id})')
                info.on_task_complete(task, task_registry)

        while True:
            task = info.pop_task(task_registry)

            if task:
                log.debug(f'Thread executor starting task {task.fn.path} ({task.id})')
                threading.Thread(target=run_task_in_thread, args=(task,), daemon=True).start()

            elif info.is_idle(task_registry):
                if info.is_shutting_down or info.is_idle_ttl_expired:
                    log.debug('Gracefully shutting down idle thread executor')
                    cls.exit(task_registry, exit_code=0)

            # The only way to kill a child thread is to kill the parent process so that's how we handle expired tasks
            if task_registry.count_expired():
                log.debug(f'Shutting down thread executor with {task_registry.count_expired()} expired tasks')
                info.is_shutting_down = True

            if task:
                continue  # Continue immediately to the next task
            time.sleep(1)
