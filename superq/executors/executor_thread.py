import threading
import time

from superq import tasks
from superq.executors import executor_process


class ThreadTaskExecutor(executor_process.ProcessTaskExecutor):
    """
    Wraps a child process that manages a pool of threads.
    """

    TYPE = 'thread'

    __slots__ = ('proc', 'info')

    @classmethod
    def run(cls, info: 'executor_process.ProcessTransceiver') -> None:
        """
        Run tasks in this child process continuously until shutdown.
        """
        info.callbacks.child['on_child_logconfig'](info.worker_name)
        task_registry = executor_process.ProcessTaskRegistry()
        cls.register_signal_handlers(task_registry, info)
        info.set_started()

        def run_task_in_thread(task: 'tasks.Task') -> None:
            try:
                task.run(worker_name=info.worker_name, worker_host=info.worker_host, run_sync=False)
            finally:
                info.on_task_complete(task, task_registry)

        while True:
            task = info.pop_task(task_registry)

            if task:
                threading.Thread(target=run_task_in_thread, args=(task,), daemon=True).start()

            elif info.is_idle(task_registry):
                if info.is_shutting_down or info.is_idle_ttl_expired:
                    cls.exit(task_registry, exit_code=0)

            # The only way to kill a child thread is to kill the parent process so that's how we handle expired tasks
            if task_registry.count_expired():
                info.is_shutting_down = True

            time.sleep(1)
