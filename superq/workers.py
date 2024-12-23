import logging
import math
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Literal, Optional

from superq import bson, callbacks, config, queues, tasks, wrapped_fn
from superq.backends import backend_base
from superq.executors import executor_base, executor_pool

WorkerType = Literal['process', 'thread', 'asyncio']
WorkerTypeSync = Literal['process', 'thread']

log = logging.getLogger(__name__)

SIGINT = signal.SIGINT.value
SIGQUIT = signal.SIGQUIT.value
SIGTERM = signal.SIGTERM.value
SIGABRT = signal.SIGABRT.value

SIGNALS_SOFT_SHUTDOWN = (SIGINT, SIGQUIT)  # The child process should initiate graceful shutdown
SIGNALS_HARD_SHUTDOWN = (SIGTERM,)  # The child process should exit asap with minimal cleanup
SIGNALS_TIMEOUT = (SIGABRT,)  # The child process has timed out and should exit gracefully if possible


class Worker:
    """
    Parent class that manages one or more task executors.
    """

    cfg: 'config.Config'
    backend: 'backend_base.BaseBackend'
    executors: list['executor_base.BaseTaskExecutor']
    cb_registry: 'callbacks.CallbackRegistry'
    fn_registry: 'queues.FnRegistry'
    last_ttl_check: datetime  # Last time we deleted old tasks from the DB
    force_shutdown_at: datetime | None  # Set when we receive a signal to shut down
    last_interval_check: datetime  # Last time we checked for interval tasks
    TaskCls: type['tasks.Task']

    __slots__ = (
        'cfg',
        'backend',
        'executors',
        'cb_registry',
        'fn_registry',
        'last_ttl_check',
        'force_shutdown_at',
        'last_interval_check',
        'TaskCls',
    )

    def __init__(
        self,
        cfg: 'config.Config',
        backend: 'backend_base.BaseBackend',
        fn_registry: dict[str, 'wrapped_fn.WrappedFn'],
        cb_registry: 'callbacks.CallbackRegistry',
        TaskCls: Optional[type['tasks.Task']] = None,
        ExtraExecutors: Optional[list[type['executor_base.BaseTaskExecutor']]] = None,
    ) -> None:
        self.cfg = cfg
        self.backend = backend
        self.fn_registry = fn_registry
        self.cb_registry = cb_registry
        self.force_shutdown_at = None
        self.TaskCls = TaskCls or tasks.Task

        # Initialize timestamps to unix epoch to ensure they always run
        self.last_interval_check = datetime(1970, 1, 1)
        self.last_ttl_check = datetime(1970, 1, 1)

        # Initialize executors
        self.executors = []
        if not self.cfg.worker_disable_process_pool:
            process_pool = executor_pool.TaskExecutorProcessPool(self.cfg, self.cb_registry.child)
            self.executors.append(process_pool)

        if not self.cfg.worker_disable_threads_pool:
            thread_pool = executor_pool.ThreadTaskExecutorProcessPool(self.cfg, self.cb_registry.child)
            self.executors.append(thread_pool)

        if not self.cfg.worker_disable_asyncio_pool:
            asyncio_pool = executor_pool.AsyncioTaskExecutorProcessPool(self.cfg, self.cb_registry.child)
            self.executors.append(asyncio_pool)

        for ExecutorCls in ExtraExecutors or []:
            extra_executor = ExecutorCls(self.cfg, self.cb_registry.child)
            self.executors.append(extra_executor)

    def init_logging(self) -> None:
        if self.cfg.worker_log_level:
            logging.getLogger('superq').setLevel(self.cfg.worker_log_level)
        self.cb_registry.worker['on_worker_logconfig'](self)

    def run(self) -> None:
        """
        Execute tasks continuously until a signal is received.
        """
        self.register_signal_handlers()
        self.init_logging()

        last_interval_check = datetime(1970, 1, 1)
        last_ttl_check = datetime(1970, 1, 1)

        # Log registered functions
        fns_str = '\n'.join(f'  {k}' for k in self.fn_registry.keys())
        log.info('Worker has registered the following tasks:\n' + fns_str)

        # Log registered callbacks
        cbs_str = '\n'.join(
            [
                '\n'.join(f'  {k}' for k in self.cb_registry.task),
                '\n'.join(f'  {k}' for k in self.cb_registry.worker),
                '\n'.join(f'  {k}' for k in self.cb_registry.child),
                '\n'.join(f'  {cb}.{fn}' for cb in self.cb_registry.fn for fn in self.cb_registry.fn[cb].keys()),
            ]
        )
        log.info('Worker has registered the following callbacks:' + cbs_str)

        log.info('Worker starting')
        self.cb_registry.worker['on_worker_start'](self)

        # Continue processing tasks until we receive a signal to shut down
        while True:
            now = datetime.now()

            if self.force_shutdown_at:
                break

            # Schedule interval tasks
            if last_interval_check < now - self.cfg.worker_scheduler_interval:
                self.schedule_interval_tasks()
                last_interval_check = now

            # Delete old tasks from the DB
            if last_ttl_check < now - self.cfg.worker_backend_task_ttl_interval:
                log.debug('Worker checking for completed tasks to delete')
                self.backend.delete_completed_tasks_older_than(now - self.cfg.backend_task_ttl)
                last_ttl_check = now

            # Fetch tasks for each worker type and submit them for execution
            for executor in self.executors:
                capacity = executor.capacity
                if not capacity:
                    log.debug(f'Worker {executor.TYPE} pool executor is at max capacity')
                    continue

                # Decide how many tasks to claim for this executor on this loop
                max_tasks = math.ceil(executor.max_concurrency * self.cfg.worker_max_fill_ratio_per_loop)
                if max_tasks <= 0:
                    max_tasks = capacity  # Use capacity if `max_tasks` is unlimited
                elif capacity >= 1:
                    max_tasks = min(max_tasks, capacity)  # Use the lower number if both are positive

                # Get the next task from the queue
                claimed_tasks = self.backend.claim_tasks(
                    max_tasks=max_tasks,  # Zero or less is treated as no limit
                    worker_type=executor.TYPE,
                    worker_host=self.cfg.worker_hostname,
                    worker_name=f'{executor.TYPE}Pool',
                )

                if claimed_tasks:
                    log.debug(f'Worker claimed {len(claimed_tasks)} tasks for {executor.TYPE} pool')

                for task in claimed_tasks:
                    log.debug(f'Worker submitting task {task.fn.path} ({task.id}) to {executor.TYPE} pool')
                    executor.submit_task(task)

            time.sleep(self.cfg.worker_poll_interval.total_seconds())

        self.shutdown()

    def shutdown(self) -> None:
        """
        Shut down all child processes and exit. If the current time is before `self.force_shutdown_at`,
        a graceful shutdown will be initiated and executors will attempt to finish all pending tasks.
        """
        now = datetime.now()
        is_graceful = bool(self.force_shutdown_at and self.force_shutdown_at <= now)

        log.info(f'Shutting down worker {"gracefully" if is_graceful else "immediately"}')
        self.cb_registry.worker['on_worker_shutdown'](self)

        # Initiate graceful shutdown on all executors
        for executor in self.executors:
            log.debug(f'Sending signal to {executor.TYPE} executor (graceful={is_graceful})')
            executor.kill(graceful=is_graceful)

        # Wait for graceful shutdown to complete
        while True:
            num_active = sum(executor.active for executor in self.executors)
            ttl = int((self.force_shutdown_at - datetime.now()).total_seconds()) if self.force_shutdown_at else 0
            if ttl > 0 and num_active > 0:
                log.debug(f'Forcing shutdown in {ttl} seconds: waiting on {num_active} active tasks')
                time.sleep(1)
                continue
            break

        # Force-kill any remaining tasks
        for executor in self.executors:
            if executor.active:
                log.warning(f'Forcing shutdown of {executor.active} tasks in {executor.TYPE} pool')
                executor.kill(graceful=False)

        # Clean exit if shutdown finished with no remaining active workers
        log.info('Shutdown complete')
        sys.exit(0)

    def register_signal_handlers(self) -> None:
        """
        Setup handling for signals that should shutdown all workers.
        """

        def graceful_shutdown(sig: int, *args: Any, **kwargs: Any) -> None:
            """
            Set `force_shutdown_at` so the worker stops on its next iteration.
            """
            if self.force_shutdown_at:
                log.warning(f'Received second signal {sig}: forcing shutdown')
                force_shutdown(sig)
            log.info(f'Received signal {sig}: gracefully shutting down workers')
            self.force_shutdown_at = datetime.now() + self.cfg.worker_grace_period

        def force_shutdown(sig: int, *args: Any, **kwargs: Any) -> None:
            """
            Interrupt the current process and immediately begin shutdown.
            """
            now = datetime.now()
            if self.force_shutdown_at and self.force_shutdown_at <= now:
                log.warning(f'Received second signal {sig}: exiting immediately')
                sys.exit(1)
            log.warning(f'Received signal {sig}: forcing worker shutdown')
            self.force_shutdown_at = now
            self.shutdown()

        for sig in SIGNALS_SOFT_SHUTDOWN:
            signal.signal(sig, graceful_shutdown)
        for sig in SIGNALS_HARD_SHUTDOWN:
            signal.signal(sig, force_shutdown)
        for sig in SIGNALS_TIMEOUT:
            signal.signal(sig, graceful_shutdown)

    def schedule_interval_tasks(self) -> None:
        """
        Schedule tasks that should run automatically at regular intervals.
        """
        for fn in self.fn_registry.values():
            if not fn.interval:
                continue

            # Do some datetime math so we can schedule tasks at consistent intervals (easier to avoid double-scheduling)
            # TODO: Deterministically add some jitter so we don't schedule too many tasks at once
            now = datetime.now()
            interval_seconds = int(fn.interval.total_seconds())  # Count the seconds in the interval
            seconds_since_epoch = int((now - tasks.TASK_EPOCH).total_seconds())  # Count the seconds since the epoch
            intervals_since_epoch = seconds_since_epoch // interval_seconds  # Count the intervals since the task epoch
            prev_run_at = tasks.TASK_EPOCH + timedelta(seconds=intervals_since_epoch * interval_seconds)
            next_run_at = prev_run_at + fn.interval

            # Define a task to get-or-create
            id = bson.ObjectId()
            old_task = self.TaskCls(
                id=id,
                fn_name=fn.fn_name,
                fn_module=fn.fn_module,
                priority=fn.priority,
                queue_name='default',
                status='WAITING',
                result_bytes=None,
                error='',
                error_type=None,
                num_tries=0,
                num_recovers=0,
                num_timeouts=0,
                num_lockouts=0,
                num_ratelimits=0,
                args=(),
                kwargs={},
                created_at=now,
                updated_at=now,
                started_at=None,
                ended_at=None,
                scheduled_for=next_run_at,
                worker_type=fn.worker_type,
                worker_host=None,
                worker_name=None,
            )

            # Attempt to schedule this task (no-op if this task is already scheduled for this time)
            # If not scheduled, this returns the already-scheduled task in the DB
            new_task = self.backend.push_interval_task(old_task)
            did_schedule = new_task.id == old_task.id

            if did_schedule:
                log.debug(f'Scheduled interval task {fn.path} ({new_task.id}) for {next_run_at.isoformat()}')
