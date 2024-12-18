import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Any

from superq import bson, callbacks, config, tasks, wrapped_fn
from superq.backends import backend_base
from superq.exceptions import TaskImportError
from superq.executors import executor_base, executor_pool

log = logging.getLogger(__name__)


class Worker:
    cfg: 'config.Config'
    backend: 'backend_base.BaseBackend'
    pools: dict['executor_base.ChildWorkerType', 'executor_base.BaseTaskExecutor']
    force_shutdown_at: datetime | None  # Set when we receive a signal to shut down
    last_interval_check: datetime  # Last time we checked for interval tasks
    last_ttl_check: datetime  # Last time we deleted old tasks from the DB
    task_cls: type['tasks.Task']
    cb: 'callbacks.CallbackRegistry'
    fn_registry: dict[str, 'wrapped_fn.WrappedFn']

    __slots__ = (
        'cfg',
        'backend',
        'pools',
        'force_shutdown_at',
        'last_interval_check',
        'last_ttl_check',
        'task_cls',
        'cb',
        'fn_registry',
    )

    def __init__(
        self,
        cfg: 'config.Config',
        backend: 'backend_base.BaseBackend',
        fn_registry: dict[str, 'wrapped_fn.WrappedFn'],
        cb: 'callbacks.CallbackRegistry',
        task_cls: type['tasks.Task'],
    ) -> None:
        self.cfg = cfg
        self.backend = backend
        self.fn_registry = fn_registry
        self.cb = cb
        self.force_shutdown_at = None
        self.last_interval_check = datetime(1970, 1, 1)
        self.last_ttl_check = datetime(1970, 1, 1)
        self.task_cls = task_cls
        self.pools = {
            'process': executor_pool.TaskExecutorProcessPool(cfg=self.cfg, callbacks=self.cb.child),
            'thread': executor_pool.ThreadTaskExecutorProcessPool(cfg=self.cfg, callbacks=self.cb.child),
            'asyncio': executor_pool.EventLoopTaskExecutorProcessPool(cfg=self.cfg, callbacks=self.cb.child),
        }

    def get_open_pools(self) -> list['executor_base.BaseTaskExecutor']:
        """
        Return a list of all worker pools that are not at max capacity.
        """
        return [p for p in self.pools.values() if p.capacity]

    def init_logging(self) -> None:
        if self.cfg.worker_log_level:
            logging.getLogger('superq').setLevel(self.cfg.worker_log_level)
        self.cb.worker['on_worker_logconfig'](self)

    @property
    def is_shutting_down(self) -> bool:
        return bool(self.force_shutdown_at)

    def run(self, run_sync: bool | None = None) -> None:  # noqa: C901
        """
        Run the main worker process in a loop forever (or until a signal is received).
        """
        self.register_signal_handlers()
        self.init_logging()

        task: tasks.Task | None = None
        run_sync = self.cfg.task_run_sync if run_sync is None else run_sync

        # Log registered functions
        fns_str = '\n'.join(f'  {k}' for k in self.fn_registry.keys())
        log.info('Worker has registered the following tasks:\n' + fns_str)

        # Log registered callbacks
        cbs_str = '\n'.join(
            [
                '\n'.join(f'  {k}' for k in self.cb.task),
                '\n'.join(f'  {k}' for k in self.cb.worker),
                '\n'.join(f'  {k}' for k in self.cb.child),
                '\n'.join(f'  {cb}.{fn}' for cb in self.cb.fn for fn in self.cb.fn[cb].keys()),
            ]
        )
        log.info('Worker has registered the following callbacks:' + cbs_str)

        log.info('Worker starting')
        self.cb.worker['on_worker_start'](self)
        poll_seconds = self.cfg.worker_poll_interval.total_seconds()

        # Continue processing tasks until we receive a signal to shut down
        while True:
            now = datetime.now()

            # Stop processing new tasks if we're shutting down
            if self.is_shutting_down:
                break

            # Schedule interval tasks
            if self.last_interval_check < now - self.cfg.worker_scheduler_interval:
                self.schedule_interval_tasks()
                self.last_interval_check = now

            # Delete old tasks from the DB
            if self.last_ttl_check < now - self.cfg.worker_backend_task_ttl_interval:
                log.debug('Worker checking for completed tasks to delete')
                self.backend.delete_completed_tasks_older_than(now - self.cfg.backend_task_ttl)
                self.last_ttl_check = now

            # Don't process new tasks if we're at max capacity (this should be rare)
            open_worker_pool_types = [p.TYPE for p in self.get_open_pools()]
            if not open_worker_pool_types:
                log.debug(f'Worker is at max capacity across all pools: sleeping {poll_seconds}s')
                time.sleep(poll_seconds)
                continue

            # Get the next task from the queue
            try:
                task = self.backend.pop(worker_types=open_worker_pool_types, worker_host=self.cfg.worker_hostname)
            except TaskImportError as e:
                log.error(str(e))

            if not task:
                time.sleep(poll_seconds)
                continue

            # Submit the task to the corresponding pool
            log.debug(f'Worker submitting task {task.fn.path} ({task.id}) to {task.fn.worker_type} pool')
            pool = self.pools[task.fn.worker_type]
            pool.submit_task(task)

            if task:
                continue  # Continue immediately to the next task
            time.sleep(poll_seconds)

        self.shutdown()

    def shutdown(self) -> None:
        log.info('Worker shutting down')
        self.cb.worker['on_worker_shutdown'](self)

        # Initiate graceful shutdown on all executors
        for pool in self.pools.values():
            log.debug(f'Gracefully stopping {pool.TYPE} executors')
            pool.kill(graceful=True)

        # Wait for graceful shutdown to complete
        while True:
            num_active = sum(p.active for p in self.pools.values())
            ttl = int((self.force_shutdown_at - datetime.now()).total_seconds()) if self.force_shutdown_at else 0
            if ttl > 0 and num_active > 0:
                log.debug(f'Forcing shutdown in {ttl} seconds: waiting on {num_active} active tasks')
                time.sleep(1)
                continue
            break

        is_graceful = True

        # Force-kill any remaining tasks
        for pool in self.pools.values():
            if pool.active:
                is_graceful = False
                log.warning(f'Forcing shutdown of {pool.active} tasks in {pool.TYPE} pool')
                pool.kill(graceful=False)

        if not is_graceful:
            time.sleep(1)  # Wait 1 second for cleanup
            log.warning('Force-shutdown complete')
            sys.exit(1)

        # Clean exit if shutdown finished with no remaining active workers
        log.info('Graceful shutdown complete')
        sys.exit(0)

    def graceful_shutdown(self, sig: int, *args: Any, **kwargs: Any) -> None:
        if self.force_shutdown_at:
            log.warning(f'Received second signal {sig}: shutting down immediately')
            sys.exit(1)
        log.info(f'Received signal {sig}: gracefully shutting down workers')
        self.force_shutdown_at = datetime.now() + self.cfg.worker_grace_period
        self.shutdown()

    def force_shutdown(self, sig: int, *args: Any, **kwargs: Any) -> None:
        if self.force_shutdown_at:
            log.warning(f'Received second signal {sig}: shutting down immediately')
            sys.exit(1)
        log.warning(f'Received signal {sig}: forcing worker shutdown')
        self.force_shutdown_at = datetime.now()
        self.shutdown()

    def register_signal_handlers(self) -> None:
        signal.signal(executor_base.SIG_SOFT_SHUTDOWN, self.graceful_shutdown)
        signal.signal(executor_base.SIG_SOFT_SHUTDOWN_ALT, self.graceful_shutdown)
        signal.signal(executor_base.SIG_TIMEOUT, self.graceful_shutdown)
        signal.signal(executor_base.SIG_HARD_SHUTDOWN, self.force_shutdown)

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
            old_task = self.task_cls(
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
