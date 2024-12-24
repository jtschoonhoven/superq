import asyncio
import importlib
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Literal, Optional

from superq import workers, wrapped_fn
from superq.backends import backend_base
from superq.bson import ObjectId
from superq.exceptions import SuperqError, TaskConcurrencyError, TaskImportError
from superq.executors import executor_base

log = logging.getLogger(__name__)

TaskStatus = Literal['WAITING', 'RUNNING', 'SUCCESS', 'FAILURE']
TaskFailureType = Literal['ERROR', 'SIGNAL', 'TIMEOUT', 'CONCURRENCY', 'RATELIMIT']

TASK_EPOCH = datetime(2024, 1, 1).replace(microsecond=0)  # Do not change: a fixed reference point for interval tasks


@dataclass(slots=True)
class Task:  # type: ignore [misc]
    """
    Represents a task scheduled to run on a worker server.
    This should be lightweight and trivial to serialize to any backend.
    """

    FN_REGISTRY: ClassVar[dict[str, 'wrapped_fn.WrappedFn']] = {}

    id: ObjectId
    fn_name: str  # Name of the function being called
    fn_module: str  # Module where the function is defined
    priority: int  # Lower values are executed first
    queue_name: Literal['default']  # Reserved for future use
    status: TaskStatus
    result_bytes: bytes | None  # Pickled result of the last successful run, or None if task has not succeeded
    error: str
    error_type: Optional['TaskFailureType']  # E.g. "ERROR" for an exception, TIMEOUT, SIGNAL, etc.
    num_tries: int  # Number of *completed* tries, starting from 0
    num_recovers: int  # Number of times the task has been restored after becoming stuck or orphaned
    num_timeouts: int  # Number of times the task has timed out
    num_lockouts: int  # Number of times the task has been rescheduled due to concurrency limits
    num_ratelimits: int  # Number of times the task has been rescheduled due to rate limits
    args: tuple[Any, ...] | None
    kwargs: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    scheduled_for: datetime  # Task won't run until this time (or later)
    worker_type: 'workers.WorkerType'  # Type of worker that will execute this task
    worker_host: str | None  # Hostname of the worker server that executed this task
    worker_name: str | None  # ID of the worker process or thread that executed this task
    api_version: Literal['2024-11-04'] = '2024-11-04'

    @property
    def fn(self) -> 'wrapped_fn.WrappedFn':
        return self._get_fn(self.fn_name, self.fn_module)

    @property
    def can_retry_for_error(self) -> bool:
        return self.num_tries < self.fn.retries_for_error or self.fn.retries_for_error < 0

    @property
    def can_retry_for_timeout(self) -> bool:
        return self.num_timeouts < self.fn.retries_for_timeout or self.fn.retries_for_timeout < 0

    @property
    def can_retry_for_signal(self) -> bool:
        return self.num_recovers < self.fn.retries_for_signal or self.fn.retries_for_signal < 0

    @property
    def can_retry_for_concurrency(self) -> bool:
        return self.num_lockouts < self.fn.retries_for_concurrency or self.fn.retries_for_concurrency < 0

    @property
    def is_max_concurrency(self) -> bool:
        """
        Return True if this task is at or above max concurrency.
        """
        if self.fn.concurrency_limit:
            concurrency = self.fn.backend.concurrency(self.fn)
            if concurrency >= self.fn.concurrency_limit:
                log.info(f'Task {self.fn.path} ({self.id}) is at max concurrency')
                return True
        return False

    @property
    def is_max_concurrency_for_kwargs(self) -> bool:
        """
        Return True if this task is at or above max concurrency.
        """
        if self.fn.concurrency_kwargs_limit and self.fn.concurrency_kwargs:
            kwargs = {k: self.kwargs.get(k) for k in self.fn.concurrency_kwargs if k and self.kwargs}
            concurrency = self.fn.backend.concurrency(self.fn, with_kwargs=kwargs)
            if concurrency >= self.fn.concurrency_kwargs_limit:
                log.info(f'Task {self.fn.path} ({self.id}) is at max concurrency for {", ".join(kwargs.keys())}')
                return True
        return False

    def on_run_start(
        self,
        started_at: datetime,
        worker_name: str | None,
        worker_host: str | None,
        run_sync: bool,
    ) -> None:
        """
        Set status to RUNNING and check concurrency limits.
        Calling function should catch `TaskConcurrencyError`.
        """
        # Postpone the task if it's at max concurrency
        if self.is_max_concurrency or self.is_max_concurrency_for_kwargs:
            self.error = 'Task is at max concurrency'
            self.error_type = 'CONCURRENCY'
            if self.can_retry_for_concurrency:
                self.reschedule(error=self.error, error_type=self.error_type, incr_num_lockouts=True, run_sync=run_sync)
            else:
                self.set_failed(error=self.error, error_type=self.error_type)
                self.fn.cb.fn[self.fn.path]['on_failure'](self)
                self.fn.cb.task['on_task_failure'](self)
            raise TaskConcurrencyError(self.error)

        # Set task to "RUNNING"
        self.result_bytes = None
        self.started_at = started_at
        self.ended_at = None
        self.status = 'RUNNING'
        self.worker_host = worker_host
        self.worker_name = worker_name
        self.save(fields=['result_bytes', 'status', 'started_at', 'ended_at', 'worker_host', 'worker_name'])

    def on_run_success(self, started_at: datetime, result: Any) -> 'Task':
        """
        Set status to SUCCESS and log metrics.
        """
        ended_at = datetime.now()

        # Set status to SUCCESS
        self.result_bytes = pickle.dumps(result)
        self.status = 'SUCCESS'
        self.error = ''
        self.error_type = None
        self.ended_at = ended_at
        self.updated_at = ended_at
        self.num_tries += 1
        self.save(fields=['result_bytes', 'status', 'error', 'error_type', 'ended_at', 'num_tries', 'updated_at'])

        # Log success and send metrics
        execution_time_ms = int((ended_at - started_at).total_seconds() * 1000)
        log.debug(f'Executed async task `{self.fn.path}` in {execution_time_ms / 1000}s')

        # Callbacks
        self.fn.cb.fn[self.fn.path]['on_success'](self)
        self.fn.cb.task['on_task_success'](self)

        return self

    def on_run_failure(self, e: BaseException, run_sync: bool) -> 'Task':
        """
        Error handler for tasks that raise an exception during execution.
        """
        if isinstance(e, SuperqError):
            self.error_type = 'ERROR'
            self.error = f'{type(e).__name__}: {e}'

        elif isinstance(e, Exception):
            self.error_type = 'ERROR'
            self.error = f'{type(e).__name__}: {e}'

        # Special handling for signals
        elif isinstance(e, (SystemExit, KeyboardInterrupt)):
            self.error = f'Task exited with {type(e).__name__}: {e}'
            self.error_type = 'SIGNAL'

            if self.can_retry_for_signal:
                log.warning(f'Retriable signal in task {self.fn_module}.{self.fn_name} ({self.id}): {self.error}')
                self.reschedule(error=self.error, error_type=self.error_type, incr_num_recovers=True, run_sync=run_sync)
            else:
                log.error(f'Non-retriable signal in task {self.fn_module}.{self.fn_name} ({self.id}): {self.error}')
                self.set_failed(error=self.error, error_type=self.error_type)
                self.fn.cb.fn[self.fn.path]['on_failure'](self)
                self.fn.cb.task['on_task_failure'](self)
            raise e  # Re-raise the signal

        elif isinstance(e, BaseException):
            log.error(f'{type(e).__name__} in task {self.fn_module}.{self.fn_name} ({self.id}): {self.error}')
            raise e

        if self.can_retry_for_error:
            log.exception(f'Retriable error in task {self.fn_module}.{self.fn_name} ({self.id}): {self.error}')
            self.reschedule(error=self.error, error_type=self.error_type, incr_num_tries=True, run_sync=run_sync)
        else:
            log.exception(f'Non-retriable error in task {self.fn_module}.{self.fn_name} ({self.id}): {self.error}')
            self.set_failed(error=self.error, error_type=self.error_type)
            self.fn.cb.fn[self.fn.path]['on_failure'](self)
            self.fn.cb.task['on_task_failure'](self)

        return self

    def run(self, worker_name: str | None, worker_host: str | None, run_sync: bool) -> 'Task':
        """
        Call this task synchronously and update its status in the DB.
        """
        started_at = datetime.now()

        try:
            self.on_run_start(started_at, worker_name=worker_name, worker_host=worker_host, run_sync=run_sync)
        except TaskConcurrencyError:
            return self

        args = self.args or ()
        kwargs = self.kwargs or {}

        try:
            # Run the function
            result = self.fn.fn(*args, **kwargs)
            return self.on_run_success(started_at, result)
        except BaseException as e:
            return self.on_run_failure(e, run_sync=run_sync)

    async def run_aio(self, worker_name: str | None, worker_host: str | None, run_sync: bool) -> 'Task':
        """
        Call this task synchronously and update its status in the DB.
        """
        started_at = datetime.now()

        if not asyncio.iscoroutinefunction(self.fn.fn):
            return self.run(worker_name=worker_name, worker_host=worker_host, run_sync=run_sync)

        try:
            self.on_run_start(started_at, worker_name=worker_name, worker_host=worker_host, run_sync=run_sync)
        except TaskConcurrencyError:
            return self

        args = self.args or ()
        kwargs = self.kwargs or {}

        try:
            # Run the function
            result = await self.fn.fn(*args, **kwargs)
            return self.on_run_success(started_at, result)

        except SuperqError as e:
            return self.on_run_failure(e, run_sync=run_sync)

    @classmethod
    def create(
        cls,
        backend: 'backend_base.BaseBackend',
        fn_name: str,
        fn_module: str,
        priority: int,
        num_tries: int,
        num_recovers: int,
        num_timeouts: int,
        num_lockouts: int,
        num_ratelimits: int,
        args: tuple[Any, ...] | None,
        kwargs: dict[str, Any] | None,
        scheduled_for: datetime | None,
        worker_type: 'executor_base.ChildWorkerType',
    ) -> 'Task':
        """
        Create a new task with the given options and persist it to the DB.
        New tasks will be executed by the worker server ASAP (unless `scheduled_for` a later time).
        """
        now = datetime.now()
        id = ObjectId()
        task = cls(
            id=id,
            fn_name=fn_name,
            fn_module=fn_module,
            priority=priority,
            queue_name='default',
            status='WAITING',
            result_bytes=None,
            error='',
            error_type=None,
            num_tries=num_tries,
            num_recovers=num_recovers,
            num_timeouts=num_timeouts,
            num_lockouts=num_lockouts,
            num_ratelimits=num_ratelimits,
            args=args,
            kwargs=kwargs,
            created_at=now,
            updated_at=now,
            started_at=None,
            ended_at=None,
            scheduled_for=scheduled_for or now,
            worker_type=worker_type,
            worker_host=None,
            worker_name=None,
        )
        task = backend.push(task)
        log.debug(
            f'Scheduled new task {fn_module}.{fn_name} ({task.id}): starting '
            f'{scheduled_for.isoformat() if scheduled_for else "asap"}'
        )
        return task

    def reschedule(
        self,
        error: str,
        error_type: TaskFailureType,
        run_sync: bool,
        incr_num_tries=False,
        incr_num_recovers=False,
        incr_num_timeouts=False,
        incr_num_lockouts=False,
    ) -> 'Task':
        """
        Run this task again after its `retry_delay`.
        """
        schedule_for = datetime.now() + self.fn.retry_delay

        log.debug(f'Rescheduling task {self.fn.path} ({self.id}) to run at {schedule_for.isoformat()}')

        fields = self.set_failed(
            error=error,
            error_type=error_type,
            save=False,
            incr_num_tries=incr_num_tries,
            incr_num_recovers=incr_num_recovers,
            incr_num_timeouts=incr_num_timeouts,
            incr_num_lockouts=incr_num_lockouts,
        )

        self.status = 'WAITING'
        self.scheduled_for = schedule_for

        if 'status' not in fields:
            fields.append('status')
        if 'scheduled_for' not in fields:
            fields.append('scheduled_for')
        self.save(fields=fields)

        # Retry callback
        self.fn.cb.task['on_task_retry'](self)

        # Retry immediately in sync mode
        if run_sync:
            return self.run(worker_name=self.worker_name, worker_host=self.worker_host, run_sync=True)
        return self

    def set_failed(
        self,
        error: str,
        error_type: TaskFailureType,
        incr_num_tries=False,
        incr_num_recovers=False,
        incr_num_timeouts=False,
        incr_num_lockouts=False,
        save=True,
    ) -> list[str]:  # Returns the list of fields that were updated
        """
        Set status=FAILURE and update other failure-related properties of this task.
        Returns the list of properties that were modified.
        """
        now = datetime.now()

        self.status = 'FAILURE'
        self.result_bytes = None
        self.error = error
        self.error_type = error_type
        self.ended_at = now
        self.updated_at = now

        fields = ['status', 'result_bytes', 'error', 'error_type', 'ended_at', 'updated_at']

        if incr_num_tries:
            self.num_tries += 1
            fields.append('num_tries')

        if incr_num_timeouts:
            self.num_timeouts += 1
            fields.append('num_timeouts')

        if incr_num_recovers:
            self.num_recovers += 1
            fields.append('num_recovers')

        if incr_num_lockouts:
            self.num_lockouts += 1
            fields.append('num_lockouts')

        if save:
            self.save(fields=fields)

        return fields

    def save(self, *, fields: list[str]) -> None:
        """
        Save changes on this instance to the DB. To avoid accidental conflicts, only named fields are updated.
        This automatically sets `updated_at` to the current time.

        Example:
            task.save(fields=['status'])
        """
        self.fn.backend.update(self, fields=fields)

    def fetch(self) -> 'Task':
        """
        Return a new copy of this task from the backend.
        """
        return self.fn.backend.fetch(self.id)

    @classmethod
    def _get_fn(cls, fn_name: str, fn_module: str) -> 'wrapped_fn.WrappedFn':
        """
        Get the async function associated with this task.
        """
        path = f'{fn_module}.{fn_name}'

        # Check the registry: includes tasks defined in modules that have been imported
        if path in cls.FN_REGISTRY:
            return cls.FN_REGISTRY[path]

        # Manually import the module and check the registry again
        try:
            module = importlib.import_module(fn_module)
        except ImportError as e:
            raise TaskImportError(f'Failed to import module "{fn_module}" for task "{fn_name}": {e}') from e

        # After importing the module, the function should be in the registry
        if path in cls.FN_REGISTRY:
            log.debug(f'Function {fn_name} not in task registry: imported with {fn_module}')
            return cls.FN_REGISTRY[path]

        # Otherwise attempt to register the function manually
        async_fn = getattr(module, fn_name, None)
        if isinstance(async_fn, wrapped_fn.WrappedFn):
            log.debug(f'Function {fn_name} not in task registry: extracted from {fn_module}')
            cls.FN_REGISTRY[path] = async_fn
            return async_fn

        raise TaskImportError(f'Failed to import task at {path}: module "{fn_module}" has no task named "{fn_name}"')
