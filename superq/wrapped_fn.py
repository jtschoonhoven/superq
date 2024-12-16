import asyncio
import pickle
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Generic, ParamSpec, TypeVar

from superq import callbacks, config, tasks
from superq.backends import backend_base
from superq.exceptions import (
    ResultTimeoutError,
    TaskConcurrencyError,
    TaskError,
    TaskExceptionError,
    TaskRatelimitError,
    TaskSignalError,
    TaskTimeoutError,
)
from superq.executors import executor_base

WrappedFnArgsType = ParamSpec('WrappedFnArgsType')
WrappedFnReturnType = TypeVar('WrappedFnReturnType')


@dataclass(slots=True)
class WrappedFn(Generic[WrappedFnArgsType, WrappedFnReturnType]):  # type: ignore [misc]
    """
    Represents a function that can be scheduled to run async on a worker server.
    When a function is decorated with `@task`, it is replaced with an instance of this class.
    """

    cfg: 'config.Config'
    fn: Callable[
        WrappedFnArgsType,
        WrappedFnReturnType | Coroutine[Any, Any, WrappedFnReturnType] | Coroutine[Any, Any, None],
    ]
    fn_name: str
    fn_module: str
    cb: 'callbacks.CallbackRegistry'
    backend: 'backend_base.BaseBackend'
    TaskCls: type['tasks.Task']
    timeout: timedelta
    priority: int
    interval: timedelta | None
    retry_delay: timedelta
    retries_for_error: int
    retries_for_signal: int
    retries_for_timeout: int
    retries_for_concurrency: int
    concurrency_limit: int | None
    concurrency_kwargs: tuple[str, ...] | None
    concurrency_kwargs_limit: int | None
    worker_type: 'executor_base.ChildWorkerType'

    @property
    def path(self) -> str:
        return f'{self.fn_module}.{self.fn_name}'

    def __call__(
        self,
        *args: WrappedFnArgsType.args,
        **kwargs: WrappedFnArgsType.kwargs,
    ) -> 'WrappedFnResult[WrappedFnReturnType]':
        """
        Schedule this function to run asap on the worker server.
        """
        result_bytes: bytes | None = None
        task = self.TaskCls.create(
            self.backend,
            fn_name=self.fn_name,
            fn_module=self.fn_module,
            priority=self.priority,
            num_tries=0,
            num_recovers=0,
            num_timeouts=0,
            num_lockouts=0,
            num_ratelimits=0,
            args=args,
            kwargs=kwargs,
            scheduled_for=None,
            worker_type=self.worker_type,
        )
        if self.cfg.task_run_sync:
            result_bytes = task.run(worker_name=None, worker_host=None, run_sync=True).result_bytes
        return WrappedFnResult(_task=task, _bytes=result_bytes)

    async def aio(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute this function async and await the result.
        """
        result = self(*args, **kwargs)
        return await result.wait_aio()

    def schedule(
        self,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        delay: timedelta | None = None,
        scheduled_for: datetime | None = None,
    ) -> 'WrappedFnResult':
        """
        Schedule this function to run later on the worker server.
        """
        result_bytes: bytes | None = None

        if delay and not scheduled_for:
            scheduled_for = datetime.now() + delay

        task = self.TaskCls.create(
            backend=self.backend,
            fn_name=self.fn_name,
            fn_module=self.fn_module,
            priority=self.priority,
            num_tries=0,
            num_recovers=0,
            num_timeouts=0,
            num_lockouts=0,
            num_ratelimits=0,
            args=args,
            kwargs=kwargs,
            scheduled_for=scheduled_for,
            worker_type=self.worker_type,
        )
        if self.cfg.task_run_sync:
            result_bytes = task.run(worker_name=None, worker_host=None, run_sync=True).result_bytes
        return WrappedFnResult(_task=task, _bytes=result_bytes)

    def on_success(self) -> Callable[['callbacks.FnCallbackFn'], 'callbacks.FnCallbackFn']:
        """
        Register a callback function that runs when this task succeeds.
        """

        def decorator(fn: 'callbacks.FnCallbackFn') -> 'callbacks.FnCallbackFn':
            self.cb.fn[self.path]['on_success'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_failure(self) -> Callable[['callbacks.FnCallbackFn'], 'callbacks.FnCallbackFn']:
        """
        Register a callback function that runs when this task fails and is not rescheduled.
        """

        def decorator(fn: 'callbacks.FnCallbackFn') -> 'callbacks.FnCallbackFn':
            self.cb.fn[self.path]['on_failure'] = callbacks.safe_cb(fn)
            return fn

        return decorator


@dataclass(slots=True)
class WrappedFnResult(Generic[WrappedFnReturnType]):  # type: ignore [misc]
    """
    A wrapper around the return value of a successful task.
    """

    _task: 'tasks.Task'
    _bytes: bytes | None

    async def wait_aio(self, timeout: timedelta | None = None) -> WrappedFnReturnType:
        poll_interval = self._task.fn.cfg.result_poll_interval.total_seconds()
        expires_at = datetime.now() + timeout if timeout else None

        while self._task.status not in ('SUCCESS', 'FAILURE'):
            if expires_at and datetime.now() >= expires_at:
                raise ResultTimeoutError(
                    f'Task {self._task.fn.path} ({self._task.id}) did not return a result within the timeout'
                )
            self._task = await self._task.fn.backend.fetch_aio(self._task.id)
            await asyncio.sleep(poll_interval)

        return self._return_or_raise_from_completed_task(self._task)

    def wait(self, timeout: timedelta | None = None) -> WrappedFnReturnType:
        poll_interval = self._task.fn.cfg.result_poll_interval.total_seconds()
        expires_at = datetime.now() + timeout if timeout else None

        while self._task.status not in ('SUCCESS', 'FAILURE'):
            if expires_at and datetime.now() >= expires_at:
                raise ResultTimeoutError(
                    f'Task {self._task.fn.path} ({self._task.id}) did not return a result within the timeout'
                )
            self._task = self._task.fn.backend.fetch(self._task.id)
            time.sleep(poll_interval)

        return self._return_or_raise_from_completed_task(self._task)

    @staticmethod
    def _return_or_raise_from_completed_task(task: 'tasks.Task') -> WrappedFnReturnType:
        if task.result_bytes is not None:
            return pickle.loads(task.result_bytes)  # type: ignore [no-any-return]
        if task.error_type == 'ERROR':
            raise TaskExceptionError(f'Task {task.fn.path} ({task.id}) failed: {task.error}')
        if task.error_type == 'TIMEOUT':
            raise TaskTimeoutError(f'Task {task.fn.path} ({task.id}) timed out: {task.error}')
        if task.error_type == 'SIGNAL':
            raise TaskSignalError(f'Task {task.fn.path} ({task.id}) was interrupted: {task.error}')
        if task.error_type == 'RATELIMIT':
            raise TaskRatelimitError(f'Task {task.fn.path} ({task.id}) is rate-limited: {task.error}')
        if task.error_type == 'CONCURRENCY':
            raise TaskConcurrencyError(f'Task {task.fn.path} ({task.id}) is at max concurrency: {task.error}')
        raise TaskError(
            f'Task {task.fn.path} ({task.id}) failed with unexpected status {task.fn.path} and '
            f'error type {task.error_type}: {task.error}'
        )
