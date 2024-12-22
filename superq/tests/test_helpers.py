import logging
import logging.handlers
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Literal

from superq.backends.backend_base import BaseBackend
from superq.backends.backend_sqlite import SqliteBackend
from superq.bson import ObjectId
from superq.callbacks import CallbackRegistry, ChildCallbackFn
from superq.config import Config
from superq.tasks import Task, TaskFailureType, TaskStatus
from superq.wrapped_fn import WrappedFn

SQLITE_PATH = str(Path(__file__).parent.resolve() / 'sqlite.db')


def setup_logging(*args: Any, **kwargs: Any) -> None:
    logging.basicConfig(level='DEBUG', stream=sys.stdout)


def create_task(
    fn: Callable[..., Any] | WrappedFn | None = None,
    id: ObjectId | None = None,
    fn_name: str | None = None,
    fn_module: str | None = None,
    priority=1,
    queue_name: Literal['default'] = 'default',
    status: TaskStatus = 'WAITING',
    result_bytes: bytes | None = None,
    error='',
    error_type: TaskFailureType | None = None,
    num_tries=0,
    num_recovers=0,
    num_timeouts=0,
    num_lockouts=0,
    num_ratelimits=0,
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    scheduled_for: datetime | None = None,
    worker_type: Literal['thread', 'process', 'asyncio'] = 'process',
    worker_host='__worker_host__',
    worker_name='__worker_name__',
    save_to_backend: BaseBackend | None = None,
) -> Task:
    """
    Test helper to create a new task. If `save_to_backend` is provided, the task will be persisted to that backend.
    """
    now = datetime.now()
    fn_module = fn_module or '__main__'

    if fn:
        if not isinstance(fn, WrappedFn):
            fn_name = fn_name or fn.__name__
            fn = wrap_fn(fn)
        else:
            fn_name = fn_name or fn.fn_name
        Task.FN_REGISTRY[f'{fn_module}.{fn_name}'] = fn

    task = Task(
        id=id or ObjectId(),
        fn_name=fn_name or '__fn_name__',
        fn_module=fn_module,
        priority=priority,
        queue_name=queue_name,
        status=status,
        result_bytes=result_bytes,
        error=error,
        error_type=error_type,
        num_tries=num_tries,
        num_recovers=num_recovers,
        num_timeouts=num_timeouts,
        num_lockouts=num_lockouts,
        num_ratelimits=num_ratelimits,
        args=args,
        kwargs=kwargs,
        created_at=created_at or now,
        updated_at=updated_at or now,
        started_at=started_at,
        ended_at=ended_at,
        scheduled_for=scheduled_for or now,
        worker_type=worker_type,
        worker_host=worker_host,
        worker_name=worker_name,
        api_version='2024-11-04',
    )

    if save_to_backend:
        save_to_backend.push(task)
    return task


def wrap_fn(
    fn: Callable[..., Any],
    cfg: Config | None = None,
    fn_name: str | None = None,
    fn_module='__fn_module__',
    callbacks: CallbackRegistry | None = None,
    backend: BaseBackend | None = None,
    interval: timedelta | None = None,
    priority: int | None = None,
    timeout=timedelta(seconds=60),
    retries_for_error: int | None = None,
    retries_for_signal: int | None = None,
    retries_for_timeout: int | None = None,
    retries_for_concurrency: int | None = None,
    concurrency_limit: int | None = None,
    concurrency_kwargs: tuple[str, ...] | str | None = None,
    concurrency_kwargs_limit: int | None = None,
    worker_type: Literal['thread', 'process', 'asyncio'] | None = None,
) -> WrappedFn:
    """
    Test helper to initialize a new WrappedFn.
    """
    cfg = cfg or Config()
    priority = priority if priority is not None else cfg.task_priority
    timeout = timeout if timeout is not None else cfg.task_timeout
    retries_for_error = retries_for_error if retries_for_error is not None else cfg.task_retries_for_error
    retries_for_signal = retries_for_signal if retries_for_signal is not None else cfg.task_retries_for_signal
    retries_for_timeout = retries_for_timeout if retries_for_timeout is not None else cfg.task_retries_for_timeout
    retries_for_concurrency = (
        retries_for_concurrency if retries_for_concurrency is not None else cfg.task_retries_for_concurrency
    )
    concurrency_kwargs = (concurrency_kwargs,) if isinstance(concurrency_kwargs, str) else concurrency_kwargs
    return WrappedFn(
        cfg,
        fn=fn,
        fn_name=fn_name or fn.__name__,
        fn_module=fn_module,
        cb=callbacks or create_callback_registry(),
        backend=backend or SqliteBackend(cfg=cfg, path=SQLITE_PATH),
        TaskCls=Task,
        timeout=timeout,
        priority=priority,
        interval=interval,
        retry_delay=cfg.task_retry_delay,
        retries_for_error=retries_for_error,
        retries_for_signal=retries_for_signal,
        retries_for_timeout=retries_for_timeout,
        retries_for_concurrency=retries_for_concurrency,
        concurrency_limit=concurrency_limit,
        concurrency_kwargs=concurrency_kwargs,
        concurrency_kwargs_limit=concurrency_kwargs_limit,
        worker_type=worker_type or cfg.worker_default_type,
    )


def create_callback_registry(on_child_logconfig: ChildCallbackFn | None = setup_logging) -> CallbackRegistry:
    """
    Initialize a new CallbackRegistry. Configures basic logging by default.
    """
    cb_registry = CallbackRegistry()
    if on_child_logconfig:
        cb_registry.child['on_child_logconfig'] = on_child_logconfig
    return cb_registry
