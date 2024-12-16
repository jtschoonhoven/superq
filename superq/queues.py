import asyncio
import functools
import inspect
import logging
from collections.abc import Coroutine
from datetime import timedelta
from typing import Any, Callable, ClassVar, Optional, Union

from superq import callbacks, tasks, workers, wrapped_fn
from superq.backends import backend_base, backend_memory
from superq.config import Config
from superq.exceptions import BackendError, TaskImportError
from superq.executors import executor_base

log = logging.getLogger(__name__)


class TaskQueue:
    """
    Main entrypoint for managing tasks, queues, backends, and workers.
    """

    FN_REGISTRY: ClassVar[dict[str, 'wrapped_fn.WrappedFn']] = {}  # Class-level variable: does not go in __slots__

    cfg: 'Config'
    worker: 'workers.Worker'
    backend: 'backend_base.BaseBackend'
    task_cls: type['tasks.Task']
    cb: 'callbacks.CallbackRegistry'

    __slots__ = ('cfg', 'worker', 'backend', 'task_cls', 'cb')

    def __init__(
        self,
        cfg: Optional['Config'] = None,
        backend: Optional['backend_base.BaseBackend'] = None,
        task_cls: Optional[type['tasks.Task']] = None,
        worker_cls: Optional[type['workers.Worker']] = None,
    ) -> None:
        self.cfg = cfg or Config()
        self.cb = callbacks.CallbackRegistry()

        # Load task and worker classes
        worker_cls = worker_cls or workers.Worker
        self.task_cls = task_cls or tasks.Task
        self.task_cls.FN_REGISTRY = self.FN_REGISTRY

        # Register backend
        if backend:
            self.backend = backend
        elif self.cfg.backend_in_memory:
            self.backend = backend_memory.MemoryBackend(self.cfg, self.task_cls)
        elif self.cfg.backend_mongo_url:
            from superq.backends.backend_mongo import MongoBackend

            self.backend = MongoBackend(self.cfg, self.task_cls)
        elif self.cfg.backend_sqlite_path:
            from superq.backends.backend_sqlite import SqliteBackend

            self.backend = SqliteBackend(self.cfg, self.task_cls)
        else:
            raise BackendError('Backend is not configured')

        # Initialize the worker
        self.worker = worker_cls(
            cfg=self.cfg,
            backend=self.backend,
            fn_registry=self.FN_REGISTRY,
            cb=self.cb,
            task_cls=self.task_cls,
        )

    def task(
        self,
        timeout: timedelta | None = None,  # Override the default timeout for this task
        priority: int | None = None,  # Set the priority for this task (lower-numbers run first)
        interval: timedelta | None = None,  # If set, this task will run automatically at this interval
        retries_for_error: int | None = None,  # Times this task may be retried after raising an exception
        retries_for_signal: int | None = None,  # Times this task may be recovered after being killed
        retries_for_timeout: int | None = None,  # Times this task may be retried after timing out
        retries_for_concurrency: int | None = None,  # Times this task may be retried if delayed by concurrency limits
        concurrency_limit: int | None = None,  # Limit the number of concurrently-running tasks for this function
        concurrency_kwargs: tuple[str, ...] | str | None = None,
        concurrency_kwargs_limit: int | None = None,
        worker_type: Optional[
            'executor_base.ChildWorkerTypeSync'
        ] = None,  # Override the default worker type for this task
    ) -> Callable[  # Return a decorator that wraps a sync or async function and returns an instance of AsyncFn
        [
            Callable[
                'wrapped_fn.WrappedFnArgsType',
                Union[
                    'wrapped_fn.WrappedFnReturnType',
                    Coroutine[Any, Any, 'wrapped_fn.WrappedFnReturnType'],
                    Coroutine[Any, Any, None],
                ],
            ]
        ],
        'wrapped_fn.WrappedFn[wrapped_fn.WrappedFnArgsType, wrapped_fn.WrappedFnReturnType]',
    ]:
        """
        Decorator to convert a function into an async task that runs on a remote worker server.
        This block is executed once per function, the first time the decorated function is imported.
        """
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        module_name = module.__name__ if module else ''

        if not module or not module_name:
            raise TaskImportError('Failed to initialize async task: module name missing')

        # Set default values for config options
        priority = priority if priority is not None else self.cfg.task_priority
        timeout = timeout if timeout is not None else self.cfg.task_timeout
        retries_for_error = retries_for_error if retries_for_error is not None else self.cfg.task_retries_for_error
        retries_for_signal = retries_for_signal if retries_for_signal is not None else self.cfg.task_retries_for_signal
        retries_for_timeout = (
            retries_for_timeout if retries_for_timeout is not None else self.cfg.task_retries_for_timeout
        )
        retries_for_concurrency = (
            retries_for_concurrency if retries_for_concurrency is not None else self.cfg.task_retries_for_concurrency
        )
        concurrency_kwargs = (concurrency_kwargs,) if isinstance(concurrency_kwargs, str) else concurrency_kwargs

        def decorator(  # Wraps a sync or async function and returns an instance of AsyncFn
            fn: Callable[
                'wrapped_fn.WrappedFnArgsType',
                Union[
                    'wrapped_fn.WrappedFnReturnType',
                    Coroutine[Any, Any, 'wrapped_fn.WrappedFnReturnType'],
                    Coroutine[Any, Any, None],
                ],
            ],
        ) -> 'wrapped_fn.WrappedFn[wrapped_fn.WrappedFnArgsType, wrapped_fn.WrappedFnReturnType]':
            """
            Decorator that receives a synchronous function and replaces it with an async task.
            This block is executed once per function, the first time the decorated function is imported.
            """
            nonlocal worker_type
            child_worker_type: executor_base.ChildWorkerType | None = worker_type

            # Ensure async functions always use asyncio workers
            if asyncio.iscoroutinefunction(fn):
                if child_worker_type and child_worker_type != 'asyncio':
                    log.warning(
                        f'Ignoring invalid worker type override "{child_worker_type}" for async decorated function '
                        f'{module_name}.{fn.__name__}: async functions always use "asyncio" workers'
                    )
                child_worker_type = 'asyncio'

            # Ensure synchronous functions never use asyncio workers
            else:
                if child_worker_type == 'asyncio':
                    log.warning(
                        f'Ignoring invalid worker type override "asyncio" for synchronous decorated function '
                        f'{module_name}.{fn.__name__}: synchronous functions cannot use "asyncio" workers'
                    )
                child_worker_type = child_worker_type or self.cfg.worker_default_type

            async_fn = wrapped_fn.WrappedFn(
                cfg=self.cfg,
                fn=fn,
                fn_name=fn.__name__,
                fn_module=module_name,
                cb=self.cb,
                backend=self.backend,
                TaskCls=self.task_cls,
                priority=priority,
                timeout=timeout,
                interval=interval,
                retry_delay=self.cfg.task_retry_delay,
                retries_for_error=retries_for_error,
                retries_for_signal=retries_for_signal,
                retries_for_timeout=retries_for_timeout,
                retries_for_concurrency=retries_for_concurrency,
                concurrency_limit=concurrency_limit,
                concurrency_kwargs=concurrency_kwargs,
                concurrency_kwargs_limit=concurrency_kwargs_limit,
                worker_type=child_worker_type,
            )

            # Register this function so we can reference it later
            self.FN_REGISTRY[async_fn.path] = async_fn

            @functools.wraps(fn)
            def wrapper(
                *args: wrapped_fn.WrappedFnArgsType.args,
                **kwargs: wrapped_fn.WrappedFnArgsType.kwargs,
            ) -> None:
                """
                Enqueue an async task. This runs each time the decorated function is called.
                """
                async_fn(*args, **kwargs)  # Replace the decorated function call with WrappedFn.__call__(...)

            return async_fn

        return decorator

    def on_task_retry(self) -> Callable[['callbacks.TaskCallbackFn'], 'callbacks.TaskCallbackFn']:
        """
        Register a callback function that runs when a task does not succeed and is rescheduled.
        """

        def decorator(fn: 'callbacks.TaskCallbackFn') -> 'callbacks.TaskCallbackFn':
            self.cb.task['on_task_retry'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_task_success(self) -> Callable[['callbacks.TaskCallbackFn'], 'callbacks.TaskCallbackFn']:
        """
        Register a callback function that runs when a task succeeds.
        """

        def decorator(fn: 'callbacks.TaskCallbackFn') -> 'callbacks.TaskCallbackFn':
            self.cb.task['on_task_success'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_task_failure(self) -> Callable[['callbacks.TaskCallbackFn'], 'callbacks.TaskCallbackFn']:
        """
        Register a callback function that runs when a task fails and is not rescheduled.
        """

        def decorator(fn: 'callbacks.TaskCallbackFn') -> 'callbacks.TaskCallbackFn':
            self.cb.task['on_task_failure'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_worker_logconfig(self) -> Callable[['callbacks.WorkerCallbackFn'], 'callbacks.WorkerCallbackFn']:
        """
        Register a callback function that runs when the worker configures logging.
        """

        def decorator(fn: 'callbacks.WorkerCallbackFn') -> 'callbacks.WorkerCallbackFn':
            self.cb.worker['on_worker_logconfig'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_worker_start(self) -> Callable[['callbacks.WorkerCallbackFn'], 'callbacks.WorkerCallbackFn']:
        """
        Register a callback function that runs when the worker server starts.
        """

        def decorator(fn: 'callbacks.WorkerCallbackFn') -> 'callbacks.WorkerCallbackFn':
            self.cb.worker['on_worker_start'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_worker_shutdown(self) -> Callable[['callbacks.WorkerCallbackFn'], 'callbacks.WorkerCallbackFn']:
        """
        Register a callback function that runs when the worker begins shutdown.
        """

        def decorator(fn: 'callbacks.WorkerCallbackFn') -> 'callbacks.WorkerCallbackFn':
            self.cb.worker['on_worker_shutdown'] = callbacks.safe_cb(fn)
            return fn

        return decorator

    def on_child_logconfig(self) -> Callable[['callbacks.ChildCallbackFn'], 'callbacks.ChildCallbackFn']:
        """
        Register a callback function that runs when a new child process or thread configures logging.
        """

        def decorator(fn: 'callbacks.ChildCallbackFn') -> 'callbacks.ChildCallbackFn':
            self.cb.child['on_child_logconfig'] = callbacks.safe_cb(fn)
            return fn

        return decorator
