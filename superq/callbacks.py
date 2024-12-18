import logging
from collections import defaultdict
from functools import partial
from typing import Any, Callable, Literal, TypeVar, Union

from superq import tasks, workers

WorkerCallback = Literal['on_worker_logconfig', 'on_worker_start', 'on_worker_shutdown']
WorkerCallbackFn = Callable[['workers.Worker'], None]

ChildCallback = Literal['on_child_logconfig']
ChildCallbackFn = Callable[[str | None], None]  # Receives the name of the child process or thread (if set)

TaskCallback = Literal['on_task_success', 'on_task_failure', 'on_task_retry']
TaskCallbackFn = Callable[['tasks.Task'], None]

FnCallback = Literal['on_success', 'on_failure']
FnCallbackFn = Callable[['tasks.Task'], None]

Cb = TypeVar('Cb', bound=Union[FnCallbackFn, TaskCallbackFn, ChildCallbackFn, WorkerCallbackFn])

log = logging.getLogger(__name__)


def safe_cb(cb: Cb) -> Cb:
    with_try_catch = partial(_with_try_catch, cb)
    with_try_catch.__name__ = cb.__name__  # type: ignore[attr-defined]
    return with_try_catch  # type: ignore[return-value]


def _with_try_catch(cb: Cb, *args: Any, **kwargs: Any) -> None:
    try:
        cb(*args, **kwargs)
    except Exception as e:
        log.exception(f'Unhandled exception in callback {cb.__name__}: {e}')


class CallbackRegistry:
    worker: dict[WorkerCallback, WorkerCallbackFn]
    task: dict[TaskCallback, TaskCallbackFn]
    child: dict[ChildCallback, ChildCallbackFn]
    fn: dict[str, dict[FnCallback, FnCallbackFn]]

    __slots__ = ('worker', 'task', 'child', 'fn')

    def __init__(self) -> None:
        self.worker = defaultdict(_null_cb)
        self.task = defaultdict(_null_cb)
        self.child = defaultdict(_null_cb)
        self.fn = defaultdict(_null_fn_cb)


def _null_fn(*args: Any, **kwargs: Any) -> None:
    """
    No-op function. Used as a placeholder.
    """
    return None


def _null_cb() -> Callable[..., Any]:
    """
    No-op callback. Used as a placeholder when no other callback is set.
    """

    return _null_fn


def _null_fn_cb() -> dict[FnCallback, FnCallbackFn]:
    """
    No-op callbacks for a wrapped function. Used as a placeholder when no other callbacks are set.
    """
    return defaultdict(_null_cb)
