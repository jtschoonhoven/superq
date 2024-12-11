import logging
from collections import defaultdict
from typing import Any, Callable, Literal, TypeVar, Union

from superq import tasks, workers

WorkerCallback = Literal['on_worker_logconfig', 'on_worker_start', 'on_worker_shutdown']
WorkerCallbackFn = Callable[['workers.Worker'], None]

ChildCallback = Literal['on_child_logconfig']
ChildCallbackFn = Callable[[str | None], None]  # Receives the name of the child process or thread (if set)

TaskCallback = Literal['on_task_failure', 'on_task_retry']
TaskCallbackFn = Callable[['tasks.Task'], None]

FnCallback = Literal['on_failure']
FnCallbackFn = Callable[['tasks.Task'], None]

Cb = TypeVar('Cb', bound=Union[FnCallbackFn, TaskCallbackFn, ChildCallbackFn, WorkerCallbackFn])

log = logging.getLogger(__name__)


def safe_cb(cb: Cb) -> Cb:
    def with_try_catch(*args: Any, **kwargs: Any) -> None:
        try:
            cb(*args, **kwargs)
        except Exception as e:
            log.exception(f'Unhandled exception in callback {cb.__name__}: {e}')

    with_try_catch.__name__ = cb.__name__

    return with_try_catch  # type: ignore[return-value]


class CallbackRegistry:
    worker: dict[WorkerCallback, WorkerCallbackFn]
    task: dict[TaskCallback, TaskCallbackFn]
    child: dict[ChildCallback, ChildCallbackFn]
    fn: dict[str, dict[FnCallback, FnCallbackFn]]

    __slots__ = ('worker', 'task', 'child', 'fn')

    def __init__(self) -> None:
        self.worker = defaultdict(lambda: lambda _: None)
        self.task = defaultdict(lambda: lambda _: None)
        self.child = defaultdict(lambda: lambda _: None)
        self.fn = defaultdict(lambda: defaultdict(lambda: lambda _: None))
