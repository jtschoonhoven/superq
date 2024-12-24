from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from superq import tasks, workers, wrapped_fn
from superq.bson import ObjectId
from superq.config import Config

ScalarType = str | int | float | bool | bytes | None
SCALARS: tuple[type[ScalarType], ...] = (str, int, float, bool, bytes, type(None))


@dataclass(slots=True)
class BaseBackend(ABC):  # type: ignore [misc]
    """
    Abstract base class for a task queue backend (e.g. Redis, MongoDB).
    """

    cfg: 'Config'
    TaskCls: type['tasks.Task']

    @abstractmethod
    def push(self, task: 'tasks.Task') -> 'tasks.Task':
        """
        Push a new task to the queue.
        """
        raise NotImplementedError()

    @abstractmethod
    def push_interval_task(self, task: 'tasks.Task') -> 'tasks.Task':
        """
        Push a new task to the queue. The task must have an `interval`.
        """
        raise NotImplementedError()

    @abstractmethod
    def claim_tasks(
        self,
        max_tasks: int,
        worker_type: 'workers.WorkerType',
        worker_host: str | None = None,
        worker_name: str | None = None,
        run_sync=False,
    ) -> list['tasks.Task']:
        """
        "Claim" tasks in Mongo by updating and returning them atomically.
        Automatically reschedules tasks and sets `status=RUNNING`.
        """
        raise NotImplementedError()

    @abstractmethod
    def update(self, task: 'tasks.Task', *, fields: list[str]) -> None:
        """
        Update a task in the queue.
        """
        raise NotImplementedError()

    @abstractmethod
    def concurrency(self, fn: 'wrapped_fn.WrappedFn', with_kwargs: dict[str, ScalarType] | None = None) -> int:
        """
        Return the number of active running tasks for this function.
        If `with_kwargs`, only returns tasks matching the given kwargs.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, task_id: ObjectId) -> 'tasks.Task':
        """
        Fetch a task by its ID.
        """
        raise NotImplementedError()

    @abstractmethod
    async def fetch_aio(self, task_id: ObjectId) -> 'tasks.Task':
        """
        Fetch a task by its ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_completed_tasks_older_than(self, delete_if_older_than: datetime) -> None:
        """
        Delete all completed tasks with a `created_at` older than the given datetime.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_all_tasks(self) -> None:
        """
        Delete all tasks.
        """
        raise NotImplementedError()
