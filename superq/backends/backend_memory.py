from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from superq import tasks, wrapped_fn
from superq.backends import backend_base
from superq.bson import ObjectId
from superq.config import Config
from superq.exceptions import TaskNotFoundError
from superq.executors import executor_base


@dataclass(slots=True)
class MemoryBackend(backend_base.BaseBackend):  # type: ignore [misc]
    """
    In-memory task queue backend. Not suitable for production.
    """

    cfg: 'Config'
    TaskCls: type['tasks.Task']
    tasks: list['tasks.Task'] = field(init=False, default_factory=list)

    def push(self, task: 'tasks.Task') -> 'tasks.Task':  # type: ignore [name-defined]
        """
        Push a new task to the queue.
        """
        self.tasks.append(task)
        return task

    def push_interval_task(self, task: 'tasks.Task') -> 'tasks.Task':  # type: ignore [name-defined]
        """
        Push a new task to the queue. The task must have an `interval`.
        """
        scheduled_task = next(
            (
                t
                for t in self.tasks
                if t.scheduled_for == task.scheduled_for and t.fn_module == task.fn_module and t.fn_name == task.fn_name
            ),
            None,
        )
        if scheduled_task:
            return scheduled_task  # Return the already-scheduled task (if exists)

        return self.push(task)  # Schedule and return the new task

    def pop(
        self,
        set_running=True,
        reschedule=True,
        prioritize=True,
        worker_types: list['executor_base.ChildWorkerType'] | None = None,
        worker_host: str | None = None,
        worker_name: str | None = None,
        run_sync=False,
    ) -> Optional['tasks.Task']:  # type: ignore [name-defined]
        """
        Pop the next task from the queue. Workers should call this method with `set_running` to "claim" a task.
        Set `reschedule=True` to automatically retry the task in case of system failure (recommended).
        """
        now = datetime.now()

        eligible = [t for t in reversed(self.tasks) if t.scheduled_for <= now and t.status in ('WAITING', 'RUNNING')]
        if worker_types is not None:
            eligible = [t for t in eligible if t.worker_type in worker_types]

        if not eligible:
            return None

        if not prioritize:
            eligible.sort(key=lambda t: (t.scheduled_for, t.id))
        else:
            eligible.sort(key=lambda t: (t.priority, t.scheduled_for, t.id))

        task = eligible[0]
        task.started_at = now
        task.updated_at = now
        task.worker_host = worker_host
        task.worker_name = worker_name

        if set_running:
            task.status = 'RUNNING'
        if reschedule:
            task.scheduled_for = now + task.fn.timeout

        return task

    def update(self, task: 'tasks.Task', *, fields: list[str]) -> None:  # type: ignore [name-defined]
        """
        Update a task in the queue.
        """
        return None

    def concurrency(
        self,
        fn: 'wrapped_fn.WrappedFn',
        with_kwargs: dict[str, 'backend_base.ScalarType'] | None = None,
    ) -> int:
        """
        Return the number of active running tasks for this function.
        If `with_kwargs`, only returns tasks matching the given kwargs.
        """
        now = datetime.now()
        started_after = now - fn.timeout
        with_kwargs = with_kwargs or {}
        return sum(
            True
            for t in self.tasks
            if t.status == 'RUNNING'
            and t.fn_module == fn.fn_module
            and t.fn_name == fn.fn_name
            and t.started_at
            and t.started_at >= started_after
            and {k: v for k, v in (t.kwargs or {}).items() if k in with_kwargs} == with_kwargs
        )

    def fetch(self, task_id: ObjectId) -> 'tasks.Task':  # type: ignore [name-defined]
        """
        Fetch a task by its ID.
        """
        task = next((t for t in self.tasks if t.id == task_id), None)
        if not task:
            raise TaskNotFoundError(f'Task {task_id} not found')
        return task

    async def fetch_aio(self, task_id: ObjectId) -> 'tasks.Task':  # type: ignore [name-defined]
        """
        Fetch a task by its ID.
        """
        return self.fetch(task_id)

    def delete_completed_tasks_older_than(self, delete_if_older_than: datetime) -> None:
        """
        Delete all completed tasks with a `created_at` older than the given datetime.
        """
        self.tasks = [
            t for t in self.tasks if t.created_at >= delete_if_older_than or t.status in ('WAITING', 'RUNNING')
        ]

    def delete_all_tasks(self) -> None:
        """
        Delete all tasks.
        """
        self.tasks = []
