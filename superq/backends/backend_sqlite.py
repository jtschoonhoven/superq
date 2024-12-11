import json
import os
import pickle
import sqlite3
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional

from superq import tasks, wrapped_fn
from superq.backends import backend_base
from superq.bson import ObjectId
from superq.config import Config
from superq.exceptions import TaskNotFoundError
from superq.executors import executor_base

DEFAULT_SQLITE_PATH = os.path.join(tempfile.gettempdir(), 'tasks.sqlite')


class SqliteBackend(backend_base.BaseBackend):
    conn: sqlite3.Connection
    cfg: 'Config'
    TaskCls: type['tasks.Task']

    __slots__ = ('conn', 'cfg', 'TaskCls')

    def __init__(
        self,
        cfg: 'Config',
        TaskCls: type['tasks.Task'],
        conn: sqlite3.Connection | None = None,
    ) -> None:
        self.cfg = cfg
        self.TaskCls = TaskCls
        self.conn = conn or sqlite3.connect(self.cfg.backend_sqlite_path or DEFAULT_SQLITE_PATH)
        self.conn.row_factory = self._row_factory
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                fn_name TEXT NOT NULL,
                fn_module TEXT NOT NULL,
                priority INTEGER NOT NULL,
                queue_name TEXT NOT NULL,
                status TEXT NOT NULL,
                result_bytes: BLOB,
                error TEXT NOT NULL,
                error_type TEXT,
                num_tries INTEGER NOT NULL,
                num_recovers INTEGER NOT NULL,
                num_timeouts INTEGER NOT NULL,
                num_lockouts INTEGER NOT NULL,
                num_ratelimits INTEGER NOT NULL,
                args TEXT NOT NULL,
                kwargs TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                scheduled_for TEXT NOT NULL,
                worker_type TEXT,
                worker_host TEXT,
                worker_name TEXT,
                __transaction_id__ TEXT,
                __prev_status__ TEXT,
                __pickled_arg_indices__ TEXT,
                __pickled_kwarg_keys__ TEXT
            )
            """
        )
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_fn_name ON tasks (fn_name)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_fn_module ON tasks (fn_module)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_priority ON tasks (priority)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_name ON tasks (queue_name)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON tasks (status)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_kwargs ON tasks (kwargs)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_for ON tasks (scheduled_for)')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_transaction_id ON tasks (__transaction_id__)')

    def push(self, task: 'tasks.Task') -> 'tasks.Task':
        """
        Push a new task to the queue.
        """
        self._insert(task)
        return task

    def push_interval_task(self, task: 'tasks.Task') -> 'tasks.Task':
        """
        Push a new task to the queue. The task must have an `interval`.
        This is a no-op if the task is already scheduled at this time, and the already-scheduled task is returned.
        """
        if self._insert(task, is_interval_task=True):  # Returns None if interval task is already scheduled
            return task

        # Fetch and return the already-scheduled task
        with self._cursor() as cursor:
            cursor.execute(
                'SELECT * FROM tasks WHERE fn_name = ? AND fn_module = ? AND scheduled_for = ? LIMIT 1',
                (task.fn_name, task.fn_module, task.scheduled_for),
            )
            task_dict = cursor.fetchone()

        return self.deserialize_task(task_dict)

    def pop(
        self,
        set_running=True,
        reschedule=True,
        prioritize=True,
        worker_types: list['executor_base.ChildWorkerType'] | None = None,
        worker_host: str | None = None,
        worker_name: str | None = None,
        run_sync=False,
    ) -> Optional['tasks.Task']:
        """
        Pop the next task from the queue. Workers should call this method with `set_running` to "claim" a task.
        Set `reschedule=True` to automatically retry the task in case of system failure (recommended).
        """
        now = datetime.now()
        transaction_id = str(uuid.uuid4())
        started_at = now.isoformat()
        updated_at = now.isoformat()
        reschedule_for = (now + self.cfg.task_timeout).isoformat()

        # SQL to claim the next task
        sql = (
            'UPDATE tasks SET'
            ' started_at = ?,'
            ' updated_at = ?,'
            ' worker_host = ?,'
            ' worker_name = ?,'
            ' __transaction_id__ = ?,'
            ' __prev_status__ = status'
        )
        params = [started_at, updated_at, worker_host, worker_name, transaction_id]

        if set_running:
            sql += ', status = "RUNNING"'
        if reschedule:
            sql += ', scheduled_for = ?'
            params.append(reschedule_for)

        sql += ' WHERE scheduled_for <= ? AND status IN ("WAITING", "RUNNING")'
        params.append(now.isoformat())

        if worker_types is not None:
            sql += f' AND worker_type IN ({", ".join(["?" for _ in worker_types])})'
            for worker_type in worker_types:
                params.append(worker_type)

        if not prioritize:
            sql += ' ORDER BY scheduled_for ASC, id ASC LIMIT 1'
        else:
            sql += ' ORDER BY priority ASC, scheduled_for ASC, id ASC LIMIT 1'

        with self._cursor(transaction=True) as cursor:
            cursor.execute(sql, params)
            if not cursor.rowcount:
                return None
            cursor.execute('SELECT * FROM tasks WHERE __transaction_id__ = ? LIMIT 1', (transaction_id,))
            task_dict: dict[str, Any] = cursor.fetchone()

        prev_status = task_dict['__prev_status__']
        task = self.deserialize_task(task_dict)

        # Handle expired tasks
        if prev_status == 'RUNNING':
            error = f'Task timed out after {int(task.fn.timeout.total_seconds())} seconds'
            error_type: tasks.TaskFailureType = 'TIMEOUT'

            if task.can_retry_for_timeout:
                task.reschedule(error=error, error_type=error_type, incr_num_timeouts=True, run_sync=run_sync)
            else:
                task.set_failed(error=error, error_type=error_type)
                task.fn.cb.fn[task.fn.path]['on_failure'](task)
                task.fn.cb.task['on_task_failure'](task)

            # Return the next task
            return self.pop(
                set_running=set_running,
                reschedule=reschedule,
                worker_host=worker_host,
                worker_name=worker_name,
                run_sync=run_sync,
            )

        return task

    def update(self, task: 'tasks.Task', *, fields: list[str]) -> None:
        """
        Update a task in the queue.
        """
        task_dict = self.serialize_task(task)

        updates: list[str] = []
        params: list[Any] = []

        for colname in fields:
            updates.append(f'{colname} = ?')
            params.append(task_dict[colname])

        sql = f'UPDATE tasks SET {", ".join(updates)} WHERE id = ?'
        params.append(str(task.id))

        with self._cursor() as cursor:
            cursor.execute(sql, params)

    def concurrency(
        self, fn: 'wrapped_fn.WrappedFn', with_kwargs: dict[str, 'backend_base.ScalarType'] | None = None
    ) -> int:
        """
        Return the number of active running tasks for this function.
        If `with_kwargs`, only returns tasks matching the given kwargs.
        """
        now = datetime.now()
        with_kwargs = with_kwargs or {}
        scheduled_after = now - fn.timeout

        sql = 'SELECT COUNT(*) FROM tasks WHERE fn_name = ? AND fn_module = ? AND status = ? AND scheduled_for > ?'
        params = [fn.fn_name, fn.fn_module, 'RUNNING', scheduled_after]

        for key, value in with_kwargs.items():
            sql += f' AND json_extract(kwargs, "$.{key}") = ?'
            params.append(value)

        with self._cursor() as cursor:
            cursor.execute(sql, params)
            result: int = cursor.fetchone()

        return result

    def fetch(self, task_id: ObjectId) -> 'tasks.Task':
        """
        Fetch a task by its ID.
        """
        with self._cursor() as cursor:
            cursor.execute('SELECT * FROM tasks WHERE id = ?', (str(task_id),))
            task_dict = cursor.fetchone()
        if not task_dict:
            raise TaskNotFoundError(f'Task {task_id} not found in sqlite backend')
        return self.deserialize_task(task_dict)

    async def fetch_aio(self, task_id: ObjectId) -> 'tasks.Task':
        """
        Fetch a task by its ID.
        """
        return self.fetch(task_id)

    def delete_completed_tasks_older_than(self, delete_if_older_than: datetime) -> None:
        """
        Delete all completed tasks with a `created_at` older than the given datetime.
        """
        with self._cursor() as cursor:
            cursor.execute(
                'DELETE FROM tasks WHERE status NOT IN ("WAITING", "RUNNING") AND created_at < ?',
                (delete_if_older_than,),
            )

    def deserialize_task(self, obj: dict[str, Any]) -> 'tasks.Task':
        """
        Deserialize a row from sqlite into a Task instance.
        """
        pickled_arg_indices: set[int] = set()
        if obj['__pickled_arg_indices__'] and obj['__pickled_arg_indices__'] != '[]':
            pickled_arg_indices = set(json.loads(obj['__pickled_arg_indices__']))

        args: list[Any] | None = None
        if obj['args'] is not None:
            args = []
            for arg_idx, arg in enumerate(json.loads(obj['args'])):
                if arg_idx in pickled_arg_indices:
                    args.append(pickle.loads(arg))
                else:
                    args.append(arg)

        picked_kwarg_keys = json.loads(obj['__pickled_kwarg_keys__'])
        if obj['__pickled_kwarg_keys__'] and obj['__pickled_kwarg_keys__'] != '[]':
            picked_kwarg_keys = set(json.loads(obj['__pickled_kwarg_keys__']))

        kwargs: dict[str, Any] | None = None
        if obj['kwargs'] is not None:
            kwargs = {}
            for key, val in json.loads(obj['kwargs']).items():
                if key in picked_kwarg_keys:
                    kwargs[str(key)] = pickle.loads(arg)
                else:
                    kwargs[str(key)] = val

        return self.TaskCls(
            id=ObjectId(obj['id']),  # type: ignore [arg-type]
            fn_name=str(obj['fn_name']),
            fn_module=str(obj['fn_module']),
            priority=int(obj['priority']),
            queue_name=str(obj['queue_name']),  # type: ignore [arg-type]
            status=str(obj['status']),  # type: ignore [arg-type]
            result_bytes=bytes(obj['result_bytes']) if obj.get('result_bytes') else None,
            error=str(obj['error']),
            error_type=str(obj['error_type']) if obj.get('error_type') else None,  # type: ignore [arg-type]
            num_tries=int(obj['num_tries']),
            num_recovers=int(obj['num_recovers']),
            num_timeouts=int(obj['num_timeouts']),
            num_lockouts=int(obj['num_lockouts']),
            num_ratelimits=int(obj['num_ratelimits']),
            args=tuple(args) if args is not None else None,
            kwargs=kwargs,
            created_at=datetime.fromisoformat(obj['created_at']),
            updated_at=datetime.fromisoformat(obj['updated_at']),
            started_at=datetime.fromisoformat(obj['started_at']) if obj['started_at'] else None,
            ended_at=datetime.fromisoformat(obj['ended_at']) if obj['ended_at'] else None,
            scheduled_for=datetime.fromisoformat(obj['scheduled_for']),
            worker_type=obj['worker_type'],
            worker_host=obj['worker_host'],
            worker_name=obj['worker_name'],
            api_version=obj['api_version'],
        )

    @classmethod
    def serialize_task(
        cls, task: 'tasks.Task'
    ) -> dict[
        str,
        'backend_base.ScalarType' | tuple['backend_base.ScalarType', ...] | dict[str, 'backend_base.ScalarType'],
    ]:
        """
        Serialize a Task instance into a flat dict of sqlite-compatible scalar values.
        """
        pickled_arg_indices: list[int] = []
        pickled_kwarg_keys: list[str] = []

        args: list[backend_base.ScalarType] | None = None
        if task.args is not None:
            args = []
            for arg_idx, arg in enumerate(task.args):
                if isinstance(arg, backend_base.SCALARS):
                    args.append(arg)
                else:
                    args.append(pickle.dumps(arg))
                    pickled_arg_indices.append(arg_idx)

        kwargs: dict[str, backend_base.ScalarType] | None = None
        if task.kwargs is not None:
            kwargs = {}
            for key, val in task.kwargs.items():
                if isinstance(val, backend_base.SCALARS):
                    kwargs[str(key)] = val
                else:
                    kwargs[str(key)] = pickle.dumps(val)
                    pickled_kwarg_keys.append(key)

        return {
            'id': str(task.id),
            'fn_name': task.fn_name,
            'fn_module': task.fn_module,
            'priority': task.priority,
            'queue_name': task.queue_name,
            'status': task.status,
            'result_bytes': task.result_bytes,
            'error': task.error,
            'error_type': task.error_type,
            'num_tries': task.num_tries,
            'num_recovers': task.num_recovers,
            'num_timeouts': task.num_timeouts,
            'num_lockouts': task.num_lockouts,
            'num_ratelimits': task.num_ratelimits,
            'args': json.dumps(args) if args is not None else None,
            'kwargs': json.dumps(kwargs) if kwargs is not None else None,
            'created_at': task.created_at.isoformat(),
            'updated_at': task.updated_at.isoformat(),
            'started_at': task.started_at.isoformat() if task.started_at else None,
            'ended_at': task.ended_at.isoformat() if task.ended_at else None,
            'scheduled_for': task.scheduled_for.isoformat(),
            'worker_type': task.worker_type,
            'worker_host': task.worker_host,
            'worker_name': task.worker_name,
            'api_version': task.api_version,
            '__transaction_id__': None,
            '__prev_status__': None,
            '__pickled_arg_indices__': json.dumps(pickled_arg_indices),
            '__pickled_kwarg_keys__': json.dumps(pickled_kwarg_keys),
        }

    @contextmanager
    def _cursor(self, transaction=False) -> Iterator[sqlite3.Cursor]:
        cursor = self.conn.cursor()
        if transaction:
            self.conn.execute('BEGIN')
        try:
            yield cursor
        except Exception as e:
            if transaction:
                self.conn.execute('ROLLBACK')
            raise e
        else:
            self.conn.commit()
        finally:
            cursor.close()

    def _insert(self, task: 'tasks.Task', is_interval_task=False) -> Optional['tasks.Task']:
        """
        Push a new task to the queue. If `is_interval_task` no insert is made if the task is already scheduled.
        Returns None if the task was not inserted.
        """
        with self._cursor(transaction=True) as cursor:
            if is_interval_task:
                cursor.execute(
                    'SELECT 1 FROM tasks WHERE fn_name = ? AND fn_module = ? AND scheduled_for = ?',
                    (task.fn_name, task.fn_module, task.scheduled_for),
                )
                exists = cursor.fetchone()
                if exists:
                    return None

            colnames: list[str] = []
            params: list[backend_base.ScalarType] = []

            for colname, value in self.serialize_task(task).items():
                colnames.append(colname)
                if isinstance(value, backend_base.SCALARS):
                    params.append(value)  # type: ignore [arg-type]
                else:
                    params.append(json.dumps(value))

            sql = f'INSERT INTO tasks ({", ".join(colnames)}) VALUES ({", ".join("?" for _ in colnames)})'
            cursor.execute(sql, params)
            return task

    @staticmethod
    def _row_factory(*args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Return a dictionary from a sqlite3.Row object.
        """
        row = sqlite3.Row(*args, **kwargs)
        return dict(row)