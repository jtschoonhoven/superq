try:
    import pymongo  # type: ignore [import-not-found]
    import pymongo.collection
except ImportError as e:
    raise ImportError('Install `pymongo` to use the superq Mongo backend: `pip install superq[pymongo]`') from e
import pickle
from datetime import datetime
from typing import Any, Optional

from superq import tasks, wrapped_fn
from superq.backends import backend_base
from superq.bson import ObjectId
from superq.config import Config
from superq.exceptions import TaskNotFoundError
from superq.executors import executor_base


class MongoBackend(backend_base.BaseBackend):
    _client: pymongo.MongoClient
    _collection: pymongo.collection.Collection | None
    cfg: 'Config'
    TaskCls: type['tasks.Task']

    __slots__ = ('_client', '_collection', 'cfg', 'TaskCls')

    def __init__(
        self,
        cfg: 'Config',
        client: pymongo.MongoClient | None = None,
        TaskCls: Optional[type['tasks.Task']] = None,
    ) -> None:
        self.cfg = cfg
        self.TaskCls = TaskCls or tasks.Task
        self._client = client or pymongo.MongoClient(self.cfg.backend_mongo_url)
        self._collection = None

    @property
    def db(self) -> pymongo.collection.Collection:
        if self._collection is None:
            self._collection = self._client[self.cfg.backend_mongo_database][self.cfg.backend_mongo_collection]
            self._collection.create_indexes(
                [
                    pymongo.IndexModel(
                        [  # Index for `push_interval_task` and `concurrency`
                            ('fn_name', pymongo.ASCENDING),
                            ('fn_module', pymongo.ASCENDING),
                            ('scheduled_for', pymongo.DESCENDING),  # Newest-to-oldeset
                        ]
                    ),
                    pymongo.IndexModel(
                        [  # Index for `pop`
                            ('status', pymongo.ASCENDING),
                            ('priority', pymongo.ASCENDING),  # 0 is higher-priority than 1
                            ('scheduled_for', pymongo.ASCENDING),  # Oldest-to-newest
                        ]
                    ),
                    pymongo.IndexModel(
                        [  # Index for `delete_tasks_older_than`
                            ('status', pymongo.ASCENDING),
                            ('created_at', pymongo.DESCENDING),  # Oldest-to-newest
                        ]
                    ),
                ]
            )
        return self._collection

    def push(self, task: 'tasks.Task') -> 'tasks.Task':
        """
        Push a new task to the queue.
        """
        task_dict = self.serialize_task(task)
        response = self.db.insert_one(task_dict)
        task.id = ObjectId(response.inserted_id)
        return task

    def push_interval_task(self, task: 'tasks.Task') -> 'tasks.Task':
        """
        Push a new task to the queue. The task must have an `interval`.
        This is a no-op if the task is already scheduled at this time, and the already-scheduled task is returned.
        """
        old_task_dict = self.serialize_task(task)

        # Get-or-create this task in the DB (returns None if the task is created)
        new_task_dict = self.db.find_one_and_update(
            {'fn_name': task.fn.fn_name, 'fn_module': task.fn.fn_module, 'scheduled_for': task.scheduled_for},
            {'$setOnInsert': old_task_dict},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER,
        )

        # If the IDs are the same then this task was successfully scheduled and we can return the input task
        if new_task_dict['_id'] == old_task_dict['_id']:
            return task

        return self.deserialize_task(new_task_dict)

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
        reschedule_for = now + self.cfg.task_timeout

        query = {'scheduled_for': {'$lte': now}, 'status': {'$in': ['WAITING', 'RUNNING']}}
        if worker_types is not None:
            query['worker_type'] = {'$in': worker_types}

        mutation = {
            '$set': {
                'started_at': now,
                'updated_at': now,
                'worker_host': worker_host,
                'worker_name': worker_name,
            },
        }
        sort = [
            ('priority', pymongo.ASCENDING),  # Sort by priority (0 is higher than 1)
            ('scheduled_for', pymongo.ASCENDING),  # Sort by schedule, oldest-to-newest
            ('_id', pymongo.ASCENDING),  # Sort by created, oldest-to-newest
        ]

        if set_running:
            mutation['$set']['status'] = 'RUNNING'
        if reschedule:
            mutation['$set']['scheduled_for'] = reschedule_for
        if not prioritize:
            sort.pop(0)  # Don't sort by `priority`

        # Find-one-and-update is atomic, so this is thread safe
        # Note that this returns the *original* task, before the mutation was applied
        task_dict = self.db.find_one_and_update(
            query, mutation, sort=sort, return_document=pymongo.ReturnDocument.BEFORE
        )
        if not task_dict:
            return None

        task = self.deserialize_task(task_dict)

        # Update the task to match what's in the DB
        task.started_at = now
        task.updated_at = now
        task.worker_host = worker_host
        task.worker_name = worker_name
        task.status = 'RUNNING' if set_running else task.status
        task.scheduled_for = reschedule_for if reschedule else task.scheduled_for

        # Handle expired tasks
        if task_dict['status'] == 'RUNNING':
            error = f'Task timed out after {int(task.fn.timeout.total_seconds())} seconds'
            error_type: tasks.TaskFailureType = 'TIMEOUT'

            # Fail tasks with no remaining retries
            if task.can_retry_for_error:
                task.reschedule(error, error_type=error_type, incr_num_timeouts=True, run_sync=run_sync)
            else:
                task.set_failed(error=error, error_type=error_type)
                task.fn.cb.fn[task.fn.path]['on_failure'](task)
                task.fn.cb.task['on_task_failure'](task)

            # Don't return the expired task: fetch a fresh one
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
        query = {'_id': task_dict.pop('_id')}
        mutation = {'$set': {field: task_dict[field] for field in fields}}
        self.db.update_one(query, mutation)

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
        with_kwargs = with_kwargs or {}
        query: dict[str, Any] = {
            'fn_module': fn.fn_module,
            'fn_name': fn.fn_name,
            'status': 'RUNNING',
            'started_at': {'$gt': now - fn.timeout},  # Exclude tasks that have expired
        }
        for key, value in with_kwargs.items():
            query[f'kwargs.{key}'] = value
        return self.db.count_documents(query)

    def fetch(self, task_id: ObjectId) -> 'tasks.Task':
        """
        Fetch a task by its ID.
        """
        task_dict = self.db.find_one({'_id': task_id})
        if not task_dict:
            raise TaskNotFoundError(f'Task {task_id} not found in sqlite backend')
        return self.deserialize_task(task_dict)

    async def fetch_aio(self, task_id: ObjectId) -> 'tasks.Task':
        """
        Fetch a task by its ID.
        """
        return self.fetch(task_id)

    def deserialize_task(self, obj: dict[str, Any]) -> 'tasks.Task':
        """
        Deserialize a mongo document into a Task instance.
        """
        pickled_arg_indices = frozenset(obj.get('__pickled_arg_indices__') or [])
        args: list[Any] | None = None

        if obj['args'] is not None:
            args = []
            for arg_idx, arg in enumerate(obj['args']):
                if arg_idx in pickled_arg_indices:
                    args.append(pickle.loads(arg))
                else:
                    args.append(arg)

        picked_kwarg_keys = frozenset(obj.get('__pickled_kwarg_keys__') or [])
        kwargs: dict[str, Any] | None = None

        if obj['kwargs'] is not None:
            kwargs = {}
            for key, val in obj['kwargs'].items():
                if key in picked_kwarg_keys:
                    kwargs[str(key)] = pickle.loads(arg)
                else:
                    kwargs[str(key)] = val

        return self.TaskCls(
            id=ObjectId(obj['_id']),
            fn_name=str(obj['fn_name']),
            fn_module=str(obj['fn_module']),
            priority=int(obj['priority']),  # type: ignore [arg-type]
            queue_name=obj['queue_name'],
            status=obj['status'],
            result_bytes=bytes(obj['result_bytes']) if obj.get('result_bytes') else None,
            error=obj['error'],
            error_type=obj.get('error_type', ''),
            num_tries=int(obj['num_tries']),
            num_recovers=int(obj['num_recovers']),
            num_timeouts=int(obj.get('num_timeouts', 0)),
            num_lockouts=int(obj.get('num_lockouts', 0)),
            num_ratelimits=int(obj.get('num_ratelimits', 0)),
            args=tuple(args) if args is not None else None,
            kwargs=kwargs,
            created_at=obj['created_at'],
            updated_at=obj['updated_at'],
            started_at=obj['started_at'],
            ended_at=obj['ended_at'],
            scheduled_for=obj['scheduled_for'],
            worker_type=obj.get('worker_type', 'process'),
            worker_host=obj['worker_host'],
            worker_name=obj['worker_name'],
            api_version=obj.get('api_version', '2024-11-04'),
        )

    def delete_completed_tasks_older_than(self, delete_if_older_than: datetime) -> None:
        """
        Delete all completed tasks with a `created_at` older than the given datetime.
        """
        self.db.delete_many({'status': {'$nin': ['WAITING', 'RUNNING']}, 'created_at': {'$lt': delete_if_older_than}})

    def serialize_task(self, task: 'tasks.Task') -> dict[str, Any]:
        """
        Serialize a Task instance to a mongo-compatible BSON dict.
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
            '_id': ObjectId(task.id),
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
            'args': args if args is not None else None,
            'kwargs': kwargs if kwargs is not None else None,
            'created_at': task.created_at,
            'updated_at': task.updated_at,
            'started_at': task.started_at if task.started_at else None,
            'ended_at': task.ended_at if task.ended_at else None,
            'scheduled_for': task.scheduled_for,
            'worker_type': task.worker_type,
            'worker_host': task.worker_host,
            'worker_name': task.worker_name,
            'api_version': task.api_version,
            '__pickled_arg_indices__': pickled_arg_indices,
            '__pickled_kwarg_keys__': pickled_kwarg_keys,
        }

    def delete_all_tasks(self) -> None:
        """
        Delete all tasks.
        """
        self.db.delete_many({})
