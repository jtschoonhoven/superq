try:
    import pymongo  # type: ignore [import-not-found]
    import pymongo.collection
except ImportError as e:
    raise ImportError('Install `pymongo` to use the superq Mongo backend: `pip install superq[pymongo]`') from e
import logging
import pickle
from datetime import datetime
from typing import Any, Optional

import pymongo.errors

from superq import tasks, workers, wrapped_fn
from superq.backends import backend_base
from superq.bson import ObjectId
from superq.config import Config
from superq.exceptions import TaskImportError, TaskNotFoundError

log = logging.getLogger(__name__)


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

    def claim_tasks(
        self,
        max_tasks: int,  # Ignored unless > 0
        worker_type: 'workers.WorkerType',
        worker_host: str | None = None,
        worker_name: str | None = None,
        run_sync=False,
    ) -> list['tasks.Task']:
        """
        "Claim" tasks in Mongo by updating and returning them atomically.
        Automatically reschedules tasks and sets `status=RUNNING`.
        """
        now = datetime.now()

        query = {  # Fetch all incomplete tasks scheduled to run before now
            'scheduled_for': {'$lte': now},
            'status': {'$in': ['WAITING', 'RUNNING']},
            'worker_type': worker_type,
        }

        task_dicts: list[dict[str, Any]] = list(
            self.db.find(query, limit=max_tasks if max_tasks >= 1 else None).sort(
                [
                    ('priority', pymongo.ASCENDING),  # Sort first by priority (0 is higher than 1)
                    ('scheduled_for', pymongo.ASCENDING),  # Sort by schedule, oldest-to-newest
                    ('_id', pymongo.ASCENDING),  # Sort last by creation date, oldest-to-newest
                ]
            )
        )

        # Delete old transaction IDs from previous attempts (this should be a rare edge case)
        task_dicts_with_transaction_ids = [t for t in task_dicts if '__transaction_id__' in t]
        if task_dicts_with_transaction_ids:
            query['_id'] = {'$in': [ObjectId(t['_id']) for t in task_dicts_with_transaction_ids]}
            self.db.update_many(query, {'$unset': {'__transaction_id__': ''}})

        # Get the IDs of the tasks we want to claim (or exit if there are no tasks to claim)
        task_ids = [ObjectId(t['_id']) for t in task_dicts]
        if not task_ids:
            return []

        # Generate a "transaction ID" that uniquely identifies this claim attempt (used to manage race conditions)
        transaction_id = ObjectId()
        mutation = {
            'status': 'RUNNING',
            'scheduled_for': now + self.cfg.task_timeout,
            'started_at': now,
            'updated_at': now,
            'worker_host': worker_host,
            'worker_name': worker_name,
            '__transaction_id__': transaction_id,
        }

        # Update the tasks to claim them
        # Because we both set and match on __transaction_id__, two workers cannot claim the same task in a race:
        # However, race conditions might cause this to return fewer tasks than requested (which is acceptable)
        # https://www.mongodb.com/docs/manual/core/write-operations-atomicity/#multi-document-transactions
        response = self.db.update_many(
            {'_id': {'$in': task_ids}, '__transaction_id__': {'$exists': False}},
            {'$set': mutation},
        )

        # Re-fetch the list of successfully-claimed tasks (if race conditions prevented us from claiming all of them)
        # Successfully-claimed tasks will all have the same transaction ID
        if response.modified_count != len(task_ids):
            query = {'_id': {'$in': task_ids}, '__transaction_id__': transaction_id}
            task_ids = [ObjectId(t['_id']) for t in self.db.find(query, projection={'_id': 1})]
            task_dicts = [t for t in task_dicts if ObjectId(t['_id']) in task_ids]  # Use task_dict from before update

        mutation_without_status_or_started_at = {k: v for k, v in mutation.items() if k not in ('status', 'started_at')}
        claimed_tasks: list[tasks.Task] = []

        # Update and deserialize the claimed tasks and handle timeouts
        for task_dict in task_dicts:
            task_dict.update(mutation_without_status_or_started_at)  # Manually apply the mutation
            task = self.deserialize_task(task_dict)

            try:
                # Add this task to the result if it is not already running
                if task.status != 'RUNNING' or not task.started_at:
                    task.status = 'RUNNING'
                    task.started_at = now
                    claimed_tasks.append(task)

                # If this task is already running and has NOT timed out, skip it
                elif task.started_at + task.fn.timeout >= now:
                    continue

                # Otherwise handle the timeout and skip this task for now
                elif task.started_at + task.fn.timeout < now:
                    error = f'Task {task.fn.path} ({task.id}) timed out after {int(task.fn.timeout.total_seconds())}s'
                    error_type: tasks.TaskFailureType = 'TIMEOUT'
                    if task.can_retry_for_error:
                        log.warning(f'{error}: rescheduling')
                        task.reschedule(error, error_type=error_type, incr_num_timeouts=True, run_sync=run_sync)
                    else:
                        log.warning(f'{error}: failed permanently')
                        task.set_failed(error=error, error_type=error_type)
                        task.fn.cb.fn[task.fn.path]['on_failure'](task)
                        task.fn.cb.task['on_task_failure'](task)

            # Handle import errors
            except TaskImportError as e:
                error = str(e)
                error_type = 'ERROR'
                if task.can_retry_for_error:
                    log.warning(f'{error}: rescheduling')
                    task.reschedule(error, error_type=error_type, incr_num_tries=True, run_sync=run_sync)
                else:
                    log.warning(f'{error}: failed permanently')
                    task.set_failed(error=error, error_type=error_type)
                    task.fn.cb.fn[task.fn.path]['on_failure'](task)
                    task.fn.cb.task['on_task_failure'](task)
                log.error(
                    f'Failed to claim task {task_dict.get("fn_name")}.{task_dict.get("fn_module")} '
                    f'({task_dict["_id"]}) due to import error: {e}'
                )

        return claimed_tasks

    def update(self, task: 'tasks.Task', *, fields: list[str]) -> None:
        """
        Update a task in the queue. This also deletes the special `__transaction_id__` field if set.
        """
        task_dict = self.serialize_task(task)
        query = {'_id': task_dict.pop('_id')}
        mutation = {'$set': {field: task_dict[field] for field in fields}, '$unset': {'__transaction_id__': ''}}
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
