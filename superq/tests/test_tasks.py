import uuid
from datetime import datetime
from unittest.mock import MagicMock, call

import mongomock
import pytest
from pytest_mock import MockFixture

from superq import BaseBackend, MongoBackend, SqliteBackend, Task, TaskQueue
from superq.config import Config
from superq.tests.test_helpers import SQLITE_PATH

cfg = Config(task_retries_for_concurrency=9)
sqlite_backend = SqliteBackend(cfg, path=SQLITE_PATH, TaskCls=Task)
mongo_backend = MongoBackend(cfg, client=mongomock.MongoClient(), TaskCls=Task)


@pytest.mark.parametrize('backend', [sqlite_backend, mongo_backend])
def test_worker_task(mocker: MockFixture, backend: BaseBackend) -> None:
    mocker.patch.object(uuid, 'uuid4', return_value='__uuid__')
    spy = MagicMock()

    q = TaskQueue(Config(task_run_sync=True), backend=backend)

    # Define an example task using the `@task` decorator
    @q.task()
    def example_task(*args, **kwargs) -> None:
        spy(*args, **kwargs)

    # Run the task and confirm our spy was called correctly
    example_task('arg', kwarg=True)
    assert spy.call_args == call('arg', kwarg=True)


@pytest.mark.parametrize('backend', [sqlite_backend, mongo_backend])
def test_worker_task_max_retries(backend: BaseBackend) -> None:
    spy = MagicMock()
    spy.side_effect = Exception('Nope!')

    q = TaskQueue(Config(task_run_sync=True, task_retries_for_concurrency=9), backend=backend)

    # Define an example task with 2 retries
    @q.task(retries_for_error=2)
    def example_task_with_max_retries(*args, **kwargs) -> None:
        spy(*args, **kwargs)  # This spy will be called if the task is allowed to run

    example_task_with_max_retries('arg', kwarg=True)
    assert spy.call_count == 3  # 1 initial try + 2 retries

    spy.reset_mock()
    spy.side_effect = None

    example_task_with_max_retries('arg', kwarg=True)
    assert spy.call_count == 1


@pytest.mark.parametrize('backend', [sqlite_backend, mongo_backend])
def test_worker_task_concurrency(backend: BaseBackend) -> None:
    spy = MagicMock()
    now = datetime.now()

    q = TaskQueue(Config(task_run_sync=True, task_retries_for_concurrency=9), backend=backend)

    backend.delete_all_tasks()

    # Helper function to mock a task as running for this user
    def mock_run_task() -> Task:
        task = Task.create(
            fn_name=fn_name,
            fn_module=fn_module,
            priority=1,
            backend=backend,
            num_tries=0,
            num_timeouts=0,
            num_recovers=0,
            num_ratelimits=0,
            num_lockouts=0,
            args=(),
            kwargs={},
            scheduled_for=now,
            worker_type='process',
        )
        task.status = 'RUNNING'
        task.started_at = now
        task.save(fields=['status', 'started_at'])
        return task

    # Define an example task using the `@task` decorator that limits concurrency per-user
    @q.task(concurrency_limit=2)
    def example_task_with_concurrency(*args, **kwargs) -> None:
        spy(*args, **kwargs)  # This spy will be called if the task is allowed to run

    fn_name = example_task_with_concurrency.fn_name
    fn_module = example_task_with_concurrency.fn_module

    # Run the task and confirm our spy was called correctly
    example_task_with_concurrency()
    assert spy.call_args == call()

    spy.reset_mock()

    # Run the task again and confirm that it was called correctly (because we're still under the concurrency limit)
    task = mock_run_task()
    example_task_with_concurrency()
    assert spy.call_args == call()

    spy.reset_mock()

    # Run the task again to confirm it was scheduled and *not* called (because we've reached the concurrency limit)
    task = mock_run_task()
    example_task_with_concurrency()
    assert spy.called is False

    spy.reset_mock()
    task.started_at = datetime(1970, 1, 1)
    task.save(fields=['started_at'])

    # The expired active task doesn't count towards the concurrency limit, so this task should run
    example_task_with_concurrency()
    assert spy.call_args == call()


@pytest.mark.parametrize('backend', [sqlite_backend, mongo_backend])
def test_worker_task_concurrency_kwargs(backend: BaseBackend) -> None:
    spy = MagicMock()
    now = datetime.now()

    q = TaskQueue(Config(task_run_sync=True, task_retries_for_concurrency=9), backend=backend)

    backend.delete_all_tasks()  # Empty database

    # Helper function to mock a task as running for this user
    def mock_run_task() -> Task:
        task = Task.create(
            fn_name=fn_name,
            fn_module=fn_module,
            priority=1,
            backend=backend,
            num_tries=0,
            num_timeouts=0,
            num_recovers=0,
            num_ratelimits=0,
            num_lockouts=0,
            args=(),
            kwargs={'user_id': 1},
            scheduled_for=now,
            worker_type='process',
        )
        task.status = 'RUNNING'
        task.started_at = now
        task.save(fields=['status', 'started_at'])
        return task

    # Define an example task using the `@task` decorator that limits concurrency per-user
    @q.task(concurrency_kwargs='user_id', concurrency_kwargs_limit=2)
    def example_task_with_concurrency(*args, **kwargs) -> None:
        spy(*args, **kwargs)  # This spy will be called if the task is allowed to run

    fn_name = example_task_with_concurrency.fn_name
    fn_module = example_task_with_concurrency.fn_module

    # Run the task and confirm our spy was called correctly
    example_task_with_concurrency(user_id=1)
    assert spy.call_args == call(user_id=1)

    spy.reset_mock()

    # Run the task again and confirm that it was called correctly (because we're still under the concurrency limit)
    task = mock_run_task()
    example_task_with_concurrency(user_id=1)
    assert spy.call_args == call(user_id=1)

    spy.reset_mock()

    # Run the task again to confirm it was scheduled and *not* called (because we've reached the concurrency limit)
    task = mock_run_task()
    example_task_with_concurrency(user_id=1)
    assert spy.called is False

    spy.reset_mock()
    task.started_at = datetime(1970, 1, 1)
    task.save(fields=['started_at'])

    # The expired active task doesn't count towards the concurrency limit, so this task should run
    example_task_with_concurrency(user_id=1)
    assert spy.call_args == call(user_id=1)
