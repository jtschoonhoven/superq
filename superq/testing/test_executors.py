import os
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from superq.backends.backend_sqlite import SqliteBackend
from superq.config import Config
from superq.executors.executor_asyncio import AsyncioTaskExecutor
from superq.executors.executor_process import ProcessTaskExecutor
from superq.executors.executor_thread import ThreadTaskExecutor
from superq.testing.testing_utils import create_backend, create_callback_registry, create_task, wrap_fn

SQLITE_PATH = str(Path(__file__).parent.resolve() / 'sqlite.db')

cfg = Config(backend_sqlite_path=SQLITE_PATH)
backend = create_backend(SqliteBackend, cfg)
my_fn = wrap_fn(lambda: None, backend=backend)


@pytest.fixture(autouse=True, scope='module')
def teardown() -> Iterator[None]:
    yield
    if os.path.exists(SQLITE_PATH):
        os.remove(SQLITE_PATH)


@pytest.mark.parametrize(['Executor'], [(AsyncioTaskExecutor,), (ThreadTaskExecutor,), (ProcessTaskExecutor,)])
def test_process_executor(Executor: type[ProcessTaskExecutor]) -> None:
    task = create_task(fn_name='my_fn', fn_module='superq.testing.test_executors', save_to_backend=backend)
    assert task.status == 'WAITING'

    executor = Executor(
        max_concurrency=2,
        tasks_per_restart=3,
        idle_ttl=timedelta(seconds=60),
        callbacks=create_callback_registry(),
        worker_name='__worker_name__',
        worker_host='__worker_host__',
    )

    executor.submit_task(task)
    executor.info.wait_until_started()

    assert executor.proc
    assert executor.proc.is_alive()

    executor.kill(graceful=True)
    executor.proc.join()

    task = task.fetch()
    assert task.status == 'SUCCESS'
