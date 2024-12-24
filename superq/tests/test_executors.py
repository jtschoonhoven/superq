from datetime import timedelta

import pytest

from superq.backends.backend_sqlite import SqliteBackend
from superq.config import Config
from superq.executors.executor_asyncio import AsyncioTaskExecutor
from superq.executors.executor_process import ProcessTaskExecutor
from superq.executors.executor_thread import ThreadTaskExecutor
from superq.tests.test_helpers import SQLITE_PATH, create_callback_registry, create_task, wrap_fn

cfg = Config(backend_sqlite_path=SQLITE_PATH)
backend = SqliteBackend(cfg, path=SQLITE_PATH)
my_fn = wrap_fn(lambda: None, backend=backend)


@pytest.mark.parametrize(['Executor'], [(AsyncioTaskExecutor,), (ThreadTaskExecutor,), (ProcessTaskExecutor,)])
def test_process_executor(Executor: type[ProcessTaskExecutor]) -> None:
    task = create_task(fn_name='my_fn', fn_module='superq.tests.test_executors', save_to_backend=backend)
    assert task.status == 'WAITING'

    executor = Executor(
        cfg=cfg,
        max_concurrency=2,
        tasks_per_restart=3,
        idle_ttl=timedelta(seconds=60),
        callbacks=create_callback_registry().child,
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
