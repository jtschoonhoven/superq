import logging
import subprocess
import sys

from superq import Config, SqliteBackend, TaskQueue, Worker, workers
from superq.tests.test_helpers import SQLITE_PATH

cfg = Config()
q = TaskQueue(cfg, backend=SqliteBackend(cfg, path=SQLITE_PATH))


@q.task(worker_type='process')
def process_task() -> str:
    return 'ok'


@q.task(worker_type='thread')
def thread_task() -> str:
    return 'ok'


@q.task()
async def asyncio_task() -> str:
    return 'ok'


@q.on_worker_logconfig()
def on_worker_logconfig(worker: Worker) -> None:
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


@q.on_child_logconfig()
def on_child_logconfig(name: str | None) -> None:
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


def test_cli() -> None:
    # Start the worker in a child process
    worker = subprocess.Popen(
        ['poetry', 'run', 'superq', 'superq.tests.test_cli'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Schedule three tasks, one for each executor type
    process_result = process_task()
    thread_result = thread_task()
    asyncio_result = asyncio_task()  # type: ignore [var-annotated]

    # Wait for the tasks to complete
    assert process_result.wait() == 'ok'
    assert thread_result.wait() == 'ok'
    assert asyncio_result.wait() == 'ok'

    # Stop the worker
    worker.send_signal(workers.SIGNALS_SOFT_SHUTDOWN[0])
    stdout, stderr = worker.communicate()  # Blocks until process exits

    # Confirm we captured logs as expected
    assert stderr == b''
    assert b'superq.workers:Worker starting' in stdout
    assert b'superq.tasks:Executed async task' in stdout
