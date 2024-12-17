import subprocess

from superq import SIG_SOFT_SHUTDOWN, Config, SqliteBackend, TaskQueue
from superq.testing.testing_utils import SQLITE_PATH

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


def test_cli() -> None:
    # Start the worker in a child process
    worker = subprocess.Popen(['poetry', 'run', 'superq', 'superq.testing.test_cli'])

    # Schedule three tasks, one for each executor type
    process_result = process_task()
    thread_result = thread_task()
    asyncio_result = asyncio_task()  # type: ignore [var-annotated]

    # Wait for the tasks to complete
    assert process_result.wait() == 'ok'
    assert thread_result.wait() == 'ok'
    assert asyncio_result.wait() == 'ok'

    # Stop the worker
    worker.send_signal(SIG_SOFT_SHUTDOWN)
    worker.wait()
