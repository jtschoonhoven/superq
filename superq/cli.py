import argparse
import importlib

from superq import TaskQueue, WorkerError


def main() -> None:
    """
    CLI entrypoint for the worker process. Only argument is a dot-separated path to your TaskQueue module.
    Usage: superq path.to.taskqueue.module
    """
    parser = argparse.ArgumentParser(description='SuperQ')
    parser.add_argument(
        'module',
        help='Dot-separated path to the entrypoint module (where superq.TaskQueue is initialized)',
    )
    args = parser.parse_args()

    # Get the queue instance from the given module
    try:
        module = importlib.import_module(args.module)
    except ImportError as e:
        raise WorkerError(f'Failed to import module {args.module}: {e}') from e

    queue = next((q for q in vars(module).values() if isinstance(q, TaskQueue)), None)

    # Run the worker
    if queue:
        queue.worker.run()

    raise WorkerError(f'Failed to locate any TaskQueue instance in {args.module}')


if __name__ == '__main__':
    main()
