import argparse
import importlib
import re
import sys
from pathlib import Path

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
        nargs='?',
    )
    parser.add_argument(
        '--version',
        action='store_true',
        help='Show the version number and exit',
    )
    args = parser.parse_args()

    # Print the version number and exit
    if args.version:
        with open(str(Path(__file__).parent.parent / 'pyproject.toml')) as f:
            match = re.search(r'version\s*=\s*"([a-zA-Z0-9\.-_]+)"', f.read())
            if match:
                version = match.group(1)
                sys.stdout.write(f'{version}\n')
                sys.exit(0)
            sys.stderr.write('Unknown\n')
            sys.exit(1)

    # Require `module`
    if not args.module:
        sys.stderr.write(f'{parser.format_help()}\n')
        sys.exit(1)

    # Get the queue instance from the given module
    try:
        module = importlib.import_module(args.module)
    except ImportError as e:
        raise WorkerError(f'Failed to import module {args.module}: {e}') from e

    queue = next((q for q in vars(module).values() if isinstance(q, TaskQueue)), None)

    # Run the worker
    if queue:
        queue.worker.run()

    sys.stderr.write(f'Failed to locate any TaskQueue instance in {args.module}')
    sys.exit(1)


if __name__ == '__main__':
    main()
