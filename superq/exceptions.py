class SuperqError(Exception):
    """
    Base class for all exceptions raised by this package.
    """


class TaskImportError(SuperqError):
    """
    Raised when a task cannot be imported.
    """


class BackendError(SuperqError):
    pass


class TaskError(SuperqError):
    """
    A task failed while running.
    """


class TaskExceptionError(TaskError):
    """
    A task failed while running because it raised an exception.
    """


class TaskTimeoutError(TaskError):
    """
    A task failed while running because it took too long.
    """


class TaskSignalError(TaskError):
    """
    A task failed while running because it received an exit signal.
    """


class TaskConcurrencyError(TaskError):
    """
    A task failed while running because it was at max concurrency.
    """


class TaskRatelimitError(TaskError):
    """
    A task failed while running because it was rate-limited.
    """


class TaskNotFoundError(TaskError):
    """
    Raised when a task cannot be found in the backend.
    """


class ResultError(SuperqError):
    """
    Raised when a function result cannot be retrieved successfully from the backend.
    """


class ResultTimeoutError(ResultError):
    """
    Raised when a function result cannot be retrieved within the timeout.
    """


class WorkerError(SuperqError):
    pass
