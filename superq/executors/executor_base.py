import signal
from abc import ABC, abstractmethod
from typing import ClassVar, Literal, TypeVar, Union

from superq import tasks

ChildWorkerTypeSync = Literal['process', 'thread']
ChildWorkerTypeAsync = Literal['asyncio']
ChildWorkerType = Union[ChildWorkerTypeSync, ChildWorkerTypeAsync]

TaskExecutorType = TypeVar('TaskExecutorType', bound='BaseTaskExecutor')

# We use the following signals for inter-process comms
SIG_SOFT_SHUTDOWN: int = signal.SIGINT.value  # The child process should initiate graceful shutdown
SIG_SOFT_SHUTDOWN_ALT: int = signal.SIGQUIT.value  # The child process should initiate graceful shutdown
SIG_HARD_SHUTDOWN: int = signal.SIGTERM.value  # The child process should exit asap with minimal cleanup
SIG_TIMEOUT: int = signal.SIGABRT.value  # The child process has timed out and should exit immediately


class BaseTaskExecutor(ABC):  # type: ignore [misc]
    """
    Abstract base class for task executors.
    """

    TYPE: ClassVar[ChildWorkerType]

    __slots__ = ()

    @property
    @abstractmethod
    def capacity(self) -> int:
        """
        Return the number of additional tasks this executor has capacity to run.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def active(self) -> int:
        """
        Return the number of incomplete (pending or running) tasks assigned to this executor.
        """
        raise NotImplementedError

    @abstractmethod
    def submit_task(self: TaskExecutorType, task: 'tasks.Task') -> TaskExecutorType:
        """
        Submit a task to run asap in this executor.
        """
        raise NotImplementedError()

    @abstractmethod
    def kill(self, graceful: bool) -> None:
        """
        Shut down this executor and stop accepting new tasks. If `graceful=True`, attempt to finish active tasks.
        """
        raise NotImplementedError()
