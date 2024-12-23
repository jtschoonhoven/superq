from abc import ABC, abstractmethod
from typing import ClassVar, Literal, TypeVar, Union

from superq import callbacks, config, tasks

ChildWorkerTypeSync = Literal['process', 'thread']
ChildWorkerTypeAsync = Literal['asyncio']
ChildWorkerType = Union[ChildWorkerTypeSync, ChildWorkerTypeAsync]
TaskExecutorType = TypeVar('TaskExecutorType', bound='BaseTaskExecutor')


class BaseTaskExecutor(ABC):  # type: ignore [misc]
    """
    Abstract base class for task executors.
    """

    TYPE: ClassVar[ChildWorkerType]

    max_concurrency: int

    __slots__ = ('max_concurrency',)

    @abstractmethod
    def __init__(
        self,
        cfg: 'config.Config',
        callbacks: dict['callbacks.ChildCallback', 'callbacks.ChildCallbackFn'],
    ) -> None:
        raise NotImplementedError()

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
