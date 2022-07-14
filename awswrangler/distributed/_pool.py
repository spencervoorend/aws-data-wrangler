"""Threading Module (PRIVATE)."""

import itertools
import logging
from typing import Any, Callable, List, Optional, Union

import boto3
from ray.util.multiprocessing import Pool

_logger: logging.Logger = logging.getLogger(__name__)


class _RayPoolExecutor:
    def __init__(
        self,
        processes: Optional[Union[bool, int]] = None,
        maxtasksperchild: Optional[int] = None,
        chunksize: Optional[int] = None
    ):
        self._exec: Pool = Pool(
            processes=None if isinstance(processes, bool) else processes,
            maxtasksperchild=maxtasksperchild,
        )
        # Static for now - pass to map later configuring depending on the task
        self._chunksize: Optional[int] = chunksize

    def map(self, func: Callable[..., List[str]], _: boto3.Session, *args: Any) -> List[Any]:
        """Map function and its args to Ray pool."""
        _logger.debug("Ray map: %s", func)
        # Discard boto3.Session object, call map unpacking the arguments ("starmap"), and block
        # until the results are available
        return self._exec.starmap(
            func, zip(itertools.repeat(None), *args), chunksize=self._chunksize,
        )
