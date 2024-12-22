import os
from collections.abc import Iterator

import pytest

from superq.tests.test_helpers import SQLITE_PATH


@pytest.fixture(autouse=True, scope='session')
def session_teardown() -> Iterator[None]:
    """
    A fixture that automatically runs at the end of each test session.
    """
    yield
    # Delete the test sqlite database (if exists)
    if os.path.exists(SQLITE_PATH):
        os.remove(SQLITE_PATH)
