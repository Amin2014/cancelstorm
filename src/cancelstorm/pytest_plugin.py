"""Optional pytest integration, loaded via the pytest11 package entry point."""

import pytest

from .core import Storm


@pytest.fixture
def cancelstorm():
    """Construct a Storm: cancelstorm(matrix(...)).run(operation).assert_passed()."""
    return Storm
