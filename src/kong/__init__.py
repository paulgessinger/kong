"""
Main kong import module. Use this to grab the :func:`kong.get_instance` function which can generate a state instance for you.
"""

import pkg_resources  # part of setuptools

__version__ = pkg_resources.get_distribution("kong-batch").version

from . import state

get_instance = state.State.get_instance
