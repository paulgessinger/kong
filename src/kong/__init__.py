"""
Main kong import module. Use this to grab the :func:`kong.get_instance` function which can generate a state instance for you.
"""
from . import state

get_instance = state.State.get_instance
