# -*- coding: utf-8 -*-

"""
interva.exceptions
-------------------

This module contains interva exceptions.
"""


class InterVAException(Exception):
    """Base exception for package"""
    pass


class ArgumentException(InterVAException):
    """Exception involving options passed to InterVA arguments."""
    pass


class DataException(InterVAException):
    """Exception involving VA data passed to InterVA."""
    pass


class HaltGUIException(InterVAException):
    """GUI signaled InterVA to stop."""
    pass
