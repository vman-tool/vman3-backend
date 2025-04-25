# -*- coding: utf-8 -*-

"""
vacheck.exceptions
~~~~~~~~~~~~~~~~~

This module contains vacheck.py exceptions.
"""


class VACheckException(Exception):
    """ Base exception for all custom exceptions"""


class VAInputException(VACheckException):
    """Exceptions involving va_input argument."""


class VAIDException(VACheckException):
    """Exceptions involving va_id argument."""
