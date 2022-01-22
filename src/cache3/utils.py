#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

from contextlib import AbstractContextManager
from typing import Any, Optional, NoReturn

# Compatible with multiple types.
empty: Any = object()


class NullContext(AbstractContextManager):
    """ Context manager that does no additional processing.

    Used as a stand-in for a normal context manager, when a particular
    block of code is only sometimes used with a normal context manager:

    cm = optional_cm if condition else nullcontext()
    with cm:
        # Perform operation, using optional_cm if condition is True
    """

    def __enter__(self) -> Any:
        """ empty method ... """

    def __exit__(self, *exc: Any) -> NoReturn:
        """ empty method ... """
