#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com
from contextlib import AbstractContextManager
from typing import *


class NullContext(AbstractContextManager):
    """ Context manager that does no additional processing.

    Used as a stand-in for a normal context manager, when a particular
    block of code is only sometimes used with a normal context manager:

    cm = optional_cm if condition else nullcontext()
    with cm:
        # Perform operation, using optional_cm if condition is True
    """

    def __init__(self, enter_result: Optional[Any] = None) -> None:
        self.enter_result: Optional[Any] = enter_result

    def __enter__(self) -> Any:
        return self.enter_result

    def __exit__(self, *exc: Any) -> NoReturn:
        pass
