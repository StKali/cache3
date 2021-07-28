#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

from typing import Any


class BaseCache:
    """ A base class that specifies the API that caching
    must implement and some default implementations.
    """

    def set(self, key: str, value: Any, timeout: float = None,
            tag: str = None) -> bool:
        """"""

    def get(self, key: str, default: Any = None, tag: str = None) -> Any:
        """"""

    def ex_set(self, key: str, value: Any, timeout: float = None,
               tag: str = None) -> bool:
        """"""

    def touch(self, key: str, timeout: float, tag: str = None) -> bool:
        """"""

    def delete(self, key: str, tag: str = None) -> bool:
        """"""

    def clear(self) -> bool:
        """"""
