#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

from typing import Any, Type, Optional, Union, Dict

from cache3.setting import DEFAULT_TAG, DEFAULT_TIMEOUT, MAX_TIMEOUT, MIN_TIMEOUT, MAX_KEY_LENGTH, DEFAULT_NAME
from cache3.validate import NumberValidate, StringValidate

Number: Type = Union[int, float]
TG: Type = Optional[str]


class BaseCache:
    """ A base class that specifies the API that caching
    must implement and some default implementations.
    """

    name: str = StringValidate(minsize=1, maxsize=MAX_KEY_LENGTH)
    timeout: Number = NumberValidate(minvalue=MIN_TIMEOUT, maxvalue=MAX_TIMEOUT)

    def __init__(
            self,
            name: str = DEFAULT_NAME,
            timeout: Number = DEFAULT_TIMEOUT,
            **kwargs
    ) -> None:
        self.name: str = name
        self.timeout: Number = timeout
        self._kwargs: Dict[str, Any] = kwargs

    def set(self, key: str, value: Any, timeout: Number = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG) -> bool:
        """ Set a value in the cache. Use timeout for the key if it's given, Otherwise use the
        default timeout
        """
        raise NotImplementedError('subclasses of BaseCache must provide a set() method.')

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:
        """ Fetch a given key from the cache. If the key does not exist, return
        default, which itself defaults to None.
        """
        raise NotImplementedError('subclasses of BaseCache must provide a get() method')

    def ex_set(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
               tag: Optional[str] = DEFAULT_TAG) -> bool:
        """ Set a value in the cache if the key does not already exist. If
        timeout is given, use that timeout for the key; otherwise use the
        default cache timeout.

        Return True if the value was stored, False otherwise.
        """
        raise NotImplementedError('subclasses of BaseCache must provide an ex_set() method')

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:
        """ Update the key's expiry time using timeout. Return True if successful
        or False if the key does not exist.
        """
        raise NotImplementedError('subclasses of BaseCache must provide a touch() method')

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:
        """ Delete a key from the cache

        Return True if delete success, False otherwise.
        """
        raise NotImplementedError('subclasses of BaseCache must provide a delete() method')

    def clear(self) -> bool:
        """ Clear all caches. """
        raise NotImplementedError('subclasses of BaseCache must provide a clear() method')

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:
        """ Displays the information of the key value if it exists in cache.

        * This api is mostly for testing purposes.

        Returns the details if the key exists, otherwise None.
        """
        raise NotImplementedError('subclasses of BaseCache must provide a inspect() method')
