#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com
import warnings
from typing import Any, Type, Optional, Union, Dict, Tuple

from cache3.setting import (
    DEFAULT_TAG, DEFAULT_TIMEOUT, MAX_TIMEOUT, MIN_TIMEOUT, MAX_KEY_LENGTH,
    DEFAULT_NAME, DEFAULT_MAX_SIZE
)
from cache3.validate import NumberValidate, StringValidate

Number: Type = Union[int, float]
TG: Type = Optional[str]


class CacheKeyWarning(RuntimeWarning):
    """ A warning that is thrown when the key is not legitimate """


class InvalidCacheKey(ValueError):
    """ An Error thrown when the key invalid """


class BaseCache:
    """ A base class that specifies the API that caching
    must implement and some default implementations.
    """

    name: str = StringValidate(minsize=1, maxsize=MAX_KEY_LENGTH)
    timeout: Number = NumberValidate(minvalue=MIN_TIMEOUT, maxvalue=MAX_TIMEOUT)
    max_size: int = NumberValidate(minvalue=0)

    def __init__(
            self,
            name: str = DEFAULT_NAME,
            timeout: Number = DEFAULT_TIMEOUT,
            max_size: int = DEFAULT_MAX_SIZE,
            **kwargs
    ) -> None:
        self.name: str = name
        self.timeout: Number = timeout
        self.max_size: int = max_size
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

    def make_key(self, key: str, tag: Optional[str]) -> str:
        """Default function to generate keys.

        Construct the key used by all other methods. By default,
        the key will be converted to a unified string format
        as much as possible. At the same time, subclasses typically
        override the method to generate a specific key.
        """
        return '%s(%s):%s(%s)' % (type(key).__name__, key, type(tag).__name__, tag)

    def make_and_validate_key(self, key: str, tag: Optional[str] = None) -> str:
        """ Validate keys and convert them into a friendlier format for storing
        key-value pairs.

        Returns a friendlier format key if key is validated, thrown ``InvalidCacheKey``
        otherwise.
        """

        key: str = self.make_key(key, tag)
        msg, validated = self.validate_key(key)
        if validated:
            return key
        raise InvalidCacheKey(msg)

    def validate_key(self, key: str) -> Tuple[Optional[str], bool]:
        """ The incoming key is validated to fit the logic of the
        backend storage. It is always closely related to ``make_key(...)``

        By default, it will check the type and length.
        """

        if not isinstance(key, str):
            return (
                'The key must be a string (%s is %s)'
                % (key, type(key).__name__), False
            )

        if len(key) > MAX_KEY_LENGTH:
            warnings.warn(
                'The key is too long( > %s), which can cause '
                'unnecessary waste of resources and risk: %s...%s.'
                % (MAX_KEY_LENGTH, key[:10], key[-10:]),
                CacheKeyWarning
            )
        return None, True

    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:
        """ Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.  """
        raise NotImplementedError('subclasses of BaseCache must provide a incr() method')

    def decr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:
        """ Subtract delta from value in the cache. If the key does not exist, raise
        a ValueError exception. """
        return self.incr(key, -delta, tag)

    def __repr__(self) -> str:
        return (
                "<%s '%s' timeout:%.2f>"
                % (self.__class__.__name__, self._name, self._timeout)
        )

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set

