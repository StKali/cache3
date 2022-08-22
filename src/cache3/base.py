#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

import functools
import pickle
from abc import ABC, abstractmethod
from time import time as current
from typing import (
    Any, Type, Optional, Union, Dict, Callable, NoReturn, List, Iterator
)

from cache3.setting import (
    DEFAULT_TAG, DEFAULT_TIMEOUT, MAX_TIMEOUT, MIN_TIMEOUT, MAX_KEY_LENGTH,
    DEFAULT_NAME, DEFAULT_MAX_SIZE, DEFAULT_CULL_SIZE, LRU_EVICT
)
from cache3.utils import empty, cached_property
from cache3.validate import NumberValidate, StringValidate, EnumerateValidate

try:
    import ujson as json
except ImportError:
    import json

Number: Type = Union[int, float]
Time: Type = Optional[Union[float, int]]
TG: Type = Optional[str]
VT: Type = int
VH: Type = Callable[[Any, VT], NoReturn]


class CacheKeyWarning(RuntimeWarning):
    """A warning that is thrown when the key is not legitimate """


class InvalidCacheKey(ValueError):
    """ An error thrown when the key invalid """


class NotImplementedEvictError(NotImplementedError):
    """ An error thrown when not implement evict method """


class AbstractCache(ABC):
    """ A base class that specifies the API that caching
    must implement and some default implementations.

    The processing logic of cache keys and values is as follows：

        key:
            store(key)         ->  store_key
            restore(store_key) ->  key

        value
            serialize(value)          -> serial_value
            deserialize(serial_value) -> value
    """

    name: str = StringValidate(minsize=1, maxsize=MAX_KEY_LENGTH)
    evict_method: str = EnumerateValidate(LRU_EVICT, )
    timeout: Time = NumberValidate(minvalue=MIN_TIMEOUT, maxvalue=MAX_TIMEOUT)
    max_size: int = NumberValidate(minvalue=0)
    cull_size: str = NumberValidate(minvalue=0)

    def __init__(
            self,
            name: str = DEFAULT_NAME,
            timeout: Time = DEFAULT_TIMEOUT,
            max_size: int = DEFAULT_MAX_SIZE,
            evict_method: str = LRU_EVICT,
            cull_size: int = DEFAULT_CULL_SIZE,
            **kwargs
    ) -> None:
        self.name: str = name
        self.timeout: Time = timeout
        self.max_size: int = max_size
        self.evict_method: str = evict_method
        self.cull_size: int = cull_size
        self._kwargs: Dict[str, Any] = kwargs

    @abstractmethod
    def set(self, key: str, value: Any, timeout: Time = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG) -> bool:
        """ Set a value in the cache. Use timeout for the key if
        it's given, Otherwise use the default timeout.
        """

    @abstractmethod
    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:
        """ Fetch a given key from the cache. If the key does not exist, return
        default, which itself defaults to None.
        """

    @abstractmethod
    def ex_set(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
               tag: Optional[str] = DEFAULT_TAG) -> bool:
        """ Set a value in the cache if the key does not already exist. If
        timeout is given, use that timeout for the key; otherwise use the
        default cache timeout.

        Return True if the value was stored, False otherwise.
        """

    def get_many(self, keys: List[str], tag: TG = DEFAULT_TAG) -> Dict[str, Any]:
        """ Fetch a bunch of keys from the cache. For certain backends (memcached,
        pgsql) this can be *much* faster when fetching multiple values.

        Return a dict mapping each key in keys to its value. If the given
        key is missing, it will be missing from the response dict.
        """

        returns: Dict[Any, Any] = dict()
        for key in keys:
            value: Any = self.get(key, empty, tag)
            if value is not empty:
                returns[key] = value
        return returns

    @abstractmethod
    def touch(self, key: str, timeout: Time, tag: TG = DEFAULT_TAG) -> bool:
        """ Update the key's expiry time using timeout. Return True if successful
        or False if the key does not exist.
        """

    @abstractmethod
    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:
        """ Delete a key from the cache

        Return True if delete success, False otherwise.
        """

    @abstractmethod
    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:
        """ Displays the information of the key value if it exists in cache.

        Returns the details if the key exists, otherwise None.
        """

    @abstractmethod
    def store_key(self, key: Any, tag: Optional[str]) -> Any:
        """ Default function to generate keys.

        Construct the key used by all other methods. By default,
        the key will be converted to a unified string format
        as much as possible. At the same time, subclasses typically
        override the method to generate a specific key.
        """

    @abstractmethod
    def restore_key(self, store_key: Any) -> Any:
        """ extract key and tag from serialize key """

    def get_backend_timeout(
            self, timeout: float = DEFAULT_TIMEOUT, now: Optional[Time] = None
    ) -> Optional[float]:
        """ Return the timeout value usable by this backend based upon the
        provided timeout.
        """
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.timeout
        if now is None:
            now: Time = current()
        return None if timeout is None else now + timeout

    @staticmethod
    def serialize(value: Any, *args, **kwargs) -> Any:
        """ Serialize the value for easy backend storage.
        By default, return directly to value doing nothing.
        """
        return value

    @staticmethod
    def deserialize(dump: Any, *args, **kwargs) -> Any:
        """ Restores the value returned by the backend to be consistent
        with when deposited. Usually it is always the opposite of the
        ``serialize(...)`` method.

        By default, return directly to value doing nothing.
        """
        return dump

    @abstractmethod
    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:
        """ Add delta to value in the cache.

        If the key does not exist, raise a ValueError exception.
        """

    def decr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:
        """ Subtract delta from value in the cache.

        If the key does not exist, raise a ValueError exception.
        """
        return self.incr(key, -delta, tag)

    @abstractmethod
    def has_key(self, key: str, tag: TG = DEFAULT_TAG) -> bool:
        """ Return True if the key is in the cache and has not expired. """

    def memoize(self, tag: Optional[str] = DEFAULT_TAG, timeout: float = DEFAULT_TIMEOUT) -> Any:
        """ The cache is decorated with the return value of the function,
        and the timeout is available. """

        if callable(tag):
            raise TypeError(
                "Mame cannot be callable. ('@cache.memoize()' not '@cache.memoize')."
            )

        def decorator(func) -> Callable[[Callable[[Any], Any]], Any]:
            """ Decorator created by memoize() for callable `func`."""

            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                """Wrapper for callable to cache arguments and return values."""
                value: Any = self.get(func.__name__, empty, tag)
                if value is empty:
                    value: Any = func(*args, **kwargs)
                    self.set(func.__name__, value, timeout, tag)
                return value
            return wrapper

        return decorator

    @abstractmethod
    def ttl(self, key: Any, tag: TG) -> Time:
        """ Return the Time-to-live value. """

    @abstractmethod
    def clear(self) -> bool:
        """ clear all caches. """

    @cached_property
    def evict(self) -> Callable:
        """ Implementation of the cache eviction policy.

        The ``_evict`` parameter is used to determine the eviction policy.
        By default, the lru algorithm is used to evict the cache.

        The behavior of a cache eviction policy always gets the method by
        ``_evict`` property, so the default behavior can be modified through
        the ``config()`` method. the mru_evict will be use, if cache.config(
        evict="mru_evict") and the cache has been implemented ``mru_evict()``.

        Returns:
            evict method if the ``evict_method`` is a callable object

        Raises:
            NotImplementedEvictError thrown when ``evict_method`` not callable
        """
        evict: Callable = getattr(self, self.evict_method, None)
        if not callable(evict):
            raise NotImplementedEvictError(
                "evict %s.%s is not callable" %
                (self.__class__.__name__, self.evict_method)
            )
        return evict

    def __repr__(self) -> str:
        return "<%s name=%s timeout=%.2f>" % (
            self.__class__.__name__, self.name, self.timeout
        )

    @abstractmethod
    def __iter__(self) -> Iterator:
        """ Iterator of cache """

    @abstractmethod
    def __len__(self) -> int:
        """Return the cache items count."""

    def __contains__(self, key: Any) -> bool:
        """ Check whether the key exists.

        Tips: Does not accept the tag argument, which is the default

        Return True if the key existed cache, otherwise False.
        """
        return self.has_key(key)

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set


class JSONMixin:

    @staticmethod
    def deserialize(dump: Any, *args, **kwargs) -> Any:
        if isinstance(dump, (int, float, bytes)):
            return dump
        return json.loads(dump)

    @staticmethod
    def serialize(value: Any, *args, **kwargs) -> Any:
        if isinstance(value, (int, float, bytes)):
            return value
        return json.dumps(value)


class PickleMixin:

    @staticmethod
    def deserialize(dump: Any, *args, **kwargs) -> Any:
        """ In order to save overhead, it is more important to implement incr
        in SQLite layer """

        if isinstance(dump, (int, float, str)):
            return dump
        return pickle.loads(dump)

    @staticmethod
    def serialize(value: Any, *args, **kwargs) -> Any:
        if isinstance(value, (int, float, str)):
            return value
        return pickle.dumps(value)
