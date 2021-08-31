#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

import doctest
from collections import OrderedDict
from threading import Lock
from time import time as current
from typing import Dict, Any, Type, Union, Optional

from cache3 import BaseCache
from cache3.utils import NullContext
from cache3.setting import DEFAULT_TIMEOUT, DEFAULT_TAG

LK: Type = Union[NullContext, Lock]
Number: Type = Union[int, float]
TG: Type = Optional[str]
Time: Type = float

_caches: Dict[Any, Any] = {}
_expire_info: Dict[Any, Any] = {}
_locks: Dict[Any, Any] = {}


# Thread unsafe cache in memory
class SimpleCache(BaseCache):
    """
    >>> cache = SimpleCache('test_cache', 60)
    >>> cache.set('name', 'venus')
    True
    >>> cache.get('name')
    'venus'
    >>> cache.delete('name')
    True
    >>> cache.get('name')
    >>> cache.set('gender', 'male', 0)
    True
    >>> cache.get('gender')
    """

    LOCK = NullContext

    def __init__(self, *args, **kwargs) -> None:
        super(SimpleCache, self).__init__(*args, **kwargs)

        # Attributes _name, _timeout from validate.
        self._cache: OrderedDict[str, Any] = _caches.setdefault(self._name, OrderedDict())
        self._expire_info: Dict[str, Any] = _expire_info.setdefault(self._name, {})
        self._lock: LK = _locks.setdefault(self._name, self.LOCK())

    def ex_set(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT, tag: Optional[str] = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        value: Any = self.serialize(value)

        with self._lock:
            if self._has_expired(key):
                self._set(key, value, timeout)
                return True
            return False

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                self._delete(key)
                return default
            value: Any = self._cache[key]
            self._cache.move_to_end(key, last=False)
        return self.deserialize(value)

    def set(self, key: Any, value: Any, timeout: Number = DEFAULT_TIMEOUT, tag: TG = DEFAULT_TAG) -> bool:
        key: str = self.make_and_validate_key(key, tag=tag)
        value: Any = self.serialize(value)
        with self._lock:
            return self._set(key, value, timeout)

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                return False
            self._expire_info[key] = current() + timeout
            return True

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            return self._delete(key)

    def clear(self) -> bool:
        with self._lock:
            self._cache.clear()
            self._expire_info.clear()
        return True

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:

        if not self._has_expired(key):
            return {
                'key': key,
                'value': self._cache[key],
                'expire': self._expire_info[key]
            }

    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                self._delete(key)
                raise ValueError("Key '%s' not found" % key)
            value: Any = self._cache[key]
            new_value: int = value + delta
            self._cache[key] = new_value
            self._cache.move_to_end(key, last=False)
        return new_value

    def _has_expired(self, key: str) -> bool:
        exp: float = self._expire_info.get(key, -1.)
        return exp is not None and exp <= current()

    def _delete(self, key: str) -> bool:
        try:
            del self._cache[key]
            del self._expire_info[key]
        except KeyError:
            return False
        return True

    def _set(self, key: str, value: Any, timeout=DEFAULT_TIMEOUT) -> bool:
        self._cache[key] = value
        self._cache.move_to_end(key, last=False)
        self._expire_info[key] = current() + timeout
        return True

    def serialize(self, value: Any, *args, **kwargs) -> Any:
        """ Serialize the value for easy backend storage.
        By default, return directly to value doing nothing.
        """
        return value

    def deserialize(self, dump: Any, *args, **kwargs) -> Any:
        """ Restores the value returned by the backend to be consistent
        with when deposited. Usually it is always the opposite of the
        ``serialize(...)`` method.

        By default, return directly to value doing nothing.
        """

        return dump

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set


# Thread safe cache in memory
class SafeCache(SimpleCache):

    LOCK = Lock


if __name__ == '__main__':
    doctest.testmod()
