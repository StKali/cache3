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

        with self._lock:
            if self._has_expired(key):
                self._set(key, value, timeout)
                return True
            return False

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:

        with self._lock:
            if self._has_expired(key):
                self._delete(key)
                return default
            value: Any = self._cache[key]
            self._cache.move_to_end(key, last=False)
        return value

    def set(self, key: Any, value: Any, timeout: Number = DEFAULT_TIMEOUT, tag: TG = DEFAULT_TAG) -> bool:
        with self._lock:
            return self._set(key, value, timeout)

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:

        with self._lock:
            if self._has_expired(key):
                return False
            self._expire_info[key] = current() + timeout
            return True

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        with self._lock:
            return self._delete(key)

    def clear(self) -> bool:
        with self._lock:
            self._cache.clear()
            self._expire_info.clear()
        return True

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

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:

        if not self._has_expired(key):
            return {
                'key': key,
                'value': self._cache[key],
                'expire': self._expire_info[key]
            }


# Thread safe cache in memory
class SafeCache(SimpleCache):

    LOCK = Lock


if __name__ == '__main__':
    doctest.testmod()
