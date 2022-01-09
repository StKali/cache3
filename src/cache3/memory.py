#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

import doctest
from collections import OrderedDict
from threading import Lock
from time import time as current
from typing import Dict, Any, Type, Union, Optional, NoReturn, Iterator

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

    def set(self, key: Any, value: Any, timeout: Number = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG) -> bool:
        key: str = self.make_and_validate_key(key, tag=tag)
        value: Any = self.serialize(value)
        with self._lock:
            return self._set(key, value, timeout)

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                self._delete(key)
                return default
            value: Any = self._cache[key]
            self._cache.move_to_end(key, last=False)
        return self.deserialize(value)

    def ex_set(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
               tag: Optional[str] = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        value: Any = self.serialize(value)

        with self._lock:
            if self._has_expired(key):
                self._set(key, value, timeout)
                return True
            return False

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                return False
            self._expire_info[key] = self.get_backend_timeout(timeout)
            return True

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            return self._delete(key)

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:

        serial_key: str = self.make_and_validate_key(key, tag)
        if not self._has_expired(serial_key):
            return {
                'key': key,
                'value': self._cache[serial_key],
                'expire': self._expire_info[serial_key]
            }

    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                self._delete(key)
                raise ValueError("Key '%s' not found" % key)
            value: Any = self.deserialize(self._cache[key])
            new_value: int = self.deserialize(value + delta)
            self._cache[key] = new_value
            self._cache.move_to_end(key, last=False)
        return new_value

    def has_key(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag=tag)
        with self._lock:
            if self._has_expired(key):
                self._delete(key)
                return False
            return True

    def clear(self) -> bool:
        with self._lock:
            self._cache.clear()
            self._expire_info.clear()
        return True

    def lru_evict(self) -> NoReturn:
        if self._cull_size == 0:
            self._cache.clear()
            self._expire_info.clear()
        else:
            count = len(self._cache) // self._cull_size
            for i in range(count):
                key, _ = self._cache.popitem()
                del self._expire_info[key]

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

        if self._timeout and len(self._cache) >= self._max_size:
            self.evictor()
        self._cache[key] = value
        self._cache.move_to_end(key, last=False)
        self._expire_info[key] = self.get_backend_timeout(timeout)
        return True

    def __iter__(self) -> Iterator:
        for serial_key, value in self._cache.items():
            if not self._has_expired(serial_key):
                key, tag = self._get_key_tag(serial_key)
                yield {key: (tag, value)}

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set


# Thread safe cache in memory
class SafeCache(SimpleCache):

    LOCK = Lock


if __name__ == '__main__':
    doctest.testmod()
