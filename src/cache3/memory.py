#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

from collections import OrderedDict
from threading import Lock
from time import time as current
from typing import Dict, Any, Type, Union, Optional, NoReturn, Tuple, List

from cache3 import BaseCache
from cache3.setting import DEFAULT_TIMEOUT, DEFAULT_TAG
from cache3.utils import NullContext

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

    Simple encapsulation of ``OrderedDict``, so it has a performance similar
    to that of a ``dict``, at the same time, it requirements for keys and
    values are also relatively loose.

    It is entirely implemented by memory, so use the required control capacity
    and expiration time to avoid wast memory.
    """

    LOCK = NullContext

    def __init__(self, *args, **kwargs) -> None:
        super(SimpleCache, self).__init__(*args, **kwargs)

        # Attributes _name, _timeout from validate.
        self._cache: OrderedDict[str, Any] = _caches.setdefault(
            self.name, OrderedDict()
        )
        self._expire_info: Dict[str, Any] = _expire_info.setdefault(self.name, {})
        self._lock: LK = _locks.setdefault(self.name, self.LOCK())

    def set(
            self, key: Any, value: Any, timeout: Number = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG
    ) -> bool:
        store_key: str = self.store_key(key, tag=tag)
        serial_value: Any = self.serialize(value)
        with self._lock:
            return self._set(store_key, serial_value, timeout)

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:

        store_key: str = self.store_key(key, tag=tag)
        with self._lock:
            if self._has_expired(store_key):
                self._delete(store_key)
                return default
            value: Any = self.deserialize(self._cache[store_key])
            self._cache.move_to_end(store_key, last=False)
        return value

    def ex_set(
            self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
            tag: Optional[str] = DEFAULT_TAG
    ) -> bool:
        """ Realize the mutually exclusive operation of data through thread lock.
        but whether the mutex takes effect depends on the lock type.
        """

        store_key: str = self.store_key(key, tag=tag)
        serial_value: Any = self.serialize(value)

        with self._lock:
            if self._has_expired(store_key):
                self._set(store_key, serial_value, timeout)
                return True
            return False

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:
        """ Renew the key. When the key does not exist, false will be returned """
        store_key: str = self.store_key(key, tag=tag)
        with self._lock:
            if self._has_expired(store_key):
                return False
            self._expire_info[store_key] = self.get_backend_timeout(timeout)
            return True

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        store_key: str = self.store_key(key, tag=tag)
        with self._lock:
            return self._delete(store_key)

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:
        """ Get the details of the key value include stored key and
        serialized value.
        """
        store_key: str = self.store_key(key, tag)
        if not self._has_expired(store_key):
            return {
                'key': key,
                'store_key': store_key,
                'store_value': self._cache[store_key],
                'value': self.deserialize(self._cache[store_key]),
                'expire': self._expire_info[store_key]
            }

    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:
        """ Will throed ValueError when the key is not existed. """
        store_key: str = self.store_key(key, tag=tag)
        with self._lock:
            if self._has_expired(store_key):
                self._delete(store_key)
                raise ValueError("Key '%s' not found" % key)
            value: Any = self.deserialize(self._cache[store_key])
            serial_value: int = self.serialize(value + delta)
            self._cache[store_key] = serial_value
            self._cache.move_to_end(store_key, last=False)
        return serial_value

    def has_key(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        store_key: str = self.store_key(key, tag=tag)
        with self._lock:
            if self._has_expired(store_key):
                self._delete(store_key)
                return False
            return True

    def ttl(self, key: Any, tag: TG) -> Time:

        store_key: Any = self.store_key(key, tag)
        if self._has_expired(store_key):
            return -1
        return self._expire_info[store_key] - current()

    def clear(self) -> bool:
        with self._lock:
            self._cache.clear()
            self._expire_info.clear()
        return True

    def get_current_size(self) -> int:
        return len(self._cache)

    def lru_evict(self) -> NoReturn:
        if self.cull_size == 0:
            self._cache.clear()
            self._expire_info.clear()
        else:
            count = len(self._cache) // self.cull_size
            for i in range(count):
                store_key, _ = self._cache.popitem()
                del self._expire_info[store_key]

    def store_key(self, key: Any, tag: TG) -> Any:
        return key, tag

    def restore_key(self, store_key: Tuple[Any, TG]) -> Tuple[Any, Any]:
        return store_key

    def _has_expired(self, store_key: str) -> bool:
        exp: float = self._expire_info.get(store_key, -1.)
        return exp is not None and exp <= current()

    def _delete(self, store_key: str) -> bool:
        try:
            del self._cache[store_key]
            del self._expire_info[store_key]
        except KeyError:
            return False
        return True

    def _set(self, store_key: str, value: Any, timeout=DEFAULT_TIMEOUT) -> bool:

        if self.timeout and len(self) >= self.max_size:
            self.evictor()
        self._cache[store_key] = value
        self._cache.move_to_end(store_key, last=False)
        self._expire_info[store_key] = self.get_backend_timeout(timeout)
        return True

    def __iter__(self) -> Tuple[Any, ...]:
        for store_key in reversed(self._cache.keys()):
            if not self._has_expired(store_key):
                key, tag = self.restore_key(store_key)
                yield key, self.deserialize(self._cache[store_key]), tag

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set


# Thread safe cache in memory
class SafeCache(SimpleCache):

    LOCK = Lock

if __name__ == '__main__':

    cache = SimpleCache()

    for i in range(3):
        cache[i] = i

    print(list(cache))