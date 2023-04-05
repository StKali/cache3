#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/7/24
# author: clarkmonkey@163.com

""" memory

"""

import functools
from collections import OrderedDict
from contextlib import AbstractContextManager
from threading import Lock
from time import time as current
from typing import Dict, Any, Iterable, Type, Optional, NoReturn, Tuple, Union, Callable, List

from .util import Time, TG, Number, get_expire, empty, lazy, memoize


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


LK: Type = Union[NullContext, Lock]
SK: Type = Tuple[Any, TG]


class MiniCache:
    """ A simple dictionary-based in-memory cache that supports automatic
    data elimination and does not support tag.

    Cache is built based on minicache, but cache is tag-supported.
    """

    def __init__(self, 
                name: str, 
                max_size: int = 1 << 30, 
                evict_size: int = 16, 
                evict_policy: str = 'lru',
                thread_safe: bool = True,
                ) -> None:
        self.name: str = name
        self.max_size: int = max_size
        self.evict_size: int = evict_size
        self.evict_policy: str = evict_policy
        self._lock: LK = Lock() if thread_safe else NullContext()
        self._cache: OrderedDict = OrderedDict()
        self._expires: OrderedDict = OrderedDict()

    def set(self, key: Any, value: Any, timeout: Time = None) -> bool:
        with self._lock:
            self._set(key, value, get_expire(timeout))
            return True
    
    def get(self, key: Any, default: Any = None) -> Any:
        
        with self._lock:
            if self._has_expired(key):
                self._del(key)
                return default
            return self._cache.get(key, default)

    def get_many(self, keys: List[Any]) -> dict:
        res: dict = {} 
        with self._lock:
            for key in keys:
                if self._has_expired(key):
                    self._del(key)
                else:
                    res[key] = self._cache[key]
        return res

    def ex_set(self, key: Any, value: Any, timeout: Time = None) -> bool:

        with self._lock:
            if self._has_expired(key):
                self._set(key, value, get_expire(timeout))
                return True
            return False
    
    def delete(self, key: Any) -> bool:
        with self._lock:
            self._del(key)
        return True
    
    def clear(self) -> bool:
        with self._lock:
            self._cache.clear()
            self._expires.clear()
        return True

    def memoize(self, timeout: Time = None) -> Callable[[TG, Time], Callable]:
        """ The cache is decorated with the return value of the function,
        and the timeout is available. """

        def decorator(func: Optional[Callable] = None) -> Callable[[Callable[[Any], Any]], Any]:
            """ Decorator created by memoize() for callable `func`."""
            if not callable(func):
                raise TypeError(
                    'The `memoize` decorator should be called with a `timeout` parameter.'
                )
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                """Wrapper for callable to cache arguments and return values."""
                value: Any = self.get(func.__name__, empty)
                if value is empty:
                    value: Any = func(*args, **kwargs)
                    self.set(func.__name__, value, timeout)
                return value
            return wrapper
        return decorator

    def incr(self, key: Any, delta: Number = 1) -> Number:
        """ Increases the value by delta (default 1) """

        with self._lock:
            if self._has_expired(key):
                self._del(key)
                raise KeyError(f'key {key!r} not found in cache')
            value = self._cache[key]
            if not isinstance(value, (int, float)) or not isinstance(delta, (int, float)):
                raise TypeError(
                    'unsupported operand type(s) for +/-: '
                    f'{type(value)!r} and {type(delta)!r}'
                )
            value += delta
            self._cache[key] = value
            self._cache.move_to_end(key, last=False)
        return value

    def decr(self, key: Any, delta: Number = 1) -> Number:
        return self.incr(key, -delta)

    def has_key(self, key: Any) -> bool:
        with self._lock:
            if self._has_expired(key):
                self._del(key)
                return False
            return True

    def touch(self, key: Any, timeout: Time = None) -> bool:
        with self._lock:
            now = current()
            if self._has_expired(key, now):
                return False 
            self._expires[key] = get_expire(timeout, now)
        return True

    def pop(self, key: Any, default: Any = empty) -> Any:
        with self._lock:
            if self._has_expired(key):
                if default is not empty:
                    return default
                raise KeyError(f'key {key!r} not found in cache')
            del self._expires[key]
            return self._cache.pop(key)

    def ttl(self, key: Any) -> Time:
        if self._has_expired(key):
            return -1
        return self._expires.get(key, -1)

    def inspect(self, key: Any) -> Optional[Dict[str, Any]]:
        
        if key not in self._cache:
            return None
        expire: Time = self._expires.get(key, -1)
        return {
            'key': key,
            'value': self._cache[key],
            'expire': expire,
            'ttl': expire if expire is None else expire - current()
        }

    def keys(self) -> Iterable[Any]:
        return iter(self._cache)

    def values(self) -> Iterable[Any]:
        return self._cache.values()

    def items(self) -> Iterable[Tuple[Any, ...]]:
        return self._cache.items()

    def _has_expired(self, key: Any, now: Time = None) -> bool:
        exp: Time = self._expires.get(key, -1)
        return exp is not None and exp < (now or current())

    def _set(self, key: Any, value: Any, expire: Time) -> None:
        self._cache[key] = value
        self._cache.move_to_end(key, last=False)
        self._expires[key] = expire

    def _del(self, key: Any) -> None:
        try:
            del self._cache[key]
            del self._expires[key]
        except KeyError:
            ...
    
    def __len__(self) -> int:
        return len(self._cache)
    
    def __repr__(self) -> str:
        return f'<MiniCache length:{len(self)}>'

    __iter__ = keys
    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set
    __contains__ = has_key


class _Caches(dict):

    def __init__(self, name: str, *args, **kwargs) -> None:
        self.name: str = name
        self.args = args
        self.kwargs = kwargs
        super(_Caches, self).__init__()

    def __missing__(self, key: Any) -> MiniCache:
        cache: MiniCache = MiniCache(f'{self.name}:{key}', *self.args, **self.kwargs)
        self[key] = cache
        return cache


class Cache:
    """ Memory-based cache instance """
    
    def __init__(self, name: str, *args, **kwargs) -> None:
        self.name: str = name
        def _factory() -> _Caches:
            return _Caches(name, *args, **kwargs)
        self._factory = _factory
        self._caches = _factory()

    def set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:
        cache = self._caches[tag]
        return cache.set(key, value, timeout)
    
    def get(self, key: Any, default: Any = None, tag: TG = None) -> Any:
        cache = self._caches[tag]
        return cache.get(key, default)

    def get_many(self, keys: List[Any], tag: TG = None) -> dict:
        cache = self._caches[tag]
        return cache.get_many(keys)

    def ex_set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:
        cache = self._caches[tag]
        return cache.ex_set(key, value, timeout)

    def pop(self, key: Any, default: Any = empty, tag: TG = None) -> Any:
        cache = self._caches[tag]
        return cache.pop(key, default)
    
    def delete(self, key: Any, tag: TG = None) -> bool:
        cache = self._caches[tag]
        return cache.delete(key)

    def clear(self) -> bool:
        self._caches = self._factory()
        return True
    
    def incr(self, key: Any, delta: Number = 1, tag: TG = None) -> Number:
        cache = self._caches[tag]
        return cache.incr(key, delta)

    def decr(self, key: Any, delta: Number = 1, tag: TG = None) -> Number:
        cache = self._caches[tag]
        return cache.decr(key, delta)

    def has_key(self, key: Any, tag: TG = None) -> bool:
        cache = self._caches[tag]
        return cache.has_key(key)

    def touch(self, key: Any, timeout: Time = None, tag: TG = None) -> bool:
        cache = self._caches[tag]
        return cache.touch(key, timeout)

    def ttl(self, key: Any, tag: TG = None) -> Time:
        cache = self._caches[tag]
        return cache.ttl(key)

    def inspect(self, key: Any, tag: TG = None) -> Optional[Dict]:
        cache = self._caches[tag]
        ins = cache.inspect(key)
        if ins is not None:
            ins['tag'] = tag
        return ins
    
    def items(self, tag: TG = empty) -> Iterable[Tuple[Any, ...]]:
        if tag is empty:
            for _tag, cache in self._caches.items():
                for m in cache.items():
                    yield m[0], m[1], _tag
        else:
            cache = self._caches[tag]
            return cache.items()

    def keys(self, tag: TG = empty) -> Iterable[Any]:
        if tag is empty:
            for cache in self._caches.values():
                for k in cache:
                    yield k
        else:
            cache = self._caches[tag]
            return cache.keys()
    
    def values(self, tag: TG = empty) -> Iterable[Any]:
        if tag is empty:
            for cache in self._caches.values():
                for v in cache.values():
                    yield v
        else:
            cache = self._caches[tag]
            return cache.values()

    def __len__(self) -> int:
        return sum(len(cache) for cache in self._caches.values())
    
    def __repr__(self) -> str:
        return f'<Cache buckets:{len(self._caches)}>'

    memoize = memoize
    __iter__ = keys
    __setitem__ = set
    __getitem__ = get
    __delitem__ = delete
    __contains__ = has_key


LazyCache = lazy(Cache)
