#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/7/24
# author: clarkmonkey@163.com

""" memory

"""

from collections import OrderedDict
from contextlib import AbstractContextManager
from threading import Lock
from time import time as current
from typing import Dict, Any, Iterable, Type, Optional, NoReturn, Tuple, Union

from .util import MultiCache, Time, TG, Number, get_expire, lazy, memoize


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


LK = Union[NullContext, Lock]
SK = Tuple[Any, TG]
DEFAULT_NAME: str = 'default.memcache'


class Payload:
    """ TODO _summary_
    """
    def __init__(self, 
        value: Any,
        store: Time = None,
        timeout: Time = None,
        ) -> None:
        """_summary_

        Args:
            value (Any): _description_
            expire (Time, optional): _description_. Defaults to None.
        """
        # 存储的值
        self.value: Any = value
        # 数据存储的时间
        self.store: Time = store or current()
        # 数据的超时时间
        self.expire: Time = get_expire(timeout, self.store)
        # 数据的最近一次访问时间
        self.access: Time = self.store
        # 累计访问次数
        self.access_count: int = 0
    def expired(self, now: Time = None) -> bool:
        """_summary_

        Args:
            now (Time, optional): _description_. Defaults to None.

        Returns:
            bool: _description_
        """
        if self.expire is None:
            return False
        return (now or current()) > self.expire
    def __repr__(self) -> str:
        return f'Payload <{self.value} access:{self.access}>'


class MiniCache:
    """ A simple dictionary-based in-memory cache that supports automatic
    data elimination and does not support tag.

    Cache is built based on minicache, but cache is tag-supported.

    底层数据结构是一个简单的hash表

    """

    def __init__(self, 
        name: str = DEFAULT_NAME, 
        max_size: Optional[int] = None,
        evict: str = 'default', 
        thread_safe: bool = True
    ) -> None:
        if max_size is not None and max_size < 1:
            raise ValueError('max_size must > 0 or None')
        self.name: str = name
        self.max_size: int = max_size
        self.evict: str = evict
        self._lock: LK = Lock() if thread_safe else NullContext()
        self._store: OrderedDict = OrderedDict()

    def set(self, key: Any, value: Any, timeout: Time = None) -> bool:
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] = self._get(key, now)
            if payload is None:
                self._store[key] = Payload(value, now, timeout)
                if self.max_size is not None and len(self._store) > self.max_size:
                    self._evict(now)
            else:
                payload.value = value
                payload.expire = get_expire(timeout, now)
            self._store.move_to_end(key)    
        return True
    
    def ex_set(self, key: Any, value: Any, timeout: Time = None) -> bool:
        """  Write the key-value relationship when the data does not exist in the cache,
        otherwise the set operation will be cancelled
        """
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] = self._get(key, now, update_access=False)
            if payload is not None:
                return False
            self._store[key] = Payload(value, now, timeout)
            self._store.move_to_end(key)
        return True

    def get(self, key: Any, default: Any = None) -> Any:
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] = self._get(key, now)
        if payload is None:
            return default
        return payload.value

    def get_many(self, *keys: Any) -> Dict[Any, Any]:
        res: dict = {}
        now: Time = current()
        with self._lock:
            for key in keys:
                payload: Optional[Payload] = self._get(key, now)
                if payload is not None:
                    res[key] = payload.value
        return res

    def incr(self, key: Any, delta: Number = 1) -> Number:
        """ Increases the value by delta (default 1) """
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] = self._get(key, now)
            if payload is None:
                raise KeyError(f'key {key!r} not found in cache')
            
            if not isinstance(payload.value, (int, float)) or not isinstance(delta, (int, float)):
                raise TypeError(
                    'unsupported operand type(s) for +/-: '
                    f'{type(payload.value)!r} and {type(delta)!r}'
                )
            payload.value += delta
        self._store.move_to_end(key)
        return payload.value

    def decr(self, key: Any, delta: Number = 1) -> Number:
        return self.incr(key, -delta)

    def clear(self) -> bool:
        with self._lock:
            self._store.clear()
        return True

    def ttl(self, key: Any) -> Time:
        """ returns the key time to live
        
        Returns:
            -1   : the key has been expired or not existed
            None : never expired
            float: life seconds
        """
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] = self._get(key, now)
        if payload is None:
            return -1
        return None if payload.expire is None else payload.expire - now

    def delete(self, key: Any) -> bool:
        with self._lock:
            return self._delete(key)

    def inspect(self, key: Any) -> Optional[Dict[str, Any]]:
        """ inspect the key in cache informations, 
        returns the dict if the key is existed else None
        """
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] = self._get(key, now)
        if payload is None:
            return None
        ins: Dict[str, Any] = vars(payload)
        ins['key'] = key
        ins['ttl'] = None if payload.expire is None else payload.expire - now
        return ins

    def pop(self, key: Any, default: Any = None) -> Any:
        now: Time = current()
        with self._lock:
            payload: Optional[Payload] =self._get(key, now)
            if payload is None:
                return default
            self._delete(key)
        return payload.value

    def exists(self, key: Any) -> bool:
        """ Return True if the key in cache else False. """
        now: Time = current()
        with self._lock:
            return self._get(key, now, update_access=False) is not None

    def touch(self, key: Any, timeout: Time = None) -> bool:
        """ Renew the key. When the key does not exist, false will be returned """

        now: Time = current()
        with self._lock:
            payload: Optional[Payload] =self._get(key, now)
            if payload is None:
                return False
            payload.expire = get_expire(timeout, now)
        return True

    def keys(self) -> Iterable[Any]:
        now: Time = current()
        for key, paylaod in self._store.items():
            if not paylaod.expired(now):
                yield key

    def values(self) -> Iterable[Any]:
        now: Time = current()
        for payload in self._store.values():
            if not payload.expired(now):
                yield payload.value

    def items(self) -> Iterable[Tuple[Any, ...]]:
        now: Time = current()
        for key, payload in self._store.items():
            if not payload.expired(now):
                yield key, payload.value

    def _evict(self, now: Time) -> None:
        
        now = now or current()
        self._clean(now)
        
        if self.max_size is None:
            return None
        
        evict_numebr: int = max(1, len(self) // 100)
        evicted_keys = []

        if self.evict == 'default':
            result = sorted(((key, payload.expire) for key, payload in self._store.items()), key=lambda x: x[1])
            evicted_keys = (item[0] for item in result[:evict_numebr])
        
        if self.evict == 'fifo':    
            evicted_keys = (key for key in list(self._store.keys())[:evict_numebr])
        
        if self.evict == 'lru':
            result = sorted(((key, payload.access) for key, payload in self._store.items()), key=lambda x: x[1])
            evicted_keys = (item[0] for item in result[:evict_numebr])

        if self.evict == 'lfu':
            result = sorted(((key, payload.access_count) for key, payload in self._store.items()), key=lambda x: x[1])
            evicted_keys = (item[0] for item in result[:evict_numebr])
        
        for key in evicted_keys:
            self._delete(key)

    def _get(self, key: Any, now: Time, update_access: bool = True) -> Optional[Payload]:
        payload: Optional[Payload] = self._store.get(key)
        if payload is not None:
            if payload.expired(now):
                self._delete(key)
            else:
                if update_access:
                    payload.access_count += 1
                    payload.access = now
                return payload
        return None
    
    def _delete(self, key: Any) -> bool:
        try:
            del self._store[key]
            return True
        except KeyError:
            return False

    def _clean(self, now: Time = None) -> None:
        """ 删除当前缓存中所有过期的key """
        now: Time = now or current()
        for key in list(self._store.keys()):
            payload: Optional[Payload] = self._store.get(key)
            if payload is not None:
                if payload.expired(now):
                    self._delete(key)

    def __len__(self) -> int:
        """ _summary_
        Warning:
            当前返回的长度是包含了过期的key的, 获取不含过期key长度的方法是主动调用
            clean 方法来删除过期的key
        Returns:
            int: count of item in current cache 
        """
        return len(self._store)
    
    def __repr__(self) -> str:
        return f'<MiniCache length:{len(self)}>'

    __iter__ = keys
    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set
    __contains__ = has_key = exists
    memoize = memoize


LazyMiniCache = lazy(MiniCache)


class Cache(MultiCache):

    def __init__(self, name: str = DEFAULT_NAME, max_size: Optional[int] = None, thread_safe: bool = True) -> None:
        """_summary_

        Args:
            name (str, optional): _description_. Defaults to DEFAULT_NAME.
            max_size (Optional[int], optional): _description_. Defaults to None.
            thread_safe (bool, optional): _description_. Defaults to True.
        """
        self._name: str = name
        self._max_size: Optional[int] = max_size
        self._thread_safe: bool = thread_safe
        self._mtx: Lock = Lock()
        self._recipes: Dict[TG, MiniCache] = {}

    def get_recipe(self, tag: TG) -> Type:
        """_summary_

        Args:
            tag (TG): _description_

        Returns:
            MiniCache: _description_
        """
        # 尽可能的减少竞争
        try:
            return self._recipes[tag]
        except KeyError:
            with self._mtx:
                if tag in self._recipes:
                    return self._recipes[tag]
                recipe = self._create_recipe(tag)
                self._recipes[tag] = recipe
            return recipe
    
    def _create_recipe(self, tag: TG) -> MiniCache:
        name: str = f'{self._name}:{tag}'
        return MiniCache(name=name, max_size=self._max_size, thread_safe=self._thread_safe)
    
    def drop(self, tag: TG) -> bool:
        with self._mtx:
            try:
                del self._recipes[tag]
            except KeyError:
                ...
        return True

    def clear(self) -> bool:
        with self._mtx:
            del self._recipes
            self._recipes = {}
        return True

    def __len__(self) -> int:
        return sum(len(recipe) for recipe in self._recipes.values())
    
    def __repr__(self) -> str:
        return f'<Cache recepies:{self._recipes}>'


LazyCache = lazy(Cache)
