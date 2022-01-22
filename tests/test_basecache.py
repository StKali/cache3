#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/7
# Author: clarkmonkey@163.com

from types import MethodType
from typing import Callable
import pytest
from cache3 import BaseCache
from cache3.setting import (
    DEFAULT_TIMEOUT, DEFAULT_EVICT, DEFAULT_CULL_SIZE, DEFAULT_NAME, DEFAULT_MAX_SIZE,
)

params: Callable = pytest.mark.parametrize
cache: BaseCache = BaseCache()


def test_success_construct():
    default_cache: BaseCache = BaseCache()
    assert default_cache.name == DEFAULT_NAME
    assert default_cache.timeout == DEFAULT_TIMEOUT
    assert default_cache.max_size == DEFAULT_MAX_SIZE
    assert default_cache.evict == DEFAULT_EVICT
    assert default_cache.cull_size == DEFAULT_CULL_SIZE


# api >>> __init__
@params('evict, cull_size', [
    ('fifo_evict', 1000),
    ('lfu_evict', 20)
])
def test_config(evict: str, cull_size: int) -> None:
    cache.config(evict=evict, cull_size=cull_size)
    assert cache.evict == evict
    assert cache.cull_size == cull_size


# api >>> __iter__, clear, delete, ex_set, get, has_key, incr, inspect,
# set, touch, get_current_size
@params('method', [
    cache.__iter__, cache.clear, cache.delete, cache.ex_set, cache.get,
    cache.has_key, cache.incr, cache.inspect, cache.set, cache.touch,
    cache.get_current_size
])
def test_implemented_method(method) -> None:
    with pytest.raises(NotImplementedError):
        arg_count: int = method.__code__.co_argcount
        if isinstance(method, MethodType):
            arg_count = arg_count - 1

        if arg_count:
            method(*((None,) * arg_count))
        method()


# api >>> __repr__
def test_repr_method():
    assert str(cache).startswith('<BaseCache')


# api >>> store, restore
@params('key, tag', [
    ('name', 'default'),
    ('name:default', '')
])
def test_store_and_reversed(key, tag):
    serial_key: str = cache.store_key(key, tag)
    assert cache.restore_key(serial_key) == [key, tag]


# api: warning >>> evictor
def test_evictor_no_method():
    with pytest.warns(RuntimeWarning, match='Not found .* evict method'):
        cache.evictor


# api: callable >>> evictor
def test_evictor_not_callable():
    cache.config(evict='lru_evict')
    cache.lru_evict = None
    with pytest.warns(RuntimeWarning, match='Invalid evict .*, It must a callable object.'):
        cache.evictor


if __name__ == '__main__':
    pytest.main(['-s', __file__])
