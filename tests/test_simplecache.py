#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/12
# Author: clarkmonkey@163.com

from typing import List, Tuple, Any

import pytest

from cache3 import SimpleCache

many_pair: List[Tuple[Any, ...]] = [
    ('object', object(), 'tag3'),    # instance value
    ('type', int, 'tag3'),    # type value
    ('callable', print, 'tag3'),    # callable value
    ('list', list((1, 2, 3)), 'tag3'),    # list value
    ('generator', range(10), 'tag3'),    # list value
]
params = pytest.mark.parametrize

cache = SimpleCache()


def setup_function():
    cache.clear()


# api >>> clear
def test_clear(data: List[Tuple[Any, ...]] = many_pair):
    for key, value, tag in data[::-1]:
        cache.set(key=key, value=value, tag=tag)
    assert len(cache._cache) != 0
    cache.clear()
    assert len(cache._cache) == 0


# api >>> ex_set
@params('key, value, tag', many_pair)
def test_ex_set(key, value, tag):

    assert cache.ex_set(key, value, tag=tag)
    assert not cache.ex_set(key, value, tag=tag)
    assert cache.delete(key, tag=tag)
    assert cache.ex_set(key, value, tag=tag)


# api >>> get
@params('key, value, tag', many_pair)
def test_get(key, value, tag):

    assert cache.set(key, value, tag=tag)
    assert cache.get(key, tag=tag)
    cache.delete(key, tag=tag)
    assert not cache.get(key)


# api >>> lru_evict TODO


if __name__ == '__main__':
    pytest.main(["-s", __file__])
