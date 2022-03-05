#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2022/1/15
# Author: clarkmonkey@163.com

from typing import List, Tuple, Any

import pytest

from cache3 import SimpleCache, SafeCache


many_pair: List[Tuple[Any, ...]] = [
    ('object', object(), 'tag3'),    # instance value
    ('type', int, 'tag3'),    # type value
    ('callable', print, 'tag3'),    # callable value
    ('list', list((1, 2, 3)), 'tag3'),    # list value
    ('generator', range(10), 'tag3'),    # list value
]
params = pytest.mark.parametrize

cache = SimpleCache()


class BaseApi:

    klass = SimpleCache

    def setup_class(self):
        self.cache = self.klass()

    def setup_function(self):
        self.cache.clear()

    # api >>> clear
    def test_clear(self, data: List[Tuple[Any, ...]] = many_pair):
        for key, value, tag in data[::-1]:
            self.cache.set(key=key, value=value, tag=tag)
        assert len(cache._cache) != 0
        self.cache.clear()
        assert len(cache._cache) == 0

    # api >>> ex_set
    @params('key, value, tag', many_pair)
    def test_ex_set(self, key, value, tag):

        assert self.cache.ex_set(key, value, tag=tag)
        assert not self.cache.ex_set(key, value, tag=tag)
        assert self.cache.delete(key, tag=tag)
        assert self.cache.ex_set(key, value, tag=tag)

    # api >>> get
    @params('key, value, tag', many_pair)
    def test_get(self, key, value, tag):

        assert self.cache.set(key, value, tag=tag)
        assert self.cache.get(key, tag=tag)
        self.cache.delete(key, tag=tag)
        assert not self.cache.get(key)

    # api >>> set
    @params('key, value, tag', many_pair)
    def test_set(self, key, value, tag):
        assert self.cache.set(key, value, tag=tag)


class TestSafeCache(BaseApi):

    klass = SafeCache


class TestSimpleCache(BaseApi):

    klass = SimpleCache


if __name__ == '__main__':
    pytest.main(["-s", __file__])
