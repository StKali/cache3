#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/7
# Author: clarkmonkey@163.com

from typing import Optional
import pytest
from cache3.base import InvalidCacheKey
from cache3 import SimpleCache, BaseCache


class FooCache(BaseCache):

    def make_key(self, key: str, tag: Optional[str]) -> str:
        return key


def test_invalid_key():
    cache = FooCache()

    with pytest.raises(NotImplementedError):
        cache.clear()
    with pytest.raises(NotImplementedError):
        cache.decr('x')
    with pytest.raises(NotImplementedError):
        cache.ex_set('x', 'x')
    with pytest.raises(NotImplementedError):
        cache.get('')
    with pytest.raises(NotImplementedError):
        cache.set('', '')
    with pytest.raises(NotImplementedError):
        cache.touch('', 1)
    with pytest.raises(NotImplementedError):
        cache.delete('')
    with pytest.raises(NotImplementedError):
        cache.inspect('')

    with pytest.warns(RuntimeWarning):
        cache.evictor()


class UserCache(SimpleCache):

    def make_key(self, key: str, tag: Optional[str]) -> str:
        return key

    lru_evict = None


class TestCacheKeyError:

    def setup(self):
        self.cache = UserCache()

    def test_type_error(self):
        with pytest.raises(InvalidCacheKey):
            self.cache = UserCache()
            self.cache[1] = 1

    def test_error_evict(self):
        self.cache.evictor()

