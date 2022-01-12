#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/6
# Author: clarkmonkey@163.com

from typing import List, Tuple, Any, Optional

import pytest

from cache3 import SimpleCache, SafeCache, SimpleDiskCache, DiskCache, JsonDiskCache
from cache3.setting import DEFAULT_TAG


def get_expire(cache, key, tag: str = DEFAULT_TAG) -> Optional:

    info = cache.inspect(key, tag)
    if info:
        return info['expire']


many_pair: List[Tuple[Any, ...]] = [
    ('string', 'hades', 'tag1'),   # normal
    ('id', 111, 'tag3'),    # integer value
    ('bytes', b'111', 'tag3'),    # bytes value
    ('mark:group:1:name', 'venus', 'tag2'),  # `:` gap mark
    ('mark:group:2:name', True, 'tag2'),  # `:` gap mark
]
params = pytest.mark.parametrize


class BaseCacheApi:

    klass = SimpleCache

    def setup_class(self):
        """ construct cache. """
        self.cache = self.klass()

    def setup_method(self):
        """ clear cache """
        self.cache.clear()

    # api >>> __delitem__, __setitem__, __getitem__
    @params('key, value, tag', many_pair)
    def test_item_operators(self, key, value, tag):
        """ tag not used """
        self.cache[key] = value
        assert self.cache[key] == value
        del self.cache[key]
        assert self.cache[key] is None
        assert not self.cache.has_key(key)

    # api >>> __iter__
    def test_iter(self, data: List[Tuple[Any, ...]] = many_pair):
        for key, value, tag in data:
            self.cache.set(key=key, value=value, tag=tag)

        result = []
        for elem in self.cache:
            assert isinstance(elem, tuple)
            result.append(elem)
        assert result == data

    # api >>> clear
    # def test_clear(self, data: List[Tuple[Any, ...]] = many_pair):
    #     for key, value, tag in data[::-1]:
    #         self.cache.set(key=key, value=value, tag=tag)
    #     assert len(self.cache._cache) != 0
    #     self.cache.clear()
    #     assert len(self.cache._cache) == 0


    # api >>> delete
    @params('key, value, tag', many_pair)
    def test_delete(self, key, value, tag):

        assert self.cache.set(key=key, value=value, tag=tag)
        assert self.cache.has_key(key, tag=tag)
        assert self.cache.delete(key, tag=tag)
        assert not self.cache.delete(key)   # return false if no specify tag
        assert not self.cache.has_key(key, tag=tag)


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


    # api >>> has_key
    # Has been covered api


    # api:success >>> incr
    @params('key, value', [
        ('integer', 0),
        ('float', 0.0)
    ])
    def test_incr(self, key, value) -> None:

        self.cache[key] = value
        for i in range(10):
            self.cache.incr(key, delta=1)
            value += 1

        assert self.cache[key] == value

    # api:invalid >>> incr
    @params('key, value', [
        ('string', '0'),
        ('bytes', b'0'),
        # ('object', object()),
        # ('list', [1, 2,3]),
        # ('tuple', (1,)),
        # ('None', None),
    ])
    def test_type_error(self, key, value):
        self.cache[key] = value
        with pytest.raises(TypeError, match=''):
            self.cache.incr(key)

    # api:timeout >>> incr
    def test_timeout_incr(self):
        key: str = 'count'
        self.cache.set(key, 0, timeout=0)
        assert not self.cache.has_key(key)
        with pytest.raises(ValueError):
            self.cache.incr(key)

    # api >>> inspect
    # Has been covered api

    # api >>> touch
    @params('key, value, timeout', [
        ('name1', 'clarkmonkey', 1),
        ('name2', 1, 1),
        ('name3', None, 1),
        # ('name4', 0.1)
    ])
    def test_timeout_touch(self, key, value, timeout):
        self.cache.set(key, value, timeout=timeout)
        expire = get_expire(self.cache, key)
        self.cache.touch(key, 100)
        new_expire = get_expire(self.cache, key)
        assert new_expire - expire > 100 - timeout - .2
        assert self.cache.touch(key, -1)
        assert not self.cache.touch(key, 1)


    # api >>> lru_evict TODO


class TestSimpleCacheApi(BaseCacheApi):

    klass = SimpleCache


class TestSafeCacheApi(BaseCacheApi):

    klass = SafeCache


class TestSimpleDiskCacheApi(BaseCacheApi):

    klass = SimpleDiskCache


class TestDiskCacheApi(BaseCacheApi):

    klass = DiskCache


class TestJsonDiskCacheApi(BaseCacheApi):

    klass = JsonDiskCache


if __name__ == '__main__':
    pytest.main(["-s", __file__])
