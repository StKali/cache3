#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/6
# Author: clarkmonkey@163.com

from typing import Any, List, Tuple, Type, Optional

import pytest

from cache3.base import CacheKeyWarning
from cache3 import BaseCache, SimpleCache, SafeCache, DiskCache
from cache3.utils import empty
from cache3.setting import MAX_KEY_LENGTH
from time import time as current

parametrize = pytest.mark.parametrize

consistency_cases: List[Tuple[Any, ...]] = [
    (1, 1, b'1', 2),
    (b'2', 1, '2', 2),
    (b'', 1, '', 2)
]

general_cases: List = [
    ('name', 'clarkmonkey'),
    (b'gender', 'male'),
    (1, None),
]

expire_cases: List = [
    ('name1', 'clarkmonkey', 1),
    (11, 1, 1),
    (112, None, 1),
    (None, None, 0.1)
]

incr_cases: List = [
    ('count1', 1, 1),
    ('count2', -1, 1),
    ('count3', 0, 1 << 30),
]

ex_set_cases: List = [
    ('key1', 'value1', 'value2'),
    (123, 456, 789),
    (b'123', None, ''),
    (b'', 45, ''),
]


class GeneralCase:

    CLASS: Optional[Type] = None

    def setup_class(self):
        self.cache: BaseCache = self.CLASS()

    # __delitem__, __setitem__, __getitem__
    @parametrize('key, value', general_cases)
    def test_logic_item_methods(self, key, value):
        self.cache[key] = value
        assert self.cache[key] == value
        del self.cache[key]
        assert not self.cache.has_key(key)

    # __init__
    def test_construct_method(self):
        cache = self.CLASS(name='simple_cache', timeout=20, max_size=1 << 10)
        start = current()
        cache.set('key', 'value')
        expire: float = self.get_expire('key')
        assert expire > start + 20

    @parametrize('k1, v1, k2, v2', consistency_cases)
    def test_consistency(self, k1, v1, k2, v2):
        self.cache[k1] = v1
        self.cache[k2] = v2
        assert self.cache[k1] == v1 and self.cache[k2] == v2

    @parametrize('key, value', general_cases)
    def test_delete_has_key(self, key, value):

        self.cache[key] = value
        assert self.cache.has_key(key)
        del self.cache[key]
        assert self.cache.get(key, empty) is empty
        assert not self.cache.has_key(key)

    def test_clear(self):
        for key, value in general_cases:
            self.cache[key] = value
        self.cache.clear()
        for key, _ in general_cases:
            assert not self.cache.has_key(key)

    @parametrize('key, value, timeout', expire_cases)
    def test_timeout_touch(self, key, value, timeout):
        self.cache.set(key, value, timeout=timeout)
        expire = self.get_expire(key)
        self.cache.touch(key, 100)
        new_expire = self.get_expire(key)
        assert new_expire - expire > 100 - timeout - .2
        assert self.cache.touch(key, -1)
        assert not self.cache.touch(key, 1)

    @parametrize('key, value, delta', incr_cases)
    def test_incr(self, key, value, delta):
        self.cache[key] = value
        res = value + delta
        assert res == self.cache.incr(key, delta)
        assert res == self.cache[key]

    @parametrize('key, value, delta', incr_cases)
    def test_decr(self, key, value, delta):
        self.cache[key] = value
        res = value - delta
        assert res == self.cache.decr(key, delta)
        assert res == self.cache[key]

    @parametrize('key, value1, value2', ex_set_cases)
    def test_ex_set(self, key, value1, value2):
        self.cache.set(key, value1)
        assert not self.cache.ex_set(key, value2)
        assert self.cache[key] == value1
        del self.cache[key]
        assert self.cache.ex_set(key, value2)
        assert self.cache[key] == value2

    def test_get_many(self):
        self.cache['a'] = 1
        self.cache['b'] = 2
        self.cache['c'] = 3
        returns = self.cache.get_many(['a', 'b', 'c'])
        assert len(returns) == 3
        assert returns['a'] == 1
        assert returns['b'] == 2
        assert returns['c'] == 3

    def test_memoize_error(self):

        with pytest.raises(TypeError) as exc:
            @self.cache.memoize
            def func():...
            assert str(exc).startswith('Name cannot be callable')

    def test_incr_expire(self):
        key, value = 'key', 'value'
        self.cache[key] = value
        assert self.cache.touch(key, -1)
        with pytest.raises(ValueError) as exc:
            self.cache.incr(key, 100)
        assert exc.value.args[0].startswith('Key')

    def test_memoize(self):

        @self.cache.memoize()
        def add(count):
            count += 1
            return count

        assert add(0) == add(234) == add(567) == add(8910) == 1

    def test_big_key(self):
        key = '1' * (MAX_KEY_LENGTH + 1)
        with pytest.warns(CacheKeyWarning):
            self.cache.set(key, '')

    def get_expire(self, key) -> float:
        inspect = self.cache.inspect(key)
        if not isinstance(inspect, dict):
            print(inspect, type(inspect))
        assert isinstance(inspect, dict)
        return inspect['expire']

    def test_config(self):

        self.cache.config(evict='lru_evict')
        assert self.cache.evictor == self.cache.lru_evict

    def teardown_class(self):
        self.cache.clear()


class TestSimpleCache(GeneralCase):
    CLASS = SimpleCache


class TestSafeCache(GeneralCase):
    CLASS = SafeCache


class TestDiskCache(GeneralCase):
    CLASS = DiskCache


if __name__ == '__main__':
    pytest.main(['-s'])