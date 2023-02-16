#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15

import pytest

params = pytest.mark.parametrize
from cache3.utils import empty
from cache3 import MiniCache, Cache, DiskCache

key_types_cases = [
    # string
    ('key', 'value'),

    # float
    (3.3, 'float1'),
    (.3, 'float2'),
    (3., 'float'),

    # integer
    (3, 'integer'),

    # empty
    (empty, 'empty'),

    # bool
    (True, 'bool-true'),
    (1, 'bool-true-mess-1'),
    ('true', 'bool-true-mess-2'),
    (False, 'bool-false'),
    (0, 'bool-false-mess-1'),
    ('false', 'bool-false-mess-2'),
]

class TestGeneralCacheApi:

    def setup(self):
        self.caches = [
            Cache('memory_cache'),
            DiskCache('disk_cache'),
        ]

    @params('key, value', key_types_cases)
    def test_set_get(self, key, value):
        for cache in self.caches:
            cache.set(key, value)
            assert cache.pop(key) == value
            cache.set(value, key)
            assert cache.pop(value) == key

    def test_set_get_tag(self):
        for cache in self.caches:
            cache.set('name', 'value', tag='1')
            assert cache.get('name', empty) == empty
            assert cache.get('name', tag='1') == 'value'

    def test_clear(self):
        for cache in self.caches:
            cache.clear()
            cache.set('k', 'v')
            assert len(cache) == 1
            assert cache.clear()
            assert len(cache) == 0
    def test_ex_set(self):
        for cache in self.caches:
            assert cache.ex_set('name', None)
            assert not cache.ex_set('name', None)
