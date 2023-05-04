#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15
# author: clarkmonkey@163.com

import time
import pytest
from cache3 import MiniCache, Cache
from utils import rand_string, rand_strings


raises = pytest.raises


class MemoizeMixin:

    CacheClass = MiniCache

    def setup_class(self):
        self.cache = self.CacheClass(rand_string())

    def setup_method(self):
        self.cache.clear()

    def test_inspect(self):
        key, value = 'name', 'value'
        # not existed key
        assert len(self.cache) == 0
        assert key not in self.cache
        assert self.cache.inspect(key) is None
        
        # existed key
        self.cache[key] = value
        inspect = self.cache.inspect(key)
        assert isinstance(inspect, dict)
        assert inspect['key'] == key
        assert inspect['value'] == value
        assert inspect['expire'] is None
        assert inspect['ttl'] is None

        # reset key with timeout parameter
        timeout = 10.
        self.cache.set(key, value, timeout)
        assert isinstance(inspect, dict)
        inspect = self.cache.inspect(key)
        assert inspect['key'] == key
        assert inspect['value'] == value
        assert inspect['expire'] > time.time() - 1
        assert inspect['ttl'] > timeout - 0.1

    def test_keys(self):
        assert list(self.cache.keys()) == []
        keys = set(rand_strings(10))
        for key in keys:
            self.cache[key] = key[::-1]
        assert set(self.cache.keys()) == keys


class TestMiniCache(MemoizeMixin):
    
    CacheClass = MiniCache

    def test_str(self):
        assert str(self.cache) == '<MiniCache length:0>'
        self.cache['name'] = None
        assert str(self.cache) == '<MiniCache length:1>'

class TestCache(MemoizeMixin):
    
    CacheClass = Cache

    def test_str(self):
        assert str(self.cache) == '<Cache buckets:0>'
        self.cache['name'] = None
        assert str(self.cache) == '<Cache buckets:1>'

    