#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/3
# Author: clarkmonkey@163.com

from typing import List

import pytest
from cache3 import BaseCache, SimpleCache, SafeCache, DiskCache, SimpleDiskCache, JsonDiskCache

MAX_SIZE_CASES: List[int] = [
    20, 30, 40, 50, 60
]


class BaseCase:

    klass = BaseCache

    @pytest.fixture()
    def cache(self, request):
        max_size = request.param
        instance = self.klass(max_size=max_size)
        yield instance
        instance.clear()

    @pytest.fixture()
    def zero_cache(self, request):
        max_size = request.param
        instance = self.klass(max_size=max_size)
        instance._cull_size = 0
        yield instance
        instance.clear()

    @pytest.mark.parametrize('cache', MAX_SIZE_CASES, indirect=True)
    def test_cull(self, cache: BaseCache):

        for i in range(cache.max_size + 2):
            cache[str(i)] = i

    @pytest.mark.parametrize('zero_cache', MAX_SIZE_CASES, indirect=True)
    def test_zero_cull(self, zero_cache: BaseCache):

        for i in range(zero_cache.max_size + 2):
            zero_cache[str(i)] = i

        assert len(zero_cache) <= zero_cache.max_size


class TestSimpleCache(BaseCase):

    klass = SimpleCache


class TestSafeCache(BaseCache):

    CLASS = SafeCache


class TestSimpleDiskCacheEvict(BaseCase):

    klass = SimpleDiskCache


class TestDiskCacheEvict(BaseCase):

    klass = DiskCache


class TestJsonDiskCacheEvict(BaseCase):

    klass = JsonDiskCache


if __name__ == '__main__':
    pytest.main(['-s', __file__])

