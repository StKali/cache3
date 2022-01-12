#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/3
# Author: clarkmonkey@163.com

from typing import List

import pytest
from cache3 import BaseCache, SimpleCache, SafeCache, DiskCache


MAX_SIZE_CASES: List[int] = [
    20, 30, 40, 50, 60
]

import logging
class BaseCase:

    CLASS = BaseCache

    @pytest.fixture()
    def cache(self, request):
        max_size = request.param
        instance = self.CLASS(max_size=max_size)
        try:
            yield instance
        except Exception as exc:
            logging.exception(exc)
        instance.clear()
        if isinstance(instance, DiskCache):
            instance.sqlite.destroy()

    @pytest.fixture()
    def zero_cache(self, request):
        max_size = request.param
        instance = self.CLASS(max_size=max_size)
        instance._cull_size = 0
        yield instance
        instance.clear()
        if isinstance(instance, DiskCache):
            instance.sqlite.destroy()

    @pytest.mark.parametrize('cache', MAX_SIZE_CASES, indirect=True)
    def test_cull(self, cache: BaseCache):

        for i in range(cache._max_size + 2):
            cache[str(i)] = i

    @pytest.mark.parametrize('zero_cache', MAX_SIZE_CASES, indirect=True)
    def test_zero_cull(self, zero_cache: BaseCache):

        for i in range(zero_cache._max_size + 2):
            zero_cache[str(i)] = i

        assert len(zero_cache._cache) <= zero_cache._max_size


class TestSimpleCache(BaseCase):

    CLASS = SimpleCache


class TestSafeCache(BaseCache):

    CLASS = SafeCache


# class TestDiskCache(DiskCache):
#
#     CLASS = DiskCache()


def test_lru_evict():
    """"""


def test_fifo_evict():
    """"""


def test_lfu_evict():
    """"""

