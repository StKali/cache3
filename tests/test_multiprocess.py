#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/23
# Author: clarkmonkey@163.com
import random
import multiprocessing

import pytest

from cache3 import SafeCache, DiskCache
# from diskcache import Cache as DiskCache


def generator():
    for _ in range(random.randint(4, 8)):
        threads: int = random.randint(8, 32)
        yield threads, [random.randint(100, 1000) for _ in range(threads)]


multi_cases = list(generator())


class BaseThreads:

    CLASS = SafeCache

    def setup(self):
        self.cache = self.CLASS()
        self.cache['count'] = 0

    @pytest.mark.parametrize('threads, args', multi_cases)
    def test_set(self, threads: int, args):
        ts = [multiprocessing.Process(target=self.add, args=(args[i],)) for i in range(threads)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert self.cache['count'] == sum(args)

    def add(self, count: int):
        for i in range(count):
            self.cache.incr('count')


class TestDiskCache(BaseThreads):

    CLASS = DiskCache

