#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/13
# Author: clarkmonkey@163.com

import random
import threading

import pytest

from cache3 import SafeCache, DiskCache


def generator():
    for _ in range(random.randint(10, 20)):
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
        ts = [threading.Thread(target=self.add, args=(args[i],)) for i in range(threads)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert self.cache['count'] == sum(args)

    def add(self, count: int):
        for i in range(count):
            self.cache.incr('count')


class TestThreadsSafeCache(BaseThreads):

    CLASS = SafeCache


class TestThreadsDiskCache(BaseThreads):

    CLASS = DiskCache

