#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2022/1/15
# Author: clarkmonkey@163.com
import threading

import pytest

from cache3 import SafeCache


count = 1 << 16   # 65,536
threads = 1 << 4
key = 'count'


def task(cache: SafeCache):

    for i in range(count):
        cache.incr(key)


def test_thread_safe():

    cache = SafeCache(timeout=1000)
    cache.set(key, 0)

    ts = [threading.Thread(target=task, args=(cache, )) for _ in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]

    assert cache.get(key) == count * threads


if __name__ == '__main__':
    pytest.main(['-s', __file__])
