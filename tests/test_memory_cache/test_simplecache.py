#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/12
# Author: clarkmonkey@163.com

import threading

import pytest

from cache3 import SimpleCache

count = 1 << 18   # 65,536
threads = 1 << 4
key = 'count'


def task(cache: SimpleCache):

    for i in range(count):
        cache.incr(key)


def test_thread_unsafe():

    cache = SimpleCache(timeout=1000)
    cache.set(key, 0)

    ts = [threading.Thread(target=task, args=(cache, )) for _ in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]

    assert cache.get(key) != count * threads


if __name__ == '__main__':
    pytest.main(['-s', __file__])