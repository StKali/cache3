#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/9/15
# author: clarkmonkey@163.com



from cache3 import MiniDiskCache
from cache3.disk import Counter


cache = MiniDiskCache('mini-disk-test', max_size=10, evict='lru')
cache.clear()
for i in range(16):
    cache.get(3)
    print(f'set {i}')
    cache.set(i, i**2)
print(list(cache.keys()))

