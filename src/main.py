#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/5
import time

from cache3 import DiskCache


cache = DiskCache(max_size=10, evict_size=4, evict_policy='fifo')
cache.clear()

for i in range(12):
    print('set', i)
    assert cache.set('key%s' % i, 'value%d' % i)

cache.flush_length(time.time())
print(len(cache))
print(list(cache.iter()))


