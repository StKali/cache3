#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/5
import time

from cache3 import DiskCache, MiniCache, Cache


# cache = DiskCache(max_size=10, evict_size=4, evict_policy='fifo')
# cache.clear()

# for i in range(12):
#     print('set', i)
#     assert cache.set('key%s' % i, 'value%d' % i)

# cache.flush_length(time.time())
# print(len(cache))
# print(list(cache.iter()))


cache = Cache('MiniCache')

for i in range(10):
    cache.set('name%d' % i, 'value %s' % i)

for i in range(10):
    cache.set('1-name%d' % i, '1-value %s' % i, tag='xxx')


print(len(cache))
for  i in cache.items():
    print(i)


print(len(cache))
