#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/16


from cache3 import Cache, DiskCache
from cache3.utils import empty

if __name__ == '__main__':
    cache = DiskCache('test')
    cache.clear()
    cache.set('key1', '1', timeout=100)
    cache.set('key2', '1')
    print(cache.ttl('key1'))
    print(cache.ttl('key2'))

