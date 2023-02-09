#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/1/24
import sys
import time
from pathlib import Path
from shutil import rmtree
from typing import NoReturn

from cache3 import DiskCache


cached_directory: Path = Path('~/.test')


def clear(path: Path) -> NoReturn:
    pt: Path = path.expanduser().absolute()
    if not pt.exists():
        return
    try:
        rmtree(pt)
    except Exception as exc:
        print('clear path: %r failed. err: %s' % (path, exc))
        sys.exit(0)


# cache = DiskCache(
#     name='test.sqlite3',
#     directory=cached_directory,
#     max_size=100,
#     step_size=10
# )

# cache = MultiDiskCache()
# print(cache)
# for i in range(200):
#     cache.set('%s name' % i, 'monkey', 100, 'tag1')
# print(cache.get('name', tag='tag1'))


# for i in range(10):
#     print(cache.set('key-%d' % i, i))
#
# print(list(cache.iter()))
#
# if cache.ex_set('object', object(), tag='tag3'):
#     print(cache.inspect('object', tag='tag3'))
#     print('set success')
#     cache.ex_set('object', 1, tag='tag3')
# else:
#     print('set failed')
#
# print(list(cache))
# print(list(cache.iter(None)))
# clear(cached_directory)
# from cache3 import SafeCache
#
#
# cache = SafeCache()
# def get_expire(cache, key, tag: str = None) -> ...:
#
#     info = cache.inspect(key, tag)
#     if info:
#         return info['expire']
# #
# print(cache.ex_set('name1', 'value1'))
# print(cache.ex_set('name2', 'value2'))
# print(cache.ex_set('name3', 'value3'))
# print(cache.get_many(['name1', 'name2', 'name3']))
# print(cache.inspect('name'))
# print(cache.touch('name', 100))
# print(cache.inspect('name'))
# print(cache.ttl('name'))



# print(cache.ex_set('name', 'value1'))
# print(cache.get('name'))
# from cache3 import SimpleCache
# cache = SimpleCache()
# cache.ex_set('name', 'monkey', timeout=None)

# cache.set(key='name', value='1', timeout=None)
# print(cache.has_key('name'))
# print(cache.incr('name'))
# cache.incr('name')


from cache3 import SimpleDiskCache


cache = DiskCache()

for i in cache:
    print(i)

