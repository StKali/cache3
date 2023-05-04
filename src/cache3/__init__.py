#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/7/24
# author: clarkmonkey@163.com

"""
Cache3 is a MIT licensed  safe and lightweight cache library, written in pure-Python.
"""

from cache3.util import lazy, LazyObject
from cache3.disk import DiskCache, LazyDiskCache
from cache3.memory import Cache, MiniCache, LazyCache

__author__: str = 'St·Kali <clarkmonkey@163.com>'
__name__: str = 'cache3'  # pylint: disable=redefined-builtin
__email__: str = 'clarkmonkey@163.com'
__version__: str = '0.4.3'

__all__: list = [
    'DiskCache', 'LazyDiskCache',
    'Cache', 'MiniCache', 'LazyCache',
    'LazyObject', 'lazy',
]
