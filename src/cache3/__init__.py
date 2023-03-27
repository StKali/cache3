#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

"""
Cache3 is a MIT licensed  safe and lightweight cache library, written in pure-Python.
"""

from cache3.utils import lazy, LazyObject
from cache3.disk import DiskCache, LazyDiskCache
from cache3.memory import Cache, MiniCache, LazyCache

__author__: str = 'St. Kali'
__name__: str = 'cache3'
__email__: str = 'clarkmonkey@163.com'
__version__: str = '0.4.1'

__all__: list = [
    'DiskCache', 'LazyDiskCache',
    'Cache', 'MiniCache', 'LazyCache',
    'LazyObject', 'lazy',
]

