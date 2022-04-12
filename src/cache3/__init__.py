#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

"""
Cache3 is a MIT licensed  safe and lightweight cache library, written in pure-Python.
"""

from typing import List

from cache3.base import BaseCache, JSONMixin, PickleMixin
from cache3.disk import SimpleDiskCache, DiskCache, JsonDiskCache
from cache3.memory import SimpleCache, SafeCache
from cache3.setting import PACKAGE_NAME, PROJECT, VERSION

__author__: str = 'clarkmonkey@163.com'
__name__: str = PACKAGE_NAME
__project_name__: str = PROJECT
__version__: str = '.'.join(map(str, VERSION))

__all__: List[str] = [
    'BaseCache',
    'SimpleCache', 'SafeCache',
    'SimpleDiskCache', 'DiskCache', 'JsonDiskCache',
    'PickleMixin', 'JSONMixin',
]

