#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

from typing import List
from cache3.base import BaseCache
from cache3.memory import SimpleCache, SafeCache
from cache3.setting import PROGRAM, VERSION

__author__: str = 'clarkmonkey@163.com'
__name__: str = PROGRAM
__version__: str = '.'.join(map(str, VERSION))
__doc__: str = "A safe and light Python cache library."

__all__: List[str] = [
    'BaseCache', 'SimpleCache', 'SafeCache'
]

