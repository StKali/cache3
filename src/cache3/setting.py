#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/24
# Author: clarkmonkey@163.com

from typing import Tuple, Type

####################
#    INFORMATION
####################
PROGRAM: str = 'Cache3'
VERSION: Tuple[int, int, int] = (0, 1, 0)

####################
#    DEFAULT SETTING
####################
DEFAULT_SQLITE_TIMEOUT: int = 30  # 30s
DEFAULT_TIMEOUT: float = 300.0  # 300s
DEFAULT_MAX_SIZE: int = 1 << 24  # 16M
DEFAULT_CULL_SIZE: int = 10  # 10 elements
DEFAULT_TAG: str = "default"
DEFAULT_NAME: str = 'default.cache3'

####################
#    LIMIT SETTING
####################
MAX_KEY_LENGTH: int = 1 << 10    # 1K
MIN_KEY_LENGTH: int = 1
MAX_TIMEOUT: int = 365 * 24 * 60 * 60
MIN_TIMEOUT: int = 0

EVICT: Type = str
EVICT_LRU: EVICT = 'lru_evict'
EVICT_FIFO: EVICT = 'fifo_evict'
EVICT_LFU: EVICT = 'lfu_evict'

DEFAULT_EVICT: EVICT = EVICT_LRU
