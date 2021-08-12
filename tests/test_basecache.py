#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/7
# Author: clarkmonkey@163.com

from typing import *

import pytest

from cache3.cache import BaseCache


def test_invalid_key() -> NoReturn:
    cache = BaseCache()

    with pytest.raises(NotImplementedError):
        cache.clear()
    with pytest.raises(NotImplementedError):
        cache.ex_set('x', 'x')
    with pytest.raises(NotImplementedError):
        cache.get('')
    with pytest.raises(NotImplementedError):
        cache.set('', '')
    with pytest.raises(NotImplementedError):
        cache.touch('', 1)
    with pytest.raises(NotImplementedError):
        cache.delete('')
