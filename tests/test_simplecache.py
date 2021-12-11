#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/12
# Author: clarkmonkey@163.com

from typing import NoReturn, Optional
from time import time as current

from cache3 import SimpleCache


class TestBaseMethod:

    def setup_class(self) -> NoReturn:
        self.cache = SimpleCache('test1', 60)

    def test_set_and_set(self) -> NoReturn:
        self.cache.set('name', 'venus')
        assert self.cache.get('name') == 'venus'

    def get_expire(self, key: str) -> Optional[float]:
        info = self.cache.inspect(key)
        if info and 'expire' in info:
            return info['expire']

    def test_touch(self) -> NoReturn:
        self.cache.set('name', 'venus', 10)
        self.cache.touch('name', 20)
        expire = self.get_expire('name')
        assert current() + expire - 20 + 0.1 > current()

    def test_ex_set(self) -> NoReturn:
        assert self.cache.ex_set('key', 'v')
        assert not self.cache.ex_set('key', 'v')


