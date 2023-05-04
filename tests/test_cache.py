#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15
# author: clarkmonkey@163.com

from random import randint
from pathlib import Path
from shutil import rmtree
from cache3 import Cache, DiskCache
from utils import rand_string, rand_strings
cases = {
    rand_string(): list(rand_strings(randint(10, 60))) for _ in range(randint(4, 10))
}


class CacheApiMixin:

    def test_items(self):
        
        for tag, keys in cases.items():
            for key in keys:
                self.cache.set(key, key[::-1], tag=tag)
        
        # keys
        for tag in cases:
            for key in self.cache.keys(tag):
                assert key in cases[tag]
        
        # values
        for tag in cases:
            for value in self.cache.values(tag):
                assert value[::-1] in cases[tag]
        
        # items
        for tag in cases:
            for key, value in self.cache.items(tag):
                assert key == value[::-1]


class TestCache(CacheApiMixin):
    """"""
    def setup_class(self):
        self.cache = Cache(rand_string())


class TestDiskCache(CacheApiMixin):
    """"""

    directory = Path('test_directory')

    def setup_class(self):
        if not self.directory.exists():
            self.directory.mkdir(exist_ok=True, parents=True)
        self.cache = DiskCache((self.directory / 'test').as_posix())

    def teardown_class(self):
        self.cache.sqlite.close()
        rmtree(self.directory.as_posix())

    def test_multi_get_expired(self):
        self.cache.clear()
        self.cache.max_size = 2
        self.cache.set(1, 1, timeout=-1)
        self.cache.get(1)
        self.cache.get(1)
        len(self.cache)

