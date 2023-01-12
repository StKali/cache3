#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/1/20
# Author: clarkmonkey@163.com

from typing import List, Tuple, Any
from pathlib import Path

import pytest

from cache3 import DiskCache


many_pair: List[Tuple[Any, ...]] = [
    ('object', object(), 'tag3'),    # instance value
    ('type', int, 'tag3'),    # type value
    ('callable', print, 'tag3'),    # callable value
    ('list', list((1, 2, 3)), 'tag3'),    # list value
    ('generator', range(10), 'tag3'),    # list value
]
params = pytest.mark.parametrize


def test_create_diskcache(tmp_path):
    paths: List[Path] = [
        tmp_path / '1',
        tmp_path / '2' / '22',
        tmp_path / '__' / 'test_dir'
    ]
    for path in paths:
        assert not path.exists()
        DiskCache(path)
        assert path.exists()


class TestDiskCache:

    def setup_class(self):
        self.cache = DiskCache()

    def setup_function(self):
        self.cache.clear()

    # api >>> clear
    def test_clear(self, data: List[Tuple[Any, ...]] = many_pair):
        self.cache.clear()
        (value, ) = self.cache.sqlite(
            'SELECT COUNT(1) FROM `cache`'
        ).fetchone()
        print(value)
        assert value == 0

    # api >>> ex_set
    @params('key, value, tag', many_pair)
    def test_ex_set(self, key, value, tag):

        assert self.cache.ex_set(key, value, tag=tag)
        assert not self.cache.ex_set(key, value, tag=tag)
        assert self.cache.delete(key, tag=tag)
        assert self.cache.ex_set(key, value, tag=tag)

    # api >>> get
    @params('key, value, tag', many_pair)
    def test_get(self, key, value, tag):

        assert self.cache.set(key, value, tag=tag)
        assert self.cache.get(key, tag=tag)
        self.cache.delete(key, tag=tag)
        assert not self.cache.get(key)


if __name__ == '__main__':
    pytest.main(["-s", __file__])