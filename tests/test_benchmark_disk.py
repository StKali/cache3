#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/1
# Author: clarkmonkey@163.com

from typing import *


import random
from string import printable
from typing import Generator

from cache3 import DiskCache


def rand_string(_min, _max) -> str:
    return ''.join(random.choice(printable) for _ in range(random.randint(_min, _max)))


def rand_strings(count: int, _min: int = 4, _max: int = 16) -> Generator:

    for _ in range(count):
        yield rand_string(_min, _max)


cache = DiskCache()

def simple_disk_set(cache: DiskCache, key, value):
    cache.set(key, value)


def simple_disk_get(cache: DiskCache, key, value):
    cache.get(key, value)


def simple_disk_ex_set(cache: DiskCache, key, value):
    cache.ex_set(key, value)


def test_disk_set(benchmark):
    benchmark.pedantic(simple_disk_set, args=(cache, 'key', 'value'), rounds=10000)


def test_disk_get(benchmark):
    benchmark.pedantic(simple_disk_get, args=(cache, rand_string(2, 16), 'value'), rounds=10000)


def test_disk_ex_set(benchmark):
    benchmark.pedantic(simple_disk_ex_set, args=(cache, rand_string(2, 16), 'value'), rounds=10000)
