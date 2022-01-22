#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/1
# Author: clarkmonkey@163.com

import random
from string import printable
from typing import Generator

from cache3 import SafeCache


def rand_string(_min, _max) -> str:
    return ''.join(
        random.choice(printable) for _ in
        range(random.randint(_min, _max))
    )


def rand_strings(count: int, _min: int = 4, _max: int = 16) -> Generator:

    for _ in range(count):
        yield rand_string(_min, _max)


cache = SafeCache()

def simple_safe_set(cache: SafeCache, key, value):
    cache.set(key, value)


def simple_safe_get(cache: SafeCache, key, value):
    cache.get(key, value)


def simple_safe_ex_set(cache: SafeCache, key, value):
    cache.ex_set(key, value)


def test_safe_set(benchmark):
    benchmark.pedantic(
        simple_safe_set, args=(cache, 'key', 'value'), rounds=10000
    )


def test_safe_get(benchmark):
    benchmark.pedantic(
        simple_safe_get, args=(cache, rand_string(2, 16), 'value'),
        rounds=10000
    )


def test_safe_ex_set(benchmark):
    benchmark.pedantic(
        simple_safe_ex_set, args=(cache, rand_string(2, 16), 'value'),
        rounds=10000
    )
