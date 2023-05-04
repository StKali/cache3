#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15
# author: clarkmonkey@163.com

import random
from string import ascii_letters, digits
from typing import Iterable

chars = ascii_letters + digits


def rand_string(_min: int = 4, _max: int = 10) -> str:
    return ''.join(
        random.choice(chars) for _ in 
        range(random.randint(_min, _max))
    )


def rand_strings(count: int, _min: int = 4, _max: int = 16) -> Iterable[str]:

    for _ in range(count):
        yield rand_string(_min, _max)
