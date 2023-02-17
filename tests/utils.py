#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15

import random
from string import printable
from typing import Iterable

def rand_string(_min: int, _max: int) -> str:
    return ''.join(
        random.choice(printable) for _ in 
        range(random.randint(_min, _max))
    )


def rand_strings(count: int, _min: int = 4, _max: int = 16) -> Iterable[str]:

    for _ in range(count):
        yield rand_string(_min, _max)
