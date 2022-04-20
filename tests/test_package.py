#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/28
# Author: clarkmonkey@163.com

import pytest
from typing import NoReturn


def test_package() -> NoReturn:

    import cache3
    assert cache3.__doc__ == """
Cache3 is a MIT licensed  safe and lightweight cache library, written in pure-Python.
"""
    assert cache3.__author__ == "St. Kali"
    assert cache3.__name__ == "cache3"


if __name__ == '__main__':
    pytest.main(["-s", __file__])
