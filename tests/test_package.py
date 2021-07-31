#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/7/28
# Author: clarkmonkey@163.com

import pytest
from typing import *


def test_package() -> NoReturn:

    import cache3
    assert cache3.__doc__ == "A safe and light Python cache library."


if __name__ == '__main__':
    pytest.main()