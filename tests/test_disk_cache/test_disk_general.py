#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2022/1/19
# Author: clarkmonkey@163.com

import os
import sqlite3.dbapi2 as sqlite3
import threading
import time
from typing import NoReturn

import pytest

from cache3 import SimpleDiskCache
from cache3.disk import SessionDescriptor


# api >>> SessionDescriptor.__set__
def test__set__() -> NoReturn:

    cache = SimpleDiskCache()
    assert isinstance(cache.session, sqlite3.Connection)
    with pytest.raises(ValueError, match='Expected .* to be an sqlite3.Connection.'):
        cache.session = 1

    cache.session = sqlite3.connect('test.sqlite')
    os.remove('test.sqlite')


# api >>> SessionDescriptor.__delete__, _close
def test__close__():

    cache = SimpleDiskCache()
    origin: sqlite3.Connection = cache.session
    assert isinstance(cache.session, sqlite3.Connection)
    del cache.session
    assert origin != cache.session


# api >>>
def test_init():

    file = 'test1.sqlite'

    def h(file):
        cache = SimpleDiskCache(name=file, configure={'timeout': 0.01})
        with cache._transact() as sqlite:
            sqlite(
                'CREATE TABLE test(id integer)'
            )
            time.sleep(1)
            sqlite(
                'DROP TABLE test'
            )

    def f(file):
        cache = SimpleDiskCache(name=file, configure={'timeout': 0.01})
        cache.set('name', 'monkey')

    threading.Thread(target=h, args=(file, )).start()
    threading.Thread(target=f, args=(file, )).start()


if __name__ == '__main__':
    pytest.main(["-s", __file__])
