#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/11/13
# Author: clarkmonkey@163.com

from typing import List

import pytest
from cache3 import SQLite
from sqlite3 import Connection

file_db: str = '.test.cache3'
memory_db: str = ':memory:'

db_cases: List = [
    memory_db, file_db
]


class TestSQLite:

    def test_constructor(self):
        with pytest.warns(RuntimeWarning):
            SQLite(':memory:', check_same_thread=True)

        with pytest.warns(UserWarning):
            SQLite(':memory:', isolation_level='')

    @pytest.mark.parametrize('db', db_cases)
    def test_close_no_session(self, db: str):
        sqlite: SQLite = SQLite(':memory:')
        assert sqlite.close()

    def test_close_memory_session(self):
        sqlite: SQLite = SQLite(memory_db)
        assert isinstance(sqlite.session, Connection)
        assert sqlite.close(True)

    @pytest.mark.parametrize('db', db_cases)
    def test_destroy(self, db: str):
        sqlite: SQLite = SQLite(db)
        assert isinstance(sqlite.session, Connection)

    def test_close_session(self):
        sqlite: SQLite = SQLite(file_db)
        assert isinstance(sqlite.session, Connection)

    def test_destroy_memory_session(self):
        sqlite: SQLite = SQLite(memory_db)
        assert isinstance(sqlite.session, Connection)
        with pytest.warns(RuntimeWarning):
            assert not sqlite.close()
