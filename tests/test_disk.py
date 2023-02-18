#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15
import pickle
from pathlib import Path

import pytest
from cache3.disk import SQLiteEntry, PickleStore, empty, BYTES, NUMBER, STRING, RAW, PICKLE
from sqlite3 import Connection
from threading import Thread
from utils import rand_string, rand_strings

raises = pytest.raises


class TestSQLiteEntry:

    def test_instance(self):
        
        # success
        assert SQLiteEntry('', 'test-sqliye0', None, 5)
        
        # invalid pragmas
        with raises(TypeError, match='pragmas want dict object but get .*'):
            SQLiteEntry('', 'test-sqlite1', None, 5, pragmas=1)

    def test_session(self, tmp_path):
        directory = tmp_path / 'test'
        directory.mkdir(exist_ok=True)
        entry = SQLiteEntry(directory.as_posix(), 'sqlite-test', None, 5)
        assert isinstance(entry.session, Connection)
        assert entry.close()

        def test_multi_threads(sqlite):
            _ = sqlite.session.execute(
                'CREATE TABLE IF NOT EXISTS `test`('
                '`key` BLOB,'
                '`value`, BLOB)'
            )

            with sqlite.transact() as sql:
                _ = sql(
                    'INSERT INTO `test`(`key`, `value`) VALUES (1, 1)'
                )
                _ = sql(
                    'SELECT * FROM `test`'
                ).fetchall()
                _ = sql(
                    'DELETE FROM `test`'
                )

        ts = [Thread(target=test_multi_threads, args=(entry, )) for _ in range(10)]
        [t.start() for t in ts]
        [t.join() for t in ts]
    
    def test_config(self, tmp_path):
        directory = tmp_path / 'test'
        directory.mkdir(exist_ok=True)
        entry = SQLiteEntry(directory.as_posix(), 'sqlite-test', None, 5)
        assert entry.config('name') is None
        assert entry.config('name',  'value')
        assert entry.config('name') == 'value'


class TestPickleStore:

    def create_store(self, path, name=pickle.HIGHEST_PROTOCOL, raw_max_size=10, charset='utf-8'):
        return PickleStore(path, name, raw_max_size=raw_max_size, charset=charset)

    def test_dumps_loads(self):

        test_dir = Path('dist_test')
        test_dir.mkdir(exist_ok=True)
        store = self.create_store(test_dir, raw_max_size=10)

        # string
        min_string = rand_string(4, 8)
        v, f = store.dumps(min_string)
        assert v == min_string
        assert f == RAW
        assert store.loads(v, f) == v

        big_string = rand_string(11, 20)
        v, f = store.dumps(big_string)
        assert v == store.signature(big_string.encode('UTF-8'))
        assert f == STRING
        assert store.loads(v, f) == big_string

        # int /float
        v, f = store.dumps(10)
        assert f == NUMBER 
        assert v == 10
        assert store.loads(v, f) == 10

        v, f = store.dumps(11.)
        assert f == NUMBER 
        assert v == 11.
        assert store.loads(v, f) == 11.

        # bytes
        big_bytes = rand_string(1000, 1100).encode('UTF-8')
        v, f = store.dumps(big_bytes)
        assert f == BYTES 
        assert store.loads(v, f) == big_bytes

        # other type
        v, f = store.dumps(empty)
        assert f == PICKLE 
        assert store.loads(v, f) == empty


