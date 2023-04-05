#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15
# author: clarkmonkey@163.com

import pickle
from pathlib import Path
from shutil import rmtree

import pytest
from cache3.disk import (
    SQLiteManager, PickleStore, empty, BYTES, NUMBER, STRING, RAW, PICKLE, EvictManager,
    EvictInterface, LRUEvict, FIFOEvict, LFUEvict, DiskCache
)
from cache3.util import Cache3Error, Cache3Warning
from sqlite3 import Connection
from threading import Thread
from utils import rand_string, rand_strings

raises = pytest.raises
warns = pytest.warns
test_directory = Path('test_directory')


def setup_module():
    if test_directory.exists():
        rmtree(test_directory.as_posix())
    test_directory.mkdir(exist_ok=True, parents=True)


def teardown_module():
    rmtree(test_directory.as_posix())


class TestSQLiteManager:

    def test_instance(self):
        test_dir = test_directory / f'test-disk-{rand_string()}'
        test_dir.mkdir(exist_ok=True, parents=True)

        # success
        assert SQLiteManager(test_dir.as_posix(), rand_string(), None, 5)
        
        # invalid pragmas
        with raises(TypeError, match='pragmas want dict object but get .*'):
            SQLiteManager(test_dir.as_posix(), rand_string(), None, 5, pragmas=1)

    def test_session(self):
        test_dir = test_directory / f'test-disk-{rand_string()}'
        test_dir.mkdir(exist_ok=True, parents=True)
        entry = SQLiteManager(test_dir.as_posix(), rand_string(), None, 5)
        assert isinstance(entry.session, Connection)
        assert entry.close()

        def test_multi_threads(sqlite):
            _ = sqlite.session.execute(
                'CREATE TABLE IF NOT EXISTS `test`('
                '`key` BLOB,'
                '`value` BLOB)'
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
    
    def test_config(self):
        test_dir = test_directory / f'test-disk-{rand_string()}'
        test_dir.mkdir(exist_ok=True, parents=True)
        entry = SQLiteManager(test_dir.as_posix(), rand_string(), None, 5)
        assert entry.config('name') is None
        assert entry.config('name',  'value')
        assert entry.config('name') == 'value'


class TestPickleStore:

    def create_store(self, path, name=pickle.HIGHEST_PROTOCOL, raw_max_size=10, charset='utf-8'):
        return PickleStore(path, name, raw_max_size=raw_max_size, charset=charset)

    def test_dumps_loads(self):
        test_dir = test_directory / f'test-disk-{rand_string()}'
        test_dir.mkdir(exist_ok=True, parents=True)
        store = self.create_store(test_dir, raw_max_size=10)

        # string
        small_string = rand_string(4, 8)
        v, f = store.dumps(small_string)
        assert v == small_string
        assert f == RAW
        assert store.loads(v, f) == v

        big_string = rand_string(11, 20)
        v, f = store.dumps(big_string)
        assert v == store.signature(big_string.encode('UTF-8'))
        assert f == STRING
        assert store.loads(v, f) == big_string

        # int
        v, f = store.dumps(10)
        assert f == NUMBER 
        assert v == 10
        assert store.loads(v, f) == 10
        
        # float
        v, f = store.dumps(11.)
        assert f == NUMBER 
        assert v == 11.
        assert store.loads(v, f) == 11.

        # bytes
        small_bytes = rand_string(4, 10).encode('UTF-8')
        v, f = store.dumps(small_bytes)
        assert f == RAW
        assert store.loads(v, f) == small_bytes

        big_bytes = rand_string(1000, 1100).encode('UTF-8')
        v, f = store.dumps(big_bytes)
        assert f == BYTES 
        assert store.loads(v, f) == big_bytes

        # other type
        v, f = store.dumps(empty)
        assert f == PICKLE 
        assert store.loads(v, f) == empty

        big_object = list('1' * 100000)
        v, f = store.dumps(big_object)
        assert f == PICKLE
        assert store.loads(v, f) == big_object

        # stored file has been deleted
        v, f = store.dumps(big_string)
        assert v == store.signature(big_string.encode('UTF-8'))
        assert f == STRING
        assert store.loads(v, f) == big_string

        # store file deleted
        assert store.delete(v) == True
        with warns(Cache3Warning):
            assert store.loads(v, f) is None
        
        # test delete
        assert store.delete(v) == False


class SuccessEvict(EvictInterface):
    name = 'success-evict-policy'


class TypeErrorEvict:
    name = 'type-error-evict-policy'


class TestManagerEvict:

    def test_register(self):

        manager = EvictManager()
        assert len(manager) == 0
        
        manager.register(SuccessEvict)
        assert len(manager) == 1
        assert SuccessEvict.name in manager

        with raises(Cache3Error, match='evict must be inherit `EvictInterface` class'):
            manager.register(TypeErrorEvict)
        
        with raises(Cache3Error, match=f'has been registered evict named {SuccessEvict.name!r}'):
            manager.register(SuccessEvict)

    def test_getitem(self):
        
        manager = EvictManager()
        assert len(manager) == 0
        manager.register(SuccessEvict)
        assert manager[SuccessEvict.name] == SuccessEvict

        not_exited_evict: str = 'not-exist-evict'
        with raises(Cache3Error, match=f'no register evict policy named {not_exited_evict!r}'):
            manager[not_exited_evict]


class TestDiskCache:
    
    def setup_class(self):
        path: Path = (test_directory / 'test-disk')
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        self.cache = DiskCache(path.as_posix(), max_size=10)

    def setup_method(self):
        self.cache.clear()

    def teardown_method(self):
        self.cache.sqlite.close()

    def test_str(self):
        assert str(self.cache).startswith('<DiskCache: ')

    def test_inspect(self):
        name, value, tag = 'name', 'value', 'tag'
        # not existed key

        assert self.cache.inspect(name) is None
        self.cache.set(name, value, tag=tag)
        assert self.cache.has_key(name, tag=tag)
        ins: dict = self.cache.inspect(name, tag=tag)
        assert isinstance(ins, dict)
        assert ins['key'] == name
        assert ins['value'] == value
        assert ins['tag'] == tag
        assert ins['kf'] == RAW
        assert ins['sk'] == name
        assert ins['kf'] == RAW
        assert ins['sv'] == value
        assert ins['expire'] is None
        assert ins['access_count'] == 0
        timeout = 10
        assert self.cache.touch(name, tag=tag, timeout=timeout)
        ins = self.cache.inspect(name, tag=tag)
        assert round(ins['expire'] - ins['store']) == timeout

    def test_try_evict(self):
        for evict in ['lru', 'lfu', 'fifo']:
            self.cache.config_evict(evict)
            for key in rand_strings(1000):
                self.cache[key] = key

    def test_config_evict(self):
        self.cache.config_evict('fifo')
        self.cache.config_evict('lru')
        self.cache.config_evict('lfu')
        self.cache.config_evict('fifo')
        self.cache.config_evict('lru')

    def test_evict_fifo(self):
        self.cache.config_evict('fifo')
        self.cache.max_size = 10

        keys = list(rand_strings(16))
        for key in keys:
            self.cache[key] = key[::-1]
        
        assert len(self.cache) == 6
        for key in keys[-6:]:
            assert self.cache[key] == key[::-1]

