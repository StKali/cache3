#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/9/15
# Author: clarkmonkey@163.com

from contextlib import contextmanager
from multiprocessing import Process
from os import getpid
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, connect, OperationalError
from threading import Lock, local, get_ident
from time import time as current, sleep
from typing import NoReturn, Type, Union, Optional, Dict, Any, List, Tuple, Callable

from cache3 import BaseCache
from cache3.setting import (
    DEFAULT_TIMEOUT, DEFAULT_TAG, DEFAULT_STORE,
    DEFAULT_SQLITE_TIMEOUT
)
from cache3.validate import DirectoryValidate

Number: Type = Union[int, float]
TG: Type = Optional[str]
Time: Type = float
PATH: Type = Union[Path, str]

# SQLite pragma configs
PRAGMAS: Dict[str, Union[str, int]] = {
    'auto_vacuum': 1,
    'cache_size': 1 << 13,  # 8, 192 pages
    'journal_mode': 'wal',
    # 'threads': 4,  # SQLite work threads count
    'temp_store': 2,  # DEFAULT: 0 | FILE: 1 | MEMORY: 2
    'mmap_size': 1 << 26,  # 64MB
    'synchronous': 1,
}

TABLES = {
    'cache': {
        'schema': (
            'CREATE TABLE IF NOT EXISTS `cache`('
            '`key` TEXT NOT NULL,'
            '`store` REAL NOT NULL,'
            '`expire` REAL NOT NULL,'
            '`access` REAL NOT NULL,'
            '`access_count` INTEGER DEFAULT 0,'
            '`tag` BLOB,'    # Don't set ``NOT NULL``
            '`value` BLOB)'
        ),
        'construct': [
            'CREATE UNIQUE INDEX IF NOT EXISTS `idx_key` ON `cache` (`key`, `tag`)',
            'CREATE INDEX IF NOT EXISTS `idx_expire` ON `cache` (`expire`)',
        ],
    },

    'info': {
        'schema': (
            'CREATE TABLE IF NOT EXISTS `info`( '
            '`count` INTEGER DEFAULT 0)'
            # '`size` INTEGER DEFAULT 0)'    # TODO add disk limit.
        ),
        'construct': [
            'CREATE UNIQUE INDEX IF NOT EXISTS `idx_count_size` ON `info` (`count`)',
        ]
    }
}


class SessionDescriptor:

    def __set_name__(self, owner: object, name: str) -> NoReturn:
        self.private_name: str = '_' + name
        self.lock: Lock = Lock()
        self.context: local = local()

    def __set__(self, instance: Any, value: Connection) -> bool:

        if not isinstance(value, Connection):
            raise ValueError(
                f'Expected {value!r} to be an sqlite3.Connection.'
            )
        with self.lock:
            setattr(instance, self.private_name, value)
        return True

    def __get__(self, instance, owner) -> Optional[Connection]:

        local_pid: int = getattr(self.context, 'pid', None)
        pid: int = getpid()

        if local_pid != pid:
            self._close()
            self.context.pid = pid

        session: Connection = getattr(self.context, 'session', None)

        if session is None:
            configure: Dict[str, Any] = getattr(instance, 'configure')
            pragmas: Dict[str, Any] = getattr(instance, 'pragmas')
            session = self.context.session = connect(
                **configure
            )
            self.config_session(session, pragmas)
        return session

    def _close(self) -> bool:

        session: Connection = getattr(self.context, 'session', None)
        if session is None:
            return True
        session.close()
        try:
            delattr(self.context, 'session')
        except AttributeError:
            pass
        return True

    @staticmethod
    def config_session(session: Connection, pragmas: Dict[str, Any]):

        start: Time = current()
        script: str = ';'.join(
            'PRAGMA %s = %s' % item for
            item in pragmas.items()
        )

        while True:
            try:
                session.executescript(script)
                break
            except OperationalError as exc:
                if str(exc) != 'database is locked':
                    raise
                diff = current() - start
                if diff > 60:
                    raise
                sleep(0.001)

    def __delete__(self, instance: object) -> bool:
        return self._close()


class DiskCache(BaseCache):

    session: SessionDescriptor = SessionDescriptor()
    directory: DirectoryValidate = DirectoryValidate()

    def __init__(self, directory=DEFAULT_STORE, *args, **kwargs):
        super(DiskCache, self).__init__(*args, **kwargs)
        self.directory: Path = directory
        self._txn_id = None
        self.args = args
        self.kwargs = kwargs

        self.configure: Dict[str, Any] = {
            'database': str(self.directory / self._name),
            'isolation_level': None,
            'timeout': DEFAULT_SQLITE_TIMEOUT,
        }
        self.configure.update(
            kwargs.get('configure', dict())
        )
        self.pragmas: Dict[str, Any] = PRAGMAS
        self.pragmas.update(
            kwargs.get('pragmas', dict())
        )
        self._make_cache_dependencies()

    @property
    def sqlite(self):
        return self.session.execute

    @contextmanager
    def _transact(self, retry=False):
        sql = self.sqlite
        tid = get_ident()
        txn_id = self._txn_id

        if tid == txn_id:
            begin = False
        else:
            while True:
                try:
                    sql('BEGIN IMMEDIATE')
                    begin = True
                    self._txn_id = tid
                    break
                except OperationalError:
                    if retry:
                        continue
                    raise TimeoutError from None

        try:
            yield sql
        except BaseException:
            if begin:
                assert self._txn_id == tid
                self._txn_id = None
                sql('ROLLBACK')
            raise
        else:
            if begin:
                assert self._txn_id == tid
                self._txn_id = None
                sql('COMMIT')

    def set(self, key: str, value: Any, timeout: Number = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag)
        value: Any = self.serialize(value)
        now: Time = current()
        expire: Optional[Number] = self.get_backend_timeout(timeout)
        with self._transact() as sqlite:
            success: bool = sqlite(
                'INSERT OR REPLACE INTO `cache`('
                '`key`, `store`, `expire`, `access`, `access_count`, `tag`, `value`'
                ') VALUES (?, ?, ?, ?, ?, ?, ?)',
                (key, now, expire, now, 0, tag, value)
            ).rowcount == 1
            if success:
                self._add_count()
                self.evictor()
        return success

    def lru_evict(self) -> NoReturn:

        # Get current k-v pair count.
        (count, ) = self.sqlite(
            'SELECT `count` FROM `info` '
            'WHERE `rowid` = 1'
        ).fetchone()

        # reduce k-v pair to follow count limit
        if count > self._max_size:
            with self._transact() as sqlite:
                sqlite(
                    'DELETE FROM `cache` WHERE `rowid` IN ('
                    'SELECT `rowid` FROM `cache` '
                    'ORDER BY `access` LIMIT ?)',
                    (self._cull_size, )
                )

    def _add_count(self) -> bool:
        return self.sqlite(
                'UPDATE `info` SET `count` = `count` + 1 '
                'WHERE `rowid` = 1'
            ).rowcount == 1

    def has_key(self, key: str, tag: TG = DEFAULT_TAG) -> bool:
        key: str = self.make_and_validate_key(key, tag)
        return bool(self.sqlite(
            'SELECT 1 FROM `cache` WHERE `key` = ? AND `expire` > ?',
            (key, current())
        ).fetchone())

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:

        key: str = self.make_and_validate_key(key, tag)
        row: Tuple[Number, Any] = self.sqlite(
            'SELECT `expire`, `value` '
            'FROM `cache` '
            'WHERE `key` = ? AND `tag` = ?',
            (key, tag)
        ).fetchone()

        if not row:
            return default

        query_expire, query_value = row
        now: Time = current()
        if query_expire is not None and query_expire < now:
            return default
        return self.deserialize(query_value)

    def ex_set(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
               tag: Optional[str] = DEFAULT_TAG) -> bool:

        if self.has_key(key, tag):
            return False

        key: str = self.make_key(key, tag)
        now: Time = current()
        expire: float = self.get_backend_timeout(timeout)
        with self._transact() as sqlite:
            success: bool = sqlite(
                'INSERT OR REPLACE INTO `cache`( '
                '`key`, `store`, `expire`, `access`, `access_count`, `tag`, `value`'
                ') VALUES (?, ?, ?, ?, ?, ?, ?) ',
                (key, now, expire, now, 0, tag, value)
            ).rowcount == 1
            if success:
                self._add_count()
        return success

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag)
        with self._transact() as sqlite:
            success: bool = sqlite(
                'DELETE FROM `cache` '
                'WHERE `key` = ? AND `tag` = ?',
                (key, tag)
            ).rowcount == 1
            if success:
                self._sub_count()
        return success

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag)
        new_expire: Number = self.get_backend_timeout(timeout)
        with self._transact() as sqlite:
            return sqlite(
                'UPDATE `cache` SET `expire` = ? '
                'WHERE `key` = ? AND `tag` = ? AND `expire` > ?',
                (new_expire, key, tag, current())
            ).rowcount == 1

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:

        key: str = self.make_and_validate_key(key, tag)
        cursor: Cursor = self.sqlite(
            'SELECT * '
            'FROM `cache` '
            'WHERE `key` = ? AND `tag` = ?',
            (key, tag)
        )
        row: Optional[Tuple] = cursor.fetchone()
        if row:
            info = {k[0]: v for k, v in zip(cursor.description, row)}
            info['value'] = self.deserialize(info['value'])
            return info

    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:

        serial_key: str = self.make_and_validate_key(key, tag)
        with self._transact() as sqlite:
            success: bool = sqlite(
                'UPDATE `cache` SET `value`=`value` + %s '
                'WHERE `key` = ? AND `tag` = ? AND `expire` > ?' % delta,
                (serial_key, tag, current())
            ).rowcount == 1

        if not success:
            raise ValueError("Key '%s' not found" % key)

        (value, ) = self.sqlite(
            'SELECT `value` FROM `cache` '
            'WHERE `key` = ? AND `tag` = ?',
            (serial_key, tag)
        ).fetchone()

        return value

    def clear(self):
        """ FIXME :: NoQA """
        try:
            self.session.executescript(
                'DELETE FROM `sqlite_sequence` WHERE `name` = cache;'
                'UPDATE `info` SET `size` = 0, `count` = 0 WHERE rowid = 1'
            )
        except OperationalError:
            self.session.executescript(
                'DELETE FROM `cache`;'
                'UPDATE `info` SET `count` = 0 WHERE rowid = 1'
            )

    def make_key(self, key: str, tag: Optional[str]) -> str:
        """ Keep type avoid keys conflict. """
        return '%s(%s)' % (type(key).__name__, key)

    def _make_cache_dependencies(self, tables: Optional[Dict[str, Any]] = None) -> NoReturn:

        # collection table schema and construct
        queries: List[str] = list()
        tables: Dict[str, Any] = tables if tables is not None else TABLES
        for name, info in tables.items():
            schema = info['schema']
            if schema:
                queries.append(schema)

            index: str = info['construct']
            if index:
                queries.extend(index)

        # Create tables.
        self.session.executescript(';'.join(queries))

        # init data table
        if not self.sqlite(
            'SELECT 1 FROM `info` WHERE `rowid` = 1'
        ).fetchone():
            self.sqlite(
                'INSERT INTO `info`(`count`) VALUES (0)'
            )

    def _sub_count(self) -> bool:

        with self._transact() as sqlite:
            return sqlite(
                'UPDATE `info` SET `count` = `count` - 1 '
                'WHERE `rowid` = 1'
            ).rowcount == 1

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set


# if __name__ == '__main__':
#     cache = DiskCache()
#     cache.set('name', 'venus')
#     print(cache.has_key('name'))
