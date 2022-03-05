#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/9/15
# Author: clarkmonkey@163.com

from contextlib import contextmanager
from os import getpid
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, connect, OperationalError
from threading import Lock, local, get_ident
from time import time as current, sleep
from typing import (
    NoReturn, Type, Union, Optional, Dict, Any, List, Tuple, Iterator, Callable
)

from cache3 import BaseCache
from cache3.base import PickleMixin, JSONMixin
from cache3.setting import (
    DEFAULT_TIMEOUT, DEFAULT_TAG, DEFAULT_STORE, DEFAULT_SQLITE_TIMEOUT
)
from cache3.utils import cached_property
from cache3.validate import DirectoryValidate

try:
    import ujson as json
except ImportError:
    import json

Number: Type = Union[int, float]
TG: Type = Optional[str]
Time: Type = Optional[float]
PATH: Type = Union[Path, str]
QY: Type = Callable[[Any, ...], Cursor]
ROW: Type = Optional[Tuple[Any, ...]]

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

TABLES: Dict[str, Any] = {
    'cache': {

        # Do not use the autoincrement primary key, which is inefficient
        # and easy to waste space and cpu. default behavior since the use
        # of AUTOINCREMENT requires additional work to be done as each row
        # is inserted and thus causes INSERTs to run a little slower.
        # At the same time autoincrement will prevent SQLite from using
        # primary key values that are less than the current value and are
        # not used.
        # docs: https://www.sqlite.org/autoinc.html

        # TODO: Generally, the cache does not need a very strict expiration time,
        #  but this precise time is very helpful for some data evict actions.
        #  In the future, the time accuracy may be reduced to improve performance

        'schema': (
            'CREATE TABLE IF NOT EXISTS `cache`('
            '`key` BLOB NOT NULL,'
            '`store` REAL NOT NULL,'
            '`expire` REAL NOT NULL,'
            '`access` REAL NOT NULL,'
            '`access_count` INTEGER DEFAULT 0,'
            '`tag` BLOB,'    # Don't set ``NOT NULL``
            '`value` BLOB)'     # cache accept NULL, (None)
        ),
        'construct': [
            # unique limit <key - tag>
            'CREATE UNIQUE INDEX IF NOT EXISTS `idx_key` '
            'ON `cache` (`key`, `tag`)',

            # avoid query primary key index tree
            'CREATE INDEX IF NOT EXISTS `idx_data_key` '
            'ON `cache` (`key`, `tag`, `expire`, `value`)',

        ],
    },

    'info': {
        'schema': (
            'CREATE TABLE IF NOT EXISTS `info`( '
            '`count` INTEGER DEFAULT 0)'
        ),
        'construct': [
            'CREATE UNIQUE INDEX IF NOT EXISTS `idx_count_size` '
            'ON `info` (`count`)',
        ]
    }
}


def dict_factory(cursor: Cursor, row: ROW) -> Dict:
    """ Format query result to dict. """
    d: dict = dict()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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

    def __get__(self, instance: 'SimpleDiskCache', owner) -> Optional[Connection]:

        local_pid: int = getattr(self.context, 'pid', None)
        pid: int = getpid()

        if local_pid != pid:
            self._close(instance)
            self.context.pid = pid
        session: Connection = getattr(self.context, instance.location, None)
        if session is None:
            configure: Dict[str, Any] = getattr(instance, 'configure')
            pragmas: Dict[str, Any] = getattr(instance, 'pragmas')
            session = connect(
                **configure
            )
            self.config_session(session, pragmas)
            setattr(self.context, instance.location, session)

        return session

    def _close(self, instance: 'SimpleDiskCache') -> bool:
        with self.lock:
            session: Connection = getattr(self.context, instance.location, None)
            if session is None:
                return True
            session.close()
            setattr(self.context, instance.location, None)
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

    def __delete__(self, instance: 'SimpleDiskCache') -> bool:
        return self._close(instance)


class SimpleDiskCache(BaseCache):
    """ A base class for all disk cache.

    Most of the methods of disk cache are implemented in this class.
    It uses the BaseCache primitive serialization and deserialization methods.
    Therefore, there are strict requirements for key and value types.

    That means key and value can only be one of the types int, float, string,
    bytes and bool supported by SQLite.

    Typically, subclasses override the serialization and deserialization methods
    of key and value to support more types. This is also true for some default
    implementations, such as ``DiskCache`` and ``JsonDiskCache``
    """

    session: SessionDescriptor = SessionDescriptor()
    directory: DirectoryValidate = DirectoryValidate()

    def __init__(self, directory: PATH = DEFAULT_STORE, *args, **kwargs) -> None:
        super(SimpleDiskCache, self).__init__(*args, **kwargs)
        self.directory: Path = directory
        self._txn_id: Optional[int] = None
        self.args: Tuple[Any] = args
        self.kwargs: Dict[str, Any] = kwargs

        self.configure: Dict[str, Any] = {
            'database': self.location,
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
        # Initialize cache table and statistics table
        self._make_cache_dependencies()

    @cached_property
    def location(self) -> str:
        return str(self.directory / self.name)

    def set(self, key: str, value: Any, timeout: Number = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG) -> bool:

        store_key: str = self.store_key(key, tag)
        serial_value: Any = self.serialize(value)
        with self._transact() as sqlite:
            row: ROW = sqlite(
                'SELECT `ROWID` '
                'FROM `cache` '
                'WHERE `key` = ? AND `tag` = ? ',
                (store_key, tag)
            ).fetchone()
            if row:
                (rowid,) = row
                return self._update_line(rowid, serial_value, timeout, tag)
            else:
                if self._insert_line(store_key, serial_value , timeout, tag):
                    self._add_count()
                    if len(self) > self.max_size:
                        self.evictor()
                else:
                    return False
        return True

    def get(self, key: str, default: Any = None, tag: TG = DEFAULT_TAG) -> Any:

        store_key: str = self.store_key(key, tag)
        row: ROW = self.sqlite(
            'SELECT `ROWID`, `value` '
            'FROM `cache` '
            'WHERE `key` = ? AND `tag` = ? '
            'AND (`expire` IS NULL OR `expire` > ?)',
            (store_key, tag, current())
        ).fetchone()

        if not row:
            return default
        (rowid, serial_value) = row

        # FIXME async
        self.sqlite(
            'UPDATE `cache` '
            'SET `access_count` = `access_count` + 1 '
            'WHERE ROWID = ?',
            (rowid, )
        )
        return self.deserialize(serial_value)

    def _has_key(self, store_key: str, tag: TG = DEFAULT_TAG) -> bool:

        return bool(self.sqlite(
            'SELECT 1 FROM `cache` '
            'WHERE `key` = ? '
            'AND tag = ? '
            'AND (`expire` IS NULL OR `expire` > ?)',
            (store_key, tag, current())
        ).fetchone())

    def ex_set(
            self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
            tag: Optional[str] = DEFAULT_TAG
    ) -> bool:
        """ Mutually exclusive sets, even across processes, can also ensure the
        atomicity of operations """

        store_key: str = self.store_key(key, tag)
        with self._transact() as sqlite:
            row: ROW = sqlite(
                'SELECT `ROWID`, `expire` '
                'FROM `cache` '
                'WHERE `key` = ? '
                'AND `tag` = ? ',
                (store_key, tag)
            ).fetchone()
            if row:
                (rowid, expire) = row
                if expire > current():
                    return False
                return self._update_line(rowid, self.serialize(value), timeout, tag)
            else:
                if self._insert_line(store_key, self.serialize(value), timeout, tag):
                    self._add_count()
                    if len(self) > self.max_size:
                        self.evictor()
                else:
                    return False
        return True

    def get_many(self, keys: List[str], tag: TG = DEFAULT_TAG) -> Dict[str, Any]:
        """ There is a limitation on obtaining a group of key values.

        TODO WARNING: the tags of this group of key values must be consistent,
            but the memory based cache strategy does not have this limitation.
            This feature will be supported in the future to ensure the
            consistency of behavior
        """

        store_keys: List[str] = [self.store_key(key, tag) for key in keys]
        snap: str = str('?, ' * len(keys)).strip(', ')
        statement: str = (
                         'SELECT `value` '
                         'FROM `cache` '
                         'WHERE `key` IN (%s) AND `tag` = ?'
                     ) % snap
        cursor: Cursor = self.sqlite(
            statement,
            (*store_keys, tag)
        )
        values: List[Any] = [self.deserialize(i[0]) for i in cursor]
        return dict(zip(keys, values))

    def touch(self, key: str, timeout: Number, tag: TG = DEFAULT_TAG) -> bool:
        """ Renew the key. When the key does not exist, false will be returned """

        store_key: str = self.store_key(key, tag)
        now: Time = current()
        new_expire: Optional[Number] = self.get_backend_timeout(timeout, now)
        with self._transact() as sqlite:
            return sqlite(
                'UPDATE `cache` SET `expire` = ? '
                'WHERE `key` = ? '
                'AND `tag` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (new_expire, store_key, tag, now)
            ).rowcount == 1

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:

        store_key: str = self.store_key(key, tag)
        with self._transact() as sqlite:
            success: bool = sqlite(
                'DELETE FROM `cache` '
                'WHERE `key` = ? AND `tag` = ?',
                (store_key, tag)
            ).rowcount == 1
            if success:
                self._sub_count()
        return success

    def inspect(self, key: str, tag: TG = DEFAULT_TAG) -> Optional[Dict[str, Any]]:
        """ Get the details of the key value, including any information,
        access times, recent access time, etc., and even the underlying
        serialized data
        """

        store_key: str = self.store_key(key, tag)
        cursor: Cursor = self.sqlite(
            'SELECT * '
            'FROM `cache` '
            'WHERE `key` = ? AND `tag` = ?',
            (store_key, tag)
        )
        cursor.row_factory = dict_factory
        row: Optional[Dict[str, Any]] = cursor.fetchone()
        if row:
            row['store_key'] = store_key
            row['key'] = key
            row['serial_value'] = row['value']
            row['value'] = self.deserialize(row['value'])
            return row

    def incr(self, key: str, delta: int = 1, tag: TG = DEFAULT_TAG) -> Number:
        """ int, float and (str/bytes) not serialize, so add in sql statement.

        The increment operation should be implemented through SQLite,
        which is not safe at the Python language level
        """
        store_key: str = self.store_key(key, tag)
        with self._transact() as sqlite:
            try:
                (value,) = sqlite(
                    'SELECT `value` FROM `cache` '
                    'WHERE `key` = ? AND `tag` = ? '
                    'AND (`expire` IS NULL OR `expire` > ?)',
                    (store_key, tag, current())
                ).fetchone()
            except TypeError as exc:
                raise ValueError("Key '%s' not found" % key) from exc

            if not isinstance(value, (int, float)):
                raise TypeError(
                    "unsupported operand type(s) for +: '%s' and '%s'"
                    % (type(value), type(delta))
                )
            sqlite(
                'UPDATE `cache` SET `value`= `value` + %s '
                'WHERE `key` = ? '
                'AND `tag` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)' % delta,
                (store_key, tag, current())
            )
        return value + delta

    def has_key(self, key: str, tag: TG = DEFAULT_TAG) -> bool:
        store_key: str = self.store_key(key, tag)
        return self._has_key(store_key, tag)

    def ttl(self, key: Any, tag: TG = DEFAULT_TAG) -> Time:

        store_key: str = self.store_key(key, tag)
        row: ROW = self.sqlite(
            'SELECT `expire` '
            'FROM `cache` '
            'WHERE `key` = ? '
            'AND `tag` = ? '
            'AND `expire` > ?',
            (store_key, tag, current())
        ).fetchone()
        if not row:
            return -1
        (expire, ) = row
        return expire - current()

    def clear(self) -> bool:
        """ Delete all data and initialize the statistics table. """

        with self._transact() as sql:
            sql(
                'DELETE FROM `cache`;'
            )
            # Delete all data and initialize the statistics table.
            # Since the default `ROWID` is used as the primary key,
            # you don't need to care whether the `ROWID` starts from
            # 0. Even if the ID is full, SQLite will select an
            # appropriate value from the unused rowid set.

            sql(
                'UPDATE `info` SET `count` = 0 '
                'WHERE ROWID = 1'
            )
        return True

    def get_current_size(self) -> int:
        (count,) = self.sqlite(
            'SELECT `count` FROM `info` '
            'WHERE `ROWID` = 1'
        ).fetchone()
        return count

    def store_key(self, key: Any, tag: Optional[str]) -> str:
        return key

    def restore_key(self, serial_key: str) -> str:
        return serial_key

    def lru_evict(self) -> NoReturn:
        """ It is called by the master logic, and there is no need to
        care about when to schedule. """

        if self.cull_size == 0:
            self.clear()

        # reduce k-v pair to follow count limit
        with self._transact() as sqlite:
            sqlite(
                'UPDATE `info` SET `count` = ('
                'SELECT COUNT(1) FROM `cache`'
                ') WHERE ROWID = 1'
            )

    @property
    def sqlite(self) -> QY:
        return self.session.execute

    @contextmanager
    def _transact(self, retry: bool = True) -> QY:
        sql: QY = self.sqlite
        tid: int = get_ident()
        txn_id: int = self._txn_id

        if tid == txn_id:
            begin: bool = False
        else:
            while True:
                try:
                    sql('BEGIN IMMEDIATE')
                    begin = True
                    self._txn_id = tid
                    break
                except OperationalError as exc:
                    if retry:
                        continue
                    raise TimeoutError(
                        'Transact timeout. (timeout=%s).' %
                        self.configure['timeout']
                    ) from exc

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

    def _add_count(self) -> bool:
        return self.sqlite(
                'UPDATE `info` SET `count` = `count` + 1 '
                'WHERE `ROWID` = 1'
            ).rowcount == 1

    def _make_cache_dependencies(
            self, tables: Optional[Dict[str, Any]] = None
    ) -> NoReturn:
        """ create tables such as ``cache``, ``info``, create index and
        initialization statistics.
        """

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
            'SELECT 1 FROM `info` WHERE `ROWID` = 1'
        ).fetchone():
            self.sqlite(
                'INSERT INTO `info`(`count`) VALUES (0)'
            )

    def _sub_count(self) -> bool:

        with self._transact() as sqlite:
            return sqlite(
                'UPDATE `info` SET `count` = `count` - 1 '
                'WHERE `ROWID` = 1'
            ).rowcount == 1

    def _insert_line(self, store_key: Any, serial_value: Any, timeout: Time, tag: str) -> bool:
        now: Time = current()
        expire: Time = self.get_backend_timeout(timeout, now)
        return self.sqlite(
            'INSERT INTO `cache`('
            '`key`, `store`, `expire`, `access`, '
            '`access_count`, `tag`, `value`'
            ') VALUES (?, ?, ?, ?, ?, ?, ?)',
            (store_key, now, expire, now, 0, tag, serial_value)
        ).rowcount == 1

    def _update_line(self, rowid: int, serial_value: Any, timeout: Time, tag: str) -> bool:
        now: Time = current()
        expire: Time = self.get_backend_timeout(timeout, now)
        return self.sqlite(
            'UPDATE `cache` SET '
            '`store` = ?, '
            '`expire` = ?, '
            '`access` = ?,'
            '`access_count` = ?, '
            '`tag` = ?, '
            '`value` = ? '
            ' WHERE `rowid` = ?',
            (now, expire, now, 0, tag, serial_value, rowid)
        ).rowcount == 1

    def __iter__(self) -> Iterator:
        """ This may take up a lot of memory, which needs special
        attention when there are a lot of data.

        Conservatively, this method is not recommended to be called
        It is only applicable to those scenarios with small data scale
        or for testing.
        """
        query_time: Time = current()
        for line in self.sqlite(
            'SELECT `key`, `value`, `tag` '
            'FROM `cache` '
            'WHERE (`expire` IS NULL OR `expire` > ?) '
            'ORDER BY `store`',
            (query_time,)
        ):
            store_key, serial_value, tag = line
            key: str = self.restore_key(store_key)
            value: Any = self.deserialize(serial_value)

            yield key, value, tag

    def __repr__(self) -> str:
        return "<%s name=%s location=%s timeout=%.2f>" % (
            self.__class__.__name__, self.name, self.location, self.timeout
        )

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set


class DiskCache(PickleMixin, SimpleDiskCache):
    """"""


class JsonDiskCache(JSONMixin, SimpleDiskCache):
    """"""
