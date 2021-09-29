#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/9/15
# Author: clarkmonkey@163.com

import warnings
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, connect, OperationalError
from time import time as current
from typing import NoReturn, Type, Union, Optional, Dict, Any, List, Tuple

from cache3 import BaseCache
from setting import DEFAULT_NAME, DEFAULT_TIMEOUT, DEFAULT_MAX_SIZE, DEFAULT_TAG

Number: Type = Union[int, float]
TG: Type = Optional[str]
Time: Type = float
PATH: Type = Union[Path, str]

# SQLite pragma configs
PRAGMAS: Dict[str, Union[str, int]] = {
    'auto_vacuum': 1,
    'cache_size': 1 << 13,  # 8, 192 pages
    'journal_mode': 'wal',
    'threads': 4,  # SQLite work threads count
    'temp_store': 2,  # DEFAULT: 0 | FILE: 1 | MEMORY: 2
    'mmap_size': 1 << 27,  # 128MB
    'synchronous': 1,
    'locking_mode': 'EXCLUSIVE',
}
DEFAULT_STORE: Path = Path('~/.pycache3')
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

_sqlite_map: dict = dict()

#
# class SingletonMixin:
#
#     __singleton_attr__ = '__singleton__'
#
#     def __new__(cls) -> 'SessionManager':
#         if getattr(cls, cls.__singleton_attr__, empty) is empty:
#             instance = super(SingletonMixin, cls).__new__(cls)
#             setattr(cls, cls.__singleton_attr__, instance)
#         return getattr(cls, cls.__singleton_attr__)


class SQLite:
    """ SQLite backend class """

    def __init__(
            self,
            database: str,
            pragma_script: Optional[str] = None,
            **kwargs
    ) -> None:
        self._database: str = database
        self._is_in_memory: bool = database in [':memory:', 'mode=memory']
        self.pragma_script: str = pragma_script
        self._kwargs: Dict[str, Any] = kwargs
        self._session: Optional[Connection] = None

        if kwargs.setdefault('check_same_thread', False):
            warnings.warn(
                'Ths `check_same_thread` option was provided and set to True.',
                RuntimeWarning
            )

        # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.isolation_level
        if kwargs.setdefault('isolation_level', None) is not None:
            warnings.warn(
                'SQLite python implements an automatic start of '
                'a transaction, but does not automatically commit',
                UserWarning
            )

    def _config_session(self, session: Connection) -> NoReturn:
        """ Set the properties of the sqlite connections. """
        if self.pragma_script:
            session.executescript(self.pragma_script)

    @property
    def session(self) -> Connection:
        if self._session is None:
            session: Connection = connect(self._database, **self._kwargs)
            self._config_session(session)
            self._session = session
        return self._session

    def close(self, force: bool = False) -> bool:
        """ if SQlite database is in memory, closing the connection desctroys the
        database. To prevent accidental data loss, such as : ignore close requests,
        ignore release lock, etc.
        """
        if self._session is None:
            return True

        elif not self._is_in_memory or force:
            self._session. close()
            self._session = None
            return True

        warnings.warn(
            'Cache build in memory, if closing the connection, '
            'will destroys the database . if you want to do that,'
            'please close(True) or destroy().',
            RuntimeWarning
        )
        return False

    def destroy(self) -> bool:
        return self.close(True)

    def execute(self, *args, **kwargs) -> Cursor:
        return self.session.execute(*args, **kwargs)

    # @contextmanager
    # def transact(self) -> Callable[[str, Any], Cursor]:
    #     """
    #
    #     docs: https://docs.python.org/3/library/sqlite3.html
    #
    #         SQLite Connection objects can be used as context managers that
    #     automatically commit or rollback transactions. In the event of an
    #     exception, the transaction is roll back; otherwise , the transaction
    #     is committed;
    #     """
    #     yield self.session

    def __getattr__(self, item: str) -> Any:
        return getattr(self.session, item)

    def __repr__(self) -> str:
        return "<%s db: %s> " % (self.__class__.__name__, self._database)

    __call__ = execute


def sqlite_factory(directory: Path, filename: str, **kwargs) -> SQLite:

    pragmas: Dict[str, Any] = PRAGMAS
    pragmas.update(
        kwargs.pop('pragmas', dict())
    )
    pragma_script: str = ';'.join('PRAGMA %s=%s' % item for item in pragmas.items())

    if not directory.exists():
        directory.mkdir(0o755)

    # Lower python interpreter unsupported ``Path`` object.
    db: str = str(directory / filename)
    sqlite: SQLite = _sqlite_map.get(db)

    if sqlite is None:
        # klass = type('%sSQLite' % filename.title(), (SQLite,), {})
        sqlite = SQLite(db, pragma_script, **kwargs)
        _sqlite_map[db] = sqlite
    return sqlite


class Cache(BaseCache):

    def __init__(
            self,
            name: Optional[str] = DEFAULT_NAME,
            timeout: float = DEFAULT_TIMEOUT,
            max_size: int = DEFAULT_MAX_SIZE,
            directory: str = DEFAULT_STORE,
            **kwargs
    ):
        super(Cache, self).__init__(name, timeout, max_size)
        self.path: Path = Path(directory or DEFAULT_STORE).expanduser().resolve()
        self._kwargs = kwargs
        self._sqlite: Optional[SQLite] = None
        # self.sqlite: SQLite = sqlite_factory(self.path, self._name, timeout=self._timeout, **kwargs)
        # Create cache tables and fill base data.
        self._make_cache_dependencies()

    @property
    def sqlite(self) -> SQLite:
        if self._sqlite is None:
            self._sqlite = sqlite_factory(self.path, self._name, timeout=DEFAULT_SQLITE_TIMEOUT, **self._kwargs)
        return self._sqlite

    def _make_sqlite(self) -> SQLite:
        return sqlite_factory(self.path, self._name, timeout=self._timeout, **self._kwargs)

    def set(self, key: str, value: Any, timeout: Number = DEFAULT_TIMEOUT,
            tag: TG = DEFAULT_TAG) -> bool:

        key: str = self.make_and_validate_key(key, tag)
        value: Any = self.serialize(value)
        now: Time = current()
        expire: Optional[Number] = self.get_backend_timeout(timeout)

        success: bool = self.sqlite(
            'INSERT OR REPLACE INTO `cache`('
            '`key`, `store`, `expire`, `access`, `access_count`, `tag`, `value`'
            ') VALUES (?, ?, ?, ?, ?, ?, ?)',
            (key, now, expire, now, 0, tag, value)
        ).rowcount == 1
        if success:
            self._add_count()
            self.evict()
        return success

    def ex_set(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT,
               tag: Optional[str] = DEFAULT_TAG) -> bool:

        if self.has_key(key, tag):
            return False

        key: str = self.make_key(key, tag)
        now: Time = current()
        expire: float = self.get_backend_timeout(timeout)

        success: bool = self.sqlite(
            'INSERT OR REPLACE INTO `cache`( '
            '`key`, `store`, `expire`, `access`, `access_count`, `tag`, `value`'
            ') VALUES (?, ?, ?, ?, ?, ?, ?) ',
            (key, now, expire, now, 0, tag, value)
        ).rowcount == 1
        if success:
            self._add_count()

        return success

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

    def delete(self, key: str, tag: TG = DEFAULT_TAG) -> bool:
        key: str = self.make_and_validate_key(key, tag)

        success: bool = self.sqlite(
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

        return self.sqlite(
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

        success: bool = self.sqlite(
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

    def lru_evict(self) -> NoReturn:

        # Get current k-v pair count.
        (count, ) = self.sqlite(
            'SELECT `count` FROM `info` '
            'WHERE `rowid` = 1'
        ).fetchone()

        # reduce k-v pair to follow count limit
        if count > self._max_size:
            self.sqlite(
                'DELETE FROM `cache` WHERE `rowid` IN ('
                'SELECT `rowid` FROM `cache` '
                'ORDER BY `access` LIMIT ?)',
                (self._cull_size, )
            )

    def clear(self):
        """ FIXME :: NoQA """
        try:
            self.sqlite.executescript(
                'DELETE FROM `sqlite_sequence` WHERE `name` = cache;'
                'UPDATE `info` SET `size` = 0, `count` = 0 WHERE rowid = 1'
            )
        except OperationalError:
            self.sqlite.executescript(
                'DELETE FROM `cache`;'
                'UPDATE `info` SET `count` = 0 WHERE rowid = 1'
            )

    def make_key(self, key: str, tag: Optional[str]) -> str:
        """ Keep type avoid keys conflict. """
        return '%s(%s)' % (type(key).__name__, key)

    def _make_cache_dependencies(self, queries: Optional[List] = None) -> NoReturn:

        base_queries: List[str] = list()
        for name, info in TABLES.items():
            schema = info['schema']
            if schema:
                base_queries.append(schema)

            index = info['construct']
            if index:
                base_queries.extend(index)

            # view = info['view']
            # if view:
            #     base_queries.extend(view)

        if queries:
            base_queries += queries
        script: str = ';'.join(base_queries)

        # Create tables.
        self.sqlite.executescript(
            script
        )

        # Fill base data.
        if not self.sqlite.execute(
            'SELECT 1 FROM `info` WHERE `rowid` = 1'
        ).fetchone():
            self.sqlite.execute(
                'INSERT INTO `info`(`count`) VALUES (0)'
            )

    def _add_count(self) -> bool:

        return self.sqlite(
            'UPDATE `info` SET `count` = `count` + 1 '
            'WHERE `rowid` = 1'
        ).rowcount == 1

    def _sub_count(self) -> bool:

        return self.sqlite(
            'UPDATE `info` SET `count` = `count` - 1 '
            'WHERE `rowid` = 1'
        ).rowcount == 1

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set
