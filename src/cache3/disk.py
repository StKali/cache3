#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/9/15
# Author: clarkmonkey@163.com
import abc
import pickle
import warnings
import functools
import sys
from contextlib import contextmanager
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, OperationalError
from threading import local, get_ident
from time import time as current, sleep
from typing import Type, Union, Optional, Dict, Any, List, Tuple, Callable, AnyStr, Iterable
from os import makedirs, getpid, path as op
from hashlib import md5
from utils import cached_property, empty

Time: Type = Optional[Union[float, int]]
TG: Type = Optional[str]
QY: Type = Callable[[Any], Cursor]
ROW: Type = Optional[Tuple[Any,...]]

RAW: int = 0
NUMBER: int = 1
STRING: int = 2
BYTES: int = 3
PICKLE: int = 4


class Cache3Error(Exception):
    """"""


if sys.version_info > (3, 10):
    from types import NoneType
else:
    NoneType: Type = type(None)

# SQLite pragma configs
default_pragmas: Dict[str, Any] = {
    'auto_vacuum': 1,
    'cache_size': 1 << 13,  # 8, 192 pages
    'journal_mode': 'wal',
    'threads': 4,  # SQLite work threads count
    'temp_store': 2,  # DEFAULT: 0 | FILE: 1 | MEMORY: 2
    'mmap_size': 1 << 26,  # 64MB
    'synchronous': 1,
}
default_charset: str = 'UTF-8'


class SQLiteEntry:
    """ 和 SQLite3 数据库交互的接口，完成链接的打开和释放 """

    def __init__(
            self,
            path: str,
            name: str,
            isolation: Optional[str] = None,
            timeout: int = 5,
            pragmas: Optional[Dict[str, Any]] = None,
    ) -> None:
        """

        Args:
            path: sqlite 文件所在的目录
            name: sqlite 文件名
            isolation: 事务级别
            timeout: sqlite 链接的超时时间
            pragmas: sqlite3 链接的选项
            **kwargs: connection 参数
        """
        self.path: str = path
        self.name: str = name
        self.timeout: int = timeout
        self.__context: local = local()
        self.__txn: Optional[int] = None
        self.__connect_configure: Dict[str, Any] = {
            'database': op.join(self.path, self.name),
            'isolation_level': isolation,
            'timeout': timeout
        }
        if not isinstance(pragmas, (dict, NoneType)):
            raise TypeError(
                f'pragmas want dict object but get {type(pragmas)}'
            )
        self.__pragmas_sql: str = ';'.join(
            'PRAGMA %s=%s' % item for
            item in (pragmas or default_pragmas).items()
        )
        if not self.created:
            init_cache_statements: List[str] = [
                # cache table
                'CREATE TABLE IF NOT EXISTS `cache`('
                '`key` BLOB NOT NULL, '
                '`kf` INTERGER NOT NULL, '
                '`value` BLOB, '  # cache accept NULL, (None)
                '`vf` INTEGER NOT NULL, ' 
                '`tag` BLOB, '  # accept None
                '`store` REAL NOT NULL,'
                '`expire` REAL,'
                '`access` REAL NOT NULL,'
                '`access_count` INTEGER DEFAULT 0)',

                # create index
                'CREATE UNIQUE INDEX IF NOT EXISTS `idx_key` '
                'ON `cache`(`key`, `tag`)',

                # create info table
                'CREATE TABLE IF NOT EXISTS `info`('
                '`key` BLOB NOT NULL, '
                '`value` BLOB'
                ')',

                # set count = 0
                'CREATE UNIQUE INDEX IF NOT EXISTS `idx_info_key` '
                'ON `info`(`key`)',

                #
                'INSERT INTO `info`(`key`, `value`) VALUES ("count", 0), ("evict", "lru")',
            ]

            init_script: str = ';'.join(init_cache_statements)
            _ = self.session.executescript(init_script)

    @property
    def created(self) -> bool:
        row = self.session.execute(
            r'SELECT COUNT(*) FROM sqlite_master '
            r'WHERE `type` = ? AND `name` = ?',
            ('table', 'cache')
        ).fetchone()
        (count, ) = row
        return count == 1

    @property
    def session(self) -> Connection:
        local_pid: int = getattr(self.__context, 'pid', -1)
        current_pid: int = getpid()
        if local_pid != current_pid:
            self.close()
            self.__context.pid = current_pid
        session: Optional[Connection] = getattr(
            self.__context, 'session', None
        )
        if session is None:
            session = self.__context.session = Connection(
                **self.__connect_configure
            )
            start: Time = current()
            while True:
                try:
                    session.executescript(self.__pragmas_sql)
                    break
                except OperationalError as exc:
                    if str(exc) == 'database is locked':
                        raise
                    diff: Time = current() - start
                    if diff > 60:
                        raise
                    sleep(0.001)
            setattr(self.__context, 'session', session)
        return session

    def close(self) -> bool:

        session: Connection = getattr(self.__context, 'session', None)
        if session is None:
            return True
        session.close()
        setattr(self.__context, 'session', None)
        return True

    @contextmanager
    def transact(self, retry: bool = True) -> QY:
        sql: QY = self.session.execute
        tid: int = get_ident()
        txn_id: Optional[int] = self.__txn

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
                        f'Transact timeout. (timeout={self.timeout}).'
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

    def config(self, key: str, value: Any = empty) -> Any:

        # get
        if value is empty:
            (value,) = self.session.execute(
                f'SELECT `value` FROM `info` WHERE `key` = ?',
                ('evict', )
            ).fetchone()
            return value

        # set
        else:
            return self.session.execute(
                f'UPDATE `info` SET value = ? '
                f'WHERE `key` = ?',
                (value, key)
            ).rowcount == 1


class PickleStore:

    def __init__(
            self,
            directory: str,
            protocol: int,
            raw_max_size: int,
            charset: str = default_charset,
    ) -> None:
        self.directory: str = directory
        self.protocol: int = protocol
        self.raw_max_size: int = raw_max_size
        self.charset: str = charset

    @staticmethod
    def signature(data: bytes) -> str:
        return md5(data).hexdigest()
    
    def dumps(self, data: Any) -> Tuple[Any, int]:

        tp: Type = type(data)

        # string
        if tp is str:
            if len(data) < self.raw_max_size:
                return data, RAW
            byte_data: bytes = data.encode(self.charset)
            sig: str = self.signature(byte_data)
            self.write(sig, byte_data)
            return sig, STRING

        # inf / float
        if tp in (int, float):
            return data, NUMBER

        # bytes
        if tp is bytes:
            if len(data) / 8 < self.raw_max_size:
                return data, BYTES
            return self.signature(data), BYTES
        
        # pickle
        pickled: bytes = pickle.dumps(data, protocol=self.protocol)
        if len(pickled) / 8 < self.raw_max_size:
            return pickled, PICKLE
        sig: str = self.signature(pickled)
        self.write(sig, pickled)
        return sig, PICKLE

    def loads(self, dump: Any, fmt: int) -> Any:
        if fmt in (RAW, NUMBER):
            return dump
        if fmt == PICKLE:
            if isinstance(dump, str):
                dump: bytes = self.read(dump)
            return pickle.loads(dump)
        data: bytes = self.read(dump)
        if fmt == BYTES:
            return data
        if fmt == STRING:
            return data.decode(self.charset)

    def write(self, sig: str, data: bytes) -> None:
        file: str = op.join(self.directory, sig)
        if not op.exists(file):
            return None
        with open(file, 'wb') as fd:
            _ = fd.write(data)

    def read(self, sig: str) -> Optional[AnyStr]:

        file: str = op.join(self.directory, sig)
        # FIXME: the value reference by many key(s)
        if not op.exists(file):
            warnings.warn('')
            return None
        with open(file, 'rb') as fd:
            return fd.read()


def get_expire(timeout: Time, now: Time = None) -> Time:
    if timeout is None:
        return None
    return (now or current()) + timeout


LRU: int = 0
LFU: int = 1
FIFO: int = 2


class EvictInterface(abc.ABC):

    name: str = ''

    @abc.abstractmethod
    def apply(self, sql: QY) -> bool:
        """"""

    @abc.abstractmethod
    def unapply(self, sql: QY) -> bool:
        """"""

    @abc.abstractmethod
    def evict(self, sql: QY, count: int) -> int:
        """"""


class LRUEvict(EvictInterface):

    name: str = 'lru'

    def apply(self, sql: QY) -> bool:
        return sql(
            'CREATE INDEX IF NOT EXISTS idx_lru '
            'ON cache(`access`)'
        ).rowcount == 1

    def unapply(self, sql: QY) -> bool:
        return sql(
            'DROP INDEX IF EXISTS idx_lru'
        ).rowcount == 1

    def evict(self, sql: QY, count: int) -> int:
        return sql(
            'DELETE FROM `cache` '
            'WHERE `rowid` IN ('
            '    SELECT `rowid` '
            '    FROM `cache` '
            '    ORDER BY `access` LIMIT ?'
            ')',
            (count,)
        ).rowcount


class LFUEvict(EvictInterface):

    name: str = 'lfu'

    def apply(self, sql: QY) -> bool:
        return sql(
            'CREATE INDEX IF NOT EXISTS idx_lfu '
            'ON cache(`access`)'
        ).rowcount == 1

    def unapply(self, sql: QY) -> bool:
        return sql(
            'DROP INDEX IF EXISTS idx_lfu'
        ).rowcount == 1

    def evict(self, sql: QY, count: int) -> int:
        return sql(
            'DELETE FROM `cache` '
            'WHERE `rowid` IN ('
            '    SELECT `rowid` '
            '    FROM `cache` '
            '    ORDER BY `access_count` LIMIT ?'
            ')',
            (count,)
        ).rowcount


class FIFOEvict(EvictInterface):

    name: str = 'fifo'

    def apply(self, sql: QY) -> bool:
        return sql(
            'CREATE INDEX IF NOT EXISTS idx_fifo '
            'ON cache(`store`)'
        ).rowcount == 1

    def unapply(self, sql: QY) -> bool:
        return sql(
            'DROP INDEX IF EXISTS idx_fifo'
        ).rowcount == 1

    def evict(self, sql: QY, count: int) -> int:
        return sql(
            'DELETE FROM `cache` '
            'WHERE `rowid` IN ('
            '    SELECT `rowid` '
            '    FROM `cache` '
            '    ORDER BY `store` LIMIT ?'
            ')',
            (count,)
        ).rowcount


class EvictManager(dict):

    def register(self, evict: Type[EvictInterface]) -> None:
        if evict.name in self:
            raise Cache3Error(
                f'has been registered evict named {evict.name!r}'
            )
        self[evict.name] = evict

    def __missing__(self, key) -> None:
        raise Cache3Error(
            f'no register evict policy named {key!r}'
        )


evict_manager = EvictManager({
    LRUEvict.name: LRUEvict,
    LFUEvict.name: LFUEvict,
    FIFOEvict.name: FIFOEvict,
})



class DiskCache:

    def __init__(
            self,
            directory: str = '~/.cache3',
            name: str = 'cache.sqlite3',
            max_size: int = 1 << 30,
            evict_policy: str = 'lru',
            evict_size: int = 1 << 7,
            **kwargs,
    ) -> None:
        self.directory: str = op.expandvars(op.expanduser(directory))
        if not op.exists(self.directory):
            makedirs(self.directory, exist_ok=True)
        self.name: str = name
        self.max_size: int = max_size
        self.evict_size: int = evict_size
        self.iter_size: int = 1 << 7
        # sqlite
        self.sqlite: SQLiteEntry = SQLiteEntry(
            path=self.directory, name=name, **kwargs.pop('sqlite_config', {})
        )
        self.evict: EvictInterface = self.config_evict(evict_policy)

        # pickle storage
        self.store = PickleStore(
            directory=self.directory,
            protocol=pickle.HIGHEST_PROTOCOL,
            raw_max_size=10,
            **kwargs.pop('store_config', {})
        )

    def config_evict(self, evict_policy: str) -> EvictInterface:

        pre_evict_policy: str = self.sqlite.config('evict')
        sql: QY = self.sqlite.session.execute
        evict: EvictInterface = evict_manager[evict_policy]()
        if pre_evict_policy != evict_policy:
            pre_evict: EvictInterface = evict_manager[pre_evict_policy]()
            pre_evict.unapply(sql)
            evict.apply(sql)
            self.sqlite.config('evict', evict_policy)
        return evict

    def set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:

        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row = sql(
                'SELECT `rowid`'
                'FROM `cache`'
                'WHERE `key` = ? AND `tag` IS ?',
                (sk, tag)
            ).fetchone()
            sv, vf = self.store.dumps(value)

            # key existed but it is expired
            if row:
                (rowid, ) = row
                if self._update_row(sql, rowid, sv, vf, timeout, tag):
                    return True
            # key not found in cache
            else:
                if self._create_row(sql, sk, kf, sv, vf, timeout, tag):
                    self._add_count(sql)
                    self.try_evict(sql)
                else:
                    return False
        return True

    def get(self, key: Any, default: Any = None, tag: TG = None) -> Any:
        sk, _ = self.store.dumps(key)
        sql = self.sqlite.session.execute
        row = sql(
            'SELECT `rowid`, `value`, `expire`, `vf` '
            'FROM `cache` '
            'WHERE `key` = ? AND `tag` IS ?',
            (sk, tag)
        ).fetchone()

        if not row:
            # not found key in cache
            return default

        (rowid, sv, expire, vf) = row
        now: Time = current()
        if expire is not None and expire < now:
            self._sub_count(sql)
            # key has expired
            return default

        sql(
            'UPDATE `cache` '
            'SET `access_count` = `access_count` + 1, `access` = ? '
            'WHERE `rowid` = ?',
            (now, rowid)
        )
        return self.store.loads(sv, vf)

    def get_many(self, keys: List[Any], tag: TG = None) -> Dict[Any, Any]:
        """ There is a limitation on obtaining a group of key values.

        TODO WARNING: the tags of this group of key values must be consistent,
            but the memory based cache strategy does not have this limitation.
            This feature will be supported in the future to ensure the
            consistency of behavior
        """

        sks: List[Any] = [self.store.dumps(key)[0] for key in keys]
        snap: str = str('?, ' * len(sks)).strip(', ')
        rs: Cursor = self.sqlite.session.execute(
            'SELECT `key`, `value`, `vf` FROM `cache`'
            'WHERE `key` IN (%s) AND `tag` IS ? ' % snap,
            (*sks, tag)
        )

        vs: dict = {sk: self.store.loads(sv, vf) for sk, sv, vf in rs}
        result: dict = dict()
        for idx, key in enumerate(keys):
            v = vs.get(sks[idx], empty)
            if v is not empty:
                result[key] = v
        return result

    def incr(self, key: Any, delta: Union[int, float] = 1, tag: TG = None) -> Union[int, float]:
        """ int, float and (str/bytes) not serialize, so add in sql statement.

        The increment operation should be implemented through SQLite,
        which is not safe at the Python language level

        Returns:
            increment result value else raise 'Cache3Error' error.
        """

        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row: ROW = sql(
                'SELECT `value`, `vf` FROM `cache` '
                'WHERE `key` = ? AND `tag` IS ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (sk, tag, current())
            ).fetchone()
            if not row:
                raise KeyError(f'key {key!r} not found')

            sv, vf = row
            value = self.store.loads(sv, vf)
            if vf != NUMBER:
                raise TypeError(
                    f'unsupported operand type(s) for +: {type(value)!r} and {type(delta)!r}'
                )
            for i in range(3):
                rowcount: int = sql(
                    'UPDATE `cache` SET `value`= `value` + ? '
                    'WHERE `key` = ? '
                    'AND `tag` IS ?',
                    (delta, sk, tag)
                ).rowcount
                if rowcount == 1:
                    break
            else:
                raise Cache3Error(
                    f'The increment operation to the {key!r} failed'
                )
        return value + delta

    def decr(self, key: Any, delta: Union[int, float]) -> Union[int, float]:
        return self.incr(key, -delta)

    @staticmethod
    def _sub_count(sql: QY) -> bool:
        return sql(
            'UPDATE `info` SET `value` = `value` - 1 '
            'WHERE `key` = "count"',
        ).rowcount == 1

    @staticmethod
    def _add_count(sql: QY) -> bool:
        return sql(
            'UPDATE `info` SET `value` = `value` + 1 '
            'WHERE `key` = "count"'
        ).rowcount == 1

    @staticmethod
    def _update_row(sql: QY, rowid: int, sv: Any, vf: int, timeout: Time, tag: TG) -> bool:
        now: Time = current()
        expire: Time = get_expire(timeout, now)
        return sql(
            'UPDATE `cache` SET '
            '`value` = ?, '
            '`vf` = ?, '
            '`tag` = ?, '
            '`store` = ?, '
            '`expire` = ?, '
            '`access` = ?, '
            '`access_count` = ? '
            'WHERE `rowid` = ?',
            (sv, vf, tag, now, expire, now, 0, rowid)
        ).rowcount == 1

    @staticmethod
    def _create_row(sql: QY, sk: Any, kf: int, sv: Any, vf: int, timeout: Time, tag: TG) -> bool:

        now: Time = current()
        expire: Time = get_expire(timeout, now)
        return sql(
            'INSERT INTO `cache`('
            '`key`, `kf`, `value`, `vf`, `tag`, `store`, `expire`, `access`, `access_count`'
            ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (sk, kf, sv, vf, tag, now, expire, now, 0)
        ).rowcount == 1

    def clear(self) -> bool:
        """ Delete all data and initialize the statistics table. """

        with self.sqlite.transact() as sql:
            sql(
                'DELETE FROM `cache`;'
            )
            # Delete all data and initialize the statistics table.
            # Since the default `rowid` is used as the primary key,
            # you don't need to care whether the `rowid` starts from
            # 0. Even if the ID is full, SQLite will select an
            # appropriate value from the unused rowid set.

            sql(
                'UPDATE `info` SET `value` = 0 '
                'WHERE `key` = "count"'
            )
        return True

    @cached_property
    def location(self) -> str:
        return (Path(self.directory) / self.name).as_posix()

    def ttl(self, key: Any, tag: TG = None) -> Time:
        sk, _ = self.store.dumps(key)
        row: ROW = self.sqlite.session.execute(
            'SELECT `expire` '
            'FROM `cache` '
            'WHERE `key` = ? '
            'AND `tag` IS ? '
            'AND `expire` IS NULL OR `expire` > ?',
            (sk, tag, current())
        ).fetchone()
        if not row:
            return -1
        (expire, ) = row
        if expire is None:
            return None
        return expire - current()

    def delete(self, key: Any, tag: TG = None) -> bool:

        sk, _ = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            success: bool = sql(
                'DELETE FROM `cache` '
                'WHERE `key` = ? AND `tag` IS ? ',
                (sk, tag)
            ).rowcount == 1
            if success:
                self._sub_count(sql)
        return success

    def inspect(self, key: Any, tag: TG = None) -> Optional[Dict[str, Any]]:
        """ Get the details of the key value, including any information,
        access times, recent access time, etc., and even the underlying
        serialized data
        """

        sk, _ = self.store.dumps(key)
        cursor = self.sqlite.session.execute(
            'SELECT * '
            'FROM `cache` '
            'WHERE `key` = ? AND `tag` IS ?',
            (sk, tag)
        )
        cursor.row_factory = lambda _cursor, _row: {col[0]: _row[idx] for idx, col in enumerate(_cursor.description)}
        row: Optional[Dict[str, Any]] = cursor.fetchone()
        if row:
            row['sk'] = row['key']
            row['key'] = self.store.loads(row['key'], row['kf'])
            row['sv'] = row['value']
            row['value'] = self.store.loads(row['value'], row['vf'])
            return row

    def pop(self, key: Any, default: Any = None, tag: TG = None) -> Any:

        sql: QY = self.sqlite.session.execute
        sk, kf = self.store.dumps(key)
        row: ROW = sql(
            'SELECT `rowid`, `value`, `vf` '
            'FROM `cache` '
            'WHERE `key` = ? '
            'AND `tag` IS ? '
            'AND (`expire` IS NULL OR `expire` > ?)',
            (sk, tag, current())
        ).fetchone()
        # return the default value if not found key in cache
        if not row:
            return default
        rowid, sv, vf = row
        value = self.store.loads(sv, vf)
        for i in range(3):
            success: bool = sql(
                'DELETE FROM `cache` '
                'WHERE `rowid` == ? ',
                (rowid, )
            ).rowcount == 1
            if success:
                _ = self._sub_count(sql)
                break
        else:
            raise Cache3Error(
                f'pop error, delete key: {key!r} from cache failed'
            )
        return value

    def flush_length(self, now: Time = None) -> None:
        now = now or current()
        self.sqlite.session.execute(
            'UPDATE `info` SET `value` = ('
            'SELECT COUNT(1) FROM `cache` '
            'WHERE `expire` IS NULL OR `expire` > ? '
            ') WHERE `key` = "count"', (now,)
        )

    @property
    def length(self) -> int:
        stamp = getattr(self, '_length', 0)
        now = current()
        if stamp + 2 < now:
            self.flush_length(now)
            setattr(self, '_length', now)
        return len(self)
    
    def has_key(self, key: Any, tag: TG = None) -> bool:
        sk, kf = self.store.dumps(key)
        return bool(self.sqlite.session.execute(
            'SELECT 1 FROM `cache` '
            'WHERE `key` = ? '
            'AND `tag` IS ? '
            'AND (`expire` IS NULL OR `expire` > ?)',
            (sk, tag, current())
        ).fetchone())

    def touch(self, key: str, timeout: Time = None, tag: TG = None) -> bool:
        """ Renew the key. When the key does not exist, false will be returned """
        now: Time = current()
        new_expire: Time = get_expire(timeout, now)
        sk, _ = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            return sql(
                'UPDATE `cache` SET `expire` = ? '
                'WHERE `key` = ? '
                'AND `tag` IS ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (new_expire, sk, tag, now)
            ).rowcount == 1

    def memoize(self, tag: TG = None, timeout: Time = 24 * 60 * 60) -> Any:
        """ The cache is decorated with the return value of the function,
        and the timeout is available. """

        if callable(tag):
            raise TypeError(
                "Mame cannot be callable. ('@cache.memoize()' not '@cache.memoize')."
            )

        def decorator(func) -> Callable[[Callable[[Any], Any]], Any]:
            """ Decorator created by memoize() for callable `func`."""

            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                """Wrapper for callable to cache arguments and return values."""
                value: Any = self.get(func.__name__, empty, tag)
                if value is empty:
                    value: Any = func(*args, **kwargs)
                    self.set(func.__name__, value, timeout, tag)
                return value
            return wrapper

        return decorator

    def ex_set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:

        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row = sql(
                'SELECT `rowid`, `expire` '
                'FROM `cache` '
                'WHERE `key` = ? '
                'AND `tag` IS ?',
                (sk, tag)
            ).fetchone()

            if row:
                (rowid, expire) = row
                if expire and expire > current():
                    return False
                sv, vf = self.store.dumps(value)
                return self._update_row(sql, rowid, sv, vf, timeout, tag)
            else:
                sv, vf = self.store.dumps(value)
                if self._create_row(sql, sk, kf, sv, vf, timeout, tag):
                    self._add_count(sql)
                    self.evict(sql)
                else:
                    return False
        return True

    def iter(self, tag: TG = None) -> Iterable[Tuple[Any, Any]]:

        count, index = self.iter_size, 0
        now: Time = current()
        sql = self.sqlite.session.execute
        while count >= self.iter_size:
            count: int = 0
            for line in sql(
                'SELECT `key`, `value` '
                'FROM `cache` '
                'WHERE `tag` IS ? '
                'AND (`expire` IS NULL OR `expire` > ?) '
                'ORDER BY `store` '
                'LIMIT ? OFFSET ?',
                (tag, now, self.iter_size, index * self.iter_size)
            ):
                count += 1
                yield line
            index += 1

    def try_evict(self, sql) -> None:
        now: Time = current()
        pre_evict: Time = getattr(self, '_evict', 0)
        if pre_evict + 2 <= now:
            if self.length < self.max_size:
                return
            sql(
                'DELETE FROM `cache` '
                'WHERE `expire` IS NOT NULL '
                'AND `expire` < ?',
                (now,)
            )
            self.flush_length(now)
            if len(self) < self.max_size:
                return
            _ = self.evict.evict(sql, self.evict_size)
            setattr(self, '_evict', current())

    def __iter__(self) -> Iterable[Tuple[Any, Any, str]]:
        """ Will take up a lot of memory, which needs special
        attention when there are a lot of data.
        Conservatively, this method is not recommended to be called
        It is only applicable to those scenarios with small data scale
        or for testing.
        """
        now: Time = current()
        n: int = self.length // self.iter_size + 1
        sql: QY = self.sqlite.session.execute
        for i in range(n):
            for line in sql(
                'SELECT `key`, `kf`, `value`, `vf`, `tag` '
                'FROM `cache` '
                'WHERE (`expire` IS NULL OR `expire` > ?) '
                'ORDER BY `store`'
                'LIMIT ? OFFSET ?',
                (now, self.iter_size, self.iter_size * i)
            ):
                if line:
                    sk, kf, sv, vf, tag = line
                    yield self.store.loads(sk, kf), self.store.loads(sv, vf), tag

    def __len__(self) -> int:
        (length, ) = self.sqlite.session.execute(
            'SELECT `value` '
            'FROM `info` '
            'WHERE `key` = "count"',
        ).fetchone()
        return length

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.location}>'

    def __setitem__(self, key, value) -> None:
        if self.set(key, value):
            raise Cache3Error(f'set {key!r} failed')

    def __getitem__(self, key) -> Any:
        return self.get(key)

    def __delete__(self, instance) -> None:
        self.delete(instance)
