#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/9/15
# author: clarkmonkey@163.com

""" DiskCache

SQLite and file system based caching.

Provides the APIs of general caching systems. 
At the same time, all data is stored in the disk (sqlite or data file), 
so the inter-process data is safe and durable.

The pickle protocol is used to complete the serialization and deserialization 
of data. Can satisfy most objects in python.

The cache eviction policies are optional, and currently supports LRU, LFU,
and FIFO. If these don't meet your needs, you can register your own cache 
eviction policies by `evict_manager.register`.
""" 

import pickle
import sys
from abc import ABC, abstractmethod, abstractproperty
from contextlib import contextmanager
from hashlib import md5
from multiprocessing import Lock
from os import makedirs, getpid, path as op
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, OperationalError
from threading import local, get_ident
from time import time as current, sleep
from typing import Type, Optional, Dict, Any, List, Tuple, Callable, Iterable
from .util import (
    MultiCache, cached_property, lazy, Time, empty, TG, Number, get_expire, memoize, Cache3Error, 
    rmfile, warning, 
)
from . import version
from . import locks
from .migrate import migrate

QY = Callable[[Any], Cursor]
ROW = Optional[Tuple[Any,...]]
# Represents the data stored in SQLite in its original format.
RAW: int = 0
# Indicates that the current data is a number (int/float).
NUMBER: int = 1
# Indicates that the data format is str.
STRING: int = 2
# Represents the data format as bytes.
BYTES: int = 3
# Indicates that Pickle objects are stored. Because the types stored are not supported by SQLite, 
# they need to be serialized into binary.
PICKLE: int = 4

_RAW: int = 0
_PICKLE: int = 1
_FILE_BYTES: int = 1
_FILE_STRING: int = 2
_FILE_PICKLE: int = 3

if sys.version_info > (3, 10):
    from types import NoneType
else:
    NoneType: Type = type(None)

# Default filesystem charset.
DEFAULT_CAHRSET: str = 'UTF-8'
# Default cache storage path.
DEFAULT_DIRECTORY: str = '~/.cache3'
# Default sqlite3 filename.
DEFAULT_NAME: str = 'default.sqlite3'
# 128KB
DEFAULT_RAW_MAX_SIZE: int = 1 << 17
# SQLite pragma configs.
DEFAULT_PRAGMAS: Dict[str, Any] = {
    'cache_size': 1 << 13,  # 8, 192 pages
    'journal_mode': 'wal',
    'threads': 4,  # SQLite work threads count
    'temp_store': 2,  # DEFAULT: 0 | FILE: 1 | MEMORY: 2
    'mmap_size': 1 << 26,  # 64MB
    'synchronous': 1,
}
# When fetching all items, it will block sqlite for a long time, especially when the data is large, 
# so getting `DEFAULT_OFFSET` items at a time and getting it multiple times will avoid this bad 
# situation.
DEFAULT_OFFSET: int = 128


class SQLiteManager:
    """ SQLite3 database interaction interface, responsible for connection management
    and transaction commission.
    """

    def __init__(
        self,
        path: str,
        name: str,
        isolation: Optional[str],
        pragmas: Optional[Dict[str, Any]] = None,
    ) -> None:
        """ Prepare the connection parameter, generate and cache the Pragmas SQL statement, create 
        a cache table, and create a database index.

        Args:
            path (str): 
                The path where the database file is saved.
            
            name (str): 
                The database file name.
            
            isolation (Optional[str]): 
                SQLite3 transaction isolation level.
            
            pragmas (Optional[Dict[str, Any]], default: None): 
                SQLite3 connection pragmas.

        Raises:
            TypeError: When the pragmas passed in is not None or a dictionary.
        """

        self.path: str = path
        self.name: str = name
        self.timeout: int = 10 * 60
        self.__local: local = local()
        self.__txn: Optional[int] = None
        self._txn_id: Optional[int] = None
        # connection configuations
        self.__conn_conf: Dict[str, Any] = {
            'database': op.join(self.path, self.name),
            'isolation_level': isolation,
            'timeout': self.timeout
        }
        if not isinstance(pragmas, (dict, NoneType)):
            raise TypeError(
                f'pragmas want dict object but get {type(pragmas)}'
            )
        # general and cached sqlite connect pragmas statment
        self.__pragmas_sql: str = ';'.join(
            f'PRAGMA {item[0]}={item[1]}' for
            item in (pragmas or DEFAULT_PRAGMAS).items()
        )
        
        if not self.created:
            init_cache_statements: List[str] = [
                # cache table
                'CREATE TABLE IF NOT EXISTS `cache`('
                '`key` BLOB NOT NULL,'
                '`kf` INTERGER NOT NULL,'
                '`value` BLOB,'  # cache accept NULL, (None)
                '`vf` INTEGER NOT NULL,' 
                '`store` REAL NOT NULL,'
                '`expire` REAL,'
                '`access` REAL NOT NULL,'
                '`access_count` INTEGER DEFAULT 0)',

                # create index
                'CREATE UNIQUE INDEX IF NOT EXISTS `idx_key` '
                'ON `cache`(`key`, `kf`, `expire`)',        
            ]
            init_script: str = ';'.join(init_cache_statements)
            _ = self.session.executescript(init_script)
            self.create_meta(str(version.VERSION))
        else:
            self.migrate()
            
    def migrate(self) -> None:
        try:
            (pre, ) = self.session.execute(
                'SELECT `version` FROM `meta` LIMIT 1'
            ).fetchone()
            try:
                major: str = int(pre.split('.')[0])
            except ValueError as exc:
                raise RuntimeError(f'invalid version: {pre!r}') from exc
            return migrate(int(major))
        except OperationalError as exc:
            # not create meta table
            if str(exc) != 'no such table: meta':
                raise RuntimeError(f'occurent unknown error: {exc!r}') from exc
            # not found version
            pre = 0
            migrate(pre)
            self.create_meta(str(version.VERSION))
    
    def create_meta(self, version: str) -> None:
        with self.transact(retry=3) as sql:
            sql('CREATE TABLE IF NOT EXISTS `meta`(`version` TEXT)')
            sql('INSERT INTO `meta`(`version`) VALUES(?)', (version,))
    
    @property
    def created(self) -> bool:
        """ Determine whether the sqlite schema is created. """

        row = self.session.execute(
            r'SELECT COUNT(*) FROM sqlite_master '
            r'WHERE `type` = ? AND `name` = ?',
            ('table', 'cache')
        ).fetchone()
        (count, ) = row
        return count == 1

    @property
    def session(self) -> Connection:
        """ Create a sqlite connection, every time you get a connection, you need to judge
        whether the current connection is independently occupied by a single thread.
        """

        local_pid: int = getattr(self.__local, 'pid', -1)
        current_pid: int = getpid()
        if local_pid != current_pid:
            self.close()
            self.__local.pid = current_pid
        session: Optional[Connection] = getattr(
            self.__local, 'session', None
        )
        if session is None:
            session = self.__local.session = Connection(
                **self.__conn_conf
            )
            start: Time = current()
            # try to create connection
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
            # cached connection
            setattr(self.__local, 'session', session)
        return session

    def close(self) -> bool:
        """ Close current active connection. """

        session: Connection = getattr(self.__local, 'session', None)
        if session is None:
            return True
        session.close()
        setattr(self.__local, 'session', None)
        return True

    @contextmanager
    def transact(self, retry: bool = True) -> QY:
        """ A context manager that will open a SQLite transaction.

        Args:
            retry: Whether to retry until success after a failed transaction rollback.

        Returns:
            return a handle to the transaction.
        """

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
        if begin:
            assert self._txn_id == tid
            self._txn_id = None
            sql('COMMIT')


class PickleStore:
    """ Determines how Python objects are stored in the DiskCache.

    1) Simple objects will be stored in a way that SQLite natively supports,
    2) Large or unsupported objects will be converted to Pickle objects and
    stored as bytecode.
    """

    def __init__(
        self,
        directory: str,
        raw_max_size: int = DEFAULT_RAW_MAX_SIZE,
        charset: str = DEFAULT_CAHRSET,
    ) -> None:
        """ All data is stored in SQLite, so when the key or value is large, it will greatly 
        reduce the read and write efficiency of the cache, especially the read efficiency. As a 
        result, excessively large data is stored in a file, and only the hash value of the file 
        is recorded in the SQLite table. This can greatly improve the efficiency of SQLite reads 
        and writes.
        
        At the same time, in order to support most of the data formats in Python, the Pickle 
        protocol is used to convert complex objects into binary objects, store them in SQLite, 
        and directly deserialize them when reading. Simple types that can be supported by SQLite 
        do not need to do so.

        Args:
            
            directory (str): 
                The directory where the large item is stored when it is converted into a file.
            
            raw_max_size (int, default: DEFAULT_RAW_MAX_SIZE): 
                When the size of an item is greater than x, it is saved in a file.
            
            charset (Optional[str], default: DEFAULT_CAHRSET): 
                Larger strings are converted to binary characters when they are used.
        """

        self.directory: str = directory
        self.protocol: int = pickle.HIGHEST_PROTOCOL
        self.raw_max_size: int = raw_max_size
        self.charset: str = charset

    @staticmethod
    def signature(data: bytes) -> str:
        return md5(data).hexdigest()
    
    def dumps(self, data: Any) -> Tuple[Any, int]:
        """ Serialize `data` to storage formatted. 
        """

        tp: Type = type(data)

        if tp in (int, float, NoneType):
            return data, RAW
        
        if tp is str:
            if len(data) < self.raw_max_size:
                return data, RAW
            else:
                data = data.encode(self.charset)
                sig: str = self.write(data)
                return sig, _FILE_STRING
                
        if tp is bytes:
            if len(data) < self.raw_max_size:
                return data, RAW
            else:        
                sig: str = self.write(data)
                return sig, _FILE_BYTES

        pickled: bytes = pickle.dumps(data, protocol=self.protocol)
        if len(pickled) < self.raw_max_size:
            return pickled, _PICKLE

        sig: str = self.write(pickled)
        return sig, _FILE_PICKLE

    def loads(self, dump: Any, fmt: int) -> Any:
        """ Deserialize `dump` to Python object.
        """
        if fmt == _RAW:
            return dump
        
        if fmt == _PICKLE:
            return pickle.loads(dump)
        
        data: bytes = self.read(dump)
        if data is None:
            # TODO not found cached file
            return empty
        
        if fmt == _FILE_BYTES:
            return data
        
        if fmt == _FILE_STRING:
            return data.encode(self.charset)
        
        if fmt == _FILE_PICKLE:
            return pickle.loads(data)

    def _lock(self, f, retry: int = 10) -> bool:
        
        flags: int = locks.LOCK_EX|locks.LOCK_NB
        if locks.lock(f, flags):
            return True
        
        for i in range(retry):
            sleep(i * 0.01)
            if locks.lock(f, flags):
                return True
        return False
    
    def _update_index(self, file: str, delta: int = 1) -> bool:

        index_file: str = f'{file}.index'
        with open(index_file, 'r+', encoding=self.charset) as fd:
            
            if not self._lock(fd):
                return False
            
            try:
                count: int = int(fd.readline())
                fd.seek(0)
                fd.write(f'{max(count + delta, 0)}')
            
            except ValueError:
                warning(f'bad index of cached file: {index_file!r}')
                fd.seek(0)
                fd.write('1')
            
            except IOError as exc:
                raise SystemError(f'occured unknown error: {exc}') from exc

            finally:
                locks.unlock(fd)
    
    def write(self, data: bytes) -> str:
        """ Write data to local file.
        """
        import os

        sig: str = self.signature(data)
        file: str = op.join(self.directory, sig)
        
        try:
            fd: int = os.open(file, os.O_CREAT|os.O_EXCL|os.O_WRONLY)
        except FileExistsError:
            # found file 
            self._update_index(file)

        if not self._lock(fd):
            raise SystemError(f'cannot write data to cached file:{file!r}')
        
        try:
            size: int = os.write(fd, data)
            if size!= len(data):
                raise IOError(f'failed to write data to file, write:{size!r}')
        finally:
            locks.unlock(fd)
            os.close(fd)
        return sig

    def read(self, sig: str) -> Optional[bytes]:
        """ Read data from a local file, when the cache file corresponding to the sig does not 
        exist, return None otherwise return the contents of the file.

        Args:
            sig (str): The md5 value of the data, i.e. the local file name.

        Returns:
            Optional[bytes]: Bytes of data stored in the local cache.
        """

        file: str = op.join(self.directory, sig)
        # TODO: the value reference by many key(s)
        if not op.exists(file):
            warning(f'stored file:{file} not found')
            return None
        with open(file, 'rb') as fd:
            return fd.read()

    def delete(self, sig: str) -> bool:
        """ delete cached file.

        Args:
            sig (str): cached file name.
        
        Returns:
            True if delete success else False.
        """

        cached_file: str = op.join(self.directory, sig)
        return rmfile(cached_file)


class Counter:
    """
    当总数超过 max_size时 触发清理
    当连续增加了 128 个key时 触发一次清理
    
    """

    def __init__(self, count: int, max_size: int) -> None:
        """ 自选计数器，当程序

        Args:
            count (int, default: 0): 
                计数器的初始大小
        """
        # 总计数器
        self.count: int = count
        self.spin_count: int = 0
        self.spin_limit: int = DEFAULT_OFFSET
        self.max_size = max_size
        self._lock: Lock = Lock() # type: ignore

    def add(self, n: Number = 1) -> bool:
        """ 计数器加1成功时返回True，当触发自旋清零时返回False """
        with self._lock:
            self.count += n
            self.spin_count += n
            if self.max_size is not None and self.count > self.max_size:
                # 重新计数
                self.spin_count = 0
                return False

            if self.spin_count > self.spin_limit:
                return False

            return True

    @property
    def max_size(self) -> Optional[int]:
        return self._max_size
    
    @max_size.setter
    def max_size(self, max_size: int) -> None:
        self._max_size = max_size
        self.spin_limit: int = DEFAULT_OFFSET if max_size is None else max(max_size // 100, DEFAULT_OFFSET)

    def reset(self) -> int:
        with self._lock:
            self.count = self.spin_count = 0
            
    def align(self, count: int) -> int:
        if count < 0:
            raise ValueError('count must be >= 0')
        with self._lock:
            self.count = count    

    def __len__(self) -> int:
        return self.count

    def __lt__(self, other: Number) -> bool:
        return self.count < other
    
    def __le__(self, other: Number) -> bool:
        return self.count <= other

    def __eq__(self, other: Number) -> bool:
        return self.count == other

    def __ne__(self, other: Number) -> bool:
        return self.count != other

    def __gt__(self, other: Number) -> bool:
        return self.count > other

    def __ge__(self, other: Number) -> bool:
        return self.count >= other
    
    def __str__(self) -> str:
        return str(self.count)


class Evictor(ABC):
    
    def install(self, sql: QY) -> None:
        """ Clears redundant indexes and creates the index needed for the current eviction policy. """
        created: bool = False
        indexes: List[Tuple[str]] = sql(
            'SELECT `name` FROM `sqlite_master` '
            'WHERE `type` = ? AND `name` LIKE "idx_evict_%"', 
            ('index', )
        ).fetchall()
        
        for *_, index in indexes:
            if index == f'idx_evict_{self.name}':
                created = True
                continue
            sql(f'DROP INDEX {index}')
        if not created:
            self.create_index(sql)

    @abstractproperty
    def name(self) -> str:
        """ evict name. """

    @abstractmethod
    def run(self, sql: QY, size: int) -> int:
        """ Implementing data eviction policy. 

        Args:
            sql (QY): Database execution handle.
            size (int): Maximum number of data to be eliminated at one time.

        Returns:
            int: Number of data for this eviction.
        """
        
    @abstractmethod
    def create_index(self, sql: QY) -> None:
        """ Create the required data table index. """


class LRUEvict(Evictor):
    
    name: str = 'lru'

    def create_index(self, sql: QY) -> None:
        sql(f'CREATE INDEX IF NOT EXISTS `idx_evict_{self.name}` ON `cache`(`access`, `expire`)')
    

    def run(self, sql: QY, size: int) -> int:
        """ 删除key 的同时必须清理cached file 文件 要保持一致性是一件让人头疼的事情

        Args:
            sql (QY): _description_
            size (int): _description_

        Returns:
            int: _description_
        """
        return sql(
            'DELETE FROM `cache` '
            'ORDER BY `access` ASC '
            'LIMIT ?', (size, )
        ).rowcount
        

class LFUEvict(Evictor):

    name: str = 'lfu'

    def create_index(self, sql: QY) -> None:
        sql(f'CREATE INDEX IF NOT EXISTS `idx_evict_{self.name}` ON `cache`(`access_count`, `expire`)')

    def run(self, sql: QY, size: int) -> int:
        return sql(
            'DELETE FROM `cache` '
            'ORDER BY `access_count` ASC '
            'LIMIT ?', (size, )
        ).rowcount
        

class FIFOEvict(Evictor):

    name: str = 'fifo'

    def create_index(self, sql: QY) -> None:
        sql(f'CREATE INDEX IF NOT EXISTS `idx_evict_{self.name}` ON `cache`(`store`, `expire`)')

    def run(self, sql: QY, size: int) -> int:
        return sql(
            'DELETE FROM `cache` '
            'ORDER BY `store` ASC '
            'LIMIT ?', (size, )
        ).rowcount


class DefaultEvict(Evictor):
    
    name: str = 'default'

    def run(self, sql: QY, size: int) -> int:
        return sql(
            'DELETE FROM `cache` '
            'ORDER BY `expire` ASC '
            'LIMIT ?', (size, )
        ).rowcount
    
    def create_index(self, sql: QY) -> None:
        sql(f'CREATE INDEX IF NOT EXISTS `idx_evict_{self.name}` ON `cache`(`expire`)')

_evictor_set: dict = {}


def register_disk_evictor(evictor: Type[Evictor]) -> None:
    if evictor.name in _evictor_set:
        raise Cache3Error(
            f'An evictor named {evictor.name!r} already exists.'
        )
    _evictor_set[evictor.name] = evictor()


register_disk_evictor(DefaultEvict)
register_disk_evictor(FIFOEvict)
register_disk_evictor(LRUEvict)
register_disk_evictor(LFUEvict)


def get_evictor(evict: str) -> Evictor:
    evictor: Evictor = _evictor_set.get(evict)
    if evictor is None:
        raise Cache3Error(f'unsupported evict policy: {evict!r}')
    return evictor


class MiniDiskCache:
    """ Disk cache based on sqlite and file system. """

    def __init__(   # pylint: disable=too-many-arguments
            self,
            directory: str = DEFAULT_DIRECTORY,
            name: str = DEFAULT_NAME,
            max_size: Optional[int] = None,
            evict: str = 'default',
            isolation: Optional[str] = None,
            pragmas: Optional[Dict[str, Any]] = None,
    ) -> None:
        """ Create a disk cache instance, which is concurrency-safe and will automatically delete 
        expired keys. When there are too many stored items, it will try to evict some expired and 
        less frequently used items.
        
        Even if a timeout is not set, it does not mean that the stored items are safe. When the 
        space is tight (max_size is reached), it may still be deleted.
        
        If you want all items to be safe until they expire, you can set `max_size` to None (this is 
        the default behavior). Because of this, when `max_size` is not specified, all items that do 
        not specify a timeout will be permanently retained in the cache.

        Args:

            directory: (default: ~/.cache3) The storage directory of cache files. When a key or 
            value that is too large is stored, the data will be saved on the disk in the form of 
            files, so there may be a large number of cache files in the directory.

            name: (str, default: default.sqlite3) The file name of the cache file.

            max_size: (int, default: None) The maximum number of items to hold in the cache.
            
            evict: (str, default: 'default')  

            isolation: (default: None) SQLite3 transaction isolation level.

            pragmas: (default: cache3.DEFAULT_PRAGMAS) SQLite database link configuration.
        """
        self.directory: str = op.expandvars(op.expanduser(directory))
        if not op.exists(self.directory):
            makedirs(self.directory, exist_ok=True)
        
        self.name: str = name
        self._max_size: Optional[int] = max_size
        # config sqlite session manager
        self.sqlite: SQLiteManager = SQLiteManager(
            path=self.directory, 
            name=name,
            isolation=isolation,
            pragmas=pragmas or DEFAULT_PRAGMAS,
        )
        self.evictor: Evictor = get_evictor(evict)
        self.evictor.install(self.sqlite.session.execute)
        # create runtime counter
        self._counter: Counter = Counter(self.alive_count, self.max_size)
        # config pickle storage
        self.store = PickleStore(
            directory=self.directory,
            charset=DEFAULT_CAHRSET,
        )
    
    @property
    def max_size(self) -> int:
        return self._max_size
    
    @max_size.setter
    def max_size(self, size: int) -> None:
        self._counter.max_size = size
        self._max_size = size
        
    @property
    def evict(self) -> str:
        return self.evictor.name

    @evict.setter
    def evict(self, evict: str) -> None:
        if self.evictor.name == evict:
            return
        self.evictor = get_evictor(self.evict)
        self.evictor.install(self.sqlite.session.execute)

    @cached_property
    def location(self) -> str:
        return (Path(self.directory) / self.name).as_posix()

    def set(self, key: Any, value: Any, timeout: Time = None) -> bool:
        """ Add a item to the cache. If `timeout` is not specified, the field never expires. 
        This doesn't mean that it really will never be deleted, and when a `max_size` is specified 
        and the number of items has been reached, it is still possible to be cleared to keep the 
        cache at a certain amount.
        
        This action will cause the counter to +1, and when the counter reaches a certain value, a 
        evict operation will be triggered.

        Args:
            key (Any): Stored in the key of the item.
            value (Any): Stored in the value of the item.
            timeout (Time, optional, default: None): Lifecycle of the currently stored item in the 
            cache.

        Returns:
            bool: Set operation was successful or not.
        """

        sk, kf = self.store.dumps(key)
        now: Time = current()
        with self.sqlite.transact() as sql:
            row = sql(
                'SELECT `rowid`, `expire` '
                'FROM `cache`'
                'WHERE `key` = ? AND `kf` = ?',
                (sk, kf)
            ).fetchone()
            sv, vf = self.store.dumps(value)
            # key existed but it is expired
            if row:
                (rowid, expire) = row
                expired: bool = expire is not None and expire <= now
                return self._update_row(sql, rowid, sv, vf, timeout, now, expired)
            # key not found in cache
            else:
                ok: bool = self._create_row(sql, sk, kf, sv, vf, timeout, now)
                if ok:
                    if not self._counter.add():
                        self._evict(sql)
                return ok

    def ex_set(self, key: Any, value: Any, timeout: Time = None) -> bool:
        """ Add a item to the cache when the item does not exist in the cache,
        otherwise the set operation will be cancelled.
        """

        sk, kf = self.store.dumps(key)
        now: Time = current()
        with self.sqlite.transact() as sql:
            row = sql(
                'SELECT `rowid`, `expire` '
                'FROM `cache` '
                'WHERE `key` = ? AND `kf` = ? ',
                (sk, kf)
            ).fetchone()
            if row:
                (rowid, expire) = row
                expired: bool = expire is not None and expire <= now
                if not expired:
                    return False
                sv, vf = self.store.dumps(value)
                return self._update_row(sql, rowid, sv, vf, timeout, now, expired)
            sv, vf = self.store.dumps(value)
            ok: bool = self._create_row(sql, sk, kf, sv, vf, timeout, now)
            if ok:
                if not self._counter.add():
                    self._evict(sql)
            return ok

    def get(self, key: Any, default: Any = None) -> Any:
        """ Get the value by using the key, and return default when the key does not exist in 
        the cache.
        """

        sk, kf = self.store.dumps(key)
        now: Time = current()
        with self.sqlite.transact() as sql:
            row = sql(
                'SELECT `rowid`, `value`, `vf` '
                'FROM `cache` '
                'WHERE `key` = ? AND `kf` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (sk, kf, now)
            ).fetchone()

            if not row:
                # not found key in cache
                return default

            (rowid, sv, vf) = row
            
            value: Any = self.store.loads(sv, vf)
            if value is empty:
                self._delete(sql, rowid=rowid)
                return default

            sql(
                'UPDATE `cache` '
                'SET `access_count` = `access_count` + 1, '
                '`access` = ? '
                'WHERE `rowid` = ?',
                (now, rowid)
            )
            return value

    def get_many(self, *keys: Any) -> Dict[Any, Any]:
        """ Get multiple values from the cache at once. """

        now: Time = current()
        result: Dict[Any] = {}
        with self.sqlite.transact() as sql:
            for key in keys:
                sk, kf = self.store.dumps(key)
                row = sql(
                    'SELECT `rowid`, `value`, `vf` '
                    'FROM `cache` '
                    'WHERE `key` = ? AND `kf` = ? '
                    'AND (`expire` IS NULL OR `expire` > ?)',
                    (sk, kf, now)
                ).fetchone()

                if not row:
                    # not found key in cache
                    continue
                (rowid, sv, vf) = row
                
                # cannot load value from store
                value: Any = self.store.loads(sv, vf)
                if value is empty:
                    self._delete(sql, rowid=rowid)
                    continue

                sql(
                    'UPDATE `cache` '
                    'SET `access_count` = `access_count` + 1, '
                    '`access` = ? '
                    'WHERE `rowid` = ?',
                    (now, rowid)
                )
                result[key] = value
        return result

    def _delete(sql: QY, rowid: int = None, sk: Any = None, kf: Any = None):
        
        if rowid is not None:
            sql(
                'DELETE FROM `cache` '
                'WHERE `rowid` = ?', (rowid, )
            )
        else:
            sql(
                'DELETE FROM `cache` '
                'WHERE `key` = ? AND `kf` = ?', 
                (sk, kf)
            )

    def incr(self, key: Any, delta: Number = 1) -> Number:
        """ Increases the value by delta (default 1).

        int, float and (str/bytes) not serialize, so add in sql statement.

        The increment operation should be implemented through SQLite,
        which is not safe at the Python language level.
        

        Args:
            key (Any): key literal value.
            delta (Number, default: 1): Increase in size.

        Raises:
            KeyError: if the key does not exist or has been eliminated.
            TypeError: if value is not a number type.
            Cache3Error: When the increased value fails to be written to the SQLite table.

        Returns:
            Number: The new value after the increase.
        """
        now: Time = current()
        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row: ROW = sql(
                'SELECT `rowid`, `value`, `vf` FROM `cache` '
                'WHERE `key` = ? AND `kf` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (sk, kf, now)
            ).fetchone()
            if not row:
                raise KeyError(f'key {key!r} not found in cache')

            rowid, sv, vf = row
            # only supported integer and float
            if vf != NUMBER or not isinstance(delta, (int, float)):
                raise TypeError(
                    f'unsupported operand type(s) for +/-: {type(value)!r} and {type(delta)!r}'
                )
            value = self.store.loads(sv, vf)
            if value is empty:
                self._delete(sql, rowid=rowid)
                raise KeyError(f'key {key!r} not found in cache')
            ok: bool = sql(
                'UPDATE `cache` SET '
                '`value`= `value` + ?, '
                '`access` = ?, '
                '`access_count` = `access_count` + 1 '
                'WHERE `key` = ? ',
                (delta, now, sk)
            ).rowcount == 1
            if not ok:
                raise Cache3Error(
                    f'The increment operation to the {key!r} failed'
                )
        return value + delta

    def decr(self, key: Any, delta: Number = 1) -> Number:
        return self.incr(key, -delta)

    def clear(self) -> bool:
        """ Delete all data and initialize the statistics table. """
        self.sqlite.session.execute(
            'DELETE FROM `cache`;'
        )
        # Delete all data and initialize the statistics table.
        # Since the default `rowid` is used as the primary key,
        # you don't need to care whether the `rowid` starts from
        # 0. Even if the ID is full, SQLite will select an
        # appropriate value from the unused rowid set.
        self._counter.reset()
        return True

    def ttl(self, key: Any) -> Time:
        """ Returns the key time to live.

        Returns:
            -1   : the key has been expired or not existed.
            None : never expired.
            float: life seconds.
        """
        now: Time = current()
        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row: ROW = sql(
                'SELECT `rowid`, `expire` '
                'FROM `cache` '
                'WHERE `key` = ? '
                'AND `kf` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (sk, kf, now)
            ).fetchone()
            if not row:
                return -1
            (rowid, expire, ) = row
            if expire is None:
                return None
            sql(
                'UPDATE `cache` SET '
                '`access` = ?, '
                '`access_count` = `access_count` + 1 '
                'WHERE `rowid` = ?', 
                (now, rowid)
            )
        return expire - now

    def delete(self, key: Any) -> bool:
        """ Deleting the specified item from the cache, regardless of whether it is valid or not, 
        whether it has timed out, and when the key does not exist, it returns success by default 
        without reporting an error.
        """

        sk, kf = self.store.dumps(key)
        ok: bool = self.sqlite.session.execute(
            'DELETE FROM `cache` '
            'WHERE `key` = ? AND `kf` = ?',
            (sk, kf)
        ).rowcount == 1
        if ok:
            self._counter.add(-1)
        return ok

    def inspect(self, key: Any) -> Optional[Dict[str, Any]]:
        """ Get the details of the key value, including any information,
        access times, recent access time, etc., and even the underlying
        serialized data.
        """
        now: Time = current()
        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row: Tuple = sql(
                'SELECT `rowid`, `key`, `kf`, `value`, `vf`, '
                '`store`, `expire`, `access`, `access_count` '
                'FROM `cache` '
                'WHERE `key` = ? '
                'AND `kf` = ? ',
                (sk, kf)
            ).fetchone()

            if row:
                (rowid, sk, kf, sv, vf, store, expire, access, access_count) = row

                key: Any = self.store.loads(sk, kf)
                if key is empty:
                    self._delete(sql, rowid=rowid)
                    return

                value: Any = self.store.loads(sv, vf)
                if value is empty:
                    self._delete(sql, rowid=rowid)
                    return

                sql(
                    'UPDATE `cache` SET '
                    '`access` = ?, '
                    '`access_count` = `access_count` + 1 '
                    'WHERE `rowid` = ?', 
                    (now, rowid)
                )

                return {
                    'key': key,
                    'value': value,
                    'ttl': None if expire is None else expire - now,
                    'sk': sk,
                    'kf': kf,
                    'sv': sv,
                    'vf': vf,
                    'store': store,
                    'expire': expire,
                    'access': access,
                    'access_count': access_count,
                }

    def pop(self, key: Any, default: Any = None) -> Any:
        """ Fetch the specified item from the cache and remove it from the cache. If it doesn't 
        exist, the default value is returned.

        Args:
            key (Any): 
                The key of the item that needs to be popped.
            
            default (Any, default: None): 
                This value is returned when the specified item is not found in the cache.

        Raises:
            Cache3Error: 
                The error is thrown when deleting an item from the cache (SQLite3) fails

        Returns:
            Any: The specified item
        """

        sk, kf = self.store.dumps(key)
        with self.sqlite.transact() as sql:
            row: ROW = sql(
                'SELECT `rowid`, `value`, `vf` '
                'FROM `cache` '
                'WHERE `key` = ? '
                'AND `kf` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (sk, kf, current())
            ).fetchone()
            # return the default value if not found key in cache
            if not row:
                return default
    
            rowid, sv, vf = row
            value = self.store.loads(sv, vf)
            if value is empty:
                self._delete(sql, rowid=rowid)
                return default
    
            ok: bool = sql(
                'DELETE FROM `cache` '
                'WHERE `rowid` == ? ',
                (rowid, )
            ).rowcount == 1
        
        if ok:
            self._counter.add(-1)
        else:
            raise Cache3Error(
                f'pop error, delete key: {key!r} from cache failed'
            )
        return value
    
    def exists(self, key: Any) -> bool:
        """ Return True if the key in cache else False. """

        sk, kf = self.store.dumps(key)
        return bool(self.sqlite.session.execute(
            'SELECT 1 FROM `cache` '
            'WHERE `key` = ? '
            'AND `kf` = ? '
            'AND (`expire` IS NULL OR `expire` > ?)',
            (sk, kf, current())
        ).fetchone())

    def touch(self, key: Any, timeout: Time = None) -> bool:
        """ Renew the key. When the key does not exist, false will be returned. """

        now: Time = current()
        new_expire: Time = get_expire(timeout, now)
        sk, kf = self.store.dumps(key)
        return self.sqlite.session.execute(
                'UPDATE `cache` SET '
                '`expire` = ?, '
                '`access` = ?, '
                '`access_count` = `access_count` + 1 '
                'WHERE `key` = ? '
                'AND `kf` = ? '
                'AND (`expire` IS NULL OR `expire` > ?)',
                (new_expire, now, sk, kf, now)
            ).rowcount == 1

    def keys(self) -> Iterable[Any]:
        """ 该方法不会触发项的访问计数+1也不会更新访问时间

        Returns:
            Iterable[Any]: _description_

        Yields:
            Iterator[Iterable[Any]]: _description_
        """
        now: Time = current()
        sql: QY = self.sqlite.session.execute
        pid: int = 0    # page id
        while True:
            items = sql(
                'SELECT `rowid`, `key`, `kf` '
                'FROM `cache` '
                'WHERE (`expire` IS NULL OR `expire` > ?) '
                'ORDER BY `store` '
                'LIMIT ? OFFSET ?',
                (now, DEFAULT_OFFSET, DEFAULT_OFFSET * pid)
            )
            count: int = 0
            for item in items:
                if item:
                    count += 1
                    rowid, sk, kf = item 
                    value: Any = self.store.loads(sk, kf)
                    if value is empty:
                        self._delete(sql, rowid=rowid)
                        continue
                    yield value
            if count != DEFAULT_OFFSET:
                break
            pid += 1

    def values(self) -> Iterable[Tuple]:
        now: Time = current()
        sql: QY = self.sqlite.session.execute
        pid: int = 0    # page id
        while True:
            items = sql(
                'SELECT `rowid`, `value`, `vf` '
                'FROM `cache` '
                'WHERE (`expire` IS NULL OR `expire` > ?) '
                'ORDER BY `store`'
                'LIMIT ? OFFSET ?',
                (now, DEFAULT_OFFSET, DEFAULT_OFFSET * pid)
            )
            count: int = 0
            for item in items:
                if item:
                    rowid, sv, vf = item
                    count += 1
                    value: Any = self.store.loads(sv, vf)
                    if value is empty:
                        self._delete(sql, rowid=rowid)
                        continue
                    yield value
            if count != DEFAULT_OFFSET:
                break
            pid += 1

    def items(self) -> Iterable[Tuple]:
        now: Time = current()
        sql: QY = self.sqlite.session.execute
        pid: int = 0  # page id
        while True:
            items = sql(
                'SELECT `key`, `kf`, `value`, `vf` '
                'FROM `cache` '
                'WHERE (`expire` IS NULL OR `expire` > ?) '
                'ORDER BY `store`'
                'LIMIT ? OFFSET ?',
                (now, DEFAULT_OFFSET, DEFAULT_OFFSET * pid)
            )
            count: int = 0
            for item in items:
                if item:
                    count += 1
                    rowid, sk, kf, sv, vf = item
                    key: Any = self.store.loads(sk, kf)
                    if key is empty:
                        self._delete(sql, rowid=rowid)

                    value: Any = self.store.loads(sv, vf)
                    if value is empty:
                        self._delete(sql, rowid=rowid)

                    yield key, value

            if count != DEFAULT_OFFSET:
                break
            pid += 1

    @property
    def alive_count(self) -> int:
        (length, ) = self.sqlite.session.execute(
            'SELECT COUNT(1) FROM `cache` '
            'WHERE `expire` IS NULL OR `expire` > ? ',
            (current(),)
        ).fetchone()
        return length

    @staticmethod
    def _update_row(sql: QY, rowid: int, sv: Any, vf: int, timeout: Time, now: Time, expired: bool) -> bool:
        
        expire: Time = get_expire(timeout, now)
        # has been expired
        if expired:
            info: Tuple[Any] = (sv, vf, now, expire, now, rowid)
            statement: str = (
                'UPDATE `cache` SET '
                '`value` = ?, '
                '`vf` = ?, '
                '`store` = ?, '
                '`expire` = ?, '
                '`access` = ?, '
                '`access_count` = 0 '
                'WHERE `rowid` = ?'
            )
        # not 
        else:
            info: Tuple[Any] = (sv, vf, expire, now, rowid)
            statement: str = (
                'UPDATE `cache` SET '
                '`value` = ?, '
                '`vf` = ?, '
                '`expire` = ?, '
                '`access` = ?, '
                '`access_count` = `access_count` + 1 '
                'WHERE `rowid` = ?'
            )

        return sql(statement, info).rowcount == 1

    # pylint: disable=too-many-arguments
    @staticmethod
    def _create_row(sql: QY, sk: Any, kf: int, sv: Any, vf: int, timeout: Time, store: Time) -> bool:

        expire: Time = get_expire(timeout, store)
        return sql(
            'INSERT INTO `cache`('
            '`key`, `kf`, `value`, `vf`, `store`, `expire`, `access`, `access_count`'
            ') VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (sk, kf, sv, vf, store, expire, store, 0)
        ).rowcount == 1

    def _evict(self, sql: QY) -> None:
        """_summary_

        Args:
            sql (QY): _description_
        """

        # delete expired item from cache
        now: Time = current()
        delete_count: int = sql(
            'DELETE FROM `cache` '
            'WHERE `expire` IS NOT NULL '
            'AND `expire` < ?',
            (now, )
        ).rowcount

        # update counter
        self._counter.add(-delete_count)
        if self.max_size is None:
            return

        if len(self) < self.max_size:
            return

        count: int = self.evictor.run(sql, max(self.max_size // 100, 2))
        if count != 0:
            self._counter.add(-count)

        return

    def __len__(self) -> int:
        return self._counter.count

    def __repr__(self) -> str:
        return f'<MiniDiskCache: {self.name}>'

    __delitem__ = delete
    __getitem__ = get
    __setitem__ = set
    __iter__ = keys
    __contains__ = has_key = exists
    # For forward compatibility with the reserved API.
    memoize = memoize


LazyMiniDiskCache = lazy(MiniDiskCache)


class DiskCache(MultiCache):
    """_summary_

    Args:
        MultiCache (_type_): _description_
    """
    
    def __init__(self,
        directory: str = DEFAULT_DIRECTORY,
        name: str = DEFAULT_NAME,
        max_size: Optional[int] = None,
        isolation: Optional[str] = None,
        pragmas: Optional[Dict[str, Any]] = None,
    ) -> None:
        """_summary_

        Args:
            directory (str, optional): _description_. Defaults to DEFAULT_DIRECTORY.
            name (str, optional): _description_. Defaults to DEFAULT_NAME.
            max_size (Optional[int], optional): _description_. Defaults to None.
            isolation (Optional[str], optional): _description_. Defaults to None.
            pragmas (Optional[Dict[str, Any]], optional): _description_. Defaults to None.
        """
        self.directory: str = directory
        self.name: str = name
        self.max_size: Optional[int] = max_size 
        self._isolation: Optional[str] = isolation
        self._pragmas: Optional[Dict[str, Any]] = pragmas
        self._mtx: Lock = Lock()
        self._recipes:Dict[str, MiniDiskCache] = {}
    
    @property
    def location(self) -> str:
        return self.directory
    
    def get_recipe(self, tag: TG) -> Type:
        """_summary_

        Args:
            tag (TG): _description_

        Returns:
            MiniCache: _description_
        """
        # 尽可能的减少竞争
        try:
            return self._recipes[tag]
        except KeyError:
            with self._mtx:
                if tag in self._recipes:
                    return self._recipes[tag]
                recipe = self._create_recipe(tag)
                self._recipes[tag] = recipe
            return recipe
    
    def _create_recipe(self, tag: TG) -> MiniDiskCache:
        name: str = f'{tag}:{self.name}'
        return MiniDiskCache(
            directory=self.directory, 
            name=name, 
            max_size=self.max_size,
            isolation=self._isolation,
            pragmas=self._pragmas
        )
    
    def drop(self, tag: TG = None) -> bool:
        with self._mtx:
            try:
                recipe: MiniDiskCache = self._recipes.pop(tag)
                recipe.sqlite.close()
                rmfile(recipe.location)
            except KeyError:
                pass
        return True

    def clear(self) -> bool:
        with self._mtx:
            for recipe in self._recipes.values():
                if not recipe.clear():
                    return False
        return True

    def __len__(self) -> int:
        return sum(len(recipe) for recipe in self._recipes.values())
    
    def __repr__(self) -> str:
        return f'<DiskCache recepies:{self._recipes}>'


LazyDiskCache = lazy(DiskCache)
