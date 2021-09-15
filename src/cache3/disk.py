#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/9/15
# Author: clarkmonkey@163.com

import warnings
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, connect, OperationalError
from typing import NoReturn, Type, Union, Optional, Dict, Any, List, Tuple

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


