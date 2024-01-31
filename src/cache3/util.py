#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/7/24
# author: clarkmonkey@163.com

""" util
Tools functions or classes for cache3
"""

import functools
import operator
import warnings
from multiprocessing import Lock
from os import remove, name as kernal
from pathlib import Path
from time import time as current
from typing import Any,  Dict, Tuple, NoReturn, Optional, Callable, Type, Union, Iterable

# Compatible with multiple types.
empty: Any = type('empty', (), {
    '__str__': lambda x: '<empty>',
    '__bool__': lambda x: False,
})
Number = Union[int, float]
Time = Optional[Number]
TG = Optional[str]


class Cache3Error(Exception):
    """ A simple base expection for cache3 """


class Cache3Warning(UserWarning):
    """A simple base warning for cache3 """


def get_expire(timeout: Time, now: Time = None) -> Time:
    """ Returns a timestamp representing the timeout time """
    if timeout is None:
        return None
    return (now or current()) + timeout


# pylint: disable=invalid-name
class cached_property:
    """ Decorator that converts a method with a single self argument into a
    property cached on the instance.
    """
    name: Optional[str] = None

    @staticmethod
    def func(instance) -> NoReturn:  # pylint: disable=method-hidden
        raise TypeError(
            'Cannot use cached_property instance without calling '
            '__set_name__() on it.'
        )

    def __init__(self, func: Callable, _: Optional[str] = None) -> None:
        self.real_func: Callable = func
        self.__doc__: str = getattr(func, '__doc__')

    def __set_name__(self, owner: Any, name: str) -> NoReturn:
        if self.name is None:
            self.name: str = name
            self.func: Callable = self.real_func
        elif name != self.name:
            raise TypeError(
                'Cannot assign the same cached_property to two different names '
                f'({self.name!r} and {name!r}).'
            )

    def __get__(self, instance: Any, cls=None) -> Any:
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


def new_method_proxy(func) -> Callable:
    def inner(self, *args):
        # pylint: disable=protected-access
        if self._wrapped is empty:
            self._setup()
        return func(self._wrapped, *args)
    return inner


class LazyObject:
    """ Accepts a factory function that will be called when the instance is actually accessed 
    LazyObject will be the proxy object for this instance.
    """

    _wrapped = None

    def __init__(self, factory: Callable) -> None:
        self.__dict__['_setup_factory'] = factory
        self._wrapped = empty

    __getattr__ = new_method_proxy(getattr)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_wrapped":
            self.__dict__["_wrapped"] = value
        else:
            if self._wrapped is empty:
                self._setup()
            setattr(self._wrapped, name, value)
    
    def __delattr__(self, name: str) -> None:
        if name == "_wrapped":
            raise TypeError('cannot delete _wrapped')
        if self._wrapped is empty:
            self._setup()
        delattr(self._wrapped, name)

    def _setup(self) -> None:
        self._wrapped = self._setup_factory()

    __bytes__ = new_method_proxy(bytes)
    __str__ = new_method_proxy(str)
    __bool__ = new_method_proxy(bool)

    __dir__ = new_method_proxy(dir)

    __class__ = property(new_method_proxy(operator.attrgetter("__class__")))
    __eq__ = new_method_proxy(operator.eq)
    __lt__ = new_method_proxy(operator.lt)
    __gt__ = new_method_proxy(operator.gt)
    __ne__ = new_method_proxy(operator.ne)
    __hash__ = new_method_proxy(hash)

    __getitem__ = new_method_proxy(operator.getitem)
    __setitem__ = new_method_proxy(operator.setitem)
    __delitem__ = new_method_proxy(operator.delitem)
    __iter__ = new_method_proxy(iter)
    __len__ = new_method_proxy(len)
    __contains__ = new_method_proxy(operator.contains)
    __add__ = new_method_proxy(operator.add)

    def __repr__(self) -> str:
        if self._wrapped is empty:
            repr_attr = self._setup_factory
        else:
            repr_attr = self._wrapped
        return f'{type(self).__name__}: {repr_attr!r}'


def lazy(factory: Callable) -> Callable:

    def wrapper(*args, **kwrags):
        def init():
            return factory(*args, **kwrags)
        return LazyObject(init)
    return wrapper


def memoize(self: Any, timeout: Time = None) -> Any:
    """ The cache is decorated with the return value of the function,
    and the timeout is available. """

    def decorator(func: Optional[Callable] = None) -> Callable[[Callable[[Any], Any]], Any]:
        """ Decorator created by memoize() for callable `func`."""
        if not callable(func):
            raise TypeError(
                'The `memoize` decorator should be called with a `timeout` parameter.'
            )
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            """Wrapper for callable to cache arguments and return values."""
            value: Any = self.get(func.__name__, empty)
            if value is empty:
                value: Any = func(*args, **kwargs)
                self.set(func.__name__, value, timeout)
            return value
        return wrapper
    return decorator


def tag_memoize(self: Any, timeout: Time = 24 * 60 * 60, tag: TG = None) -> Any:
    """ The cache is decorated with the return value of the function,
    and the timeout is available. """

    def decorator(func: Optional[Callable] = None) -> Callable[[Callable[[Any], Any]], Any]:
        """ Decorator created by memoize() for callable `func`."""
        if not callable(func):
            raise TypeError(
                'The `memoize` decorator should be called with a `timeout` parameter.'
            )
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


class SpinCounter:
    """_summary_
    """

    def __init__(self, count: int = 0, spin_size: int = 128) -> None:
        """ 自选计数器，当程序

        Args:
            count (int, default: 0): 
                计数器的初始大小

            spin_size (int, default: 128):
                当累计 spin_size 次后进行自旋操作

        """
        # 总计数器
        self.count: int = count
        # 自旋大小
        self.spin_size: Number = spin_size
        # 自旋计数器
        self.spin_count: Number = 0
        self._lock: Lock = Lock() # type: ignore

    def add(self, n: Number = 1) -> bool:
        """ 计数器加1成功时返回True，当触发自旋清零时返回False """
        with self._lock:
            self.spin_count += n
            self.count += n
            if self.spin_count > self.spin_size:
                return True
            self.spin_count = self.spin_count - self.spin_size
            return False

    def reset(self) -> int:
        with self._lock:
            self.spin_count = self.count = 0
            
    def align(self, count: int) -> int:
        if count < 0:
            raise ValueError('count must be >= 0')
        with self._lock:
            self.count = count    

    def __len__(self) -> int:
        return self.count

    def __lt__(self, other: Number) -> bool:
        return self._ct < other
    
    def __le__(self, other: Number) -> bool:
        return self._ct <= other

    def __eq__(self, other: Number) -> bool:
        return self._ct == other

    def __ne__(self, other: Number) -> bool:
        return self._ct != other

    def __gt__(self, other: Number) -> bool:
        return self._ct > other

    def __ge__(self, other: Number) -> bool:
        return self._ct >= other
    
    def __str__(self) -> str:
        return str(self._ct)


class MultiCache:

    def get_recipe(self, tag: TG) -> Any:
        raise NotImplementedError('"get_recipe" has not been implemented yet.')
    
    def set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:
        recipe = self.get_recipe(tag)
        return recipe.set(key, value, timeout)

    def ex_set(self, key: Any, value: Any, timeout: Time = None, tag: TG = None) -> bool:
        recipe = self.get_recipe(tag)
        return recipe.ex_set(key, value, timeout)

    def get(self, key: Any, default: Any = None, tag: TG = None) -> Any:
        recipe = self.get_recipe(tag)
        return recipe.get(key, default)

    def get_many(self, *keys: Any, tag: TG = None) -> dict:
        recipe = self.get_recipe(tag)
        return recipe.get_many(*keys)

    def incr(self, key: Any, delta: Number = 1, tag: TG = None) -> Number:
        recipe = self.get_recipe(tag)
        return recipe.incr(key, delta)

    def decr(self, key: Any, delta: Number = 1, tag: TG = None) -> Number:
        recipe = self.get_recipe(tag)
        return recipe.decr(key, delta)

    def ttl(self, key: Any, tag: TG = None) -> Time:
        recipe = self.get_recipe(tag)
        return recipe.ttl(key)

    def delete(self, key: Any, tag: TG = None) -> bool:
        recipe = self.get_recipe(tag)
        return recipe.delete(key)
    
    def inspect(self, key: Any, tag: TG = None) -> Optional[Dict]:
        recipe = self.get_recipe(tag)
        ins = recipe.inspect(key)
        if ins is not None:
            ins['tag'] = tag
        return ins

    def pop(self, key: Any, default: Any = None, tag: TG = None) -> Any:
        recipe = self.get_recipe(tag)
        return recipe.pop(key, default)
    
    def exists(self, key: Any, tag: TG = None) -> bool:
        recipe = self.get_recipe(tag)
        return recipe.exists(key)

    def touch(self, key: Any, timeout: Time = None, tag: TG = None) -> bool:
        recipe = self.get_recipe(tag)
        return recipe.touch(key, timeout)

    def keys(self, tag: TG = None) -> Iterable[Any]:
        recipe = self.get_recipe(tag)
        return recipe.keys()

    def values(self, tag: TG = None) -> Iterable[Any]:
        recipe = self.get_recipe(tag)
        return recipe.values()

    def items(self, tag: TG = None) -> Iterable[Tuple[Any, Any]]:
        recipe = self.get_recipe(tag)
        return recipe.items()
    
    def clear(self) -> bool:
        raise NotImplementedError('Method "clear" has not been implemented yet.')

    memoize = tag_memoize
    __iter__ = keys
    __setitem__ = set
    __getitem__ = get
    __delitem__ = delete
    __contains__ = has_key = exists


def rmfile(file: Union[str, Path]) -> bool:
    try:
        remove(file)
    except FileNotFoundError:
        return True
    except OSError as exc:
        warning(f'failed to delete file: {file!r}, err: {exc!r}')
        return False
    return True


def warning(s: str) -> None:
    warnings.warn(s, Cache3Warning)


# --------------------------------
#   File Lock
# --------------------------------

# Based partially on an example by Jonathan Feignberg in the Python
# Cookbook [1] (licensed under the Python Software License) and a ctypes port by
# Anatoly Techtonik for Roundup [2] (license [3]).
# [1] https://code.activestate.com/recipes/65203/
# [2] https://sourceforge.net/p/roundup/code/ci/default/tree/roundup/backends/portalocker.py  # NOQA
# [3] https://sourceforge.net/p/roundup/code/ci/default/tree/COPYING.txt
#
# Example Usage::
#    >>> from cache3.util import locks
#    >>> with open('./file', 'wb') as f:
#    ...     locks.lock(f, locks.LOCK_EX)
#    ...     f.write('cache3')



def _fd(f):
    """Get a filedescriptor from something which could be a file or an fd."""
    return f.fileno() if hasattr(f, "fileno") else f


if kernal == "nt":
    import msvcrt
    from ctypes import (
        POINTER,
        Structure,
        Union,
        WinDLL,
        byref,
        c_int64,
        c_ulong,
        c_void_p,
        sizeof,
    )
    from ctypes.wintypes import BOOL, DWORD, HANDLE

    LOCK_SH = 0  # the default
    LOCK_NB = 0x1  # LOCKFILE_FAIL_IMMEDIATELY
    LOCK_EX = 0x2  # LOCKFILE_EXCLUSIVE_LOCK

    # --- Adapted from the pyserial project ---
    # detect size of ULONG_PTR
    if sizeof(c_ulong) != sizeof(c_void_p):
        ULONG_PTR = c_int64
    else:
        ULONG_PTR = c_ulong
    PVOID = c_void_p

    # --- Union inside Structure by stackoverflow:3480240 ---
    class _OFFSET(Structure):
        _fields_ = [("Offset", DWORD), ("OffsetHigh", DWORD)]

    class _OFFSET_UNION(Union):
        _anonymous_ = ["_offset"]
        _fields_ = [("_offset", _OFFSET), ("Pointer", PVOID)]

    class OVERLAPPED(Structure):
        _anonymous_ = ["_offset_union"]
        _fields_ = [
            ("Internal", ULONG_PTR),
            ("InternalHigh", ULONG_PTR),
            ("_offset_union", _OFFSET_UNION),
            ("hEvent", HANDLE),
        ]

    LPOVERLAPPED = POINTER(OVERLAPPED)

    # --- Define function prototypes for extra safety ---
    kernel32 = WinDLL("kernel32")
    LockFileEx = kernel32.LockFileEx
    LockFileEx.restype = BOOL
    LockFileEx.argtypes = [HANDLE, DWORD, DWORD, DWORD, DWORD, LPOVERLAPPED]
    UnlockFileEx = kernel32.UnlockFileEx
    UnlockFileEx.restype = BOOL
    UnlockFileEx.argtypes = [HANDLE, DWORD, DWORD, DWORD, LPOVERLAPPED]

    def lock(f, flags):
        hfile = msvcrt.get_osfhandle(_fd(f))
        overlapped = OVERLAPPED()
        ret = LockFileEx(hfile, flags, 0, 0, 0xFFFF0000, byref(overlapped))
        return bool(ret)

    def unlock(f):
        hfile = msvcrt.get_osfhandle(_fd(f))
        overlapped = OVERLAPPED()
        ret = UnlockFileEx(hfile, 0, 0, 0xFFFF0000, byref(overlapped))
        return bool(ret)

else:
    try:
        import fcntl

        LOCK_SH = fcntl.LOCK_SH  # shared lock
        LOCK_NB = fcntl.LOCK_NB  # non-blocking
        LOCK_EX = fcntl.LOCK_EX
    except (ImportError, AttributeError):
        # File locking is not supported.
        LOCK_EX = LOCK_SH = LOCK_NB = 0

        # Dummy functions that don't do anything.
        def lock(f, flags):
            # File is not locked
            return False

        def unlock(f):
            # File is unlocked
            return True

    else:

        def lock(f, flags):
            try:
                fcntl.flock(_fd(f), flags)
                return True
            except BlockingIOError:
                return False

        def unlock(f):
            fcntl.flock(_fd(f), fcntl.LOCK_UN)
            return True
