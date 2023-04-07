#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2021/7/24
# author: clarkmonkey@163.com

""" util
Tools functions or classes for cache3
"""

import functools
import operator
from time import time as current
from typing import Any, NoReturn, Optional, Callable, Type, Union

# Compatible with multiple types.
empty: Any = type('empty', (), {
    '__str__': lambda x: '<empty>',
    '__bool__': lambda x: False,
})
Number: Type = Union[int, float]
Time: Type = Optional[Number]
TG: Type = Optional[str]


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


def memoize(self: Any, timeout: Time = 24 * 60 * 60, tag: TG = None) -> Any:
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
