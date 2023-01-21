#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/17
# Author: clarkmonkey@163.com

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Type, NoReturn, Union, Callable

Number: Type = Union[str, int]


class Validator(ABC):

    def __set_name__(self, owner: object, name: str) -> NoReturn:
        self.private_name: str = '_' + name

    def __get__(self, obj: object, obj_type: Type = None) -> Any:
        return getattr(obj, self.private_name)

    def __set__(self, obj: object, value: Any) -> NoReturn:
        validate_value: Any = self.validate(value)
        value = value if validate_value is None else validate_value
        setattr(obj, self.private_name, value)

    @abstractmethod
    def validate(self, value: Any) -> Any:
        """ Validate method. """


class NumberValidate(Validator):

    def __init__(self, minvalue: Number = None, maxvalue: Number = None) -> None:
        self.minvalue: Number = minvalue
        self.maxvalue: Number = maxvalue

    def validate(self, value) -> NoReturn:
        
        if value is None:
            return

        if not isinstance(value, (int, float)):
            raise TypeError(
                'Expected %r to be an int or float' % value
            )

        if self.minvalue is not None and value < self.minvalue:
            raise ValueError(
                'Expected %r to be at least %r' % (value, self.minvalue)
            )

        if self.maxvalue is not None and value > self.maxvalue:
            raise ValueError(
                'Expected %r to be no more than %r' % (value, self.maxvalue)
            )


class StringValidate(Validator):

    def __init__(
            self, minsize: int = None,
            maxsize: int = None,
            predicate: Optional[Callable] = None
    ) -> None:
        self.minsize: int = minsize
        self.maxsize: int = maxsize
        self.predicate: Optional[Callable] = predicate

    def validate(self, value: str) -> NoReturn:

        if not isinstance(value, str):
            raise TypeError(
                'Expected %r to be an str' % value
            )
        if self.minsize is not None and len(value) < self.minsize:
            raise ValueError(
                'Expected %r to be no smaller than %r' % (value, self.minsize)
            )
        if self.maxsize is not None and len(value) > self.maxsize:
            raise ValueError(
                'Expected %r to be no bigger than %r' % (value, self.maxsize)
            )
        if self.predicate is not None and not self.predicate(value):
            raise ValueError(
                'Expected %r to be true for %r' % (self.predicate, value)
            )


class EnumerateValidate(Validator):

    def __init__(self, *options: str) -> None:
        self.options: set = set(options)

    def validate(self, value: Any) -> NoReturn:

        if value not in self.options:
            raise ValueError(
                'Expected %r to be one of %r' % (value, self.options)
            )


class DirectoryValidate(Validator):

    def validate(self, directory: Any) -> Optional[Path]:
        if not isinstance(directory, (str, Path)):
            raise ValueError(
                'Expected %r to be a like-path object' % directory
            )

        path: Path = Path(directory).expanduser().absolute()
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            return path

        return path
