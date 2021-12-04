#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2021/8/17
# Author: clarkmonkey@163.com

from typing import *

from abc import ABC, abstractmethod
from pathlib import Path


class Validator(ABC):

    def __set_name__(self, owner: object, name: str) -> NoReturn:
        self.private_name = '_' + name

    def __get__(self, obj: object, obj_type: Type=None):
        return getattr(obj, self.private_name)

    def __set__(self, obj: object, value: Any):
        validate_value: Any = self.validate(value)
        value = value if validate_value is None else validate_value
        setattr(obj, self.private_name, value)

    @abstractmethod
    def validate(self, value: Any) -> Any:
        """ Validate method. """


class NumberValidate(Validator):

    def __init__(self, minvalue=None, maxvalue=None):
        self.minvalue = minvalue
        self.maxvalue = maxvalue

    def validate(self, value):
        if not isinstance(value, (int, float)):
            raise TypeError(f'Expected {value!r} to be an int or float')
        if self.minvalue is not None and value < self.minvalue:
            raise ValueError(
                f'Expected {value!r} to be at least {self.minvalue!r}'
            )
        if self.maxvalue is not None and value > self.maxvalue:
            raise ValueError(
                f'Expected {value!r} to be no more than {self.maxvalue!r}'
            )


class StringValidate(Validator):

    def __init__(self, minsize=None, maxsize=None, predicate=None):
        self.minsize = minsize
        self.maxsize = maxsize
        self.predicate = predicate

    def validate(self, value):
        if not isinstance(value, str):
            raise TypeError(f'Expected {value!r} to be an str')
        if self.minsize is not None and len(value) < self.minsize:
            raise ValueError(
                f'Expected {value!r} to be no smaller than {self.minsize!r}'
            )
        if self.maxsize is not None and len(value) > self.maxsize:
            raise ValueError(
                f'Expected {value!r} to be no bigger than {self.maxsize!r}'
            )
        if self.predicate is not None and not self.predicate(value):
            raise ValueError(
                f'Expected {self.predicate} to be true for {value!r}'
            )


class EnumerateValidate(Validator):

    def __init__(self, *options: str) -> None:
        self.options = set(options)

    def validate(self, value: Any) -> NoReturn:

        if value not in self.options:
            raise ValueError(f'Expected {value!r} to be one of {self.options!r}')


class DirectoryValidate(Validator):

    def validate(self, directory: Any) -> Any:
        if not isinstance(directory, (str, Path)):
            raise ValueError(f'Expected {directory!r} to be a like-path object')

        path: Path = Path(directory).expanduser().absolute()
        if not path.exists():
            path.mkdir()
            return path

        return path
