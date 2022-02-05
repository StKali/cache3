#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2022/1/15
# Author: clarkmonkey@163.com

import os
import shutil

import pytest

from cache3.validate import NumberValidate, StringValidate, DirectoryValidate, EnumerateValidate

file: str = '.test'


class Checker:

    def __init__(self, number=1, string='hello', directory='hello', enumerator='a'):
        self.number = number
        self.string = string
        self.directory = directory
        self.enumerator = enumerator

    number = NumberValidate(minvalue=0, maxvalue=1000)
    string = StringValidate(minsize=2, maxsize=10, predicate=str.islower)
    directory = DirectoryValidate()
    enumerator = EnumerateValidate('a', 'b', 'c')


def test_number():

    with pytest.raises(TypeError, match=f'Expected .* to be an int or float'):
        Checker(number='a')

    with pytest.raises(ValueError, match='Expected .* to be at least .*'):
        Checker(number=-11)

    with pytest.raises(ValueError, match='Expected .* to be no more than .*'):
        Checker(number=11111)


def test_string():

    with pytest.raises(TypeError, match='Expected .* to be an str'):
        Checker(string=11)

    with pytest.raises(ValueError, match='Expected .* to be no smaller than .*'):
        Checker(string='')

    with pytest.raises(ValueError, match='Expected .* to be no bigger than .*'):
        Checker(string='12345678901234567890')

    with pytest.raises(ValueError, match='Expected .* to be true for .*'):
        Checker(string='HELLO')


def test_enumerate():

    with pytest.raises(ValueError, match='Expected .* to be one of .*'):
        Checker(enumerator='2345678')


def test_directory():

    with pytest.raises(ValueError, match='Expected .* to be a like-path object'):
        Checker(directory=8)

    if os.path.exists(file):
        shutil.rmtree(file)
    assert not os.path.exists(file)
    Checker(directory=file)
    assert os.path.exists(file)
    shutil.rmtree(file)
