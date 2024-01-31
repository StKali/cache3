#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2024/04/19
# author: clarkmonkey@163.com

from collections import namedtuple


class Version(namedtuple('Version', 'major,minor,patch,extend')):
    
    def __repr__(self) -> str:
        return f'{self.major}.{self.minor}.{self.patch}'
    
    def compatible(self, version: str) -> bool:
        return version.split('.')[0] == str(self.major)


VERSION: Version = Version(1, 0, 0, '')

if __name__ == '__main__':
    print(VERSION)
    print(VERSION.major)
    assert isinstance(VERSION.major, int)
    