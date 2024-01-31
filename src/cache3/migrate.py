#!/usr/bin/python
# -*- coding: utf-8 -*-
# date: 2024/04/19
# author: clarkmonkey@163.com

from .version import VERSION


def migrate_0to1(connect) -> bool:
    """ 
        1 删除所有大key 或 value 的转存文件
        2 删除所有大 key 或 value所在的缓存项
        3 创建版本表

    Args:
        connect (_type_): _description_

    Returns:
        bool: _description_
    """
    

_migrate_table: list = [
    migrate_0to1,
]

def migrate(major: int) -> None:
    """_summary_

    Args:
        major (int): _description_

    Raises:
        SystemError: _description_

    Returns:
        _type_: _description_
    """
    
    if major > VERSION.major:
        raise SystemError(f'you cannot downgrade from {major!r} to {VERSION.major!r}')
    
    if major == VERSION.major:
        return
    
    _migrate_table[major]()
    return migrate(major+1)
