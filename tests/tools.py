#!/usr/bin/python
# -*- coding: utf-8 -*-
# DATE: 2022/1/18
# Author: clarkmonkey@163.com

import shutil
from cache3 import setting


def delete_local_file():
    shutil.rmtree(
        setting.DEFAULT_STORE.expanduser().absolute()
    )


if __name__ == '__main__':
    delete_local_file()
