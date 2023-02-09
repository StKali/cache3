#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/5


from sqlite3.dbapi2 import Connection


conn = Connection(':memory:')

cur = conn.cursor()
cur.execute('CREATE TABLE t(name text);')
cur.execute('INSERT INTO t(name) VALUES(?)', ('monkey', ))
print(cur.execute('SELECT * FROM t;').fetchall())

