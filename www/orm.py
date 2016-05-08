# -*- coding: utf-8 -*-

import asynico
import logging
import aiomysql

def log(aql, args = ()):
    logging.info("SQL: %s" % sql)

@asynico.coroutine
def creat_pool(loop, **kw):
    logging.info("Create database connection pool...")
    global __pool
    __pool = yield form aiomysql.create_pool(
        host    = kw.get('host', 'localhost'),
        port    = kw.get('port', 3306),
        user    = kw['user'],
        password= kw['password'],
        db      = kw['db'],
        charset = kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize =kw.get('maxsize', 10),
        minsize =kw.get('minsize', 1),
        loop    = loop
    )

@asynico.coroutine
def select(sql, args, size = None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows return %s' % len(rs))
        return rs

@asynico.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replac('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

def creat_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
        return ', '.join(L)

class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name = None, primary_key = False, default = None, ddl = 'varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
    def __init__(self, name = None, default = False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name = None, primary_key = False, default = 0):
        super().__init__(name, 'bigint', primary_key, default)




