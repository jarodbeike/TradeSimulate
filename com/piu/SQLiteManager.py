# -*- coding: utf-8 -*-
# encoding: utf-8
# !/usr/bin/python

# python sqlite
# DB-API 2.0 interface for SQLite databases

import sqlite3
import os
import time

'''SQLite数据库是一款非常小巧的嵌入式开源数据库软件，也就是说
没有独立的维护进程，所有的维护都来自于程序本身。
在python中，使用sqlite3创建数据库的连接，当我们指定的数据库文件不存在的时候
连接对象会自动创建数据库文件；如果数据库文件已经存在，则连接对象不会再创建
数据库文件，而是直接打开该数据库文件。
    连接对象可以是硬盘上面的数据库文件，也可以是建立在内存中的，在内存中的数据库
    执行完任何操作后，都不需要提交事务的(commit)

    创建在硬盘上面： conn = sqlite3.connect('test.db')
    创建在内存上面： conn = sqlite3.connect('"memory:')

    下面我们一硬盘上面创建数据库文件为例来具体说明：
    conn = sqlite3.connect('hongten.db')
    其中conn对象是数据库链接对象，而对于数据库链接对象来说，具有以下操作：

        commit()            --事务提交
        rollback()          --事务回滚
        close()             --关闭一个数据库链接
        cursor()            --创建一个游标

    cu = conn.cursor()
    这样我们就创建了一个游标对象：cu
    在sqlite3中，所有sql语句的执行都要在游标对象的参与下完成
    对于游标对象cu，具有以下具体操作：

        execute()           --执行一条sql语句
        executemany()       --执行多条sql语句
        close()             --游标关闭
        fetchone()          --从结果中取出一条记录
        fetchmany()         --从结果中取出多条记录
        fetchall()          --从结果中取出所有记录
        scroll()            --游标滚动

'''

# 数据库文件绝句路径
DB_FILE_PATH = ''
# 是否打印sql
SHOW_SQL = True


def get_conn(path):
    '''获取到数据库的连接对象，参数为数据库文件的绝对路径
    如果传递的参数是存在，并且是文件，那么就返回硬盘上面改
    路径下的数据库文件的连接对象；否则，返回内存中的数据接
    连接对象'''
    conn = sqlite3.connect(path)
    if os.path.exists(path) and os.path.isfile(path):
        # print('硬盘上面:[{}]'.format(path))
        return conn
    else:
        conn = None
        # print('内存上面:[:memory:]')
        return sqlite3.connect(':memory:')


def get_cursor(conn):
    '''该方法是获取数据库的游标对象，参数为数据库的连接对象
    如果数据库的连接对象不为None，则返回数据库连接对象所创
    建的游标对象；否则返回一个游标对象，该对象是内存中数据
    库连接对象所创建的游标对象'''
    if conn is not None:
        return conn.cursor()
    else:
        return get_conn('').cursor()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


###############################################################
#               创建|删除表操作     START
###############################################################
def drop_table(conn, table):
    '''如果表存在,则删除表，如果表中存在数据的时候，使用该
    方法的时候要慎用！'''
    if table is not None and table != '':
        sql = 'DROP TABLE IF EXISTS `' + table + '`'
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu = get_cursor(conn)
        cu.execute(sql)
        conn.commit()
        print('删除数据库表[{}]成功!'.format(table))
        close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(table))


def create_table(conn, sql):
    '''创建数据库表'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        conn.commit()
        # print('创建数据库表成功!')
        close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))


###############################################################
#               创建|删除表操作     END
###############################################################

def close_all(conn, cu):
    '''关闭数据库游标对象和数据库连接对象'''
    try:
        if cu is not None:
            cu.close()
    finally:
        if cu is not None:
            cu.close()


###############################################################
#               数据库操作CRUD     START
###############################################################

def save(conn, sql, data):
    '''插入数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def save_without_commit(conn, sql, data):
    '''插入数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            if SHOW_SQL:
                print('执行sql:[{}],参数:[{}]'.format(sql, data))
            cu.execute(sql, data)
            return cu.lastrowid
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def fetchall(conn, sql):
    '''查询所有数据'''
    if sql is not None and sql != '':
        conn.row_factory = dict_factory
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        r = cu.fetchall()
        return r
        # if len(r) > 0:
        #    for e in range(len(r)):
        #        print(r[e])
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def fetchall_with_condition(conn, sql, data):
    '''查询所有数据'''
    if sql is not None and sql != '':
        conn.row_factory = dict_factory
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}],参数:[{}]'.format(sql, data))
        cu.execute(sql, data)
        r = cu.fetchall()
        return r
        # if len(r) > 0:
        #    for e in range(len(r)):
        #        print(r[e])
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def fetchone(conn, sql, data):
    '''查询一条数据'''
    if sql is not None and sql != '':
        if data is not None:
            conn.row_factory = dict_factory
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                r = cu.fetchall()
                if len(r) > 0:
                    for e in range(len(r)):
                        return r[e]
                        # print(r[e])
        else:
            print('the [{}] equal None!'.format(data))
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def update(conn, sql, data):
    '''更新数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def update_without_commit(conn, sql, data):
    '''更新数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            if SHOW_SQL:
                print('执行sql:[{}],参数:[{}]'.format(sql, data))
            cu.execute(sql, data)
            return cu.lastrowid
    else:
        print('the [{}] is empty or equal None!'.format(sql))


def delete(conn, sql, data):
    '''删除数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))


###############################################################
#               数据库操作CRUD     END
###############################################################

###############################################################
#               业务操作     START
###############################################################
def create_table_order():
    # print('创建order表...')
    create_table_sql = '''CREATE TABLE IF NOT EXISTS `order` (
                          `orderId` INTEGER PRIMARY KEY AUTOINCREMENT,
                          `ownerId` int NOT NULL,
                          `side` TINYINT NOT NULL,
                          `currencyPair` varchar(20) NOT NULL,
                          `rate` double NOT NULL,
                          `amount` double NOT NULL,
                          `filledAmount` double DEFAULT NULL,
                          `lastFilledRate` double DEFAULT NULL,
                          `status` TINYINT NOT NULL,
                          `createTime` DATETIME DEFAULT NULL,
                          `updateTime` DATETIME DEFAULT NULL
                        )'''
    conn = get_conn(DB_FILE_PATH)
    create_table(conn, create_table_sql)


def create_table_trade():
    # print('创建trade表...')
    create_table_sql = '''CREATE TABLE IF NOT EXISTS `trade` (
                          `tradeId` INTEGER PRIMARY KEY AUTOINCREMENT,
                          `ownerId` int NOT NULL,
                          `orderId` int(11) NOT NULL,
                          `side` TINYINT NOT NULL,
                          `currencyPair` varchar(20) NOT NULL,
                          `rate` double NOT NULL,
                          `amount` double NOT NULL,
                          `filledAmount` double NOT NULL,
                          `lastFilledRate` double NOT NULL,
                          `lastFilledAmount` double NOT NULL,
                          `createTime` DATETIME DEFAULT NULL,
                          `updateTime` DATETIME DEFAULT NULL
                        )'''
    conn = get_conn(DB_FILE_PATH)
    create_table(conn, create_table_sql)


def create_table_balance():
    # print('创建balance表...')
    create_table_sql = '''CREATE TABLE IF NOT EXISTS `balance` (
                          `balanceId` INTEGER PRIMARY KEY AUTOINCREMENT,
                          `ownerId` int NOT NULL,
                          `currency` varchar(20) NOT NULL,
                          `currentBalance` double NOT NULL,
                          `buyAmount` double NOT NULL,
                          `sellAmount` double NOT NULL,
                          `freezeAmount` double NOT NULL,
                          `createTime` DATETIME DEFAULT NULL,
                          `updateTime` DATETIME DEFAULT NULL
                        )'''
    conn = get_conn(DB_FILE_PATH)
    create_table(conn, create_table_sql)


def create_table_balance_log():
    # print('创建balanceLog表...')
    create_table_sql = '''CREATE TABLE IF NOT EXISTS `balanceLog` (
                          `balanceLogId` INTEGER PRIMARY KEY AUTOINCREMENT,
                          `ownerId` int NOT NULL,
                          `balanceId` int(11) NOT NULL,
                          `currency` varchar(20) NOT NULL,
                          `beforeCurrentBalance` double NOT NULL,
                          `endCurrentBalance` double NOT NULL,
                          `beforeBuyAmount` double NOT NULL,
                          `endBuyAmount` double NOT NULL,
                          `beforeSellAmount` double NOT NULL,
                          `endSellAmount` double NOT NULL,
                          `beforeFreezeAmount` double NOT NULL,
                          `endFreezeAmount` double NOT NULL,
                          `dataType` TINYINT NOT NULL,
                          `dataId` int(11) NOT NULL,
                          `bizType` TINYINT NOT NULL,
                          `createTime` DATETIME DEFAULT NULL,
                          `updateTime` DATETIME DEFAULT NULL
                        )'''
    conn = get_conn(DB_FILE_PATH)
    create_table(conn, create_table_sql)


def drop_table_test(table_name):
    conn = get_conn(DB_FILE_PATH)
    drop_table(conn, table_name)


def init_balance(ownerId, currency, currentBalance):
    save_sql = '''INSERT INTO balance (ownerId, currency, currentBalance, buyAmount, sellAmount, freezeAmount, createTime, updateTime) values (?, ?, ?, ?, ?, ?, ?, ?)'''
    now_time = get_now_time()
    data = [(ownerId, currency, currentBalance, 0, 0, 0, now_time, now_time)]
    conn = get_conn(DB_FILE_PATH)
    save(conn, save_sql, data)


def check_balance(ownerId, currency):
    fetchone_sql = 'SELECT * FROM `balance` WHERE ownerId = ? and currency = ? '
    data = [(ownerId, currency)]
    conn = get_conn(DB_FILE_PATH)
    return fetchone(conn, fetchone_sql, data)


def update_balance(conn, ownerId, currency, lastBuyAmount, lastSellAmount, lastFreezeAmount):
    update_sql = '''UPDATE `balance` SET buyAmount = buyAmount + ?, sellAmount = sellAmount + ?, freezeAmount = freezeAmount + ?, updateTime = ? WHERE ownerId = ? and currency = ?'''
    data = (lastBuyAmount, lastSellAmount, lastFreezeAmount, get_now_time(), ownerId, currency)
    return update_without_commit(conn, update_sql, data)


def insert_balance_log(conn, ownerId, balanceId, currency, beforeCurrentBalance, endCurrentBalance, beforeBuyAmount,
                      endBuyAmount, beforeSellAmount, endSellAmount, beforeFreezeAmount, endFreezeAmount, dataType,
                      dataId, bizType):
    save_sql = '''INSERT INTO balanceLog (ownerId, balanceId, currency, beforeCurrentBalance, endCurrentBalance, beforeBuyAmount, endBuyAmount, beforeSellAmount,  endSellAmount, beforeFreezeAmount, endFreezeAmount, dataType, dataId, bizType, createTime, updateTime) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    now_time = get_now_time()
    data = (ownerId, balanceId, currency, beforeCurrentBalance, endCurrentBalance, beforeBuyAmount, endBuyAmount,
            beforeSellAmount, endSellAmount, beforeFreezeAmount, endFreezeAmount, dataType, dataId, bizType, now_time,
            now_time)
    return save_without_commit(conn, save_sql, data)


def insert_order(conn, ownerId, side, currencyPair, rate, amount, filledAmount, lastFilledRate, status):
    save_sql = '''INSERT INTO `order`(ownerId, side, currencyPair, rate, amount, filledAmount, lastFilledRate, status, createTime, updateTime) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    now_time = get_now_time()
    data = (ownerId, side, currencyPair, rate, amount, filledAmount, lastFilledRate, status, now_time, now_time)
    return save_without_commit(conn, save_sql, data)


def update_order(conn, orderId, lastFilledAmount, lastFilledRate, status):
    update_sql = '''UPDATE `order` SET filledAmount = filledAmount + ?, lastFilledRate = ?, status = ?, updateTime = ? WHERE orderId = ? '''
    data = (lastFilledAmount, lastFilledRate, status, get_now_time(), orderId)
    return update_without_commit(conn, update_sql, data)


def query_order_not_filled(ownerId, side, statusAccepted, statusHalfFilled):
    fetchone_sql = '''SELECT * FROM `order` WHERE ownerId = ? and side = ? and status in (? , ?) '''
    data = (ownerId, side, statusAccepted, statusHalfFilled)
    conn = get_conn(DB_FILE_PATH)
    return fetchall_with_condition(conn, fetchone_sql, data)


def insert_trade(conn, ownerId, orderId, side, currencyPair, rate, amount, filledAmount, lastFilledRate,
                 lastFilledAmount):
    save_sql = '''INSERT INTO `trade`(ownerId, orderId, side, currencyPair, rate, amount, filledAmount, lastFilledRate, lastFilledAmount, status, createTime, updateTime) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    now_time = get_now_time()
    data = [(ownerId, orderId, side, currencyPair, rate, amount, filledAmount, lastFilledRate, lastFilledAmount, now_time, now_time)]
    save_without_commit(conn, save_sql, data)


def query_trade(owner_id):
    fetchone_sql = '''SELECT * FROM `trade` WHERE ownerId = ? order by tradeId desc'''
    data = owner_id
    conn = get_conn(DB_FILE_PATH)
    return fetchall_with_condition(conn, fetchone_sql, data)


###############################################################
#               业务操作     END
###############################################################

def get_now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def init_data(ownerId, currencyOther, currencyBase, currentBalanceBase):
    '''初始化方法'''
    # 数据库文件相对路径
    global DB_FILE_PATH
    DB_FILE_PATH = 'tradeCounter.db'
    # 是否打印sql
    global SHOW_SQL
    SHOW_SQL = False
    print('show_sql : {}'.format(SHOW_SQL))
    create_table_order()
    create_table_trade()
    create_table_balance()
    create_table_balance_log()
    before = check_balance(ownerId, currencyBase)
    if before is None:
        init_balance(ownerId, currencyBase, currentBalanceBase)
    end = check_balance(ownerId, currencyOther)
    if end is None:
        init_balance(ownerId, currencyOther, 0)
        # table_name = 'order'
        # drop_table_test(table_name)
        # table_name = 'trade'
        # drop_table_test(table_name)
        # table_name = 'balance'
        # drop_table_test(table_name)
        # table_name = 'balanceLog'
        # drop_table_test(table_name)
