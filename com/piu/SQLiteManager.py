# -*- coding: utf-8 -*-
# encoding: utf-8
#!/usr/bin/python

#python sqlite
#DB-API 2.0 interface for SQLite databases

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

#global var
#数据库文件绝句路径
DB_FILE_PATH = ''
#是否打印sql
SHOW_SQL = True

def get_conn(path):
    '''获取到数据库的连接对象，参数为数据库文件的绝对路径
    如果传递的参数是存在，并且是文件，那么就返回硬盘上面改
    路径下的数据库文件的连接对象；否则，返回内存中的数据接
    连接对象'''
    conn = sqlite3.connect(path)
    if os.path.exists(path) and os.path.isfile(path):
        print('硬盘上面:[{}]'.format(path))
        return conn
    else:
        conn = None
        print('内存上面:[:memory:]')
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

###############################################################
####            创建|删除表操作     START
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
        print('the [{}] is empty or equal None!'.format(sql))

def create_table(conn, sql):
    '''创建数据库表'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        conn.commit()
        print('创建数据库表成功!')
        close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))

###############################################################
####            创建|删除表操作     END
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
####            数据库操作CRUD     START
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

def fetchall(conn, sql):
    '''查询所有数据'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        r = cu.fetchall()
        return r
        #if len(r) > 0:
        #    for e in range(len(r)):
        #        print(r[e])
    else:
        print('the [{}] is empty or equal None!'.format(sql)) 

def fetchone(conn, sql, data):
    '''查询一条数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                r = cu.fetchall()
                if len(r) > 0:
                    for e in range(len(r)):
                        return r[e]
                        #print(r[e])
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
####            数据库操作CRUD     END
###############################################################


###############################################################
####            测试操作     START
###############################################################

def save_test():
    '''保存数据测试...'''
    print('保存数据测试...')
    save_sql = '''INSERT INTO student values (?, ?, ?, ?, ?, ?)'''
    data = [(1, 'Hongten', '男', 20, '广东省广州市', '13423****62'),
            (2, 'Tom', '男', 22, '美国旧金山', '15423****63'),
            (3, 'Jake', '女', 18, '广东省广州市', '18823****87'),
            (4, 'Cate', '女', 21, '广东省广州市', '14323****32')]
    conn = get_conn(DB_FILE_PATH)
    save(conn, save_sql, data)

def fetchall_test():
    '''查询所有数据...'''
    print('查询所有数据...')
    fetchall_sql = '''SELECT * FROM student'''
    conn = get_conn(DB_FILE_PATH)
    fetchall(conn, fetchall_sql)

def fetchone_test():
    '''查询一条数据...'''
    print('查询一条数据...')
    fetchone_sql = 'SELECT * FROM student WHERE ID = ? '
    data = 1
    conn = get_conn(DB_FILE_PATH)
    fetchone(conn, fetchone_sql, data)

def update_test():
    '''更新数据...'''
    print('更新数据...')
    update_sql = 'UPDATE student SET name = ? WHERE ID = ? '
    data = [('HongtenAA', 1),
            ('HongtenBB', 2),
            ('HongtenCC', 3),
            ('HongtenDD', 4)]
    conn = get_conn(DB_FILE_PATH)
    update(conn, update_sql, data)

def delete_test():
    '''删除数据...'''
    print('删除数据...')
    delete_sql = 'DELETE FROM student WHERE NAME = ? AND ID = ? '
    data = [('HongtenAA', 1),
            ('HongtenCC', 3)]
    conn = get_conn(DB_FILE_PATH)
    delete(conn, delete_sql, data)

###############################################################
####            测试操作     END
###############################################################

###############################################################
####            检查数据表并初始化     START
###############################################################
def create_table_order():
    print('创建order表...')
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
    print('创建trade表...')
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
    print('创建balance表...')
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
    
def create_table_balanceLog():
    print('创建balanceLog表...')
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
    print('删除数据库表...')
    conn = get_conn(DB_FILE_PATH)
    drop_table(conn, table_name)

def initBalance(ownerId, currency, currentBalance):
    save_sql = '''INSERT INTO balance (ownerId, currency, currentBalance, buyAmount, sellAmount, freezeAmount, createTime, updateTime) values (?, ?, ?, ?, ?, ?, ?, ?)'''
    nowTime = GetNowTime()
    data = [(ownerId, currency, currentBalance, 0, 0, 0, nowTime, nowTime)]
    conn = get_conn(DB_FILE_PATH)
    save(conn, save_sql, data)

def checkBalance(ownerId, currency):
    fetchone_sql = 'SELECT * FROM `balance` WHERE ownerId = ? and currency = ? '
    data = [(ownerId, currency)]
    conn = get_conn(DB_FILE_PATH)
    return fetchone(conn, fetchone_sql, data)

def saveOrder(ownerId, side, ):
    save_sql = '''INSERT INTO `order` values (?, ?, ?, ?, ?, ?)'''
    data = [(1, 'Hongten', '男', 20, '广东省广州市', '13423****62')]
    conn = get_conn(DB_FILE_PATH)
    save(conn, save_sql, data)

def updateOrder():
    update_sql = 'UPDATE order SET name = ? WHERE ID = ? '
    data = [('HongtenAA', 1)]
    conn = get_conn(DB_FILE_PATH)
    update(conn, update_sql, data)
###############################################################
####            检查数据表并初始化     END
###############################################################

def GetNowTime():
    return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

#用户ID
OWNER_ID = 1
CURRENCY = 'btc'
#买卖方向
SIDE_BUY = 1
SIDE_SELL = 2
#数据来源
DATA_Type_ORDER = 1
DATA_Type_TRADE = 1
#业务类型
BIZ_Type_BUY_ORDER = 1
BIZ_Type_BUY_TRADE = 2
BIZ_Type_BUY_CANCEL = 3
BIZ_Type_SELL_ORDER = 4
BIZ_Type_SELL_TRADE = 5
BIZ_Type_SELL_CANCEL = 6

def init():
    '''初始化方法'''
    #数据库文件相对路径
    global DB_FILE_PATH
    DB_FILE_PATH = 'tradeCounter.db'
    #是否打印sql
    global SHOW_SQL
    SHOW_SQL = True
    print('show_sql : {}'.format(SHOW_SQL))
    create_table_order()
    create_table_trade()
    create_table_balance()
    create_table_balanceLog()
    one = checkBalance(OWNER_ID, CURRENCY)
    if one is None:
        initBalance(OWNER_ID, CURRENCY, 100)
    else:
        print('已经存在: ', one)
    #table_name = 'order'
    #drop_table_test(table_name)
    #table_name = 'trade'
    #drop_table_test(table_name)
    #table_name = 'balance'
    #drop_table_test(table_name)
    #table_name = 'balanceLog'
    #drop_table_test(table_name)