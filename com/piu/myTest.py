# -*- coding: utf-8 -*-
# encoding: utf-8
#!/usr/bin/python

import threading
import time
import SQLiteManager
from gateAPI import GateIO
from Constants import Constant

#用户ID
OWNER_ID = 1
CURRENCY_OTHER = 'eos'
CURRENCY_BASE = 'ustd'
#买卖方向
SIDE_BUY = 1
SIDE_SELL = 2
#订单状态
STATUS_ACCEPTED = 1
STATUS_HALF_FILLED = 2
STATUS_TOTAL_FILLED = 3
STATUS_CANCELLED = 4
#数据来源
DATA_Type_ORDER = 1
DATA_Type_TRADE = 2
#业务类型
BIZ_Type_BUY_ORDER = 1
BIZ_Type_BUY_TRADE = 2
BIZ_Type_BUY_CANCEL = 3
BIZ_Type_SELL_ORDER = 4
BIZ_Type_SELL_TRADE = 5
BIZ_Type_SELL_CANCEL = 6

## 填写 apiKey APISECRET
apiKey = Constant.apiKey
secretKey = Constant.secretKey
## Provide constants
API_QUERY_URL = Constant.API_QUERY_URL
API_TRADE_URL = Constant.API_TRADE_URL
## Create a gate class instance
gate_query = GateIO(API_QUERY_URL, apiKey, secretKey)
gate_trade = GateIO(API_TRADE_URL, apiKey, secretKey)

def tradeAction(arg):
    time.sleep(1)
    print('the arg is:%s' % arg)

def orderAction(arg):
    time.sleep(1)
    print('the arg is:%s' % arg)
    print(gate_trade.sell('etc_btc', '0.001', '11222'))

def main():
    #数据表的创建和资产的初始化在其他地方进行
    SQLiteManager.initData(OWNER_ID, CURRENCY_OTHER, CURRENCY_BASE, 100000)
    #线程A，定时5秒从订单数据表中获取尚未完全成交的订单，检查订单内的币种行情数据是否和订单的可以进行成交，如果可以就创建成交记录，调整资产
    tradeThread = threading.Thread(target=tradeAction,args=('tradeThread',))
    tradeThread.start()
    #线程B，检查指定的币种行情数据，决定是否创建订单，是否取消订单
    orderThread = threading.Thread(target=orderAction,args=('orderThread',))
    orderThread.start()

if __name__ == '__main__':
    main()
