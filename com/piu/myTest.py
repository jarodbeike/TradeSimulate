# -*- coding: utf-8 -*-
# encoding: utf-8
#!/usr/bin/python

from SQLiteManager import init
import threading
import time
from gateAPI import GateIO
from constants import Constant

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
    print(gate_trade.sell('etc_btc', '0.001', '11222'))

def main():
    #数据表的创建和资产的初始化在其他地方进行
    init()
    #线程A，定时5秒从订单数据表中获取尚未完全成交的订单，检查订单内的币种行情数据是否和订单的可以进行成交，如果可以就创建成交记录，调整资产
    tradeThread = threading.Thread(target=tradeAction,args=('tradeThread',))
    tradeThread.start()
    #线程B，检查指定的币种行情数据，决定是否创建订单，是否取消订单
    orderThread = threading.Thread(target=orderAction,args=('orderThread',))
    orderThread.start()

if __name__ == '__main__':
    main()
