# -*- coding: utf-8 -*-
# encoding: utf-8
# !/usr/bin/python

import threading
import time
from decimal import Decimal
import SQLiteManager
from gateAPI import GateIO
from Constants import Constant

# 用户ID
OWNER_ID = 1
CURRENCY_OTHER = 'eos'
CURRENCY_BASE = 'usdt'
# 买卖方向
SIDE_BUY = 1
SIDE_SELL = 2
# 订单状态
STATUS_ACCEPTED = 1
STATUS_HALF_FILLED = 2
STATUS_TOTAL_FILLED = 3
STATUS_CANCELLED = 4
# 数据来源
DATA_TYPE_ORDER = 1
DATA_TYPE_TRADE = 2
# 业务类型
BIZ_TYPE_BUY_ORDER = 1
BIZ_TYPE_BUY_TRADE = 2
BIZ_TYPE_BUY_CANCEL = 3
BIZ_TYPE_SELL_ORDER = 4
BIZ_TYPE_SELL_TRADE = 5
BIZ_TYPE_SELL_CANCEL = 6
# 挂单位置
Last_Distant_Percent = 0.1

# 填写 apiKey APISECRET
apiKey = Constant.apiKey
secretKey = Constant.secretKey
# Provide constants
API_QUERY_URL = Constant.API_QUERY_URL
API_TRADE_URL = Constant.API_TRADE_URL
# Create a gate class instance
gate_query = GateIO(API_QUERY_URL, apiKey, secretKey)
gate_trade = GateIO(API_TRADE_URL, apiKey, secretKey)


def check_and_build_order(owner_id, currency, last, currency_pair, side, biz_type):
    balance_base = SQLiteManager.check_balance(owner_id, currency)
    balance_id = balance_base.get("balanceId")
    current_balance = balance_base.get("currentBalance")
    buy_amount = balance_base.get("buyAmount")
    sell_amount = balance_base.get("sellAmount")
    freeze_amount = balance_base.get("freezeAmount")
    available_amount = Decimal(current_balance) - Decimal(freeze_amount) + (
        Decimal(sell_amount) - Decimal(buy_amount));
    amount = 100;
    last_freeze_amount = Decimal(last) * Decimal(amount)
    # 购买力足够，使用事务进行DB操作
    if available_amount > last_freeze_amount:
        conn = SQLiteManager.get_conn(SQLiteManager.DB_FILE_PATH)
        order_id = SQLiteManager.insert_order(conn, owner_id, side, currency_pair, last, amount, 0, 0,
                                              STATUS_ACCEPTED)
        SQLiteManager.update_balance(conn, owner_id, currency, 0, 0,
                                     str(last_freeze_amount.quantize(Decimal('0.0000'))))
        end_freeze_amount = last_freeze_amount + Decimal(freeze_amount)
        SQLiteManager.insert_balance_log(conn, owner_id, balance_id, currency,
                                         current_balance, current_balance, buy_amount, buy_amount,
                                         sell_amount, sell_amount, freeze_amount,
                                         str(end_freeze_amount.quantize(Decimal('0.0000'))),
                                         DATA_TYPE_ORDER, order_id, biz_type)
        conn.commit()


def show_list(some_list):
    if some_list is not None and len(some_list) > 0:
        for e in range(len(some_list)):
            print(some_list[e])


def trade_in_transaction(owner_id, order_id, side, currency_pair, currency_in, currency_out, trade_rate, rate, amount,
                         biz_type):
    unfreeze_amount = Decimal(rate) * Decimal(amount)
    trade_amount = Decimal(trade_rate) * Decimal(amount)

    balance_out = SQLiteManager.check_balance(owner_id, currency_out)
    out_balance_id = balance_out.get("balanceId")
    out_current_balance = balance_out.get("currentBalance")
    out_buy_amount = balance_out.get("buyAmount")
    out_sell_amount = balance_out.get("sellAmount")
    out_freeze_amount = balance_out.get("freezeAmount")

    balance_in = SQLiteManager.check_balance(owner_id, currency_in)
    in_balance_id = balance_in.get("balanceId")
    in_current_balance = balance_in.get("currentBalance")
    in_buy_amount = balance_in.get("buyAmount")
    in_sell_amount = balance_in.get("sellAmount")
    in_freeze_amount = balance_in.get("freezeAmount")

    conn = SQLiteManager.get_conn(SQLiteManager.DB_FILE_PATH)
    SQLiteManager.update_order(conn, order_id, amount, trade_rate, STATUS_TOTAL_FILLED)
    SQLiteManager.insert_trade(conn, owner_id, order_id, side, currency_pair, rate, amount, amount,
                               trade_rate, amount)

    SQLiteManager.update_balance(conn, owner_id, currency_out, 0,
                                 str(trade_amount.quantize(Decimal('0.0000'))),
                                 str(-unfreeze_amount.quantize(Decimal('0.0000'))))
    end_out_sell_amount = Decimal(out_sell_amount) + trade_amount
    end_out_freeze_amount = Decimal(out_freeze_amount) - unfreeze_amount
    SQLiteManager.insert_balance_log(conn, owner_id, out_balance_id, currency_out,
                                     out_current_balance, out_current_balance, out_buy_amount,
                                     out_buy_amount,
                                     out_sell_amount, str(end_out_sell_amount.quantize(Decimal('0.0000'))),
                                     out_freeze_amount,
                                     str(end_out_freeze_amount.quantize(Decimal('0.0000'))),
                                     DATA_TYPE_TRADE, order_id, biz_type)

    SQLiteManager.update_balance(conn, owner_id, currency_in,
                                 str(trade_amount.quantize(Decimal('0.0000'))), 0, 0)
    end_other_buy_amount = Decimal(out_buy_amount) + trade_amount
    SQLiteManager.insert_balance_log(conn, owner_id, in_balance_id, currency_in,
                                     in_current_balance, in_current_balance, in_buy_amount,
                                     str(end_other_buy_amount.quantize(Decimal('0.0000'))),
                                     in_sell_amount, in_sell_amount, in_freeze_amount,
                                     in_freeze_amount,
                                     DATA_TYPE_TRADE, order_id, biz_type)
    conn.commit()


def trade_action(arg):
    currency_pair = CURRENCY_OTHER + '_' + CURRENCY_BASE
    # {"eos_usdt":{"decimal_places":4,"min_amount":0.0001,"fee":0.2}}
    while (1):
        ticker = gate_query.ticker(currency_pair)
        buy_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_BUY, STATUS_ACCEPTED,
                                                                   STATUS_HALF_FILLED)
        if buy_order_open_list is not None:
            for e in range(len(buy_order_open_list)):
                buy_order = buy_order_open_list[e]
                rate = buy_order.get("rate")
                amount = buy_order.get("amount")
                order_id = buy_order.get("orderId")
                last = ticker.get("last")
                if Decimal(last) >= Decimal(rate):
                    trade_in_transaction(OWNER_ID, order_id, SIDE_BUY, currency_pair, CURRENCY_OTHER, CURRENCY_BASE,
                                         last, rate, amount, BIZ_TYPE_BUY_TRADE)
                    print('订单成交{}', format(buy_order))

        sell_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_SELL, STATUS_ACCEPTED,
                                                                    STATUS_HALF_FILLED)
        if sell_order_open_list is not None:
            for e in range(len(sell_order_open_list)):
                sell_order = sell_order_open_list[e]
                rate = sell_order.get("rate")
                amount = sell_order.get("amount")
                order_id = sell_order.get("orderId")
                last = ticker.get("last")
                if Decimal(last) <= Decimal(rate):
                    trade_in_transaction(OWNER_ID, order_id, SIDE_SELL, currency_pair, CURRENCY_BASE, CURRENCY_OTHER,
                                         last, rate, amount, BIZ_TYPE_SELL_TRADE)
                    print('订单成交{}', format(sell_order))

        time.sleep(10)


def order_action(arg):
    currency_pair = CURRENCY_OTHER + '_' + CURRENCY_BASE
    # {"eos_usdt":{"decimal_places":4,"min_amount":0.0001,"fee":0.2}}
    while (1):
        ticker = gate_query.ticker(currency_pair)
        last = ticker.get("last")
        # 买入订单创建，在挂订单不能超过10个
        buy_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_BUY, STATUS_ACCEPTED,
                                                                   STATUS_HALF_FILLED)
        if buy_order_open_list is None or len(buy_order_open_list) < 10:
            check_and_build_order(OWNER_ID, CURRENCY_BASE, last, currency_pair, SIDE_BUY, BIZ_TYPE_BUY_ORDER)
        # 展示已有未成交订单
        # showList(buyOrderOpenList)
        end_balance_base = SQLiteManager.check_balance(OWNER_ID, CURRENCY_BASE)
        print(end_balance_base)

        # 卖出订单创建，在挂订单不能超过10个
        sell_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_SELL, STATUS_ACCEPTED,
                                                                    STATUS_HALF_FILLED)
        if sell_order_open_list is None or len(sell_order_open_list) < 10:
            check_and_build_order(OWNER_ID, CURRENCY_OTHER, last, currency_pair, SIDE_SELL, BIZ_TYPE_SELL_ORDER)
        # 展示已有未成交订单
        # showList(sellOrderOpenList)
        end_balance_other = SQLiteManager.check_balance(OWNER_ID, CURRENCY_OTHER)
        print(end_balance_other)

        time.sleep(10)


def main():
    # 数据表的创建和资产的初始化在其他地方进行
    SQLiteManager.init_data(OWNER_ID, CURRENCY_OTHER, CURRENCY_BASE, 100000)
    # 线程A，定时5秒从订单数据表中获取尚未完全成交的订单，检查订单内的币种行情数据是否和订单的可以进行成交，如果可以就创建成交记录，调整资产
    trade_thread = threading.Thread(target=trade_action, args=('tradeThread',))
    trade_thread.start()
    # 线程B，检查指定的币种行情数据，决定是否创建订单，是否取消订单
    order_thread = threading.Thread(target=order_action, args=('orderThread',))
    order_thread.start()


if __name__ == '__main__':
    main()
