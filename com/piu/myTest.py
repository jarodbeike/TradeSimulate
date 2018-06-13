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
                if Decimal(last) > Decimal(rate):
                    unfreeze_amount = Decimal(last) * Decimal(amount)
                    trade_amount = Decimal(last) * Decimal(amount)

                    balance_base = SQLiteManager.check_balance(OWNER_ID, CURRENCY_BASE)
                    base_balance_id = balance_base.get("balanceId")
                    base_current_balance = balance_base.get("currentBalance")
                    base_buy_amount = balance_base.get("buyAmount")
                    base_sell_amount = balance_base.get("sellAmount")
                    base_freeze_amount = balance_base.get("freezeAmount")

                    balance_other = SQLiteManager.check_balance(OWNER_ID, CURRENCY_OTHER)
                    other_balance_id = balance_other.get("balanceId")
                    other_current_balance = balance_other.get("currentBalance")
                    other_buy_amount = balance_other.get("buyAmount")
                    other_sell_amount = balance_other.get("sellAmount")
                    other_freeze_amount = balance_other.get("freezeAmount")

                    conn = SQLiteManager.get_conn(SQLiteManager.DB_FILE_PATH)
                    SQLiteManager.update_order(conn, order_id, amount, last, STATUS_TOTAL_FILLED)
                    SQLiteManager.insert_trade(conn, OWNER_ID, order_id, SIDE_BUY, currency_pair, rate, amount, amount,
                                               last, amount)

                    SQLiteManager.update_balance(conn, OWNER_ID, CURRENCY_BASE,
                                                 str(trade_amount.quantize(Decimal('0.0000'))), 0,
                                                 str(-unfreeze_amount.quantize(Decimal('0.0000'))))
                    end_base_freeze_amount = Decimal(base_freeze_amount) - unfreeze_amount
                    end_base_sell_amount = Decimal(base_sell_amount) + trade_amount
                    SQLiteManager.insert_balance_log(conn, OWNER_ID, base_balance_id, CURRENCY_BASE,
                                                     base_current_balance, base_current_balance, base_buy_amount,
                                                     base_buy_amount,
                                                     base_sell_amount, end_base_sell_amount, base_freeze_amount,
                                                     str(end_base_freeze_amount.quantize(Decimal('0.0000'))),
                                                     DATA_TYPE_TRADE, order_id, BIZ_TYPE_BUY_TRADE)

                    SQLiteManager.update_balance(conn, OWNER_ID, CURRENCY_OTHER,
                                                 str(trade_amount.quantize(Decimal('0.0000'))), 0, 0)
                    end_other_buy_amount = Decimal(base_buy_amount) + trade_amount
                    SQLiteManager.insert_balance_log(conn, OWNER_ID, other_balance_id, CURRENCY_OTHER,
                                                     other_current_balance, other_current_balance, other_buy_amount,
                                                     end_other_buy_amount,
                                                     base_sell_amount, base_sell_amount, base_freeze_amount,
                                                     base_freeze_amount,
                                                     DATA_TYPE_TRADE, order_id, BIZ_TYPE_BUY_TRADE)

        sell_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_SELL, STATUS_ACCEPTED,
                                                                    STATUS_HALF_FILLED)
        if sell_order_open_list is not None:
            for e in range(len(sell_order_open_list)):
                print(sell_order_open_list[e])
        time.sleep(10)


def order_action(arg):
    currency_pair = CURRENCY_OTHER + '_' + CURRENCY_BASE
    # {"eos_usdt":{"decimal_places":4,"min_amount":0.0001,"fee":0.2}}
    while (1):
        ticker = gate_query.ticker(currency_pair)
        # 买入订单创建，在挂订单不能超过10个
        buy_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_BUY, STATUS_ACCEPTED,
                                                                   STATUS_HALF_FILLED)
        if buy_order_open_list is None or len(buy_order_open_list) < 10:
            last = ticker.get("last")
            balance_base = SQLiteManager.check_balance(OWNER_ID, CURRENCY_BASE)
            balance_id = balance_base.get("balanceId")
            current_balance = balance_base.get("currentBalance")
            buy_amount = balance_base.get("buyAmount")
            sell_amount = balance_base.get("sellAmount")
            freeze_amount = balance_base.get("freezeAmount")
            available_amount = Decimal(current_balance) - Decimal(freeze_amount) + (
                Decimal(sell_amount) - Decimal(buy_amount));
            amount = 100;
            last_freeze_amount = Decimal(last) * Decimal(amount)
            print(
                'buy_availableAmount:[{}],buy_lastFreezeAmount:[{}]'.format(
                    available_amount.quantize(Decimal('0.0000')),
                    last_freeze_amount.quantize(
                        Decimal('0.0000'))))
            # 购买力足够，使用事务进行DB操作
            if available_amount > last_freeze_amount:
                conn = SQLiteManager.get_conn(SQLiteManager.DB_FILE_PATH)
                order_id = SQLiteManager.insert_order(conn, OWNER_ID, SIDE_BUY, currency_pair, last, amount, 0, 0,
                                                      STATUS_ACCEPTED)
                SQLiteManager.update_balance(conn, OWNER_ID, CURRENCY_BASE, 0, 0,
                                             str(last_freeze_amount.quantize(Decimal('0.0000'))))
                end_freeze_amount = last_freeze_amount + Decimal(freeze_amount)
                SQLiteManager.insert_balance_log(conn, OWNER_ID, balance_id, CURRENCY_BASE,
                                                 current_balance, current_balance, buy_amount, buy_amount,
                                                 sell_amount, sell_amount, freeze_amount,
                                                 str(end_freeze_amount.quantize(Decimal('0.0000'))),
                                                 DATA_TYPE_ORDER, order_id, BIZ_TYPE_BUY_ORDER)
                conn.commit()
        # 展示已有未成交订单
        # showList(buyOrderOpenList)
        end_balance_base = SQLiteManager.check_balance(OWNER_ID, CURRENCY_BASE)
        print(end_balance_base)

        # 卖出订单创建，在挂订单不能超过10个
        sell_order_open_list = SQLiteManager.query_order_not_filled(OWNER_ID, SIDE_SELL, STATUS_ACCEPTED,
                                                                    STATUS_HALF_FILLED)
        if sell_order_open_list is None or len(sell_order_open_list) < 10:
            last = ticker.get("last")
            balance_base = SQLiteManager.check_balance(OWNER_ID, CURRENCY_OTHER)
            balance_id = balance_base.get("balanceId")
            current_balance = balance_base.get("currentBalance")
            buy_amount = balance_base.get("buyAmount")
            sell_amount = balance_base.get("sellAmount")
            freeze_amount = balance_base.get("freezeAmount")
            available_amount = Decimal(current_balance) - Decimal(freeze_amount) + (
                Decimal(sell_amount) - Decimal(buy_amount));
            amount = 100;
            last_freeze_amount = Decimal(last) * Decimal(amount)
            print('sell_availableAmount:[{}],sell_lastFreezeAmount:[{}]'.format(
                available_amount.quantize(Decimal('0.0000')), last_freeze_amount.quantize(Decimal('0.0000'))))
            # 购买力足够，使用事务进行DB操作
            if available_amount > last_freeze_amount:
                conn = SQLiteManager.get_conn(SQLiteManager.DB_FILE_PATH)
                order_id = SQLiteManager.insert_order(conn, OWNER_ID, SIDE_SELL, currency_pair, last, amount, 0, 0,
                                                      STATUS_ACCEPTED)
                SQLiteManager.update_balance(conn, OWNER_ID, CURRENCY_OTHER, 0, 0,
                                             str(last_freeze_amount.quantize(Decimal('0.0000'))))
                end_freeze_amount = last_freeze_amount + Decimal(freeze_amount)
                SQLiteManager.insert_balance_log(conn, OWNER_ID, balance_id, CURRENCY_OTHER,
                                                 current_balance, current_balance, buy_amount, buy_amount,
                                                 sell_amount, sell_amount, freeze_amount,
                                                 str(end_freeze_amount.quantize(Decimal('0.0000'))),
                                                 DATA_TYPE_ORDER, order_id, BIZ_TYPE_SELL_ORDER)
                conn.commit()
        # 展示已有未成交订单
        # showList(sellOrderOpenList)
        end_balance_other = SQLiteManager.check_balance(OWNER_ID, CURRENCY_OTHER)
        print(end_balance_other)

        time.sleep(10)


def show_list(some_list):
    if some_list is not None and len(some_list) > 0:
        for e in range(len(some_list)):
            print(some_list[e])


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
