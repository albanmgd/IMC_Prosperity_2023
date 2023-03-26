from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order

class Trader:
    limits = {
        "PEARLS": 20, "BANANAS": 20, "COCONUTS": 600, "PINA_COLADAS": 300
    }
    pearl_lim = {"low": 11, "mid": 6, "high": 3}
    pearl_pos = {"low":  0, "mid": 0, "high": 0}

    def run(self, state):
        result = {}
        # loop through listings
        for symbol in state.listings.keys():
            if symbol == "PEARLS":
                orders = self.get_pearls_orders(symbol, state)
            else:
                orders = []
            orders = self.trim_orders(symbol, state, orders)
            self.print_orders(orders)
            result[symbol] = orders

        return result

    def trim_orders(self, symbol, state, orders):
        if symbol in state.position.keys():
            position = state.position[symbol]
        else:
            position = 0
        if not orders:
            return orders
        # break orders into two sublists, because the aggregated sell/buy volumes matter
        buys = [order for order in orders if order.quantity > 0]
        sells = [order for order in orders if order.quantity < 0]
        # if there are buy orders, lets check if there is any surplus
        if buys:
            sum_buy = sum([b.quantity for b in buys])
            buy_surplus = position + sum_buy - self.limits[symbol]
            if buy_surplus > 0:
                print("We have Surplus on", symbol, "Position is", position, "we want to buy", sum_buy)
                # trim buy orders
                # targetting the least probable to go through orders first
                # we sort them and reduce the volumes until we don't have the problem anymore
                buys.sort(key=lambda x: x.price, reverse=True)
                # buys are sorted based on their price. the lowest price in the list is less likely to be picked up
                for i in range(buy_surplus):
                    # each iteration we:
                    # -reduce the least probable order volume by 1
                    # -if its volume hits 0, we remove it from our buys
                    buys[0].quantity = buys[0].quantity - 1
                    if buys[0].quantity == 0:
                        buys.pop(0)

            # but how about fitting the order? :)
            if symbol != "PEARLS":
                buy_surplus = 0
            if buy_surplus < 0:
                print("We have room to grow on buy side", symbol, "Position is", position, "we want to buy", sum_buy)
                # fit buy orders to limits
                # increment every buy order volume 1 by 1 until we reach limit
                # we don't necessarily have to sort them now, we can experiment with it later
                # buys.sort(key=lambda x: x.price)
                for buy in buys:
                    if buy_surplus < 0:
                        buy.quantity = buy.quantity + int((-buy_surplus) / len(buys))
                        buy_surplus += int((-buy_surplus) / len(buys))
                if buy_surplus < 0:
                    buys[0].quantity = buys[0].quantity - buy_surplus

        # if there are sell orders, lets check if there is any surplus
        if sells:
            sum_sell = sum([b.quantity for b in sells])
            sell_surplus = -self.limits[symbol] - (position + sum_sell)
            if sell_surplus > 0:
                print("We have Surplus on", symbol, "Position is", position, "we want to buy", sum_sell)
                # trim sell orders
                # targettin the least probable to go through order first
                # we sort them and reduce the volumes until we don't have the problem anymore
                sells.sort(key=lambda x: x.price)
                # sells are sorted based on their price, descending. the highest price is least attractive for bots
                for i in range(sell_surplus):
                    # each iteration we:
                    # -increase the least attractive order volume by 1 (sells are negative)
                    # -if its volume hits 0, we remove it from our sells
                    sells[0].quantity = sells[0].quantity + 1
                    if sells[0].quantity == 0:
                        sells.pop(0)

            # how about fitting the order? :)
            if symbol != "PEARLS":
                sell_surplus = 0
            if sell_surplus < 0:
                print("We have room to grow on sell side", symbol, "Position is", position, "we want to sell", sum_sell)
                # sells.sort(key=lambda x: x.price)
                for sell in sells:
                    if sell_surplus < 0:
                        sell.quantity = sell.quantity - int((-sell_surplus) / len(sells))
                        sell_surplus += int((-sell_surplus) / len(sells))
                if sell_surplus < 0:
                    sells[0].quantity = sells[0].quantity - sell_surplus

        # now we have buys and sells that are trimmed. We combine them in a list and return it
        result = buys + sells
        return result

    def print_orders(self, orders: list[Order]):
        print("-------------- orders we put ------------------------")
        for order in orders:
            if order.quantity < 0:
                print("SELL order for ", order.symbol, " p: ", order.price, " q: ", order.quantity)
            else:
                print("BUY order for ", order.symbol, " p: ", order.price, " q: ", order.quantity)
        print("-------------- orders we put ------------------------")


    @staticmethod
    def get_vwap_price(order_depth):
        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
            + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
           / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))

        return vwap

    def get_pearls_orders(self, symbol, state):
        order_depth = state.order_depths[symbol]
        if symbol in state.own_trades.keys():
            own_trades = state.own_trades[symbol]
        # we need own_trades cuz we're keeping position in our own way
        if symbol not in state.position.keys() or state.position[symbol] == 0:
            for key in self.pearl_pos.keys():
                self.pearl_pos[key] = 0
        elif own_trades:
            for trade in own_trades:
                if trade.seller == "SUBMISSION":
                    if trade.price >= 10004:
                        key = "high"
                    elif trade.price >= 10002:
                        key = "mid"
                    else:
                        key = "low"
                    self.pearl_pos[key] -= trade.quantity
                if trade.buyer == "SUBMISSION":
                    if trade.price <= 9996:
                        key = "high"
                    elif trade.price <= 9998:
                        key = "mid"
                    else:
                        key = "low"
                    self.pearl_pos[key] += trade.quantity
        orders = []
        # we have our positions loaded, limits are in pearl_lim
        vwap = self.get_vwap_price(order_depth)
        # processing market orders
        # buy
        for price, volume in order_depth.sell_orders.items():
            if price < vwap:
                if price <= 9996:
                    key = "high"
                elif price <= 9998:
                    key = "mid"
                else:
                    key = "low"
                # we are at limit, no buying
                if self.pearl_pos[key] >= self.pearl_lim[key]:
                    continue
                if -volume + self.pearl_pos[key] > self.pearl_lim[key]:
                    volume = self.pearl_pos[key] - self.pearl_lim[key]
                orders.append(Order(symbol, price, -volume))
                # update the corresponding pos
                self.pearl_pos[key] -= volume
        # sell
        for price, volume in order_depth.buy_orders.items():
            if price > vwap:
                if price >= 10004:
                    key = "high"
                elif price >= 10002:
                    key = "mid"
                else:
                    key = "low"
                if self.pearl_pos[key] <= -self.pearl_lim[key]:
                    continue
                if -volume + self.pearl_pos[key] < -self.pearl_lim[key]:
                    volume = self.pearl_pos[key] + self.pearl_lim[key]
                orders.append(Order(symbol, price, -volume))
                # update the corresponding pos
                self.pearl_pos[key] -= volume

        if not orders:
            for i in range(2, 5):
                orders.append(Order(symbol, vwap-i*0.6, 5-i))
                orders.append(Order(symbol, vwap+i*0.6, i-5))

        return orders
