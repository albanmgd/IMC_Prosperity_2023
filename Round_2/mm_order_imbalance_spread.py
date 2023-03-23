from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order


class Trader:
    # Limits for each product
    limits = {"PEARLS": 20, "BANANAS": 20, "COCONUTS": 600, "PINA_COLADAS": 300}

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        # This strategy computes the fair price of the asset using the vwap method, then defines a price at which we
        # are ready to trade

        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            if symbol == "PINA_COLADAS" or symbol == "COCONUTS":
                current_pos = self.get_current_pos_symbol(symbol, state)
                limit = self.limits.get(symbol)
                available_buy_pos = self.get_available_pos(symbol, state, "BUY")
                available_sell_pos = self.get_available_pos(symbol, state, "SELL")
                print("Our current position on: " + symbol + ' is: ' + str(current_pos))
                print("Our own trades for " + symbol + ' are: ')
                print(state.own_trades.get(symbol))
                print("Market trades for " + symbol + ' are: ')
                print(state.market_trades.get(symbol))
                print("We can buy: " + str(available_buy_pos))
                print("We can sell: " + str(available_sell_pos))

                # Retrieve the Order Depth containing all the market BUY and SELL orders for the symbol
                order_depth: OrderDepth = state.order_depths[symbol]

                # Initialize the list of Orders to be sent as an empty list
                orders: list[Order] = []

                # Computing the fair value of the asset based on VWAP algo:
                # fair_value_asset = (sum([k * v for k, v in order_depth.buy_orders.items()])
                #                     + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
                #                    / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))
                best_ask = min(order_depth.sell_orders.keys())
                best_ask_volume = order_depth.sell_orders[best_ask]

                best_bid = max(order_depth.buy_orders.keys())
                best_bid_volume = order_depth.buy_orders[best_bid]
                order_imbalance = (best_bid_volume - abs(best_ask_volume)) / (best_bid_volume + abs(best_ask_volume))
                fair_value_asset = (best_bid + best_ask) / 2  # just the mid-price so far

                # Computing the spread & fair prices
                spread = min(order_depth.sell_orders.keys()) - max(order_depth.buy_orders.keys())
                buy_spread = (- 4 * order_imbalance / 15 + 3 / 5) * spread
                sell_spread = (4 * order_imbalance / 15 + 3 / 5) * spread
                fair_buy_price = fair_value_asset - buy_spread / 2  # Willing to buy lower than my valuation
                fair_sell_price = fair_value_asset + sell_spread / 2

                print("For " + symbol + " our fair price is: " + str(fair_value_asset))
                print("We quote on the buy side at: " + str(fair_buy_price))
                print("We quote on the sell side at: " + str(fair_sell_price))
                # Check if there are any SELL orders in the  market
                orders.append(Order(symbol, fair_buy_price, min(best_bid_volume, available_buy_pos)))
                orders.append(Order(symbol, fair_sell_price, - min(abs(best_ask_volume), abs(available_buy_pos))))
                if (len(order_depth.sell_orders) != 0) :
                    # Adding volume until we reach a price where we don't to buy anymore
                    total_volume_buy = sum(
                        value for key, value in order_depth.sell_orders.items() if key < fair_buy_price)
                    buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items() if
                                           key < fair_buy_price]
                    if total_volume_buy != 0:
                        print(str(len(buy_orders_to_place)) + " buy orders to place:")
                        for buy_order_to_place in buy_orders_to_place:
                            buy_price = buy_order_to_place[0]
                            buy_volume = - buy_order_to_place[1]
                            orders.append(Order(symbol, buy_price, buy_volume))
                            print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

                if (len(order_depth.buy_orders) != 0):
                    total_volume_sell = sum(
                        value for key, value in order_depth.buy_orders.items() if key > fair_sell_price)
                    sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items() if
                                            key > fair_sell_price]
                    if total_volume_sell != 0:
                        print(str(len(sell_orders_to_place)) + " sell orders to place:")
                        for sell_order_to_place in sell_orders_to_place:
                            price = sell_order_to_place[0]
                            volume = - sell_order_to_place[1]
                            orders.append(Order(symbol, price, volume))
                            print("SELL " + str(symbol) + " price: ", str(price) + " volume: ", str(volume))
                print("The orders we sent are:")
                print(orders)
            # Add all the above orders to the result dict
                result[symbol] = orders

        return result

    @staticmethod
    def get_current_pos_symbol(symbol, state):
        if symbol in state.position.keys():
            current_pos = state.position[symbol]
        else:
            current_pos = 0
        return current_pos

    def get_available_pos(self, symbol, state, side):
        limit = self.limits.get(symbol)
        current_pos = self.get_current_pos_symbol(symbol, state)
        if side == "BUY":
            if current_pos >= 0:
                return (limit - current_pos)
            else:
                return (abs(current_pos) + limit)
        if side == "SELL":
            if current_pos >= 0:
                return (- limit - current_pos)
            else:
                return (abs(current_pos) - limit)



