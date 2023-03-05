from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order


class Trader:

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        # This strategy computes the fair price of the asset using the vwap method, then defines a price at which we
        # are ready to trade

        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            # Looking at the position to know where we are at:
            # current_pos = state.position[symbol]

            # Retrieve the Order Depth containing all the market BUY and SELL orders for the symbol
            order_depth: OrderDepth = state.order_depths[symbol]

            # Initialize the list of Orders to be sent as an empty list
            orders: list[Order] = []

            # Computing the fair value of the asset based on VWAP algo:
            fair_value_asset = (sum([k * v for k, v in order_depth.buy_orders.items()])
                                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
                                / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))

            # Computing the spread & fair prices
            spread = min(order_depth.sell_orders.keys()) - max(order_depth.buy_orders.keys())
            fair_buy_price = fair_value_asset - spread / 2  # Willing to buy lower than my valuation
            fair_sell_price = fair_value_asset + spread / 2

            # Check if there are any SELL orders in the  market
            if len(order_depth.sell_orders) != 0:
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

            if len(order_depth.buy_orders) != 0:
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

            # Add all the above orders to the result dict
            result[symbol] = orders

        return result
