from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order


class Trader:

    # Creating a class attribute to store all the data we receive; populated iteratively
    df = pd.DataFrame()

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        # This strategy computes the fair price of the asset using the vwap method, then defines a price at which we
        # are ready to trade

        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            # Add the data for this symbol
            self.store_data(symbol, state)

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

            # Computing the order imbalance
            mask_ob_buy = False
            mask_ob_sell = False
            try:
                best_ask = min(order_depth.sell_orders.keys())
                best_ask_volume = order_depth.sell_orders[best_ask]

                best_bid = max(order_depth.buy_orders.keys())
                best_bid_volume = order_depth.buy_orders[best_bid]
                order_imbalance = (best_bid_volume - best_ask_volume) / (best_bid_volume + best_ask_volume)
                mask_ob_buy = order_imbalance > 0.5
                mask_ob_sell = order_imbalance < 0.5
            except Exception as e:
                print(e)

            # Check if there are any SELL orders in the  market
            if (len(order_depth.sell_orders) != 0) & (mask_ob_buy):
                # Adding volume until we reach a price where we don't to buy anymore
                total_volume_buy = sum(
                    value for key, value in order_depth.sell_orders.items() if key < fair_value_asset)
                buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items() if
                                       key < fair_value_asset]
                if total_volume_buy != 0:
                    print(str(len(buy_orders_to_place)) + " buy orders to place:")
                    for buy_order_to_place in buy_orders_to_place:
                        buy_price = buy_order_to_place[0]
                        buy_volume = - buy_order_to_place[1]
                        orders.append(Order(symbol, buy_price, buy_volume))
                        print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

            if (len(order_depth.buy_orders) != 0) & (mask_ob_sell):
                total_volume_sell = sum(
                    value for key, value in order_depth.buy_orders.items() if key > fair_value_asset)
                sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items() if
                                        key > fair_value_asset]
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

    def store_data(self, symbol, state: TradingState):
        # Appends data related to a symbol to the dataframe that stores all the info
        try:
            order_depth = state.order_depths[symbol]
            best_ask = min(order_depth.sell_orders.keys())
            best_ask_volume = order_depth.sell_orders[best_ask]
            timestamp = state.timestamp
            best_bid = min(order_depth.buy_orders.keys())
            best_bid_volume = order_depth.sell_orders[best_ask]
            mid_price = (best_bid + best_ask) / 2

            # Add new row to df class attribute
            row = [{'timestamp': timestamp, 'symbol': symbol, 'best_bid': best_bid, 'vol_best_bid': best_bid_volume,
                    'best_ask': best_ask, 'vol_best_ask': best_ask_volume, 'mid_price': mid_price}]
            self.df = pd.concat([self.df, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception as e:  # Could happen if nothing in the order book for one side/both sides
            print(e)
