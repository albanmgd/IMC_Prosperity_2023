from typing import Dict, List
from datamodel import OrderDepth, TradingState, Order
import pandas as pd
import numpy as np
import statistics as stat
import math as mt


class Trader:
    # Creating a class attribute to store all the data we receive; populated iteratively
    df_data_market = pd.DataFrame()

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        # Initialize the method output dict as an empty dict
        result = {}

        # Iterate over all the keys (the available products) contained in the order depths
        for symbol in state.listings.keys():
            # Add the data for this symbol first
            self.store_data_market(symbol, state)

            # Then we look for an opportunity based on past data (we need to have at least one row in df_data_market)
            order_depth = state.order_depths[symbol]

            # We look at every buy order in the order book that is higher than any past sell order (since we know
            # current O.B. can't be crossed)
            if len(order_depth.sell_orders) != 0 and len(self.df_data_market > 0):
                # We get the appropriate past orders
                mask_symbol = self.df_data_market["symbol"] == symbol
                df_past_orders = self.df_data_market[mask_symbol]

                lowest_ask = min(order_depth.sell_orders.keys())
                volume_lowest_ask = order_depth.sell_orders.get(lowest_ask)
                mask_opportunities = df_past_orders["best_bid_1"] > lowest_ask
                df_opportunities = df_past_orders[mask_opportunities]

                # We now have the set of all possible opportunities; we send orders for the most lucratives ones
                print(df_opportunities)


        return result

    def store_data_market(self, symbol, state: TradingState):
        # Appends data related to a symbol to the dataframe that stores all the info
        try:
            timestamp = state.timestamp
            order_depth = state.order_depths[symbol]
            # Sell side
            ask_1 = min(order_depth.sell_orders.keys())
            volume_ask_1 = order_depth.sell_orders[ask_1]
            ask_2 = list(order_depth.sell_orders.keys())[1]
            volume_ask_2 = order_depth.sell_orders[ask_2]
            ask_3 = max(order_depth.sell_orders.keys())
            volume_ask_3 = order_depth.sell_orders[ask_3]

            # Buy side
            bid_1 = max(order_depth.buy_orders.keys())
            volume_bid_1 = order_depth.buy_orders[bid_1]
            bid_2 = list(order_depth.buy_orders.keys())[1]
            volume_bid_2 = order_depth.buy_orders[bid_2]
            bid_3 = min(order_depth.buy_orders.keys())
            volume_bid_3 = order_depth.buy_orders[bid_3]

            mid_price = (bid_1 + ask_1) / 2
            spread = ask_1 - bid_1

            # Add new row to df class attribute
            row = [{'timestamp': timestamp, 'symbol': symbol, 'bid_1': bid_1, 'vol_bid_1': volume_bid_1, 'bid_2': bid_2,
                    'vol_bid_2': volume_bid_2, 'bid_3': bid_3, 'vol_bid_3': volume_bid_3, 'ask_1': ask_1,
                    'vol_ask_1': volume_ask_1, 'ask_2': ask_2, 'vol_ask_2': volume_ask_2, 'ask_3': ask_3,
                    'vol_ask_3': volume_ask_3, 'mid_price': mid_price, "spread": spread}]
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception as e:  # Could happen if nothing in the order book for one side/both sides
            print(e)

