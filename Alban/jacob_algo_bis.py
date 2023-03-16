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
        print("Market Data:")
        print(self.df_data_market.tail())

        # Iterate over all the keys (the available products) contained in the order depths
        for symbol in state.listings.keys():
            # Add the data for this symbol first
            self.store_data_market(symbol, state)

            # Then we look for an opportunity based on past data (we need to have at least one row in df_data_market)
            order_depth = state.order_depths[symbol]

            # We look at every buy order in the order book that is higher than any past sell order (since we know
            # current O.B. can't be crossed)
            if len(order_depth.sell_orders) != 0 and len(self.df_data_market) > 0:
                # We get the appropriate past orders
                mask_symbol = self.df_data_market["symbol"] == symbol
                df_past_orders = self.df_data_market[mask_symbol]

                # First looking at the most attractive order we can profit from
                lowest_ask = min(order_depth.sell_orders.keys())
                volume_lowest_ask = order_depth.sell_orders.get(lowest_ask)

                # First looking for the most competitive orders
                # orders_1 = self.get_opportunities_for_price(symbol, df_past_orders, lowest_ask, volume_lowest_ask, 1, "SELL")
                # result[symbol] = orders_1




        return result


    def get_opportunities_for_price(self, symbol, df_past_orders, price_level, volume_price_level, level_order_book: int, side: str):
        orders: list[Order] = []

        if side == "SELL":
            price_level_ob = "bid_" + str(level_order_book)
            volume_level_ob = "vol_bid_" + str(level_order_book)

            # Dropping rows where we don't have value for the column we're looking at
            df_past_orders = df_past_orders.dropna(subset=[price_level_ob])
            print("Dataframe of the past orders for the " + side + "and level " + str(level_order_book))
            print(df_past_orders.tail())
            # print(df_past_orders.head())
            mask_opportunities = df_past_orders[price_level_ob] > price_level
            df_opportunities = df_past_orders[mask_opportunities]
            # Then we send orders by most attractive opportunities:
            if len(df_opportunities) > 0:
                # We want a series of unique prices
                buy_order_prices = sorted(df_opportunities[price_level_ob].drop_duplicates().to_list())
                # And now we aggregate by price
                for price in buy_order_prices:
                    df = df_opportunities.copy()  # don't alter the original data
                    mask_price = df[price_level_ob] == price  # we know there are some
                    df = df[mask_price]
                    aggregated_volume = sum(df[volume_level_ob].to_list())
                    # volume_prices = [max(vol, volume_price_level) for vol in df_opportunities[volume_level_ob].to_list()] # Then we take the max we can submit
                    orders = self.send_bulk_orders(symbol, price, aggregated_volume)

            return orders



    def send_bulk_orders(self, symbol, buy_order_prices, volume_prices):
        orders: list[Order] = []
        # Both lists have the same length in theory
        for i in range(0, len(buy_order_prices)):
            orders.append(Order(symbol, buy_order_prices[i], volume_prices[i]))
        return orders








    def store_data_market(self, symbol, state: TradingState):
        # Appends data related to a symbol to the dataframe that stores all the info
        try:
            timestamp = state.timestamp
            order_depth = state.order_depths[symbol]
            ask_1 = np.nan
            volume_ask_1 = np.nan
            ask_2 = np.nan
            volume_ask_2 = np.nan
            ask_3 = np.nan
            volume_ask_3 = np.nan

            bid_1 = np.nan
            volume_bid_1 = np.nan
            bid_2 = np.nan
            volume_bid_2 = np.nan
            bid_3 = np.nan
            volume_bid_3 = np.nan

            mid_price = np.nan
            spread = np.nan

            # Sell side
            if len(order_depth.sell_orders) == 1:
                ask_1 = min(order_depth.sell_orders.keys())
                volume_ask_1 = order_depth.sell_orders[ask_1]

            if len(order_depth.sell_orders) == 2:
                ask_1 = min(order_depth.sell_orders.keys())
                volume_ask_1 = order_depth.sell_orders[ask_1]
                ask_2 = list(order_depth.sell_orders.keys())[1]
                volume_ask_2 = order_depth.sell_orders[ask_2]

            if len(order_depth.sell_orders) == 3:
                ask_1 = min(order_depth.sell_orders.keys())
                volume_ask_1 = order_depth.sell_orders[ask_1]
                ask_2 = list(order_depth.sell_orders.keys())[1]
                volume_ask_2 = order_depth.sell_orders[ask_2]
                ask_3 = max(order_depth.sell_orders.keys())
                volume_ask_3 = order_depth.sell_orders[ask_3]

            # Buy side
            if len(order_depth.buy_orders) == 1:
                bid_1 = max(order_depth.buy_orders.keys())
                volume_bid_1 = order_depth.buy_orders[bid_1]

            if len(order_depth.buy_orders) == 2:
                bid_1 = max(order_depth.buy_orders.keys())
                volume_bid_1 = order_depth.buy_orders[bid_1]
                bid_2 = list(order_depth.buy_orders.keys())[1]
                volume_bid_2 = order_depth.buy_orders[bid_2]

            if len(order_depth.buy_orders) == 3:
                bid_1 = max(order_depth.buy_orders.keys())
                volume_bid_1 = order_depth.buy_orders[bid_1]
                bid_2 = list(order_depth.buy_orders.keys())[1]
                volume_bid_2 = order_depth.buy_orders[bid_2]
                bid_3 = min(order_depth.buy_orders.keys())
                volume_bid_3 = order_depth.buy_orders[bid_3]

            if (len(order_depth.buy_orders) > 0) and (len(order_depth.sell_orders) > 0):
                mid_price = (bid_1 + ask_1) / 2 # if we're here we know bid_1 & ask_1 are defined
                spread = ask_1 - bid_1

            # Add new row to df class attribute
            row = [{'timestamp': timestamp, 'symbol': symbol, 'bid_1': bid_1, 'vol_bid_1': volume_bid_1, 'bid_2': bid_2,
                    'vol_bid_2': volume_bid_2, 'bid_3': bid_3, 'vol_bid_3': volume_bid_3, 'ask_1': ask_1,
                    'vol_ask_1': volume_ask_1, 'ask_2': ask_2, 'vol_ask_2': volume_ask_2, 'ask_3': ask_3,
                    'vol_ask_3': volume_ask_3, 'mid_price': mid_price, "spread": spread}]
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception:  # Could happen if nothing in the order book for one side/both sides
            pass

