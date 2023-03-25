from typing import Dict, List
from datamodel import OrderDepth, TradingState, Order
import pandas as pd
import numpy as np
import statistics as stat
import math as mt


class Trader:
    # Creating a class attribute to store all the data we receive; populated iteratively
    df_data_market = pd.DataFrame()
    limits = {"PEARLS": 20, "BANANAS": 20}

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        # Initialize the method output dict as an empty dict
        result = {}
        # print("Market Data:")
        # print(self.df_data_market.tail())

        # Iterate over all the keys (the available products) contained in the order depths
        for symbol in state.listings.keys():
            # Add the data for this symbol first
            self.store_data_market(symbol, state)

            # Then we look for an opportunity based on past data (we need to have at least one row in df_data_market)
            order_depth = state.order_depths[symbol]

            if symbol in state.position.keys():
                current_pos = state.position[symbol]
            else:
                current_pos = 0

            # We look at every buy order in the order book that is higher than any past sell order (since we know
            # current O.B. can't be crossed)
            if len(order_depth.sell_orders) != 0 and len(self.df_data_market) > 0:
                # We get the appropriate past orders
                mask_symbol = self.df_data_market["symbol"] == symbol
                df_past_orders = self.df_data_market[mask_symbol]

                # Getting the data related to this side of the order book
                orders_sell_side = self.get_opps_for_order_book(symbol, current_pos, df_past_orders, order_depth,
                                                                "SELL")
                result[symbol] = orders_sell_side
            return result

    def get_opps_for_order_book(self, symbol, current_pos, df_past_orders, order_depth: OrderDepth, side: str):
        orders = []
        position_limit = self.limits.get(symbol)

        # print(position_limit)
        if side == "SELL":  # Then we filter the buy side
            # price_level = min(order_depth.sell_orders.keys())
            # volume_order_book = sum(order_depth.sell_orders.values)

            # Stack the bid columns and reset the index
            bids = df_past_orders[['bid_1', 'bid_2', 'bid_3']].stack().reset_index(level=1, drop=True)
            # Stack the vol columns and reset the index
            vols = df_past_orders[['vol_bid_1', 'vol_bid_2', 'vol_bid_3']].stack().reset_index(level=1, drop=True)
            # Create a DataFrame with the stacked bids and vols
            df_stacked = pd.concat([bids, vols], axis=1, keys=['bid', 'vol'])
            # print(df_stacked.head())

            # Group the DataFrame by bid and sum the volumes
            grouped = df_stacked.groupby('bid')['vol'].sum()

            # Convert the grouped object to a dictionary
            price_dict = grouped.to_dict()

            total_volume = 0
            for price_sell_order, volume_sell_order in sorted(order_depth.sell_orders.items()):
                print("For the following order on the ask side: " + str(
                    price_sell_order) + " with a volume of " + str(volume_sell_order))

                # Filter the dictionary to keep only the desired keys
                filtered_dict = {k: v for k, v in price_dict.items() if k > price_sell_order}
                print("The set of orders we could take advantage of is:")
                print(filtered_dict)

                volume_order = 0
                for price, volume in sorted(filtered_dict.items(), reverse=True):

                    # print(price, volume) Checking if the total volume of the orders we are sending is < to the max
                    # vol we can submit and volume of the order is < to the volume in the O.B.
                    if (total_volume < (position_limit - current_pos)) and (abs(volume_order) < abs(volume_sell_order)):
                        volume_to_submit = min(abs(volume_sell_order), abs(volume), abs(position_limit - current_pos))
                        volume_order += volume_to_submit
                        current_pos += volume_to_submit
                        # print(price, volume_to_submit)
                        # updating the associated volume available
                        filtered_dict[price] = volume - volume_to_submit  # need to be a - since on the sell side r.n.
                        orders.append(Order(symbol, price, - volume_to_submit))  # and a + here
                        total_volume += volume_to_submit
                        # Adding the order to cross the order book at the actual timestamp
                        orders.append(Order(symbol, price_sell_order, - volume_sell_order))
                        print("The order for this price are: ")
                        print(orders)
                    # Finally, need to consider if we're crossing the order book

            return orders

    def get_opportunities_for_price(self, symbol, df_past_orders, price_level, volume_price_level,
                                    level_order_book: int, side: str):
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
                mid_price = (bid_1 + ask_1) / 2  # if we're here we know bid_1 & ask_1 are defined
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
