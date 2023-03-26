from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order


class Trader:
    limits = {"PEARLS": 20, "BANANAS": 20, "COCONUTS": 600, "PINA_COLADAS": 300, "BERRIES": 250, "DIVING_GEAR": 50}
    # Creating a class attribute to store all the data we receive; populated iteratively
    df_data_market = pd.DataFrame()

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""

        # Initialize the method output dict as an empty dict
        result = {}
        for symbol in state.listings.keys():  # done once and for all the symbols; useful to have same data across !=
            # timestamps
            # Add the data for this symbol
            self.store_data_market(symbol, state)

        # Looping through all the symbols
        for symbol in state.listings.keys():
            if symbol == "BERRIES":
                result["BERRIES"] = self.get_orders_berries(state)

        return result

    def get_orders_berries(self, state: TradingState):
        orders_berries: list[Order] = []

        # Getting the stored data
        mask_berries = self.df_data_market["symbol"] == "BERRIES"
        df_berries = self.df_data_market[mask_berries]
        # print("The stored order book for BERRIES is: ")
        # print(df_berries.tail().to_string())

        # Getting the order book:
        print("The order book for BERRIES is:")
        order_book_berries = state.order_depths["BERRIES"]

        # Getting our last trades on BERRIES
        own_trades = self.get_own_trades_symbol("BERRIES", state)

        # Aaand getting our current position on BERRIES atm
        current_pos_berries = self.get_pos_symbol("BERRIES", state)
        print("The current position on BERRIES is:" + str(current_pos_berries))

        # PARAMETERS: can be modified when backtesting: max sizing we want to fill for a price and dt between trades
        max_volume_per_order = 15
        timestamp_delta = 1000
        max_timestamp_between_trades_first_phase = 10000  # which is 20 iterations in the end
        max_timestamp_between_trades_second_phase = 3000
        max_timestamp_between_trades_last_phase = 15000  # lasts much longer, makes sense to wait more between trades
        desired_pos_first_phase = 250
        desired_pos_second_phase = -250
        desired_pos_last_phase = -10  # Letting this run at the close

        # We first begin by the building our long position
        if 5000 < state.timestamp < 350000:
            # Best ask; we want to be filled at the lowest available price
            ask_price_1 = min(order_book_berries.sell_orders.keys())
            ask_volume_1 = order_book_berries.sell_orders.get(ask_price_1)

            # Initiating the control of the volume
            volume_to_send_berries = desired_pos_first_phase - current_pos_berries
            volume_to_submit = min(abs(ask_volume_1), abs(volume_to_send_berries), max_volume_per_order)

            # Computing the stats I want to look at before buying
            fifth_pct = np.percentile(df_berries['ask_price_1'], 5)
            tenth_pct = np.percentile(df_berries['ask_price_1'], 10)
            twenty_pct = np.percentile(df_berries['ask_price_1'], 20)
            # We want to make sure we don't end up buying iteration after iteration if we are in a downtrend
            if own_trades is not None:
                mask_timestamp = state.timestamp - own_trades[0].timestamp > timestamp_delta  # Adding the [0] in case we did multiple; not supposed to happen
                trade_timestamp = own_trades[0].timestamp
            else:
                mask_timestamp = True
                trade_timestamp = 0

            if (ask_price_1 < fifth_pct) and mask_timestamp:  # If possible we get filled here
                orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))
            elif (ask_price_1 < tenth_pct) and mask_timestamp:  # If possible we get filled here
                orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))
            elif (ask_price_1 < twenty_pct) and mask_timestamp:
                orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))
            elif (state.timestamp - trade_timestamp) == max_timestamp_between_trades_first_phase:
                orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))

        # Now looking to build the short position
        if 400000 <= state.timestamp <= 550000:
            # Getting rid of previous data
            df_short_berries = self.df_data_market.copy()
            mask_timestamp = 395000 <= df_short_berries["timestamp"]  # Leaving some time to compute stats 
            df_short_berries = df_short_berries[mask_timestamp]

            # Best bid; we want to be filled at the highest available price
            bid_price_1 = max(order_book_berries.buy_orders.keys())
            bid_volume_1 = order_book_berries.sell_orders.get(bid_price_1)

            # Initiating the control of the volume
            volume_to_send_berries = desired_pos_second_phase - current_pos_berries  # -500 in theory
            volume_to_submit = min(abs(bid_volume_1), abs(volume_to_send_berries), max_volume_per_order)

            # Computing the stats I want to look at before buying
            ninety_fifth_pct = np.percentile(df_short_berries['ask_price_1'], 95)
            ninety_pct = np.percentile(df_short_berries['ask_price_1'], 90)
            # We want to make sure we don't end up buying iteration after iteration if we are in a downtrend
            if own_trades is not None:
                mask_timestamp = state.timestamp - own_trades[0].timestamp > timestamp_delta
                trade_timestamp = own_trades[0].timestamp
            else:
                mask_timestamp = True
                trade_timestamp = 0

            if (bid_price_1 > ninety_fifth_pct) and mask_timestamp:  # If possible we get filled here
                orders_berries.append(Order("BERRIES", bid_price_1, - volume_to_submit))
            elif (bid_price_1 > ninety_pct) and mask_timestamp:
                orders_berries.append(Order("BERRIES", bid_price_1, - volume_to_submit))
            elif (state.timestamp - trade_timestamp) == max_timestamp_between_trades_second_phase:
                orders_berries.append(Order("BERRIES", bid_price_1, - volume_to_submit))

        # FINALLY looking to unload the short position
        if 555000 <= state.timestamp <= 1000000:  # Giving some time to compute data
            if (state.timestamp <= 775000) or (state.timestamp >= 825000):  # Might be overfitting but worst case is we
                # lose 10% of the time window
                # Getting rid of previous data
                df_long_berries = self.df_data_market.copy()
                mask_timestamp = 550000 <= df_long_berries["timestamp"]
                df_long_berries = df_long_berries[mask_timestamp]

                # Best ask; we want to be filled at the lowest available price
                ask_price_1 = min(order_book_berries.sell_orders.keys())
                ask_volume_1 = order_book_berries.sell_orders.get(ask_price_1)

                # Initiating the control of the volume
                volume_to_send_berries = desired_pos_last_phase - current_pos_berries  # should be abt 235 in theory
                volume_to_submit = min(abs(ask_volume_1), abs(volume_to_send_berries), max_volume_per_order)

                # Computing the stats I want to look at before buying
                fifth_pct = np.percentile(df_long_berries['ask_price_1'], 5)
                tenth_pct = np.percentile(df_long_berries['ask_price_1'], 10)
                twenty_pct = np.percentile(df_long_berries['ask_price_1'], 20)
                # We want to make sure we don't end up buying iteration after iteration if we are in a downtrend
                if own_trades is not None:
                    mask_timestamp = (state.timestamp - own_trades[0].timestamp) > timestamp_delta  # Adding the [0] if
                    # multiple trades; not supposed to happen
                    trade_timestamp = own_trades[0].timestamp
                else:
                    mask_timestamp = True
                    trade_timestamp = 0  # No trade yet <=> trade at date 0

                if (ask_price_1 < fifth_pct) and mask_timestamp:  # If possible we get filled here
                    orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))
                elif (ask_price_1 < tenth_pct) and mask_timestamp:
                    orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))
                elif (ask_price_1 < twenty_pct) and mask_timestamp:
                    orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))
                elif (state.timestamp - trade_timestamp) == max_timestamp_between_trades_last_phase:  # Otherwise we DCA
                    orders_berries.append(Order("BERRIES", ask_price_1, volume_to_submit))

        print("The orders we submitted are:")
        print(orders_berries)
        return orders_berries

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
            row = [{'timestamp': timestamp, 'symbol': symbol, 'bid_price_1': bid_1, 'bid_volume_1': volume_bid_1, 'bid_price_2': bid_2,
                    'bid_volume_2': volume_bid_2, 'bid_price_3': bid_3, 'bid_volume_3': volume_bid_3, 'ask_price_1': ask_1,
                    'ask_volume_1': volume_ask_1, 'ask_price_2': ask_2, 'ask_volume_2': volume_ask_2, 'ask_price_3': ask_3,
                    'ask_volume_3': volume_ask_3, 'mid_price': mid_price, "spread": spread}]
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception:  # Could happen if nothing in the order book for one side/both sides
            pass

    @staticmethod
    def get_pos_symbol(symbol, state):
        if symbol in state.position.keys():
            current_pos = state.position[symbol]
        else:
            current_pos = 0
        return current_pos

    @staticmethod
    def get_own_trades_symbol(symbol, state):
        if symbol in state.own_trades.keys():
            own_trades = state.own_trades[symbol]
        else:
            own_trades = None
        return own_trades
