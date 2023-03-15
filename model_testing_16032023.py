from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order

class Trader:
    # Creating a class attribute to store all the data we receive; populated iteratively
    df_data_market = pd.DataFrame()
    # Class attribute to monitor all our trades; state.position only gives actual size on a symbol
    df_data_trades = pd.DataFrame()
    # Defining the position limits
    limits = {"PEARLS": 20, "BANANAS": 20}

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            orders: list[Order] = []
            # Add the data for this symbol
            self.store_data_market(symbol, state)

            # Add the previous trades we did
            self.store_data_position(symbol, state)

            # Retrieve the Order Depth containing all the market BUY and SELL orders for the symbol
            order_depth: OrderDepth = state.order_depths[symbol]

            # Getting the position limit for this product & our position
            position_limit = self.limits.get(symbol)
            if symbol in state.position.keys():
                current_pos = state.position[symbol]
            else:
                current_pos = 0

            # Check if there are crossed orders in the orderbook and the buy price is higher or equal to sell price
            if (len(order_depth.sell_orders) > 0) and (len(order_depth.buy_orders) > 0):
                min_sell = min(order_depth.sell_orders.keys())
                max_buy = max(order_depth.buy_orders.keys())
                if min_sell <= max_buy:
                    orders = self.get_orders_crossed_orderbook(symbol, state, current_pos, position_limit)

            # If there are no crossed orders, use the get_orders_both_sides_quoted() function
            if not orders:
                orders = self.get_orders_both_sides_quoted(symbol, state, current_pos, position_limit)

            # Add all the orders to the result dict
            result[symbol] = orders

        return result

    def store_data_market(self, symbol, state: TradingState):
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
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception as e:  # Could happen if nothing in the order book for one side/both sides
            print(e)

    def store_data_position(self, symbol: str, state: TradingState):
        # Goal of this method is to store each trade we make. Since we have the data of the trades we make at the
        # previous timestamps, there will always be a one-period lag
        if symbol in state.own_trades.keys():
            own_trades = state.own_trades[symbol]
            # Now we loop through a list of Trade objects
            if own_trades is not None:
                qty_x_price_current_trades = sum([trade.price * trade.quantity for trade in own_trades])
                total_qty_current_trades = sum([trade.quantity for trade in own_trades])
                rows = []

                for trade in own_trades:
                    timestamp = trade.timestamp
                    price = trade.price
                    qty = trade.quantity
                    avg_price = np.nan  # useful just for the first iteration

                    # if (len(self.df_data_trades) > 0) and (timestamp == state.timestamp - 100):
                    if timestamp == state.timestamp - 100:  # We only want to look once at each trade
                        # if len(self.df_data_trades) == 0: # only if the dataframe is empty

                        if len(self.df_data_trades) > 0:
                            subset = self.df_data_trades[
                                (self.df_data_trades['symbol'] == symbol) & (self.df_data_trades['qty'] != 0)]

                            # Calculate net position
                            net_position = subset['qty'].sum() + total_qty_current_trades

                            if net_position == 0:
                                avg_price = np.nan
                            else:
                                # Calculate total cost
                                total_cost = (subset['price'] * subset['qty']).sum() + qty_x_price_current_trades
                                # Calculate weighted average price
                                avg_price = total_cost / net_position
                        rows.append({'timestamp': timestamp, 'symbol': symbol, 'price': price, 'qty': qty, 'avg_price':
                            avg_price})

                self.df_data_trades = pd.concat([self.df_data_trades, pd.DataFrame(rows)])
                print(self.df_data_trades.tail())

    @staticmethod
    def get_fair_price_asset(symbol, state):
        order_depth: OrderDepth = state.order_depths[symbol]

        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
               / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))

        return vwap

    @staticmethod
    def estimate_spreads(symbol, current_pos, position_limit, state):
        order_depth = state.order_depths[symbol]

        # First step: estimating the spread only if both sides present in the order book
        if (len(order_depth.buy_orders) != 0) & (len(order_depth.sell_orders) != 0):
            spread_market = min(order_depth.sell_orders.keys()) - max(order_depth.buy_orders.keys())

            spread_buy_side = current_pos * spread_market / (4 * position_limit) + spread_market / 4
            spread_sell_side = - current_pos * spread_market / (4 * position_limit) + spread_market / 4

        else:  # ROOM FOR IMPROVEMENT: FIND THE SPREAD WHEN NO DATA IN THE CURRENT O.B
            spread_buy_side = 0
            spread_sell_side = 0
        return {"buy_spread": spread_buy_side, "sell_spread": spread_sell_side}

    def get_orders_both_sides_quoted(self, symbol, state, current_pos, position_limit):
        # This method gets the fair price using vwap method and then computes the spread based on our current position

        print("Both sides of the market are quoted for " + symbol)
        print("Position on " + symbol + " is: " + str(current_pos))
        # Initialize the list of Orders to be sent as an empty list
        orders: list[Order] = []
        order_depth: OrderDepth = state.order_depths[symbol]

        # Computing the fair value of the asset based on VWAP algo:
        fair_value_asset = self.get_fair_price_asset(symbol, state)
        print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        # Computing the spread & fair prices
        spreads = self.estimate_spreads(symbol, current_pos, position_limit, state)
        buy_spread = spreads["buy_spread"]
        sell_spread = spreads["sell_spread"]
        print("The estimated buy spread for " + symbol + " is: " + str(buy_spread))
        print("The estimated sell spread for " + symbol + " is: " + str(sell_spread))

        fair_buy_price = fair_value_asset - buy_spread  # Willing to buy lower than my valuation
        fair_sell_price = fair_value_asset + sell_spread

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
        return orders



    def get_orders_crossed_orderbook(self, symbol, state, current_pos, position_limit):
        """
        This method gets the fair price using vwap method and then computes the spread based on our current position.
        We only trade when in different timestamp, buy price is higher or equal sell price.
        """
        print("Both sides of the market are quoted for " + symbol)
        print("Position on " + symbol + " is: " + str(current_pos))
        # Initialize the list of Orders to be sent as an empty list
        orders: list[Order] = []
        order_depth: OrderDepth = state.order_depths[symbol]

        # Computing the fair value of the asset based on VWAP algo:
        fair_value_asset = self.get_fair_price_asset(symbol, state)
        print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        # Computing the spread & fair prices
        spreads = self.estimate_spreads(symbol, current_pos, position_limit, state)
        buy_spread = spreads["buy_spread"]
        sell_spread = spreads["sell_spread"]
        print("The estimated buy spread for " + symbol + " is: " + str(buy_spread))
        print("The estimated sell spread for " + symbol + " is: " + str(sell_spread))

        fair_buy_price = fair_value_asset - buy_spread  # Willing to buy lower than my valuation
        fair_sell_price = fair_value_asset + sell_spread

        if len(self.df_data_market) > 0:
            last_timestamp = self.df_data_market.iloc[-1]['timestamp']
        else:
            last_timestamp = 0

        buy_prices = sorted([k for k in order_depth.buy_orders.keys() if k >= fair_sell_price])
        sell_prices = sorted([k for k in order_depth.sell_orders.keys() if k <= fair_buy_price])

        while len(buy_prices) > 0 and len(sell_prices) > 0:
            buy_price = buy_prices[0]
            sell_price = sell_prices[0]
            if last_timestamp != state.timestamp and buy_price >= sell_price:
                buy_volume = -order_depth.buy_orders[buy_price]
                sell_volume = -order_depth.sell_orders[sell_price]
                trade_volume = min(buy_volume, sell_volume)
                orders.append(Order(symbol, sell_price, trade_volume))
                orders.append(Order(symbol, buy_price, -trade_volume))
                print("TRADE " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(trade_volume))
                last_timestamp = state.timestamp

                if buy_volume == trade_volume:
                    buy_prices.pop(0)
                else:
                    order_depth.buy_orders[buy_price] += trade_volume

                if sell_volume == trade_volume:
                    sell_prices.pop(0)
                else:
                    order_depth.sell_orders[sell_price] += trade_volume
            else:
                break

        return orders
