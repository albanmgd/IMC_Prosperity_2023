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
    # df_data_trades = pd.DataFrame()
    # Logging performed trades
    df_data_trades = pd.DataFrame()
    dolphin_sightings_history = []
    dolphin_change_history=[]
    Gear_price_history=[]

    # Defining the position limits
    limits = {"PEARLS": 20, "BANANAS": 20,"COCONUTS": 600, "PINA_COLADAS": 300, "DIVING_GEAR": 50}

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            if symbol == "DIVING_GEAR":

                orders: list[Order] = []
                # Add the data for this symbol
                # self.store_data_market(symbol, state)

                # Add the previous trades we did
                #self.store_data_position(symbol, state)
                
                # Add the previous market trades
                # self.store_data_position(symbol, state)

                # Retrieve the Order Depth containing all the market BUY and SELL orders for the symbol
                order_depth: OrderDepth = state.order_depths[symbol]

                # Getting the position limit for this product & our position
                position_limit = self.limits.get(symbol)
                if symbol in state.position.keys():
                    current_pos = state.position[symbol]
                else:
                    current_pos = 0

                # First we see if both sides of the market are quoted
                if (len(order_depth.sell_orders) > 0) and (len(order_depth.buy_orders) > 0):
                    orders = self.get_orders_both_sides_quoted(symbol, state, current_pos, position_limit)

                orders = self.trim_orders(symbol, state, orders)

                # Add all the above orders to the result dict
                result[symbol] = orders

        return result

    def print_orders(self, orders: list[Order]):
        print("-------------- orders we put ------------------------")
        for order in orders:
            if order.quantity < 0:
                print("SELL order for ", order.symbol, " p: ", order.price, " q: ", order.quantity)
            else:
                print("BUY order for ", order.symbol, " p: ", order.price, " q: ", order.quantity)
        print("-------------- orders we put ------------------------")

    # results: only trimming, with attractiveness sort => 2050
    # results: trimming, with attractiveness sort + fitting with no sort => 2077
    # results: trimming + fitting no sort => 2077
    # results: trimming, with reverse-attractiveness sort + fitting with no sort => 2077

    def trim_orders(self, symbol, state, orders):
        if symbol in state.position.keys():
            position = state.position[symbol]
        else:
            position = 0
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
                buys.sort(key=lambda x: x.quantity, reverse=True)
                # buys are sorted based on their price. the lowest price in the list is less likely to be picked up
                for i in range(buy_surplus):
                    # each iteration we:
                    # -reduce the least probable order volume by 1
                    # -if its volume hits 0, we remove it from our buys
                    buys[0].quantity = buys[0].quantity - 1
                    if buys[0].quantity == 0:
                        buys.pop(0)

            # but how about fitting the order? :)
            if buy_surplus < 0:
                print("We have room to grow on buy side", symbol, "Position is", position, "we want to buy", sum_buy)
                # fit buy orders to limits
                # increment every buy order volume 1 by 1 until we reach limit
                # we don't necessarily have to sort them now, we can experiment with it later
                #buys.sort(key=lambda x: x.price)
                for buy in buys:
                    if buy_surplus < 0:
                        buy.quantity = buy.quantity + int((-buy_surplus)/len(buys))
                        buy_surplus += int((-buy_surplus)/len(buys))

        # if there are sell orders, lets check if there is any surplus
        if sells:
            sum_sell = sum([b.quantity for b in sells])
            sell_surplus = -self.limits[symbol] - (position + sum_sell)
            if sell_surplus > 0:
                print("We have Surplus on", symbol, "Position is", position, "we want to buy", sum_sell)
                # trim sell orders
                # targettin the least probable to go through order first
                # we sort them and reduce the volumes until we don't have the problem anymore
                sells.sort(key=lambda x: x.quantity)
                # sells are sorted based on their price, descending. the highest price is least attractive for bots
                for i in range(sell_surplus):
                    # each iteration we:
                    # -increase the least attractive order volume by 1 (sells are negative)
                    # -if its volume hits 0, we remove it from our sells
                    sells[0].quantity = sells[0].quantity + 1
                    if sells[0].quantity == 0:
                        sells.pop(0)

            # how about fitting the order? :)
            if sell_surplus < 0:
                print("We have room to grow on sell side", symbol, "Position is", position, "we want to sell", sum_sell)
                #sells.sort(key=lambda x: x.price)
                for sell in sells:
                    if sell_surplus < 0:
                        sell.quantity = sell.quantity - int((-sell_surplus)/len(sells))
                        sell_surplus += int((-sell_surplus)/len(sells))


        # now we have buys and sells that are trimmed. We combine them in a list and return it
        result = buys + sells
        self.print_orders(result)
        return result



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
            vwap_store = self.get_fair_price_asset(symbol, state)
            vwap_price = vwap_store["average_value"]  # This function works all the time
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

            if len(self.df_data_market) != 0:
                mask_symbol = self.df_data_market["symbol"] == symbol
                df_symbol = self.df_data_market[mask_symbol]    
                vwap_mean = df_symbol['vwap_price'].tail(10).mean()
            else:
                vwap_mean = vwap_price
            

            # Add new row to df class attribute
            row = [{'timestamp': timestamp, "symbol": symbol, 'bid_1': bid_1, 'vol_bid_1': volume_bid_1, 'bid_2': bid_2,
                    'vol_bid_2': volume_bid_2, 'bid_3': bid_3, 'vol_bid_3': volume_bid_3, 'ask_1': ask_1,
                    'vol_ask_1': volume_ask_1, 'ask_2': ask_2, 'vol_ask_2': volume_ask_2, 'ask_3': ask_3,
                    'vol_ask_3': volume_ask_3, 'mid_price': mid_price, "vwap_price": vwap_price, "spread": spread,"vwap_mean": vwap_mean}]
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])

        # We now get the mean of the past 20 vwap prices

            # print(self.df.tail())

        except Exception:  # Could happen if nothing in the order book for one side/both sides
            pass

        except Exception as e:  # Could happen if nothing in the order book for one side/both sides
            print(e)

    def store_market_trades(self, symbol: str, state: TradingState):
        # Goal of this method is to store each trade we make. Since we have the data of the trades we make at the
        # previous timestamps, there will always be a one-period lag
        if symbol in state.market_trades.keys():
            market_trades = state.market_trades[symbol]
            # Now we loop through a list of Trade objects
            if market_trades is not None:
                qty_x_price_current_trades = sum([trade.price * trade.quantity for trade in market_trades])
                total_qty_current_trades = sum([trade.quantity for trade in market_trades])
                rows = []

                trade_volume = 0
                trade_qty = 0
                for trade in market_trades:
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

                            #moved seashells
                            trade_volume += [price * qty]
                            trade_qty += qty

                if trade_volume == 0:
                    avg_price = np.nan
                else:
                    # Calculate total cost
                    avg_price = trade_volume/trade_qty
                rows.append({'timestamp': timestamp, 'symbol': symbol, 'avg_price': avg_price, 'trade_qty': trade_qty})
                # print("Traded " + str(symbol) + " price: ", str(price) + " volume: ", str(qty))
                self.df_data_trades = pd.concat([self.df_data_trades, pd.DataFrame(rows)])
                print(self.df_data_trades.tail())



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
                        print("Traded " + str(symbol) + " price: ", str(price) + " volume: ", str(qty))
                self.df_data_trades = pd.concat([self.df_data_trades, pd.DataFrame(rows)])
                print(self.df_data_trades.tail())

    @staticmethod
    # def get_fair_price_asset(symbol, state):
    #     order_depth: OrderDepth = state.order_depths[symbol]

    #     vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
    #             + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
    #            / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))

    #     return vwap

    def get_fair_price_asset(symbol, state):
        order_depth: OrderDepth = state.order_depths[symbol]

        average_bid = sum([k * v for k, v in order_depth.buy_orders.items()]) / (sum(order_depth.buy_orders.values()))
        average_ask = sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()]) / abs(
            sum(order_depth.sell_orders.values()))
        average_spread = average_ask - average_bid
        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
               / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))
        average_value = vwap

        return {"average_value": average_value, "average_spread": average_spread}

    def get_alternate_buy_price_asset(self, symbol, state):
        order_depth: OrderDepth = state.order_depths[symbol]

        keyy = max(order_depth.buy_orders.keys())
        del order_depth.buy_orders[keyy]
        #order_depth.buy_orders.items().pop(0)

        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
               / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))
        alternate_buy_value = vwap

        return alternate_buy_value

    def get_alternate_sell_price_asset(self, symbol, state):
        order_depth: OrderDepth = state.order_depths[symbol]

        keyy = min(order_depth.sell_orders.keys())
        del order_depth.sell_orders[keyy]
        #order_depth.sell_orders.items().pop(0)

        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
               / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))
        alternate_sell_value = vwap

        return alternate_sell_value

    @staticmethod
    def get_mid_price(symbol, state):
        order_depth = state.order_depths[symbol]

        # First step: estimating the spread only if both sides present in the order book
        mid_price = (min(order_depth.sell_orders.keys()) + max(order_depth.buy_orders.keys()))/2
        best_bid = max(order_depth.buy_orders.keys())
        best_bid_vol = order_depth.buy_orders[best_bid]
        best_ask = min(order_depth.sell_orders.keys())
        best_ask_vol = order_depth.sell_orders[best_ask]
        return {"mid_price": mid_price, "best_bid": best_bid, "best_ask_vol": best_ask_vol, "best_ask": best_ask, "best_bid_vol": best_bid_vol}

    @staticmethod
    def estimate_spreads(symbol, current_pos, position_limit, state):
        order_depth = state.order_depths[symbol]

        # First step: estimating the spread only if both sides present in the order book
        if (len(order_depth.buy_orders) != 0) & (len(order_depth.sell_orders) != 0):
            spread_market = min(order_depth.sell_orders.keys()) - max(order_depth.buy_orders.keys())
            buy_spread = max(order_depth.buy_orders.keys()) - min(order_depth.buy_orders.keys())
            sell_spread = max(order_depth.sell_orders.keys()) - min(order_depth.sell_orders.keys())


        # This tried to take into account our current position into the equation for what to offer - Will revisit this later after testing a bit
        # spread_buy_side = spread_market / 2
        # spread_sell_side = spread_market / 2
        # spread_buy_side = current_pos * spread_market / (4 * position_limit) + spread_market / 4
        # spread_sell_side = - current_pos * spread_market / (4 * position_limit) + spread_market / 4

        else:  # ROOM FOR IMPROVEMENT: FIND THE SPREAD WHEN NO DATA IN THE CURRENT O.B
            buy_spread = 0
            sell_spread = 0
            spread_market = 0
        return {"buy_spread": buy_spread, "sell_spread": sell_spread, "spread_market": spread_market}

    def get_orders_both_sides_quoted(self, symbol, state, current_pos, position_limit):
        # This method gets the fair price using vwap method and then computes the spread based on our current position

        # print("Both sides of the market are quoted for " + symbol)
        print("Position on " + symbol + " is: " + str(current_pos))
        # Initialize the list of Orders to be sent as an empty list
        orders: list[Order] = []
        order_depth: OrderDepth = state.order_depths[symbol]

        GEAR_values = self.get_mid_price("DIVING_GEAR",state)
        GEAR_value = GEAR_values["mid_price"]
        best_GEAR_bid = GEAR_values["best_bid"]
        best_GEAR_ask = GEAR_values["best_ask"]
        best_GEAR_bid_vol = GEAR_values["best_bid_vol"]
        best_GEAR_ask_vol = GEAR_values["best_ask_vol"]

        self.Gear_price_history.append(GEAR_value)

        current_dolphins = state.observations["DOLPHIN_SIGHTINGS"]
        if len(self.dolphin_sightings_history) > 1:
            previous_dolphins = self.dolphin_sightings_history[-1]
        else:
            previous_dolphins = current_dolphins
        self.dolphin_sightings_history.append(current_dolphins)
        dolphin_change = current_dolphins-previous_dolphins
        self.dolphin_change_history.append(dolphin_change)
        print(self.dolphin_sightings_history[-10:])

        gear_price_series = pd.Series(self.Gear_price_history)
        ratios_mav5 = gear_price_series.rolling(window=5, center = False).mean()
        current_ratio = ratios_mav5[-1:]

        if current_dolphins - previous_dolphins > 10:
            buy_price = best_GEAR_ask
            buy_volume = best_GEAR_ask_vol
            orders.append(Order(symbol, buy_price, buy_volume))
            print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        if current_dolphins - previous_dolphins < -10:
            sell_price = best_GEAR_bid
            sell_volume = best_GEAR_bid_vol
            orders.append(Order(symbol, sell_price, sell_volume))
            print("SELL " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        if current_pos > 0:
            if GEAR_value < current_ratio:
                sell_price = best_GEAR_bid
                sell_volume = best_GEAR_bid_vol
                orders.append(Order(symbol, sell_price, sell_volume))
                print("SELL " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        if current_pos < 0:    
            if GEAR_value > current_ratio:
                buy_price = best_GEAR_ask
                buy_volume = best_GEAR_ask_vol
                orders.append(Order(symbol, buy_price, buy_volume))
                print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))









        # # Computing the fair value of the asset based on simple maths
        # buy_spread = max(order_depth.buy_orders.keys()) - min(order_depth.buy_orders.keys())
        # sell_spread = max(order_depth.sell_orders.keys()) - min(order_depth.sell_orders.keys())
        # market_values = self.get_fair_price_asset(symbol, state)
        # market_spread = market_values["average_spread"]

        # mask_symbol = self.df_data_market["symbol"] == symbol
        # df_symbol = self.df_data_market[mask_symbol]  

        # if len(df_symbol) >= 10:   
        #     current_mean = df_symbol['vwap_mean'].tail(1)
        #     current_mean = current_mean.values[0]
        #     print(current_mean)
        #     highest_mean = df_symbol['vwap_mean'].tail(10).max()
        #     print(highest_mean)
        #     lowest_mean = df_symbol['vwap_mean'].tail(10).min()
        #     if highest_mean > current_mean and current_mean> lowest_mean:            
        #         fair_value_asset = current_mean
        #     else:
        #         fair_value_asset = market_values["average_value"]
        #         if market_spread < buy_spread:
        #                 fair_value_asset = self.get_alternate_buy_price_asset(symbol, state)
        #         if market_spread < sell_spread:
        #                 fair_value_asset = self.get_alternate_sell_price_asset(symbol, state)
        # else:
        # fair_value_asset = market_values["average_value"]
        # if market_spread < buy_spread:
        #     fair_value_asset = self.get_alternate_buy_price_asset(symbol, state)
        # if market_spread < sell_spread:
        #     fair_value_asset = self.get_alternate_sell_price_asset(symbol, state)
        

        # print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        # # Computing the spread & fair prices
        # spreads = self.estimate_spreads(symbol, current_pos, position_limit, state)
        # buy_spread = spreads["buy_spread"]
        # sell_spread = spreads["sell_spread"]
        # market_spread = spreads["spread_market"]
        # print("The estimated buy spread for " + symbol + " is: " + str(buy_spread))
        # print("The estimated sell spread for " + symbol + " is: " + str(sell_spread))

        # # Estimating a profit range limits

        # fair_buy_price = fair_value_asset - market_spread / 2  # Willing to buy lower than my valuation
        # fair_sell_price = fair_value_asset + market_spread / 2


            

        # total_volume_buy = 0
        # total_volume_buy = sum(
        #     value for key, value in order_depth.sell_orders.items() if key < fair_buy_price)
        # buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items() if
        #                        key < fair_buy_price]
        # if total_volume_buy != 0:
        #     print(str(len(buy_orders_to_place)) + " buy orders to place:")
        #     for buy_order_to_place in buy_orders_to_place:
        #         buy_price = buy_order_to_place[0]
        #         buy_volume = - buy_order_to_place[1]
        #         orders.append(Order(symbol, buy_price, buy_volume))
        #         print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        # # Trying to account for outliers
        # # if sell_spread > market_spread:
        # #     total_volume_buy = sum(
        # #         value for key, value in order_depth.sell_orders.items() if key < (fair_buy_price + buy_spread - market_spread))
        # #     buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items() if
        # #                         key < (fair_buy_price + sell_spread - market_spread)]
        # #     if total_volume_buy != 0:
        # #         print(str(len(buy_orders_to_place)) + " buy orders to place:")
        # #         for buy_order_to_place in buy_orders_to_place:
        # #             buy_price = buy_order_to_place[0]
        # #             buy_volume = - buy_order_to_place[1]
        # #             orders.append(Order(symbol, buy_price, buy_volume))
        # #         buy_price = max(buy_orders_to_place[0])
        # #         buy_volume = - mt.floor((position_limit - current_pos)/4)
        # #         orders.append(Order(symbol, buy_price, buy_volume))

        # total_volume_sell = sum(
        #     value for key, value in order_depth.buy_orders.items() if key > fair_sell_price)
        # sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items() if
        #                         key > fair_sell_price]
        # if total_volume_sell != 0:
        #     print(str(len(sell_orders_to_place)) + " sell orders to place:")
        #     for sell_order_to_place in sell_orders_to_place:
        #         price = sell_order_to_place[0]
        #         volume = - sell_order_to_place[1]
        #         orders.append(Order(symbol, price, volume))
        #         print("SELL " + str(symbol) + " price: ", str(price) + " volume: ", str(volume))

        # # if buy_spread > market_spread:
        # #     total_volume_sell = sum(
        # #         value for key, value in order_depth.buy_orders.items() if key > (fair_sell_price - sell_spread + market_spread))
        # #     sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items() if
        # #                             key > (fair_sell_price - buy_spread + market_spread)]
        # #     if total_volume_sell != 0:
        # #         print(str(len(sell_orders_to_place)) + " sell orders to place:")
        # #         for sell_order_to_place in sell_orders_to_place:
        # #             price = sell_order_to_place[0]
        # #             volume = - sell_order_to_place[1]
        # #             orders.append(Order(symbol, price, volume))

        # #         sell_price = min(sell_orders_to_place[0])
        # #         sell_volume = - mt.floor((position_limit - current_pos)/4)
        # #         orders.append(Order(symbol, sell_price, sell_volume))
        # # if total_volume_sell == 0 and current_pos > 0:
        # #         sell_price = mt.ceil(fair_value_asset)
        # #         sell_volume = -current_pos
        # #         orders.append(Order(symbol, sell_price, sell_volume))   
        # #         print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        # if symbol == 'PERLS':

        #     if (position_limit + current_pos > 5):
        #         sell_price = mt.ceil(fair_value_asset) + 1
        #         sell_volume = - mt.floor((position_limit + current_pos))
        #         orders.append(Order(symbol, sell_price, sell_volume))
        #         orders.append(Order(symbol, sell_price, sell_volume))
        #         print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        # else:
        #     if (-current_pos)/position_limit < 0.75 and market_spread > 2:
        #         sell_price = mt.ceil(fair_value_asset)+ mt.ceil(market_spread/3)
        #         sell_volume = - mt.floor((position_limit + current_pos)/6)
        #         orders.append(Order(symbol, sell_price, sell_volume))
        #         print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        #     # sell_price = mt.ceil(fair_value_asset) + 2
        #     # sell_volume = - mt.floor((position_limit + current_pos)/8)
        #     # orders.append(Order(symbol, sell_price, sell_volume))
        #     # print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        # # if total_volume_buy == 0 and current_pos < 0:
        # #         buy_price = mt.floor(fair_value_asset)
        # #         buy_volume = -current_pos
        # #         orders.append(Order(symbol, buy_price, buy_volume))   
        # #         print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        # if symbol == 'PERLS':
        #     if (position_limit - current_pos > 5):
        #         buy_price = mt.floor(fair_value_asset) - 1
        #         buy_volume = mt.floor((position_limit - current_pos))
        #         orders.append(Order(symbol, buy_price, buy_volume))
        #         print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
    
        # else:
        #     if (current_pos)/position_limit < 0.75 and market_spread > 2:
        #         buy_price = mt.floor(fair_value_asset) - mt.ceil(market_spread/3)
        #         buy_volume = mt.floor((position_limit - current_pos)/6)
        #         orders.append(Order(symbol, buy_price, buy_volume))
        #         print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
    
            # buy_price = mt.floor(fair_value_asset) - 2
            # buy_volume = mt.floor((position_limit-current_pos)/8)
            # orders.append(Order(symbol, buy_price, buy_volume))
            # print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        return orders

    # def get_orders_buy_side_quoted_only(self, symbol, state, current_pos, position_limit):
    #     """"""
    # This algorithm is used when we only have buy orders in the order book. In that case, the vwap price is
    # necessarily below the best bid, but we know there is a strong buying pressure. Therefore we can afford being
    # less competitive on the ask side