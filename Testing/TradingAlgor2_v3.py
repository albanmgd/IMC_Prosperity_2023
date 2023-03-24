from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
import json
from datamodel import Order, ProsperityEncoder, Symbol, TradingState, OrderDepth
from typing import Any

class Logger:
    def __init__(self) -> None:
        self.logs = ""

    def print(self, *objects: Any, sep: str = " ", end: str = "\n") -> None:
        self.logs += sep.join(map(str, objects)) + end

    def flush(self, state: TradingState, orders: dict[Symbol, list[Order]]) -> None:
        print(json.dumps({
            "state": state,
            "orders": orders,
            "logs": self.logs,
        }, cls=ProsperityEncoder, separators=(",", ":"), sort_keys=True))

        self.logs = ""

logger = Logger()


class Trader:
    # Creating a class attribute to store all the data we receive; populated iteratively
    df_data_market = pd.DataFrame()
    # Class attribute to monitor all our trades; state.position only gives actual size on a symbol
    df_data_trades = pd.DataFrame()
    #Logging performed trades
    # df_data_logs = pd.DataFrame()

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

            # First we see if both sides of the market are quoted
            if (len(order_depth.sell_orders) > 0) and (len(order_depth.buy_orders) > 0):
                orders = self.get_orders_both_sides_quoted(symbol, state, current_pos, position_limit)

            # Add all the above orders to the result dict
            result[symbol] = orders
        logger.flush(state, orders)

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

            # Add new row to df class attribute
            row = [{'timestamp': timestamp, 'symbol': symbol, 'bid_1': bid_1, 'vol_bid_1': volume_bid_1, 'bid_2': bid_2,
                    'vol_bid_2': volume_bid_2, 'bid_3': bid_3, 'vol_bid_3': volume_bid_3, 'ask_1': ask_1,
                    'vol_ask_1': volume_ask_1, 'ask_2': ask_2, 'vol_ask_2': volume_ask_2, 'ask_3': ask_3,
                    'vol_ask_3': volume_ask_3, 'mid_price': mid_price, "vwap_price": vwap_price, "spread": spread}]
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception:  # Could happen if nothing in the order book for one side/both sides
            pass

        except Exception as e:  # Could happen if nothing in the order book for one side/both sides
            logger.print(e)
            

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
                        logger.print("Traded " + str(symbol) + " price: ", str(price) + " volume: ", str(qty))
                self.df_data_trades = pd.concat([self.df_data_trades, pd.DataFrame(rows)])
                logger.print(self.df_data_trades.tail())

    @staticmethod
    # def get_fair_price_asset(symbol, state):
    #     order_depth: OrderDepth = state.order_depths[symbol]

    #     vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
    #             + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
    #            / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))

    #     return vwap

    def get_fair_price_asset(symbol,state):
        order_depth: OrderDepth = state.order_depths[symbol]

        average_bid = sum([k * v for k, v in order_depth.buy_orders.items()]) / (sum(order_depth.buy_orders.values()))
        average_ask = sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()]) / abs(sum(order_depth.sell_orders.values()))
        average_spread = average_ask - average_bid
        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
               / (sum(order_depth.buy_orders.values()) - sum(order_depth.sell_orders.values()))
        average_value = vwap


        return {"average_value": average_value, "average_spread": average_spread}

    def get_alternate_buy_price_asset(self, symbol,state):
        order_depth: OrderDepth = state.order_depths[symbol]
        
        buy_orders_list = list(order_depth.buy_orders.items())[1:]
        buy_orders_dict = dict(buy_orders_list)

        vwap = (sum([k * v for k, v in buy_orders_dict.items()])
                + sum([abs(k) * abs(v) for k, v in order_depth.sell_orders.items()])) \
            / (sum(buy_orders_dict.values()) - sum(order_depth.sell_orders.values()))
        alternate_buy_value = vwap

        return alternate_buy_value

    def get_alternate_sell_price_asset(self, symbol,state):
        order_depth: OrderDepth = state.order_depths[symbol]
        
        sell_order_list = list(order_depth.sell_orders.items())[1:]
        sell_order_dict = dict(sell_order_list)

        vwap = (sum([k * v for k, v in order_depth.buy_orders.items()])
                + sum([abs(k) * abs(v) for k, v in sell_order_dict.items()])) \
               / (sum(order_depth.buy_orders.values()) - sum(sell_order_dict.values()))
        alternate_sell_value = vwap

        return alternate_sell_value
        
    @staticmethod
    def estimate_spreads(symbol, current_pos, position_limit, state):
        order_depth = state.order_depths[symbol]

        # First step: estimating the spread only if both sides present in the order book
        if (len(order_depth.buy_orders) != 0) & (len(order_depth.sell_orders) != 0):
            spread_market = min(order_depth.sell_orders.keys()) - max(order_depth.buy_orders.keys())
            buy_spread = max(order_depth.buy_orders.keys()) - min(order_depth.buy_orders.keys())
            sell_spread = max(order_depth.sell_orders.keys()) - min(order_depth.sell_orders.keys())


        #This tried to take into account our current position into the equation for what to offer - Will revisit this later after testing a bit
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

        #print("Both sides of the market are quoted for " + symbol)
        logger.print("Position on " + symbol + " is: " + str(current_pos))
        # Initialize the list of Orders to be sent as an empty list
        orders: list[Order] = []
        order_depth: OrderDepth = state.order_depths[symbol]

        # Computing the fair value of the asset based on simple maths
        buy_spread = max(order_depth.buy_orders.keys())- min(order_depth.buy_orders.keys())
        sell_spread = max(order_depth.sell_orders.keys())- min(order_depth.sell_orders.keys())
        market_values = self.get_fair_price_asset(symbol, state)
        market_spread = market_values["average_spread"]

        fair_value_asset = market_values["average_value"]
        if market_spread < buy_spread:
            fair_value_asset = self.get_alternate_buy_price_asset(symbol, state)
        if market_spread < sell_spread:
            fair_value_asset = self.get_alternate_sell_price_asset(symbol, state)


        logger.print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        # Computing the spread & fair prices
        spreads = self.estimate_spreads(symbol, current_pos, position_limit, state)
        buy_spread = spreads["buy_spread"]
        sell_spread = spreads["sell_spread"]
        market_spread = spreads["spread_market"]
        logger.print("The estimated buy spread for " + symbol + " is: " + str(buy_spread))
        logger.print("The estimated sell spread for " + symbol + " is: " + str(sell_spread))

        #Estimating a profit range limits
        
          

        fair_buy_price = fair_value_asset - market_spread/2 # Willing to buy lower than my valuation
        fair_sell_price = fair_value_asset + market_spread/2

        total_volume_buy = 0
        total_volume_buy = sum(
            value for key, value in order_depth.sell_orders.items() if key < fair_buy_price)
        buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items() if
                               key < fair_buy_price]
        if total_volume_buy != 0:
            logger.print(str(len(buy_orders_to_place)) + " buy orders to place:")
            for buy_order_to_place in buy_orders_to_place:
                buy_price = buy_order_to_place[0]
                buy_volume = - buy_order_to_place[1]
                orders.append(Order(symbol, buy_price, buy_volume))
                logger.print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
   

        #Trying to account for outliers
        # if sell_spread > market_spread:
        #     total_volume_buy = sum(
        #         value for key, value in order_depth.sell_orders.items() if key < (fair_buy_price + buy_spread - market_spread))
        #     buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items() if
        #                         key < (fair_buy_price + sell_spread - market_spread)]
        #     if total_volume_buy != 0:
        #         print(str(len(buy_orders_to_place)) + " buy orders to place:")
        #         for buy_order_to_place in buy_orders_to_place:
        #             buy_price = buy_order_to_place[0]
        #             buy_volume = - buy_order_to_place[1]
        #             orders.append(Order(symbol, buy_price, buy_volume))
        #         buy_price = max(buy_orders_to_place[0])
        #         buy_volume = - mt.floor((position_limit - current_pos)/4)
        #         orders.append(Order(symbol, buy_price, buy_volume))    
        
        total_volume_sell = sum(
            value for key, value in order_depth.buy_orders.items() if key > fair_sell_price)
        sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items() if
                                key > fair_sell_price]
        if total_volume_sell != 0:
            logger.print(str(len(sell_orders_to_place)) + " sell orders to place:")
            for sell_order_to_place in sell_orders_to_place:
                price = sell_order_to_place[0]
                volume = - sell_order_to_place[1]
                orders.append(Order(symbol, price, volume))
                logger.print("SELL " + str(symbol) + " price: ", str(price) + " volume: ", str(volume))
        

        # if buy_spread > market_spread:
        #     total_volume_sell = sum(
        #         value for key, value in order_depth.buy_orders.items() if key > (fair_sell_price - sell_spread + market_spread))
        #     sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items() if
        #                             key > (fair_sell_price - buy_spread + market_spread)]
        #     if total_volume_sell != 0:
        #         print(str(len(sell_orders_to_place)) + " sell orders to place:")
        #         for sell_order_to_place in sell_orders_to_place:
        #             price = sell_order_to_place[0]
        #             volume = - sell_order_to_place[1]
        #             orders.append(Order(symbol, price, volume))

        #         sell_price = min(sell_orders_to_place[0])
        #         sell_volume = - mt.floor((position_limit - current_pos)/4)
        #         orders.append(Order(symbol, sell_price, sell_volume))  
        
        # if current_pos < 20:
        #     if buy_spread < market_spread and sell_spread < market_spread:
        #         if (position_limit + current_pos > 5):
        #             sell_price = mt.ceil(fair_value_asset) + 1
        #             sell_volume = - mt.floor((position_limit + current_pos)/4)
        #             orders.append(Order(symbol, sell_price, sell_volume))   
        #             print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))

        #         if (position_limit - current_pos > 5):
        #             buy_price = mt.floor(fair_value_asset) - 1
        #             buy_volume = mt.floor((position_limit-current_pos)/4)
        #             orders.append(Order(symbol, buy_price, buy_volume))   
        #             print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        # else:
        #     print("Position limit of 20 for" + str(symbol) + "has been reached. No more orders will be placed.")

        if current_pos < position_limit - 5:
            if buy_spread < market_spread:
                buy_price = mt.floor(fair_value_asset) - 1
                buy_volume = mt.floor((position_limit - current_pos) / 4)
                orders.append(Order(symbol, buy_price, buy_volume))
                logger.print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
                
            elif sell_spread < market_spread:
                sell_price = mt.ceil(fair_value_asset) + 1
                sell_volume = - mt.floor((position_limit + current_pos) / 4)
                orders.append(Order(symbol, sell_price, sell_volume))
                logger.print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        elif current_pos > position_limit + 5:
            if sell_spread < market_spread:
                sell_price = mt.ceil(fair_value_asset) + 1
                sell_volume = - mt.floor((position_limit + current_pos) / 4)
                orders.append(Order(symbol, sell_price, sell_volume))
                logger.print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
            elif buy_spread < market_spread:
                buy_price = mt.floor(fair_value_asset) - 1
                buy_volume = mt.floor((position_limit - current_pos) / 4)
                orders.append(Order(symbol, buy_price, buy_volume))
                logger.print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        else:
            logger.print("Position limit of " + str(position_limit) + " for " + str(symbol) + " has been reached. No more orders will be placed.")        


        return orders

        

    # def get_orders_buy_side_quoted_only(self, symbol, state, current_pos, position_limit):
    #     """"""
        # This algorithm is used when we only have buy orders in the order book. In that case, the vwap price is
        # necessarily below the best bid, but we know there is a strong buying pressure. Therefore we can afford being
        # less competitive on the ask side