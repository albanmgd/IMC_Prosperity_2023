from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order


class Trader:
    # Limits for each product
    df_data_market = pd.DataFrame()
    df_data_trades = pd.DataFrame()
    ratio_history=[]
    dolphin_sightings_history = []
    dolphin_change_history=[]
    Gear_price_history=[]
    jump_timestamps = []
    picnic_pt = []
    picnic_trend = {"down": False, "up": False}
    
    limits = {"PEARLS": 20, "BANANAS": 20, "COCONUTS": 600, "PINA_COLADAS": 300, "BERRIES": 250, "DIVING_GEAR": 50,
              "BAGUETTE": 150, "DIP": 300, "UKULELE": 70, "PICNIC_BASKET": 70}

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():  # done once and for all the symbols; useful to have same pasta across !=
            # timestamps
            # Add the data for this symbol
            if symbol == "BAGUETTE" or symbol == "UKULELE" or symbol == "DIP" or symbol == "PICNIC_BASKET":
                self.store_data_market(symbol, state)

        for symbol in state.listings.keys():
            # if symbol == "BERRIES":  
            #     # timestamps
            #     # Add the data for this symbol
            #     self.store_data_market(symbol, state)
            if symbol == "DOLPHIN_SIGHTINGS":
                continue
            # Initialize the list of Orders to be sent as an empty list
            orders: list[Order] = []
            # Retrieve the Order Depth containing all the market BUY and SELL orders for the symbol
            order_depth: OrderDepth = state.order_depths[symbol]
            position_limit = self.limits.get(symbol)
            
            if symbol in state.position.keys():
                current_pos = state.position[symbol]
            else:
                current_pos = 0
                # First we see if both sides of the market are quoted
            if (len(order_depth.sell_orders) > 0) and (len(order_depth.buy_orders) > 0):
                if symbol == "PEARLS" or symbol == "BANANAS":
                    orders = self.get_orders_pearl_banana(symbol, state, current_pos, position_limit)
                    orders = self.trim_orders(symbol, state, orders)
                if symbol == "COCONUTS" or symbol == "PINA_COLADAS": 
                    orders = self.get_orders_coco_pina(symbol, state, current_pos, position_limit)
                 
                if symbol == "DIVING_GEAR": 
                    orders = self.get_orders_diving_gear(symbol, state, current_pos, position_limit)
                    # orders = self.trim_orders(symbol, state, orders)
                if symbol == "BERRIES":  
                    orders = self.get_orders_berries(state)
                if symbol == "DIP":  # can only look at dip since we'll trade them as a pair
                    orders_basket = self.get_orders_basket(state)
                    result["DIP"] = orders_basket.get("DIP")
                    result["PICNIC_BASKET"] = orders_basket.get("PICNIC_BASKET")
                    result["UKULELE"] = orders_basket.get("UKULELE")
                    result["BAGUETTE"] = orders_basket.get("BAGUETTE")
            
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
            if symbol == "PEARLS" or symbol == "BANANAS":
        
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
            if symbol == "PEARLS" or symbol == "BANANAS":
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
            row = [{'timestamp': timestamp, 'symbol': symbol, 'bid_price_1': bid_1, 'bid_volume_1': volume_bid_1,
                    'bid_price_2': bid_2,
                    'bid_volume_2': volume_bid_2, 'bid_price_3': bid_3, 'bid_volume_3': volume_bid_3,
                    'ask_price_1': ask_1,
                    'ask_volume_1': volume_ask_1, 'ask_price_2': ask_2, 'ask_volume_2': volume_ask_2,
                    'ask_price_3': ask_3,
                    'ask_volume_3': volume_ask_3, 'mid_price': mid_price, "spread": spread}]
            self.df_data_market = pd.concat([self.df_data_market, pd.DataFrame(row)])
            # print(self.df.tail())

        except Exception:  # Could happen if nothing in the order book for one side/both sides
            pass


    def store_data_position(self, symbol: str, state: TradingState):
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

        else:  # ROOM FOR IMPROVEMENT: FIND THE SPREAD WHEN NO DATA IN THE CURRENT O.B
            buy_spread = 0
            sell_spread = 0
            spread_market = 0
        return {"buy_spread": buy_spread, "sell_spread": sell_spread, "spread_market": spread_market}

# ------------------ PEARL & BANANA --------------------- 
    def get_orders_pearl_banana(self, symbol, state, current_pos, position_limit):
        print("Position on " + symbol + " is: " + str(current_pos))
        # Initialize the list of Orders to be sent as an empty list
        orders: list[Order] = []
        order_depth: OrderDepth = state.order_depths[symbol]

        # Computing the fair value of the asset based on simple maths
        buy_spread = max(order_depth.buy_orders.keys()) - min(order_depth.buy_orders.keys())
        sell_spread = max(order_depth.sell_orders.keys()) - min(order_depth.sell_orders.keys())
        market_values = self.get_fair_price_asset(symbol, state)
        market_spread = market_values["average_spread"]

        fair_value_asset = market_values["average_value"]
        if symbol == 'PEARLS' or symbol == 'BANANAS':
            if market_spread < buy_spread:  
                fair_value_asset = self.get_alternate_buy_price_asset(symbol, state)
            if market_spread < sell_spread:
                fair_value_asset = self.get_alternate_sell_price_asset(symbol, state)
        print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        # Computing the spread & fair prices
        spreads = self.estimate_spreads(symbol, current_pos, position_limit, state)
        buy_spread = spreads["buy_spread"]
        sell_spread = spreads["sell_spread"]
        market_spread = spreads["spread_market"]
        print("The estimated buy spread for " + symbol + " is: " + str(buy_spread))
        print("The estimated sell spread for " + symbol + " is: " + str(sell_spread))

        # Estimating a profit range limits

        fair_buy_price = fair_value_asset - market_spread / 2  # Willing to buy lower than my valuation
        fair_sell_price = fair_value_asset + market_spread / 2
        total_volume_buy = 0
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

        if symbol == 'PEARLS':
            if (-current_pos)/position_limit < 0.75:
                sell_price = mt.ceil(fair_value_asset)+ mt.ceil(market_spread/3)
                sell_volume = - mt.floor((position_limit + current_pos)/2)
                orders.append(Order(symbol, sell_price, sell_volume))
                print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        else:
            if (-current_pos)/position_limit < 0.75 and market_spread > 2:
                sell_price = mt.floor(fair_value_asset) + mt.ceil(market_spread/3)
                sell_volume = - mt.floor((position_limit + current_pos)/4)
                orders.append(Order(symbol, sell_price, sell_volume))
                print("Trying to BUY " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
            
        if symbol == 'PEARLS':
            if (current_pos)/position_limit < 0.75:
                buy_price = mt.floor(fair_value_asset) - mt.ceil(market_spread/3)
                buy_volume = mt.floor((position_limit - current_pos)/2)
                orders.append(Order(symbol, buy_price, buy_volume))
                print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
    
        else:
            if (current_pos)/position_limit < 0.75 and market_spread > 2:
                buy_price = mt.floor(fair_value_asset) - mt.ceil(market_spread/3)
                buy_volume = mt.floor((position_limit - current_pos)/4)
                orders.append(Order(symbol, buy_price, buy_volume))
                print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        return orders

# ------------------ COCO & PINA --------------------- 
    def get_orders_coco_pina(self, symbol, state, current_pos, position_limit):
        print("Position on " + symbol + " is: " + str(current_pos))
        orders: list[Order] = []
        order_depth: OrderDepth = state.order_depths[symbol]

        buy_spread = max(order_depth.buy_orders.keys()) - min(order_depth.buy_orders.keys())
        sell_spread = max(order_depth.sell_orders.keys()) - min(order_depth.sell_orders.keys())
        market_values = self.get_fair_price_asset(symbol, state)
        market_spread = market_values["average_spread"]

        fair_value_asset = market_values["average_value"]
        if market_spread < buy_spread:
            fair_value_asset = self.get_alternate_buy_price_asset(symbol, state)
        if market_spread < sell_spread:
            fair_value_asset = self.get_alternate_sell_price_asset(symbol, state)

        print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        spreads = self.estimate_spreads(symbol, current_pos, position_limit, state)
        buy_spread = spreads["buy_spread"]
        sell_spread = spreads["sell_spread"]
        market_spread = spreads["spread_market"]
        print("The estimated buy spread for " + symbol + " is: " + str(buy_spread))
        print("The estimated sell spread for " + symbol + " is: " + str(sell_spread))

        fair_buy_price = fair_value_asset - market_spread / 2  # Willing to buy lower than my valuation
        fair_sell_price = fair_value_asset + market_spread / 2

        lower_limit = 1.8721
        upper_limit = 1.8809
        sigma = 0.001
        fair_limit = 1.8762
        inner_upper_limit = fair_limit + sigma
        inner_lower_limit = fair_limit - sigma
        
        if symbol == "COCONUTS":
            COCO_values = self.get_mid_price("COCONUTS",state)
            COCO_value = COCO_values["mid_price"]
            best_COCO_bid = COCO_values["best_bid"]
            best_COCO_ask = COCO_values["best_ask"]
            best_COCO_bid_vol = COCO_values["best_bid_vol"]
            best_COCO_ask_vol = COCO_values["best_ask_vol"]
            PC_values = self.get_mid_price("PINA_COLADAS",state)
            PC_value = PC_values["mid_price"]
            best_PC_bid = PC_values["best_bid"]
            best_PC_ask = PC_values["best_ask"]
            best_PC_bid_vol = PC_values["best_bid_vol"]
            best_PC_ask_vol = PC_values["best_ask_vol"]
            ratio = PC_value/COCO_value
            self.ratio_history.append(ratio)
            print(ratio)
            old_ratio_max = max(self.ratio_history[-5:])
            old_ratio_min = min(self.ratio_history[-5:])
            print(old_ratio_max)
            print(old_ratio_min)

            if (PC_value/COCO_value) > upper_limit and old_ratio_min == ratio:
                #Then we want to sell PC and buy COCO
                buy_price = best_COCO_ask
                buy_volume = min(-best_COCO_ask_vol,round(best_PC_bid_vol*fair_limit))
                orders.append(Order(symbol, buy_price, buy_volume))
                print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

            elif (PC_value/COCO_value) < lower_limit and old_ratio_max == ratio:
                #Because the spread is always thin we just buy the best offer
                #on the market and even double the volume depending on hwo large the overhaul is
                sell_price = best_COCO_bid
                sell_volume = -min(best_COCO_bid_vol,-round(best_PC_ask_vol*fair_limit)) #making sure the position is balanced
                orders.append(Order(symbol, sell_price, sell_volume))
                print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))

                # COCO_value = COCO_value /1.8764*(PC_value/COCO_value)
                #Then we want to sell COCO and buy PC
            elif inner_upper_limit > (PC_value/COCO_value) > inner_lower_limit:
                if current_pos > 0:
                    sell_price = max(order_depth.buy_orders.keys())+1
                    sell_volume = -current_pos + round(current_pos/8)#* (1.87294-PC_value/COCO_value)/0.003696
                    orders.append(Order(symbol, sell_price, sell_volume))
                    print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
                    sell_price = max(order_depth.buy_orders.keys())
                    sell_volume = -round(current_pos/8)#* (1.87294-PC_value/COCO_value)/0.003696
                    orders.append(Order(symbol, sell_price, sell_volume))
                    print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))        
                if current_pos < 0:
                    buy_price = min(order_depth.sell_orders.keys())-1
                    buy_volume = -current_pos + round(current_pos/8)
                    orders.append(Order(symbol, buy_price, buy_volume))
                    print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
                    buy_price = min(order_depth.sell_orders.keys())
                    buy_volume = -round(current_pos/8)
                    orders.append(Order(symbol, buy_price, buy_volume))
                    print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))

        if symbol == "PINA_COLADAS":
            COCO_values = self.get_mid_price("COCONUTS",state)
            COCO_value = COCO_values["mid_price"]
            best_COCO_bid = COCO_values["best_bid"]
            best_COCO_ask = COCO_values["best_ask"]
            best_COCO_bid_vol = COCO_values["best_bid_vol"]
            best_COCO_ask_vol = COCO_values["best_ask_vol"]
            PC_values = self.get_mid_price("PINA_COLADAS",state)
            PC_value = PC_values["mid_price"]
            best_PC_bid = PC_values["best_bid"]
            best_PC_ask = PC_values["best_ask"]
            best_PC_bid_vol = PC_values["best_bid_vol"]
            best_PC_ask_vol = PC_values["best_ask_vol"]
            ratio = PC_value/COCO_value
            self.ratio_history.append(ratio)
            old_ratio_max = max(self.ratio_history[-5:])
            old_ratio_min = min(self.ratio_history[-5:])
            print(ratio)
            print(old_ratio_max)
            print(old_ratio_min)

            if PC_value/COCO_value > upper_limit and old_ratio_min == ratio:
                sell_price = best_PC_bid
                sell_volume =  -min(best_PC_bid_vol, round(-best_COCO_ask_vol/fair_limit)) #* (1.87294-PC_value/COCO_value)/0.003696
                orders.append(Order(symbol, sell_price, sell_volume))
                print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))

            elif PC_value/COCO_value < lower_limit and old_ratio_max == ratio:
                buy_price = best_PC_ask
                buy_volume = min(-best_PC_ask_vol, round(best_COCO_bid_vol/fair_limit)) 
                orders.append(Order(symbol, buy_price, buy_volume))
                print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
            elif inner_upper_limit > (PC_value/COCO_value) > inner_lower_limit:
                if current_pos > 0:
                    sell_price = max(order_depth.buy_orders.keys())+1
                    sell_volume = -current_pos#* (1.87294-PC_value/COCO_value)/0.003696
                    orders.append(Order(symbol, sell_price, sell_volume))
                    print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
                    sell_price = max(order_depth.buy_orders.keys())
                    sell_volume = -current_pos/8#* (1.87294-PC_value/COCO_value)/0.003696
                    orders.append(Order(symbol, sell_price, sell_volume))
                    print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
                if current_pos < 0:
                    buy_price = min(order_depth.sell_orders.keys())
                    buy_volume = -current_pos/8
                    orders.append(Order(symbol, buy_price, buy_volume))
                    print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
                    buy_price = min(order_depth.sell_orders.keys())-1
                    buy_volume = -current_pos
                    orders.append(Order(symbol, buy_price, buy_volume))
                    print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        print("The estimated fair price for " + symbol + " is: " + str(fair_value_asset))    
            
        if symbol == 'PEARLS' or symbol == 'BANANAS':

            total_volume_buy = 0
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

        if symbol == 'PEARLS' or symbol == 'BANANAS':

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

        if symbol == 'PEARLS':
            if (-current_pos)/position_limit < 0.75:
                sell_price = 10004
                sell_volume = - mt.floor((position_limit + current_pos)/2)
                orders.append(Order(symbol, sell_price, sell_volume))
                print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        
        elif symbol == 'BANANAS':
            if (-current_pos)/position_limit < 0.75 and market_spread > 2:
                sell_price = mt.ceil(fair_value_asset)+ mt.ceil(market_spread/2)
                sell_volume = - mt.floor((position_limit + current_pos)/2)
                orders.append(Order(symbol, sell_price, sell_volume))
                print("Trying to SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))


        if symbol == 'PEARLS':
            if (current_pos)/position_limit < 0.75:
                buy_price = 9996
                buy_volume = mt.floor((position_limit - current_pos)/2)
                orders.append(Order(symbol, buy_price, buy_volume))
                print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
       
        elif symbol == 'BANANAS':
            if (current_pos)/position_limit < 0.75 and market_spread > 2:
                buy_price = mt.floor(fair_value_asset) - mt.ceil(market_spread/2)
                buy_volume = mt.floor((position_limit - current_pos)/2)
                orders.append(Order(symbol, buy_price, buy_volume))
                print("Trying to BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        
        return orders

# ------------------ DIVING GEAR --------------------- 
    def get_orders_diving_gear(self, symbol, state, current_pos, position_limit):
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
        if len(self.dolphin_sightings_history) > 2:
            previous_dolphins = self.dolphin_sightings_history[-1]
            previous_dolphins2 = self.dolphin_sightings_history[-2]
        else:
            previous_dolphins = current_dolphins
            previous_dolphins2 = current_dolphins
        self.dolphin_sightings_history.append(current_dolphins)
        dolphin_change = current_dolphins-previous_dolphins
        self.dolphin_change_history.append(dolphin_change)
        #print(self.dolphin_sightings_history[-10:])
        self.Gear_price_history.append(GEAR_value)
        if len(self.Gear_price_history) > 25:
            self.Gear_price_history.pop(0)
        # then you just
        last_25_avg = np.mean(self.Gear_price_history)
        # gear_price_series = pd.Series(self.Gear_price_history)
        # ratios_mav5 = gear_price_series.rolling(window=5, center = False).mean()
        # current_ratio = ratios_mav5.values[-1:]

        #print(current_ratio)

        if current_dolphins - previous_dolphins >= 10:
            jump_timestamp = state.timestamp
            self.jump_timestamps.append(jump_timestamp)
            buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items()]
            for buy_order_to_place in buy_orders_to_place:
                buy_price = buy_order_to_place[0]
                buy_volume = - buy_order_to_place[1]
                orders.append(Order(symbol, buy_price, buy_volume))
                print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))

        if previous_dolphins - previous_dolphins2 >= 10:
            buy_orders_to_place = [(key, value) for key, value in order_depth.sell_orders.items()]
            for buy_order_to_place in buy_orders_to_place:
                buy_price = buy_order_to_place[0]
                buy_volume = - buy_order_to_place[1]
                orders.append(Order(symbol, buy_price, buy_volume))
                print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))


        if current_dolphins - previous_dolphins <= -10:
            jump_timestamp = state.timestamp
            self.jump_timestamps.append(jump_timestamp)
            sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items()]
            print(str(len(sell_orders_to_place)) + " sell orders to place:")

            for sell_order_to_place in sell_orders_to_place:
                price = sell_order_to_place[0]
                volume = - sell_order_to_place[1]
                orders.append(Order(symbol, price, volume))
                print("SELL " + str(symbol) + " price: ", str(price) + " volume: ", str(volume))

        if previous_dolphins - previous_dolphins2 <= -10:
            sell_orders_to_place = [(key, value) for key, value in order_depth.buy_orders.items()]
            print(str(len(sell_orders_to_place)) + " sell orders to place:")

            for sell_order_to_place in sell_orders_to_place:
                price = sell_order_to_place[0]
                volume = - sell_order_to_place[1]
                orders.append(Order(symbol, price, volume))
                print("SELL " + str(symbol) + " price: ", str(price) + " volume: ", str(volume))

        if current_pos > 0:
            if GEAR_value < last_25_avg and (state.timestamp - self.jump_timestamps[-1])> 10000:
                sell_price = best_GEAR_bid
                sell_volume = -best_GEAR_bid_vol
                orders.append(Order(symbol, sell_price, sell_volume))
                print("SELL " + str(symbol) + " price: ", str(sell_price) + " volume: ", str(sell_volume))
        
        if current_pos < 0:    
            if GEAR_value > last_25_avg and (state.timestamp - self.jump_timestamps[-1])> 10000:
                buy_price = best_GEAR_ask
                buy_volume = -best_GEAR_ask_vol
                orders.append(Order(symbol, buy_price, buy_volume))
                print("BUY " + str(symbol) + " price: ", str(buy_price) + " volume: ", str(buy_volume))
        
        return orders

# ------------------ BERRIES --------------------- 
    def get_orders_berries(self, state: TradingState):
        orders_berries: list[Order] = []

        # Getting the stored data
        # mask_berries = self.df_data_market["symbol"] == "BERRIES"
        # df_berries = self.df_data_market[mask_berries]

        # Getting the order book:
        print("The order book for BERRIES is:")
        order_book_berries = state.order_depths["BERRIES"]

        # Getting our last trades on BERRIES
        # own_trades = self.get_own_trades_symbol("BERRIES", state)

        # Aaand getting our current position on BERRIES atm
        current_pos_berries = self.get_pos_symbol("BERRIES", state)
        print("The current position on BERRIES is:" + str(current_pos_berries))

        # PARAMETERS: can be modified when backtesting: max sizing we want to fill for a price and dt between trades
        max_volume_per_order = 1

        # # We first begin by the building our long position
        if 100000 < state.timestamp < 350000:
            if (state.timestamp % 1000) == 0:
                # Best ask; we want to be filled at the lowest available price
                ask_price_1 = min(order_book_berries.sell_orders.keys())
                # ask_volume_1 = order_book_berries.sell_orders.get(ask_price_1)
                orders_berries.append(Order("BERRIES", ask_price_1, max_volume_per_order))

        # Now looking to build the short position
        if 450000 <= state.timestamp <= 550000:
            # if 10000 < state.timestamp < 350000:
            if (state.timestamp % 200) == 0:
                # Best ask; we want to be filled at the lowest available price
                bid_price_1 = max(order_book_berries.buy_orders.keys())
                # ask_volume_1 = order_book_berries.sell_orders.get(ask_price_1)
                orders_berries.append(Order("BERRIES", bid_price_1, - max_volume_per_order))

            # FINALLY looking to unload the short position
        if 700000 <= state.timestamp <= 1000000:
            # if 100000 < state.timestamp < 350000:
            if (state.timestamp <= 775000) or (state.timestamp >= 825000):  # 350k timestamps to buy back 230 position
                if (state.timestamp % 1000) == 0:  # leaves 0 opened in the end
                    # Best ask; we want to be filled at the lowest available price
                    ask_price_1 = min(order_book_berries.sell_orders.keys())
                    # ask_volume_1 = order_book_berries.sell_orders.get(ask_price_1)
                    orders_berries.append(Order("BERRIES", ask_price_1, max_volume_per_order))

        print("The orders we submitted are:")
        print(orders_berries)
        return orders_berries


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

# ------------------ PICNIC BASKET --------------------- 
    def get_orders_basket(self, state: TradingState):
        # orders_coconuts: list[Order] = []
        # orders_pinas: list[Order] = []

        dic_orders = {"DIP": None, "PICNIC_BASKET": None, "UKULELE": None, "BAGUETTE": None}  # Initiating the output
        df_market = self.df_data_market

        # Getting the stored data
        mask_baguette = (df_market["symbol"] == "BAGUETTE")
        df_baguette = df_market[mask_baguette].reset_index(drop=True)

        mask_ukulele = (df_market["symbol"] == "UKULELE")
        df_ukulele = df_market[mask_ukulele].reset_index(drop=True)

        mask_dip = (df_market["symbol"] == "DIP")
        df_dip = df_market[mask_dip].reset_index(drop=True)

        mask_basket = (df_market["symbol"] == "PICNIC_BASKET")
        df_basket = df_market[mask_basket].reset_index(drop=True)

        df_replication = pd.DataFrame()
        df_replication["min_buy_price_basket"] = 2 * df_baguette["ask_price_1"] + df_ukulele["ask_price_1"] + 4 * \
                                                 df_dip["ask_price_1"]

        df_replication["mid_price"] = 2 * df_baguette["mid_price"] + df_ukulele["mid_price"] + 4 * \
                                      df_dip["mid_price"]
        df_replication["spread"] = df_basket["bid_price_1"] - df_replication["min_buy_price_basket"]

        # Getting the order books
        order_book_baguette = state.order_depths["BAGUETTE"]
        order_book_ukulele = state.order_depths["UKULELE"]
        order_book_dip = state.order_depths["DIP"]
        order_book_basket = state.order_depths["PICNIC_BASKET"]

        if len(df_basket) > 1:  # we don't do anything until we have at least 200 datapoints
            # Getting positions
            current_pos_baguette = self.get_pos_symbol("BAGUETTE", state)
            print("The current position on BAGUETTE is: " + str(current_pos_baguette))
            current_pos_ukulele = self.get_pos_symbol("UKULELE", state)
            print("The current position on UKULELE is: " + str(current_pos_ukulele))
            current_pos_dip = self.get_pos_symbol("DIP", state)
            print("The current position on DIP is: " + str(current_pos_dip))
            current_pos_basket = self.get_pos_symbol("PICNIC_BASKET", state)
            print("The current position on PICNIC_BASKET is: " + str(current_pos_basket))

            log_mid_prices_basket = np.log(df_basket["mid_price"])
            log_mid_prices_replication = np.log(df_replication["mid_price"])
            spread = log_mid_prices_basket.values[-1] - log_mid_prices_replication.values[-1]
            print("The spread is: " + str(round(spread, 4)))

            if spread > 0.0075:  # I don't want to offset my positions here, they can only increase
                print("We're in short basket zone.")

                desired_pos_basket = - 35 * (spread ** 2 - 0.0075 ** 2) / (0.01 ** 2 - 0.0075 ** 2) - 35
                desired_pos_baguette = 70 * (spread ** 2 - 0.0075 ** 2) / (0.01 ** 2 - 0.0075 ** 2) + 70
                desired_pos_dip = 140 * (spread ** 2 - 0.0075 ** 2) / (0.01 ** 2 - 0.0075 ** 2) + 140
                desired_pos_ukulele = 35 * (spread ** 2 - 0.0075 ** 2) / (0.01 ** 2 - 0.0075 ** 2) + 35

                volume_to_send_basket = min(0, desired_pos_basket - current_pos_basket)
                volume_to_send_baguette = max(0, desired_pos_baguette - current_pos_baguette)
                volume_to_send_dip = max(0, desired_pos_dip - current_pos_dip)
                volume_to_send_ukulele = max(0, desired_pos_ukulele - current_pos_ukulele)

            elif spread < 0.0025:  # I don't want to offset my positions here, they can only increase
                print("We're in long basket zone.")
                desired_pos_basket = - 35 * spread ** 2 / (0.0025 ** 2) + 70
                desired_pos_baguette = 70 * spread ** 2 / (0.0025 ** 2) - 140
                desired_pos_dip = 140 * spread ** 2 / (0.0025 ** 2) - 280
                desired_pos_ukulele = 35 * spread ** 2 / (0.0025 ** 2) - 70

                volume_to_send_basket = max(0, desired_pos_basket - current_pos_basket)
                volume_to_send_baguette = min(0, desired_pos_baguette - current_pos_baguette)
                volume_to_send_dip = min(0, desired_pos_dip - current_pos_dip)
                volume_to_send_ukulele = min(0, desired_pos_ukulele - current_pos_ukulele)

            else:  # where the fun begins
                print("We're in holding zone.")

                desired_pos_basket = current_pos_basket
                desired_pos_ukulele = current_pos_ukulele
                desired_pos_dip = current_pos_dip
                desired_pos_baguette = current_pos_baguette

                volume_to_send_basket = 0
                volume_to_send_baguette = 0
                volume_to_send_dip = 0
                volume_to_send_ukulele = 0

            print("The volume to send on UKULELE is: " + str(volume_to_send_ukulele))
            print("The volume to send on DIP is: " + str(volume_to_send_dip))
            print("The volume to send on BAGUETTE is: " + str(volume_to_send_baguette))
            print("The volume to send on BASKET is: " + str(volume_to_send_basket))

            print("The desired position on UKULELE is: " + str(desired_pos_ukulele))
            print("The desired position on DIP is: " + str(desired_pos_dip))
            print("The desired position on BAGUETTE is: " + str(desired_pos_baguette))
            print("The desired position on BASKET is: " + str(desired_pos_basket))

            orders_basket = self.get_orders_with_volume("PICNIC_BASKET", order_book_basket, volume_to_send_basket)
            orders_baguette = self.get_orders_with_volume("BAGUETTE", order_book_baguette, volume_to_send_baguette)
            orders_dip = self.get_orders_with_volume("DIP", order_book_dip, volume_to_send_dip)
            orders_ukulele = self.get_orders_with_volume("UKULELE", order_book_ukulele, volume_to_send_ukulele)

            dic_orders["PICNIC_BASKET"] = orders_basket
            dic_orders["BAGUETTE"] = orders_baguette
            dic_orders["DIP"] = orders_dip
            dic_orders["UKULELE"] = orders_ukulele

            print("The orders sent are:")
            print(dic_orders)
        return dic_orders

    @staticmethod
    def get_orders_with_volume(symbol, order_book, volume_to_send):
        orders: list[Order] = []
        total_volume_sent = 0
        if volume_to_send > 0:
            for price, volume in sorted(order_book.sell_orders.items()):
                if abs(total_volume_sent) < abs(volume_to_send):
                    volume_to_submit = min(abs(volume), abs(volume_to_send),
                                           abs(volume_to_send - total_volume_sent))
                    total_volume_sent += volume_to_submit
                    volume_to_send -= volume_to_submit
                    orders.append(Order(symbol, price, volume_to_submit))  # and a + here

        # On this side, we want to sell cocos & buy pinas
        elif volume_to_send < 0:
            for price, volume in sorted(order_book.buy_orders.items(), reverse=True):
                if abs(total_volume_sent) < abs(volume_to_send):
                    volume_to_submit = min(abs(volume), abs(volume_to_send),
                                           abs(volume_to_send - total_volume_sent))
                    total_volume_sent += volume_to_submit
                    volume_to_send += volume_to_submit
                    orders.append(Order(symbol, price, - volume_to_submit))  # and a + here
        return orders
