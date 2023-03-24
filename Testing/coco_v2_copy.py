from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order

class Trader:
    limits = {"PEARLS": 20, "BANANAS": 20, "COCONUTS": 600, "PINA_COLADAS": 300}

    def __init__(self):
        self.position_coconuts = 0
        self.position_pinas = 0
        self.last_mid_price = None

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            # Getting the position limit for this product & our position
            position_limit = self.limits.get(symbol)
            order_depth: OrderDepth = state.order_depths[symbol]

            if symbol == "COCONUTS":  # can only look at coconuts since we'll trade them as a pair
                orders_coco_pina = self.get_orders_coco_pina(state)
                result["COCONUTS"] = orders_coco_pina.get("COCONUTS")
                result["PINA_COLADAS"] = orders_coco_pina.get("PINA_COLADAS")

            # Add all the above orders to the result dict
            elif symbol == "PEARLS" or symbol == "BANANAS":
                result[symbol] = None

        return result

    def get_orders_coco_pina(self, state: TradingState):
        order_depth = state.order_depths[symbol]
        orders_coconuts: List[Order] = []
        orders_pinas: List[Order] = []

        # Condition 1: Position limits
        pos_limit_coconuts = self.limits["COCONUTS"]
        pos_limit_pinas = self.limits["PINA_COLADAS"]

        # Condition 2: Cannot trade PINA_COLADAS without COCONUTS
        if self.position_coconuts == 0:
            return {"COCONUTS": orders_coconuts, "PINA_COLADAS": orders_pinas}

        # Condition 3: Bid volume/Ask volume
        bid_volume_coco = sum([o.volume for o in state.order_depths["COCONUTS"].bids])
        ask_volume_coco = sum([o.volume for o in state.order_depths["COCONUTS"].asks])
        bid_volume_pina = sum([o.volume for o in state.order_depths["PINA_COLADAS"].bids])
        ask_volume_pina = sum([o.volume for o in state.order_depths["PINA_COLADAS"].asks])

        # Condition 4: Mid price
        mid_price_coco = (state.order_depths["COCONUTS"].bids[0].price + state.order_depths["COCONUTS"].asks[0].price) / 2
        mid_price_pina = (state.order_depths["PINA_COLADAS"].bids[0].price + state.order_depths["PINA_COLADAS"].asks[0].price) / 2
        mid_price = (mid_price_coco + mid_price_pina) / 2
        price_diff = abs(mid_price_coco - mid_price_pina)

        # Condition 5: Spread
        vol = (bid_volume_coco + ask_volume_coco) / (bid_volume_pina + ask_volume_pina)
        spread = 0.01
        if vol < 0.1:
            spread *= 1.2
        elif vol > 0.2:
            spread *= 0.8
        
        # Calculate profit for each trade
        profit = 0
        if self.last_mid_price is not None:
            if self.position_coconuts > 0:
                # Sold COCONUTS
                profit += (mid_price_coco - self.last_mid_price) * self.position_coconuts
            elif self.position_coconuts < 0:
                # Bought COCONUTS
                profit += (self.last_mid_price - mid_price_coco) * abs(self.position_coconuts)
            if self.position_pinas > 0:
                # Bought PINA_COLADAS
                profit += (mid_price_pina - self.last_mid_price) * self.position_pinas
            elif self.position_pinas < 0:
                # Sold PINA_COLADAS
                profit += (self.last_mid_price - mid_price_pina) * abs(self.position_pinas)

        # Buy or sell COCONUTS
        if self.position_coconuts > 0:
            # Sell COCONUTS
            if np.random.rand() < 0.5:
                sell_price = mid_price_coco - spread
                sell_volume = min(self.position_coconuts, ask_volume_coco)
                self.position_coconuts -= sell_volume
                order = Order(sell_price, sell_volume, "COCONUTS", "SELL")
                orders_coconuts.append(order)
                # Update profit
                profit -= sell_price * sell_volume
            # Buy PINA_COLADAS
            else:
                buy_price = mid_price_pina + spread
                buy_volume = min(self.position_pinas, bid_volume_pina)
                self.position_pinas -= buy_volume
                order = Order(buy_price, buy_volume, "PINA_COLADAS", "BUY")
                orders_pinas.append(order)
                # Update profit
                profit -= buy_price * buy_volume

        elif self.position_coconuts < 0:
            # Buy COCONUTS
            if np.random.rand() < 0.5:
                buy_price = mid_price_coco + spread
                buy_volume = min(abs(self.position_coconuts), bid_volume_coco)
                self.position_coconuts += buy_volume
                order = Order(buy_price, buy_volume, "COCONUTS", "BUY")
                orders_coconuts.append(order)
                # Update profit
                profit += buy_price * buy_volume
            # Sell PINA_COLADAS
            else:
                sell_price = mid_price_pina - spread
                sell_volume = min(abs(self.position_pinas), ask_volume_pina)
                self.position_pinas += sell_volume
                order = Order(sell_price, sell_volume, "PINA_COLADAS", "SELL")
                orders_pinas.append(order)
                # Update profit
                profit += sell_price * sell_volume

        # Save current mid price for next iteration
        self.last_mid_price = mid_price

        # Check for position limits
        if abs(self.position_coconuts) > pos_limit_coconuts or abs(self.position_pinas) > pos_limit_pinas:
            orders_coconuts = []
            orders_pinas = []

        # Return orders for COCONUTS and PINA_COLADAS
        return {"COCONUTS": orders_coconuts, "PINA_COLADAS": orders_pinas}

