
from typing import Dict, List
import pandas as pd
import numpy as np
import statistics as stat
import math as mt
from datamodel import OrderDepth, TradingState, Order


class Trader:
    limits = {"PEARLS": 20, "BANANAS": 20, "COCONUTS": 600, "PINA_COLADAS": 300}

    def run(self, state: TradingState) -> Dict[str, List[Order]]:
        """"""
        # This strategy computes the fair price of the asset using the vwap method, then defines a price at which we
        # are ready to trade

        # Initialize the method output dict as an empty dict
        result = {}

        # Looping through all the symbols
        for symbol in state.listings.keys():
            # Getting the position limit for this product & our position
            position_limit = self.limits.get(symbol)
            # Retrieve the Order Depth containing all the market BUY and SELL orders for the symbol
            order_depth: OrderDepth = state.order_depths[symbol]

            if symbol == "COCONUTS":  # can only look at coconuts since we'll trade them as a pair
                orders_coco_pina = self.get_orders_coco_pina(state)
                result["COCONUTS"] = orders_coco_pina.get("COCONUTS")
                result["PINA_COLADAS"] = orders_coco_pina.get("PINA_COLADAS")

            # Add all the above orders to the result dict
            elif symbol == "PEARLS" or symbol == "BANANAS":
                pass
                # result[symbol] = None
        return result

    def get_orders_coco_pina(self, state: TradingState):
        orders_coconuts: list[Order] = []
        orders_pinas: list[Order] = []

        order_book_coco = state.order_depths["COCONUTS"]
        order_book_pinas = state.order_depths["PINA_COLADAS"]

        # vwap_coco = self.get_vwap_price(order_book_coco)
        # vwap_pinas = self.get_vwap_price(order_book_pinas)
        # vwap_ratio_pina_coco = vwap_pinas / vwap_coco
        # print("The ratio of vwap prices is:" + str(vwap_ratio_pina_coco))

        vwap_coco = max(order_book_coco.buy_orders.keys())
        vwap_pinas = min(order_book_pinas.sell_orders.keys())
        vwap_ratio_pina_coco = vwap_pinas / vwap_coco
        print("The ratio of bid_1/ask_1 prices is:" + str(vwap_ratio_pina_coco))

        max_ratio = 1.8875  # MUST BE IMPROVED
        min_ratio = 1.8625  # MUSTT BE IMPROVE

        # Getting positions on coco & pinas
        current_pos_coco = self.get_pos_symbol("COCONUTS", state)
        print("The current position on COCONUTS is: " + str(current_pos_coco))
        current_pos_pinas = self.get_pos_symbol("PINA_COLADAS", state)
        print("The current position on PINA COLADAS is: " + str(current_pos_pinas))

        desired_pos_coco = - 1200 * (vwap_ratio_pina_coco - min_ratio) / (max_ratio - min_ratio) + 600
        print("The desired position on COCONUTS is: " + str(desired_pos_coco))
        desired_pos_pinas = 600 * (vwap_ratio_pina_coco - max_ratio) / (max_ratio - min_ratio) + 300
        print("The desired position on PINA COLADAS is: " + str(desired_pos_pinas))

        volume_to_send_coco = desired_pos_coco - current_pos_coco
        print("The volume to send on COCONUTS is: " + str(volume_to_send_coco))
        volume_to_send_pinas = desired_pos_pinas - current_pos_pinas
        print("The volume to send on PINA COLADAS is: " + str(volume_to_send_pinas))

        # Now we send the orders; we want to be filled first, then send orders a bit "foolish"
        total_volume_sent_coco = 0
        total_volume_sent_pinas = 0

        # On this side, we want to buy cocos & sell pinas
        if volume_to_send_coco > 0:
            for price, volume in sorted(order_book_coco.sell_orders.items()):
                if abs(total_volume_sent_coco) < abs(volume_to_send_coco):
                    volume_to_submit = min(abs(volume), abs(volume_to_send_coco), abs(volume_to_send_coco - total_volume_sent_coco))
                    total_volume_sent_coco += volume_to_submit
                    volume_to_send_coco += volume_to_submit
                    orders_coconuts.append(Order("COCONUTS", price, volume_to_submit))  # and a + here

        # On this side, we want to sell cocos & buy pinas
        elif volume_to_send_coco < 0:
            for price, volume in sorted(order_book_coco.buy_orders.items(), reverse=True):
                if abs(total_volume_sent_coco) < abs(volume_to_send_coco):
                    volume_to_submit = min(abs(volume), abs(volume_to_send_coco), abs(volume_to_send_coco - total_volume_sent_coco))
                    total_volume_sent_coco += volume_to_submit
                    volume_to_send_coco += volume_to_submit
                    orders_coconuts.append(Order("COCONUTS", price, - volume_to_submit))  # and a + here

        # SAME FOR PINAS
        if volume_to_send_pinas > 0:
            for price, volume in sorted(order_book_pinas.sell_orders.items()):
                if abs(total_volume_sent_pinas) < abs(volume_to_send_pinas):
                    volume_to_submit = min(abs(volume), abs(volume_to_send_pinas), abs(volume_to_send_pinas - total_volume_sent_pinas))
                    total_volume_sent_pinas += volume_to_submit
                    volume_to_send_pinas -= volume_to_submit
                    orders_pinas.append(Order("PINA_COLADAS", price, volume_to_submit))  # and a + here

        # On this side, we want to sell cocos & buy pinas
        elif volume_to_send_pinas < 0:
            for price, volume in sorted(order_book_pinas.buy_orders.items(), reverse=True):
                if abs(total_volume_sent_pinas) < abs(volume_to_send_pinas):
                    volume_to_submit = min(abs(volume), abs(volume_to_send_pinas), abs(volume_to_send_pinas - total_volume_sent_pinas))
                    total_volume_sent_pinas += volume_to_submit
                    volume_to_send_pinas += volume_to_submit
                    orders_pinas.append(Order("PINA_COLADAS", price, - volume_to_submit))  # and a + here

        dic_orders = {"COCONUTS": orders_coconuts, "PINA_COLADAS": orders_pinas}
        print("The orders sent are:")
        print(dic_orders)
        return dic_orders


    @staticmethod
    def get_pos_symbol(symbol, state):
        if symbol in state.position.keys():
            current_pos = state.position[symbol]
        else:
            current_pos = 0
        return current_pos
