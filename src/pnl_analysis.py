"""
Use a priority queue to define the most early one of the trades that are opened
pop that first
"""

from heapq import heappush, heappop
from itertools import count
from dataclasses import dataclass
from abc import ABC
from collections import defaultdict
from datetime import datetime
import logging
import loggers
from mongo_utils import MongoManager
from typing import List, Optional


logger = logging.getLogger("main")


@dataclass
class Trade:
    symbol:str
    qty: float
    prx: float
    dt: datetime
    trade_fee: float
    stamp_tax: float
    transmit_fee: float
    regulate_fee: float
    side: str


class EMTradesPriorityQueue:
    """
    This is a no short situation
    Meaning we can only sell before buy 
    """
    def __init__(self) -> None:
        self.buy_trades = []
        self.counter = count()
        self.is_long = None
        self.mongo_manager = MongoManager("fund")
        self.realized_pnl = 0

    def dequeue(self)->Optional[Trade]:
        try:
            yield heappop(self.buy_trades)[-1]
        except IndexError:
            return None
        
    def enqueue(self, trade:Trade):
        heappush(self.buy_trades, (trade.dt, trade))
    
    def load_trade(self, symbol:str):
        clc = self.mongo_manager.db.get_collection("em_trades")
        res = clc.find({"symbol":symbol}, sort=[("datetime",1)])
        if not res:
            raise ValueError(f'symbol={symbol} not found')
        trades = 0
        for doc in res:
            yield Trade(doc["symbol"], doc["trade_prx"], doc["trade_amt"], doc["datetime"], doc["trade_fee"], doc["stamp_tax"], doc["transmit_fee"], doc["regulate_fee"], doc["buysell"])
            trades+=1
        logger.info(f"Fetched {trades} trades")

    def calculate_pnl(self, symbol:str):
        logger.info(f"Start calculating pnl of {symbol.encode('utf-8')}")

        for trade in self.load_trade(symbol):
            if trade.side == "B":
                self.enqueue(trade)
            else:
                # sell_fee = trade.transmit_fee+trade.trade_fee+trade.stamp_tax+trade.regulate_fee
                sell_prx = trade.prx
                sell_qty = trade.qty
                while sell_qty>0:
                    buy_trade = self.dequeue()
                    # buy_fee = buy_trade.transmit_fee+buy_trade.trade_fee+buy_trade.stamp_tax+buy_trade.regulate_fee
                    if sell_qty>buy_trade.qty:
                        diff_qty = buy_trade.qty
                    else:
                        diff_qty = sell_qty
                        if sell_qty<buy_trade.qty:
                            buy_trade.qty -= sell_qty
                            self.enqueue(buy_trade)
                        else:
                            logger.info(f"Liquidate trade at {trade.dt}")
                    self.realized_pnl+=diff_qty*(sell_prx-buy_trade.prx)
                    sell_qty-=diff_qty

        logger.info(f"Realized pnl: {self.realized_pnl}")


if __name__=="__main__":
    q = EMTradesPriorityQueue()
    q.calculate_pnl("中远海能")