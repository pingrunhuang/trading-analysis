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


class EMTradesPriorityQueue:
    def __init__(self) -> None:
        self._elements = defaultdict(list)
        self.counter = count()
        self.is_long = None
        self.mongo_manager = MongoManager("fund")

    def dequeue(self, symbol:str)->Optional[Trade]:
        try:
            return heappop(self._elements[symbol])[-1]
        except IndexError:
            return None
        
    def enqueue(self, trade: Trade):        
        heappush(self._elements[trade.symbol], (trade.dt, next(self.counter), trade))
    
    def load_trade(self, symbol:str):
        clc = self.mongo_manager.db.get_collection("em_trades")
        res = clc.find({"symbol":symbol}, sort=[("datetime",1)])
        if not res:
            raise ValueError(f'symbol={symbol} not found')
        for doc in res:
            if doc["buysell"] == "S":
                yield Trade(doc["symbol"], doc["trade_prx"], doc["trade_amt"], doc["datetime"], doc["trade_fee"], doc["stamp_tax"], doc["transmit_fee"], doc["regulate_fee"])
            else:
                yield Trade(doc["symbol"], doc["trade_prx"], -doc["trade_amt"], doc["datetime"], doc["trade_fee"], doc["stamp_tax"], doc["transmit_fee"], doc["regulate_fee"])
    

    def calculate_pnl(self, symbol:str):
        
        for trade in self.load_trade(symbol):
            self.enqueue(trade)
        logger.info(f"Finished enqueue trades")
        trade = self.dequeue(symbol)
        result = 0
        while trade:
            total_fee = trade.transmit_fee+trade.trade_fee+trade.stamp_tax+trade.regulate_fee
            total_ntl = trade.prx*trade.qty
            result = result + total_ntl+total_fee
            logger.info(f"current pnl at {trade.dt}: {result}")
            trade = self.dequeue(symbol)
        logger.info(f"final pnl: {result}")
        return result

if __name__=="__main__":
    q = EMTradesPriorityQueue()
    q.calculate_pnl("中远海能")