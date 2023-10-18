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
    def __init__(self) -> None:
        self.buy_trades = []
        self.sell_trades = []
        self.counter = count()
        self.is_long = None
        self.mongo_manager = MongoManager("fund")

    def dequeue(self, side:str)->Optional[Trade]:
        try:
            if side == "S":
                return heappop(self.sell_trades)[-1]
            elif side == "B":
                return heappop(self.buy_trades)[-1]
        except IndexError:
            return None
        
    def enqueue(self, trade: Trade, container:list):
        heappush(container, (trade.dt, next(self.counter), trade))
    
    def load_trade(self, symbol:str):
        clc = self.mongo_manager.db.get_collection("em_trades")
        res = clc.find({"symbol":symbol}, sort=[("datetime",1)])
        if not res:
            raise ValueError(f'symbol={symbol} not found')
        for doc in res:
            yield Trade(doc["symbol"], doc["trade_prx"], doc["trade_amt"], doc["datetime"], doc["trade_fee"], doc["stamp_tax"], doc["transmit_fee"], doc["regulate_fee"], doc["buysell"])

    def calculate_pnl(self, symbol:str):
        
        for trade in self.load_trade(symbol):
            if trade.side == "S":
                self.enqueue(trade, self.sell_trades)
            elif trade.side == "B":
                self.enqueue(trade, self.buy_trades)

        logger.info(f"Finished enqueueing buy trades: {len(self.buy_trades)}")
        logger.info(f"Finished enqueueing sell trades: {len(self.sell_trades)}")
        sell_trade = self.dequeue("S")
        buy_trade = self.dequeue("B")
        result = 0
        total_fee = 0
        buy_qty = 0
        buy_prx = 0
        buy_ntl = 0
        while sell_trade or buy_trade:
            if buy_trade:
                total_fee += buy_trade.transmit_fee+buy_trade.trade_fee+buy_trade.stamp_tax+buy_trade.regulate_fee
                buy_ntl = buy_trade.prx*buy_trade.qty
                logger.info(f"Current open position: {buy_qty}")

            if sell_trade:
                total_fee += sell_trade.transmit_fee+sell_trade.trade_fee+sell_trade.stamp_tax+sell_trade.regulate_fee
                sell_ntl = sell_trade.qty*sell_trade.prx
                pnl = sell_ntl - buy_ntl
            
            result = result + total_ntl
            logger.info(f"current pnl at {trade.dt}: {result}")
            trade = self.dequeue(symbol)
        logger.info(f"final pnl: {result}")
        return result

if __name__=="__main__":
    q = EMTradesPriorityQueue()
    q.calculate_pnl("中远海能")