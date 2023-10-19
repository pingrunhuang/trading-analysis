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
            return heappop(self.buy_trades)[-1]
        except IndexError:
            return None
        
    def enqueue(self, trade:Trade):
        heappush(self.buy_trades, (trade.dt, trade))
    
    def assemble_trade(self, doc:dict):
        return Trade(doc["symbol"], doc["trade_amt"], doc["trade_prx"], doc["datetime"], doc["trade_fee"], doc["stamp_tax"], doc["transmit_fee"], doc["regulate_fee"], doc["buysell"])
    
    def load_trade(self, filter:dict):
        clc = self.mongo_manager.db.get_collection("em_trades")
        res = clc.find(filter, sort=[("datetime",1)])
        if not res:
            raise ValueError(f'documents with filter={filter} not found')
        trades = 0
        for doc in res:
            yield self.assemble_trade(doc)
            trades+=1
        logger.info(f"Fetched {trades} trades")
    
    def _calculate_pnl(self, trade:Trade):
        logger.info(f"Checking trade: {trade}")
        if trade.side == "B":
            self.enqueue(trade)
        else:
            logger.info(f"Closing position: {trade}")
            sell_fee = trade.transmit_fee+trade.trade_fee+trade.stamp_tax+trade.regulate_fee
            sell_prx = trade.prx
            sell_qty = trade.qty
            open_dates = []
            while sell_qty>0:
                buy_trade = self.dequeue()
                buy_fee = buy_trade.transmit_fee+buy_trade.trade_fee+buy_trade.stamp_tax+buy_trade.regulate_fee
                open_dates.append(buy_trade.dt)
                if sell_qty>buy_trade.qty:
                    diff_qty = buy_trade.qty
                else:
                    diff_qty = sell_qty
                    if sell_qty<buy_trade.qty:
                        buy_trade.qty -= sell_qty
                        self.enqueue(buy_trade)
                    else:
                        logger.info(f"position opened at {open_dates}, closed at {trade.dt}")
                        self.realized_pnl-=sell_fee
                self.realized_pnl+=diff_qty*(sell_prx-buy_trade.prx)
                self.realized_pnl-=buy_fee
                sell_qty-=diff_qty
            

    def calculate_pnl(self, symbol:str):
        logger.info(f"Start calculating pnl of {symbol}")
        for trade in self.load_trade({"symbol":symbol}):
            self._calculate_pnl(trade)
        logger.info(f"Realized pnl: {self.realized_pnl}")


if __name__=="__main__":
    q = EMTradesPriorityQueue()
    q.calculate_pnl("中远海能")