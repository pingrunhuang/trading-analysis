# -*- coding: utf-8 -*-
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
    
    def is_open(self):
        return len(self.buy_trades)!=0
        
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
            sell_fee = trade.transmit_fee+trade.trade_fee+trade.stamp_tax+trade.regulate_fee
            sell_prx = trade.prx
            sell_qty = trade.qty
            open_dates = []
            profit=0
            while sell_qty>0:
                buy_trade = self.dequeue()
                if not buy_trade:
                    raise ValueError(f"No buy trades exists, sell trade impossible")
                buy_fee = buy_trade.transmit_fee+buy_trade.trade_fee+buy_trade.stamp_tax+buy_trade.regulate_fee
                logger.warning(f"buy_fee={buy_fee}")
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
                        logger.warning(f"sell_fee={sell_fee}")
                profit += diff_qty*(sell_prx-buy_trade.prx)
                logger.warning(f"profit={profit}")
                self.realized_pnl+=profit
                self.realized_pnl-=buy_fee
                sell_qty-=diff_qty

            if not self.is_open():
                profit=0
                logger.info("="*200)


    def calculate_pnl(self, filter:dict):
        logger.info(f"Start calculating pnl of {filter}")
        for trade in self.load_trade(filter):
            self._calculate_pnl(trade)
        logger.info(f"Realized pnl: {self.realized_pnl}")
        logger.info(f"Long position={len(self.buy_trades)}")


if __name__=="__main__":
    q = EMTradesPriorityQueue()
    q.calculate_pnl({"symbol_id":"159954"})