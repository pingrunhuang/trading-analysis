from src.pnl_analysis import EMTradesPriorityQueue
import src.loggers
import logging
import argparse
import json

logger = logging.getLogger("main")
parser = argparse.ArgumentParser(description="trading analysis")

if __name__=="__main__":
    parser.add_argument("--symbol_id", type=str, default="")
    kwargs = parser.parse_args()
    filters = {"symbol_id":kwargs.symbol_id}
    logger.debug(filters)
    q = EMTradesPriorityQueue()
    q.calculate_pnl(filters)