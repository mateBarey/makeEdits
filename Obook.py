import asyncio
from asyncio import * 
from pprint import pprint
import datetime 

class OrderBook:
    def __init__(self):
        self.bids = {}  # Dictionary to store bid order book levels
        self.asks = {}  # Dictionary to store ask order book levels
        self.last_update_id = None  # Last update ID received
        self.total_levels = 15  # Total levels to keep in the order book

    async def process_initial_response(self, initial_response):
        # Process the initial response to initialize the order book
        self.bids = {}  # Clear existing bid order book
        self.asks = {}  # Clear existing ask order book

        bids_data = initial_response.get("bids", [])
        asks_data = initial_response.get("asks", [])

        for level in bids_data:
            price = float(level['price'])
            size = float(level['size'])
            self.bids[price] = size

        for level in asks_data:
            price = float(level['price'])
            size = float(level['size'])
            self.asks[price] = size

        self.trim_order_book()

    async def update_order_book(self, updates):
        sides = ['bids', 'asks']
        o_book_coroutines = [self.update_order_book_help(updates, side) for side in sides]
        await asyncio.gather(*o_book_coroutines)

        self.trim_order_book()

    async def make_obook(self):
        obook = {'bids': [self.bids], 'asks': [self.asks]}
        return obook 

    async def update_order_book_help(self, updates, side):
        order_book_side = self.bids if side == "bids" else self.asks

        for price, size in updates[side]:
            price = float(price)
            size = float(size)

            if size == 0:
                order_book_side.pop(price, None)
            else:
                order_book_side[price] = size

    def trim_order_book(self):
        # Keep only the top total_levels of bids and asks
        self.bids = dict(sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:self.total_levels])
        self.asks = dict(sorted(self.asks.items(), key=lambda x: x[0])[:self.total_levels])
