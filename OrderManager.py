import time
import asyncio
import time
import pandas as pd
from asyncio import gather
from collections import OrderedDict
import datetime as dt
from hbot51 import *
import lob_helpers
from typing import Optional 

class OrderManager:
    def __init__(self, hbot):
        self.hbot = hbot
        self.orders = OrderedDict()
        self.orderbook_data = {}
        self.expired_orders = {}
        self.completed_orders = {}
        self.retry_tickers = {}
        self.retry_order_events = {}
        self.order_stop_events = {}
        self.account_data = {}

    #1st initial order goes through make order called within make_order retry..
    # the retry order event should use make_orderbook data from lob helper but should be ordered here
    async def monitor_order(self, ticker, size, side, price, id, status, reduceOnly):
        retry_event = self.retry_order_events.setdefault(id, asyncio.Event())
        stop_event = self.order_stop_events.setdefault(id, asyncio.Event())
        retries = 0
        if reduceOnly or self.orders[id]['order_type'].upper() == 'MARKET':
            self.retry_tickers[ticker] = id 
            print(f"Entering Monitor order loop for {id} with {side} {ticker} at price level of {price} with order type {self.orders[id]['order_type']}")
        try:
            while not stop_event.is_set():

                current_status, remainingSize, t0 = await self._get_updated_params(id)
                if not t0:
                    t0 = self.orders[id]['t0']
                order_type = self.orders[id]['order_type']
                print(f"Current updated params for order {id} of {side} {ticker} and order type {self.orders[id]['order_type']} are:")
                print(f"status: {current_status},remainingSize: {remainingSize}")
                # Handling for fully filled orders
                if await self._is_order_completed(id, current_status, remainingSize, size, reduceOnly,order_type):
                    print(f"order filled for order {id} {side} {ticker} of order type {self.orders[id]['order_type']} ")
                    break  # No further action needed

                # Handling for reduceOnly orders
                if (reduceOnly or order_type == 'MARKET') and current_status == 'CANCELED' and remainingSize != 0:
                    retry_event.set()

                # Handling for partial fills and order expiration
                if (0 < remainingSize < size) and (current_status == 'CANCELED' or (await lob_helpers._elapsed_time(t0))):
                    retry_event.set()

                # Handle retries
                if retry_event.is_set():
                    
                    new_price = await self._get_order_price(ticker,side,order_type,reduceOnly)
                    new_order_size = remainingSize
                    # for reduce , market or partially filled   need to get orderbook data 
                    newt0 = time.perf_counter()
                    new_order_id, new_status = await self._place_order(ticker, side, new_order_size, new_price, order_type, reduceOnly)
                    
                    # Link to the new order ID
                    self.orders[id][new_order_id]= {'status':new_status,'price':new_price,'size':new_order_size,'t0':newt0}
                 
                    # Add the new order to the tracking
                    await self._update_order(id, new_status, new_order_size,new_price,new_order_id)
                    retry_event.clear()
                    retries += 1

                await asyncio.sleep(2)
        except asyncio.CancelledError:
            print(f"Order ID: {id} got cancelled")

    async def _get_updated_params(self, order_id):
        order = self.orders.get(order_id, {})
        if not order:
            print(f"Order ID {order_id} not found.")
            return None, None, None  # status, remainingSize, t0

        nested_order_keys = [key for key in order if isinstance(order[key], dict)]
        if nested_order_keys:
            print('does something reach nested order')
            # Get the latest nested order
            latest_order_key = nested_order_keys[-1]
            if not self.account_data: # reason this would work is because as soon as ws updates account data it updated orderbook data every time batch data is sent in kafka
                order_ = await self.hbot.get_order(latest_order_key)
                status = order_['order']['status']
                remainingSize = float(order_['order']['remainingSize'])
                return status, remainingSize, order[latest_order_key].get('t0')
            latest_order = order[latest_order_key]
            return latest_order.get('status'), latest_order.get('remainingSize'), latest_order.get('t0')
        else:
            # Use the main order's details
            if not self.account_data:
                order_ = await self.hbot.get_order(order_id)
                status = order_['order']['status']
                remainingSize = float(order_['order']['remainingSize'])
                return status, remainingSize, order.get('t0')
            return order.get('status'), order.get('remainingSize'), order.get('t0')
        
    async def _receive_order_update(self, id, status, remaining_size,price):
        if id in self.orders:
            await self._update_order(id, status, float(remaining_size),price)
            return
        nested_id = next((nested_id for _, nested_order in self.orders.items() for nested_id in nested_order if nested_id == id), None)
        if nested_id:
            await self._update_order(id, status, float(remaining_size),price, nested_id)
        else:
            print(f"Order ID {id} not found in OrderManager.")
                
    async def _get_order_price(self,ticker,side,order_type,redn):
        if not self.orderbook_data:
            obook = await self.hbot.get_orderbook(ticker)
        else:
            obook = self.orderbook_data[ticker]
        if order_type.upper() == 'MARKET':
            bid = float(obook['bids'][0]['price'])
            ask = float(obook['asks'][0]['price'])
            return lob_helpers.side_helper(side,bid,ask,redn)
            #return await lob_helpers._get_best_price_helper(bid,ask,redn,side)

        elif order_type.upper() == 'LIMIT':
            bid = float(obook['bids'][1]['price'])
            ask = float(obook['asks'][1]['price'])
            return lob_helpers.side_helper(side,bid,ask,redn)

    async def find_level_of_orderbook(self, ticker: str, price: float, side: str, reduction: bool, order_type: str):
        """
        Finds the level of an order book for a given ticker, price, and side.

        Parameters:
        - ticker: The ticker symbol of the asset.
        - price: The price to find in the order book.
        - side: The side of the market ('SHORT' or otherwise).
        - reduction: Indicates whether price reduction is considered.
        - order_type: The type of the order.

        Returns:
        - The level in the order book where the price is found, or None if not found.
        """
        try:
            if not self.orderbook_data:
                obook = await self.hbot.get_orderbook(ticker)
            else:
                obook = self.orderbook_data[ticker]

            # Determine bid or ask side based on 'side' and 'reduction' parameters
            if reduction:
                bid_or_ask = 'bids' if side.upper() == 'SHORT' else 'asks'
            else:
                bid_or_ask = 'asks' if side.upper() == 'SHORT' else 'bids'

            # Iterate through the order book levels to find the matching price
            for level in range(11):  # Assuming there are 11 levels in the order book
                if price == float(obook[bid_or_ask][level]['price']):
                    return level
        except Exception as e:
            print(f'Error , {e}')
        
        return None  # Return None if price not found in any level
        
    async def _calculate_average_price(self, id, orders_dict):
        if id not in orders_dict:
            print(f"No order found with ID {id} in the provided orders dictionary.")
            return None
        ticker = orders_dict[id]['ticker']
        markets = await self.hbot.get_markets()
        rd = find_dec_places(markets[ticker]['tickSize'])

        order = orders_dict[id]
        nested_order_ids = [key for key in order if isinstance(order[key], dict)]
        total_price = sum(order[nested_id]['price'] * order[nested_id]['size'] for nested_id in nested_order_ids if\
                           'price' in order[nested_id] and 'size' in order[nested_id])
        total_size = sum(order[nested_id]['size'] for nested_id in nested_order_ids if 'size' in order[nested_id])

        if total_size:
            markets_ =  await self.hbot.get_markets()
            rd = find_dec_places(float(markets_[order['ticker']]['tickSize']))

            average_price = round(total_price / total_size , rd)
            return average_price
        return round(order.get('price') ,rd)

    async def _update_order(self, id, status, remaining_size, price,nested_id=None):
        target_order = self.orders[id][nested_id] if nested_id else self.orders[id]
        if status and remaining_size is not None:
            target_order['status'] = status
            target_order['remainingSize'] = remaining_size
            target_order['price'] = price 

    async def _start_order_monitoring(self, ticker, side, size, price, id, status, reduceOnly, t0, order_type):
        task = asyncio.create_task(self.monitor_order(
            ticker, size, side, price, id, status, reduceOnly))
        self.orders[id] = {'task': task, 'reduceOnly': reduceOnly, 'price': float(price), 'size': float(size), 'side': side, 'status': status,
                           'elapsed_time': None, 'ticker': ticker, 'remainingSize': float(size), 't0': t0, 'order_type': order_type}
        return id 
    
    async def _update_obook_data(self,obook_data):
        self.orderbook_data = obook_data

    async def _update_account_data(self,account_data):
        self.account_data = account_data

    async def _record_orders(self):
        async def create_df_from_orders(orders_dict, calculate_avg_price_fn):
            records = [
                {
                    'id': order_id,
                    'average_price': await calculate_avg_price_fn(order_id, orders_dict),
                    'status': order_data.get('status'),
                    'initial_size': order_data.get('size'),
                    'remaining_size': order_data.get('remainingSize'),
                    'side': order_data.get('side'),
                    'ticker': order_data.get('ticker'),
                    'order_type': order_data.get('order_type'),
                    'reduce_only': order_data.get('reduceOnly')
                }
                for order_id, order_data in orders_dict.items()
            ]
            return pd.DataFrame(records)

        if len(self.completed_orders.keys()) % 2 == 0:
            name_1 = f'orderManager_log_completed_orders{dt.datetime.now().month}{dt.datetime.now().day}{dt.datetime.now().year}.csv'
            df_completed = await create_df_from_orders(self.completed_orders, self._calculate_average_price)
            df_completed.to_csv(name_1)

        if len(self.expired_orders.keys()) % 2 == 0:
            name_2 = f'orderManager_log_expired_orders{dt.datetime.now().month}{dt.datetime.now().day}{dt.datetime.now().year}.csv'
            df_expired = await create_df_from_orders(self.expired_orders, self._calculate_average_price)
            df_expired.to_csv(name_2)

    async def _cancel_order_monitoring(self, id):
        if id in self.order_stop_events:
            self.order_stop_events[id].set()
        if id in self.orders:
            self.orders[id]['task'].cancel()

    async def _shutdown(self):
        orders_cancel = [self._cancel_order_monitoring(
            id) for id in self.orders.keys()]
        orders_cancel = await gather(*orders_cancel, return_exceptions=True)

    async def _is_order_completed(self, id, current_status, remainingSize, size, reduceOnly, order_type):
        # Clone the order without the 'task' key
        order = {k: v for k, v in self.orders[id].items() if k != 'task'}

        if remainingSize == 0:
            # Ensure the 'id' key exists in 'completed_orders' before updating
            if id not in self.completed_orders:
                self.completed_orders[id] = {}
            self.completed_orders[id].update(order)

            # Calculate and set the average price
            average_price = await self._calculate_average_price(id, self.orders)
            self.completed_orders[id]['average_price'] = average_price
            return True

        if not reduceOnly and (current_status in ['CANCELED'] and remainingSize == size and (order_type != 'MARKET')):
            # Ensure the 'id' key exists in 'expired_orders' before updating
            if id not in self.expired_orders:
                self.expired_orders[id] = {}
            self.expired_orders[id].update(order)
            return True

        print(f'reduceOnly:{reduceOnly}, remainingSize:{remainingSize} size: {size}')
        # Additional condition to check if the price level has jumped above 10
        if not reduceOnly and remainingSize == size:
            # Assuming 'ticker', 'price', and 'side' are obtainable from the order or elsewhere
            ticker = order.get('ticker')  # Placeholder, replace with actual way to obtain the ticker
            price = order.get('price')  # Placeholder, replace with actual way to obtain the price
            side = order.get('side')  # Placeholder, replace with actual way to obtain the side
            level = await self.find_level_of_orderbook(ticker, price, side, reduceOnly, order_type)
            print(f'found price level of {level}')
            if level is not None and level > 5:
                # Mark as expired order if price level has jumped above 10
                if id not in self.expired_orders:
                    self.expired_orders[id] = {}
                self.expired_orders[id].update(order)
                return True

        return False

    
    async def _place_order(self, ticker, side, size, price, order_type, reduceOnly):
        # Actual order placement logic using hbotf
        order_response = await self.hbot.place_order(ticker, side, size, price, order_type, False, reduceOnly)
        await asyncio.sleep(1.5)
        return order_response['order']['id'], order_response['order']['status']

if __name__ == '__main__':
    async def test_om():
        eth_add =  os.getenv("eth_add")
        eth_add2 =  os.getenv("eth_add2")
        pkey = os.getenv("pkey")
        pkey2 = os.getenv("pkey2")
        client = Client(host='https://api.dydx.exchange')
        DYDX_API_CREDENTIALS = {
            "key": os.environ.get("ek2", ""),
            "secret": os.environ.get("es2", ""),
            "passphrase": os.environ.get("epp2", ""),
        }
        hstk = os.getenv("eck2")
        hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'],DYDX_API_CREDENTIALS['secret'],DYDX_API_CREDENTIALS['passphrase'],'0',hstk,eth_add2,None)
        ticker = 'RUNE-USD'

        markets = await hbotf.get_markets()
        rd = find_dec_places(markets[ticker]['tickSize'])
        step = find_dec_places(float(markets[ticker]['stepSize']))
        obook = await hbotf.get_orderbook(ticker)
        price = round(float(obook['asks'][1]['price']),rd)
        size = round(100/price,step)
        reduceOnly = False
        side = 'SELL'
        order_type = 'LIMIT'
        order = await hbotf.place_order(ticker,side,size,price,order_type,False,reduceOnly)
        print(order)
        await asyncio.sleep(1.5)
        id = order['order']['id']
        status = order['order']['status']
        t0 = time.perf_counter()
        order_manager = OrderManager(hbotf)
        original_size = 1
        
        order_id = await order_manager._start_order_monitoring(ticker,side,size ,price, id,status,reduceOnly, t0,order_type)
        task = await order_manager.orders[order_id]['task']

    asyncio.run(test_om())
