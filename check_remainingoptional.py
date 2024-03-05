async def check_remaining(self, ticker, side_order, id, price, redn):
    # Check if there is an OrderManager instance for this ticker
    if ticker not in self.Order_Managers:
        print(f"No OrderManager found for ticker {ticker}")
        return None

    order_manager = self.Order_Managers[ticker]

    # Function to get the latest order info
    def get_latest_order_info(order_id):
        order_info = order_manager.orders.get(order_id)

        if order_info:
            # Find the latest 'newOrderId' using list comprehension
            new_order_ids = [order_info[k] for k in order_info if 'newOrderId' in k]
            # Get the most recent order information
            for new_id in reversed(new_order_ids):
                if new_id in order_manager.orders:
                    return order_manager.orders[new_id]

        # Check in completed or expired orders if not found in current orders
        return order_manager.completed_orders.get(order_id) or order_manager.expired_orders.get(order_id)

    order_info = get_latest_order_info(id)

    if not order_info:
        print(f"Order ID {id} not found for ticker {ticker}")
        return None

    remaining = order_info['remainingSize']
    current_status = order_info['status']

    # Log the current status of the order
    self.logger.log(logging.INFO, f"{ticker} order with ID {id} is {current_status} with remaining size {remaining}")

    # Handle different scenarios based on the order status
    if current_status == 'CANCELED':
        msg = f"{ticker} order {id} was not successfully closed"
        c_ord = await self.cancel_make_ord(ticker, price, remaining, msg)
        return c_ord
    elif current_status == 'FILLED' and float(remaining) == 0:
        ord_f = self.remove_order(ticker, redn, price)
        return ord_f

    return remaining
import asyncio 


async def check_remaining(self, ticker, side_order, id, price, redn):
    size = self.queue[ticker][1]

    if ticker not in self.Order_Managers:
        print(f"No OrderManager found for ticker {ticker}")
        return None

    order_manager = self.Order_Managers[ticker]

    # Check if the order is currently being processed
    if id in order_manager.orders:
        print(f"Awaiting order with ID {id} to complete...")
        await order_manager.orders[id]['task']  # Await the completion of the task
        await asyncio.sleep(1.5)  # Give a brief moment for status updates

    # Refresh order info from either completed or expired orders
    order_info = None
    if id in order_manager.completed_orders:
        order_info = order_manager.completed_orders[id]
    if not redn and id in order_manager.expired_orders:
        order_info = order_manager.expired_orders[id]

    if not order_info:
        print(f"Order ID {id} not found for ticker {ticker}")
        return None

    remaining = order_info['remainingSize']
    current_status = order_info['status']

    self.logger.log(logging.INFO, f"{side_order} {ticker} order with ID {id} is {current_status} with remaining size {remaining}")

    if current_status == 'CANCELED' and float(remaining) == float(size):
        msg = f"{ticker} order {id} was not successfully closed"
        c_ord = await self.cancel_make_ord(ticker, price, remaining, msg)
        return c_ord
    elif current_status == 'FILLED' and float(remaining) == 0:
        ord_f = self.remove_order(ticker, redn, price)
        return ord_f

    return remaining

async def decide_order(self,ticker,price,reducing_order,side2,side,id):
    remaining_pseudo = await self.check_remaining(ticker,side2,id,side,price,reducing_order)
    if type(remaining_pseudo) == type({'hello':'yes'}):
        return remaining_pseudo
    else:
        remaining = remaining_pseudo
        if float(remaining) == 0:
            order = self.remove_order(ticker,reducing_order,price)
            return order 
        c_ord = await self.cancel_make_ord(ticker,price,remaining)
        return c_ord 
    
async def decide_market(self,ticker,redn,side2,id):
    side = self.queue[ticker][2]
    price = self.queue[ticker][0]
    #before if else get obook and look at 0 or 1 order and assume your price is this
    remaining_pseudo = await self.check_remaining(ticker,side2,id,side,price,redn)
    if type(remaining_pseudo) == type({'hello':'yes'}):
        return remaining_pseudo
    else:
        remaining = remaining_pseudo
        # need to cancel by id then make a market order  -- use Fill loop here
        print(f'Lob has predicted market order due to bid ask cross for {side} {ticker} at {price}, cancelling previous order')
        self.logger.log(logging.INFO,f'Lob has predicted market order due to bid ask cross for {side} {ticker} at {price} cancelling previous order')
        cancel = await self.cancel_ord(ticker,side,id,side2)
        if type(cancel) != type(True):
            print(f'Order Cancellation performed here is the order {cancel}')
            self.logger.log(logging.INFO,f'Order Cancellation performed here is the order {cancel}')
        else:
            print(f'Order did not need to be cancelled for {id} {side} {ticker} because it is no longer active')
            self.logger.log(logging.INFO,f'Order did not need to be cancelled for {id} {side} {ticker} because it is no longer active')

        o_book= await self.hbot.get_orderbook(ticker)
        best_bid = float(o_book['bids'][0]['price'])
        best_ask = float(o_book['asks'][0]['price'])

        if redn:
            if side.upper() == 'SHORT':
                price = best_bid  
            else:
                price = best_ask 
            # remove ticker active 
            # return order success with price_closed
            #order = await hbotf.place_order(ticker,side,size,price,'MARKET',False,True)
            print(f'Performing market order for {side} {ticker} and redn {redn} at {price}, this could take a bit')
            self.logger.log(logging.INFO,f'Performing market order for {side} {ticker} and redn {redn} at {price}, this could take a bit')
            order = await self.force_market_order(ticker,side,remaining,price,redn,side2)
            return order 
        
        else:
            if side.upper() == 'SHORT':
                price = best_ask 
            elif side.upper() == 'LONG':
                price = best_bid 
            # remove ticker from queue
            # add ticker to active 
            # and return order success order added to active 
            print(f'Performing market order for {side} {ticker} and redn {redn} at {price}, this could take a bit')
            self.logger.log(logging.INFO,f'Performing market order for {side} {ticker} and redn {redn} at {price}, this could take a bit')
            order = await self.force_market_order(ticker,side,remaining,price,redn,side2)
            return order 

async def not_found_price(self,ticker,price,redn,initial_obook,id,side2):
    '''
    look for if order is cancelled from very big price jump in dirn of 
    order book or if price was filled and is on opposite side
    '''
    side = self.queue[ticker][2]
    remaining_pseudo = await self.check_remaining(ticker,side2,id,price,redn)
    if type(remaining_pseudo) == type({'hello':'yes'}):
        return remaining_pseudo
    else:
        remaining = remaining_pseudo

        if not redn:
            new_side = side_helper_f(side,True,False)
        else:
            new_side = side_helper_f(side,False,False)
        opp_obook = order_obook(new_side,initial_obook)
        await self.logger.log(logging.ERROR,f' this is the opposite side of orderboook in case of jump {opp_obook}')
        if price in opp_obook:
            print(f'Price {price} for ticker {ticker} has been filled ')
            await self.logger.log(logging.INFO,f'Price {price} for ticker {ticker} has been filled ')
            self.queue[ticker] += [0]
            ord_f = await self.decide_order(ticker,price,redn,side2,side,id)
            return ord_f
        else:
            msg = 'seems order has dropped down or jumped up high in orderbook'
            await self.logger.log(logging.INFO,f'Order Cancelled for {side} {ticker} with size {self.queue[ticker][1]} and price {price} time limit of 600s was exceeded')
            c_ord = await self.cancel_make_ord(ticker,price,remaining,msg)
            return c_ord 
        

async def cancel_or_wait_limit_ord(self,ticker):
    price = self.queue[ticker][0]
    side = self.queue[ticker][2]
    size = self.queue[ticker][1]
    if self.queue[ticker][3] != True  and len(self.active_queue) >= 3  and ticker not in self.active_queue :
        print(f' There are either 3 or more Orders in Active Queue {len(self.active_queue.keys())} cancelling order for {ticker}')
        self.logger.log(logging.INFO,f' There are either 3 or more Orders in Active Queue {len(self.active_queue.keys())} \
                        cancelling order for {ticker}')

        print(f'Order Cancelled for {side} {ticker} with size {size} and price {price} because queue exceeds limit')
        await self.logger.log(logging.INFO,f'Order Cancelled for {side} {ticker} with size {size} and price {price} \
                                because queue exceeds limit')
        await self.remove_ticker(ticker,'limit')

        await self.print_queue_active_queues()
        return {'action':'Order Cancelled','status':f'Order Cancelled for {side} {ticker} with size {size} and price\
                    {price} time limit of 600s was exceeded','retries':0}
    return False   
    


async def make_order_retry(self, ticker, side, size, pr_want, redn, rd, stl, tar, retry=10):
    # Check if the order already meets the stop loss or target
    if not redn and (stl == pr_want or pr_want == tar):
        await self.logger.log(logging.ERROR, f'{side} {ticker} at price {pr_want} is already at stl {stl} or target {tar}, cancelling order')
        return {'action': 'Order Cancelled', 'status': f'{side} {ticker} at price {pr_want} is already at stl {stl} or target {tar}, cancelling order'}

    retries = 0
    while retries < retry:
        try:
            order = await self.make_order(ticker, side, size, pr_want, redn, rd, stl, tar)
            print(f'Order for {ticker}: {order}')
            await self.logger.log(logging.INFO, f'Order for {ticker}: {order}')

           #dont need to place order here or check remaining ... will happen  in wait_on_lim_ord fn

            if order and ('price' in order or 'price_closed' in order):
                return order

        except Exception as e:
            print(f"Exception while making order: {e}")
            await self.logger.log(logging.ERROR, f"Exception while making order for {ticker}: {e}")

        print(f'Retry number {retries} for {ticker}')
        await self.logger.log(logging.ERROR, f'Retry number {retries} for {ticker}')
        await asyncio.sleep((random.randint(3, 8)))
        retries += 1
    await self.logger.log(logging.INFO, f'Queue is full with {self.count()} for trader helper order cancelled for {ticker}')
    return {'action': 'Order Cancelled', 'status': f'Queue is full with {self.count()} for trader helper order cancelled for {ticker}'}


