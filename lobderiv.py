from abc import ABC, abstractmethod
from logging import * 
from asynclog import *
from bson.binary import Binary, UUID_SUBTYPE
import uuid 
import asyncio
from lob_helpers import * 
from lob_predictor import *
import time 
import datetime 
import random
from lob_exec_base import *
from wsv_handler import * 
from QueueEstimate import QueueEstimate

class LOBhelper(make_best_trades):
    def __init__(self,hbot,logger,bal,trader):
        super().__init__(hbot,logger,bal,trader)
        self.wsAssistant_trades = wsAssistant(self.hbot,['v3_trades'],1)
        self.wsAssistant_orderbook = wsAssistant(self.hbot,['v3_orderbook'],4)
        self.wsAssistant_account = wsAssistant(self.hbot,['v3_account'],1)
        self.ws_obook_positions = self.hbot.ws_obook_positions
        self.ws_positions = self.hbot.ws_market_positions
    
    async def cancel_make_ord(self,ticker,price,remaining,msg=''):
        side = self.queue[ticker][2]
        size = self.queue[ticker][1]
        print(f'Order Cancelled for {self.queue[ticker][2]} {ticker} with size {self.queue[ticker][1]} and price {price} , {msg}')
        await self.logger.log(logging.INFO,f'Order Cancelled for {self.queue[ticker][2]} {ticker} with \
                              size {self.queue[ticker][1]} and price {price} ,{msg}')
        await self.remove_ticker(ticker,'limit')

        await super().print_queue_active_queues()

        return {'action':f'Order Cancelled for {side} {ticker} with size {size} and price \
                {price} , {msg}','remaining':remaining}

    async def cancel_ord(self,market,side,id):
        try:
            ord = await self.hbot.cancel_order(id=id)
            return ord 
        except Exception as e:
            print(f'Error canceling order for {side} {market}, order\
                  with id {id}, order is probably no longer active so has already been cancelled, {e}')
            self.logger.log(logging.ERROR,f'Error canceling order for {side} {market}, order\
                  with id {id}, order is probably no longer active so has already been cancelled,{e}')
            return False 
        
    async def check_remaining(self,ticker,side2,id,price,redn):
        
        side = self.queue[ticker][2]
        try:
            act_orders = await self.hbot.get_active_orders(market=ticker,side=side2,id=id)
            await asyncio.sleep(0.2)
            act_orders = get_ord_by_id(act_orders,id)
            remaining = act_orders['remainingSize']
            print(f'Checking active orders to see if {side} {ticker} order with {id} is still active with size\
                   remaining {remaining} for {price}')
            self.logger.log(logging.INFO,f'Checking active orders to see if {side} {ticker} order with {id} \
                            is still active with size remaining {remaining} for {price}')
        except Exception as e:
            print(f'Error retrieving active orders for {side} {ticker} with {id}, {e}')
            self.logger.log(logging.ERROR,f'Error retrieving active orders for {side} {ticker} with {id}, {e}')
            get_ord = await self.hbot.get_order(id)
            await asyncio.sleep(0.2)
            remaining = get_ord['order']['remainingSize']
            print(f'Error, retreiving active orders for {side} {ticker} with id {id},order is probably cancelled or filled with remaining size {remaining} checking order by id')
            self.logger.log(logging.ERROR,f'Error, retreiving active orders for {side} {ticker} with id {id},order is probably cancelled or filled with remaining size {remaining} checking order by id')
            
            if get_ord['order']['status'] == 'CANCELED':
                msg = f'{side} {ticker} was not successfully closed '
                c_ord = await self.cancel_make_ord(ticker,price,remaining,msg)
                return c_ord 
            elif get_ord['order']['status'] == 'FILLED' and float(remaining) == 0:
                ord_f = self.remove_order(ticker,redn,price)
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
            
    async def add_ticker(self,ticker,expected_price,size,side,order_type_ml,redn,rd):
        '''
        add Ticker ,price, size, type of order, reduce/not reduce information to queue or active queue
        '''
        can_add = False 
        async with self.position_lock:
            new_a = self.market_buff() + [ticker]
            new_a = list(set(new_a))
            if len(new_a) <= 3:
                can_add = True

        #this method should call ws feeds: trades, obooks, accounts
        # and add ws_obook_positions[ticker] = 'open
        if can_add or redn:
            if order_type_ml == 'limit' or order_type_ml == 'TAKE_PROFIT' or order_type_ml == 'STOP_LIMIT':
                self.ws_obook_positions[ticker] = 'open' #new
                print(f'Order for ticker {ticker} being added to queue for {order_type_ml} order type, price\
                       {expected_price} and size : {size}')
                await self.logger.log(logging.INFO, f'Order for ticker {ticker} being added to queue for\
                                       {order_type_ml} order type, price {expected_price} and size {size} ')

                if ticker not in self.queue:
                    self.queue[ticker] = [expected_price,size,side,redn,rd]
                    async with self.semaphore:
                        #consumer of obook, trade and price queues will be the wait on limit ord
                        #producer will be the ws feeds for all 3 so wait on limit ord must execute all 4 functions
                        # we want order result to be returned but not necessarily anything from websocket
                        tasks = [asyncio.create_task(self.wsAssistant.get_web_socket_feed(channel)) for channel in self.queue_ws_channels]
                        pend = await self.wait_on_limit_ord(ticker,expected_price,side,order_type_ml)
                            
                        return pend 
            
            else: #method should run v3_markets ws feed and add ws_market_positions[ticker]= 'open'
                if ticker not in self.active_queue:
                    self.ws_market_positions[ticker] = 'open' #new 
                    margin_reqt = size*expected_price/self.lev 

                    fee = self.margin_fees['maker']*expected_price*size
                    await self.update_bal_and_add_ticker(ticker,expected_price,size,side,fee,margin_reqt)
                    print(f'Order for ticker {ticker} being added to active queue')
                    await self.logger.log(logging.INFO, f'Order for ticker {ticker} being added to active queue')

                    self.active_queue[ticker] = [expected_price,size,side,redn,rd]

                    if not redn and order_type_ml == 'market':
                        return {'action':'order added to active succesfully ','price':expected_price,'ticker':ticker,'side':side,'redn':redn}

        else:
            print(f'Order for ticker {ticker} cannot be added to queue for {order_type_ml} order type,\
                price {expected_price} and size : {size} , because Queue has been exceeded\
                    at len {len(self.active_queue.keys())}')
            self.logger.log(logging.INFO,f'Order for ticker {ticker} cannot be added to queue for {order_type_ml} order type,\
                price {expected_price} and size : {size} , because Queue has been exceeded\
                    at len {len(self.active_queue.keys())}')
            
            print(f'Order Cancelled for {side} {ticker} with size {size} and price {expected_price} because queue exceeds limit')
            await self.logger.log(logging.INFO,f'Order Cancelled for {side} {ticker} with size {size} and price {expected_price} \
                                  because queue exceeds limit')
            return {'action':'Order Cancelled','status':f'Order Cancelled for {side} {ticker} with size {size} \
                    and price {expected_price} queue size exceeded','retries':0}
        

    async def wait_on_limit_ord(self,ticker,price,side,order_type_ml):
        '''
        Wait on Orderbook for fill estimate place in queue based on orderbook snapshots
        '''
        #side2 called side order use more descriptive names

        reducing_order = self.queue[ticker][3]
        rd = self.queue[ticker][4]
        size = self.queue[ticker][1]
        side_order = side_helper_f(side,reducing_order,True)

        order = await self.hbot.place_order(ticker,side_order,size,price,order_type_ml,False,reducing_order)
        await asyncio.sleep(0.3)
        id = order['order']['id']
        remaining = float(order['order']['remainingSize'])
        timer0 = time.perf_counter()

        side = side_helper_f(side,reducing_order,False)
        obook_items = await self.trader.update_and_get_info(ticker,'OBOOK')
        #initial_obook = await self.hbot.get_orderbook(ticker)
        initial_obook = obook_items[0]
        obook_enum = obook_items[1]
        try:
            level0, _, initial_queue_len = await o_book_stats(initial_obook, price, side,reducing_order,False)
        except Exception as e:
            print(f'possible jump in order for {ticker} , error: {e}')
            #check opposite side of orderbook to see if price has filled
            ord = await self.not_found_price(ticker,price,reducing_order,initial_obook,id,side_order)
            return ord 
        total_initial_queue_len = initial_queue_len*1.05 + self.queue[ticker][1]
        print(f'adding total_queue len {total_initial_queue_len} to {ticker}')
        queue_estimate = QueueEstimate(self.logger,total_initial_queue_len)
        self.queue[ticker] += [queue_estimate]

        while ticker in self.queue:

            #check if orders are active or cancelled if so note remaining size
            remaining_pseudo = await self.check_remaining(ticker,side_order,id,price,reducing_order)
            if type(remaining_pseudo) == type({'hello':'yes'}):
                return remaining_pseudo
            else:
                remaining = remaining_pseudo
           
            # mantain queue within less then 4 or 3 orders 
            limit_or_cancel = await self.cancel_or_wait_limit_ord(ticker)
            if limit_or_cancel != False:
                return limit_or_cancel
            await asyncio.sleep(1)
            t1 = datetime.datetime.now()

            #get orderbook and calculate stats see new prediction to look for bid ask crosses and evaluate for new approx queue len
            #current_o_book = await self.hbot.get_orderbook(ticker)
            obook_items = await self.trader.update_and_get_info(ticker,'OBOOK')
            current_o_book = obook_items[0]
            pred = await make_pred(ticker,self.hbot,current_o_book)
            pred = await self.side_for_obook_pred(pred,(self.queue[ticker][2]),ticker,reducing_order)
            if pred == 'market':
                await self.logger.log(logging.INFO,f'executing market order for {ticker} after lob pred evaluated to kill limit order')
                ord = await self.decide_market(ticker,reducing_order,(self.queue[ticker][2]),id)
                return ord 
            
            current_trade_items = await self.trader.update_and_get_info(ticker,'TRADES')
            current_trades = current_trade_items[0]
            current_trades = current_trades['trades']
            try:

                level1, _, _ = await o_book_stats(current_o_book, price, side,reducing_order,False)
            except Exception as e:
                print(f'possible jump in order for {ticker}, error {e}')
                #print(f'Price {price} for ticker {ticker} no longer in orderbook assume fill '
                ord = await self.not_found_price(ticker,price,reducing_order,current_o_book,id,side_order)
                return ord
            await self.estimate_place_in_queue(current_trades, t1, price,reducing_order,side\
                    ,ticker,level0,level1,current_o_book,initial_obook,timer0)
            await asyncio.sleep(1)
            
            print(f'Evaluating orderbook fill for {ticker} , initital place in queue  is \
                    {total_initial_queue_len} and current_queue is {round(self.queue[ticker][5],rd)} timer of 600s ')
            
            await self.logger.log(logging.INFO,f'Evaluating orderbook fill for {ticker} , initital place in queue  is \
                    {total_initial_queue_len} and current_queue is {round(self.queue[ticker][5],rd)} ')
        
            # if time limit is exceeded check remaining if 0 return if not cancel and send remaining
            if  float(remaining) == 0 or abs(time.perf_counter() - timer0) >= 660:
                            #decide_order(self,ticker,price,reducing_order,timer0,orderFill=False)
                order = await self.decide_order(ticker,price,reducing_order,side_order,side,id)
                return order
            level0 = level1
            initial_obook = current_o_book

            total_initial_queue_len = self.queue[ticker][5]
