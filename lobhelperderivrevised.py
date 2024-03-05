from asynclog import *
from bson.binary import Binary, UUID_SUBTYPE
import uuid 
import asyncio
import lob_predictor
import time 
import datetime 
import random
from lob_exec_base import *
from wsv_handler import * 
from OrderManager import OrderManager
from QueueEstimate import QueueEstimate
import lob_helpers


class LOBhelper(make_best_trades):
    def __init__(self,hbot,logger,bal,trader):
        super().__init__(hbot,logger,bal,trader)
        self.ws_obook_positions = self.hbot.ws_obook_positions
        self.ws_market_positions = self.hbot.ws_market_positions
        self.wsAssistant_orderbook = wsAssistant(self.hbot,['v3_orderbook'],4)
        self.wsAssistant_trades = wsAssistant(self.hbot,['v3_trades'],0)
        self.wsAssistant_accounts = wsAssistant(self.hbot,['v3_account'],0)
        self.init_channels = False
        self.trades_data = {}
        self.queue_len_safety_factor = 0.05
        self.setup_queue_tickers = {}
        self.completed_orders = self.orderManager.completed_orders
        
    async def ws_assistants_init(self):
        '''
        method to initialize diff wsAssistant consumers and start each ws_feed for each channel
        '''
        if not self.init_channels:
            await self.wsAssistant_orderbook.initialize_data_instances()
            await self.wsAssistant_trades.initialize_data_instances()
            await self.wsAssistant_accounts.initialize_data_instances()
            await asyncio.sleep(3)
            self.init_channels = True

    async def ws_updates(self):
        while self.ws_obook_positions :
            try:
                for ticker in self.ws_obook_positions: #data_need = [item for item in data if item[0] == ticker ]
                    account_data = await self.wsAssistant_accounts.data_get(ticker)
                    trade_data = await self.wsAssistant_trades.data_get(ticker)
                    obook_data = await self.wsAssistant_orderbook.data_get(ticker)
                    if account_data:
                        if ticker in self.orderManager:
                            await self.orderManager._receive_order_update(
                                account_data['id'],
                                account_data['status'],
                                account_data['remainingSize'],
                                account_data['price']
                            )
                    if trade_data:
                        self.trades_data[ticker] = trade_data 
                    if obook_data:
                        self.orderbook_data[ticker] = obook_data
                        self.orderManager._update_obook_data(self.orderbook_data)
            except Exception as e:
                print(f'Error with kafka retrieval, for {ticker} , {e}')


    async def add_ticker(self, ticker, expected_price, size, side, order_type_ml, redn, rd):
        """
        Add ticker with order details to the appropriate queue.
        """
        can_add = False
        async with self.position_lock:
            new_active_tickers = list(set(self.market_buff() + [ticker]))
            can_add = len(new_active_tickers) <= 3 or redn

        if can_add:
            if order_type_ml.upper() in ('LIMIT', 'TAKE_PROFIT', 'STOP_LIMIT'):
                self.ws_obook_positions[ticker] = 'open'
                if ticker not in self.queue:
                    self.queue[ticker] = [expected_price, size, side, redn, rd]
                    msg = f"Limit order for {ticker} added to queue. Price: {expected_price}, Size: {size}."
                    print(msg)
                    await self.logger.log(logging.INFO, msg)
                    return await self.wait_on_limit_ord(ticker, expected_price, side, order_type_ml)

            else: 
                if ticker not in self.active_queue:
                    self.ws_market_positions[ticker] = 'open'
                    margin_reqt = size * expected_price / self.lev
                    fee = self.margin_fees['maker'] * expected_price * size
                    await self.update_bal_and_add_ticker(ticker, expected_price, size, side, fee, margin_reqt)
                    self.active_queue[ticker] = [expected_price, size, side, redn, rd]
                    msg = f"Order for {side} {ticker} added to active queue."
                    print(msg)
                    await self.logger.log(logging.INFO, msg)
                    return {'action':f'Order for {ticker} added to active succesfully ','price':expected_price,'ticker':ticker,\
                                    'side':side,'redn':redn}
            
        msg = f"Unsupported order type {order_type_ml} for {ticker}. Order not added, Queue fulll or not suitable "
        print(msg)
        await self.logger.log(logging.INFO, msg)
        return {'action':'Order Cancelled','status':f'Order Cancelled for {side} {ticker} with size {size} \
                    and price {expected_price} queue size exceeded','retries':0}
    
    async def remove_setup_queue_tickers(self,ticker):
        if ticker in self.setup_queue_tickers:
            msg = f'removing {ticker} from setup_queue'
            self.setup_queue_tickers = {k:v for k,v in self.setup_queue_tickers.items() if k != ticker}
        else:
            print(f'{ticker} not found in setup_queue')

    async def cancel_make_ord(self,ticker,msg=''):
        """
        cancel paper trade only
        """
        side = self.queue[ticker][2]
        size = self.queue[ticker][1]
        price = self.queue[ticker][0]
        msg = f'Order Cancelled for {self.queue[ticker][2]} {ticker} with size {self.queue[ticker][1]} and price {price} , {msg}'
        print(msg)
        await self.logger.log(logging.INFO,msg)
        await self.remove_ticker(ticker,'limit')
        await self.remove_setup_queue_tickers(ticker)
        await super().print_queue_active_queues()
        return {'action':f'Order Cancelled for {side} {ticker} with size {size} and price \
                {price} '}

    async def cancel_ord(self,market,id):
        """
        cancel order and close posn
        """
        side = self.queue[market][2]
        try:
            ord = await self.hbot.cancel_order(id=id)
            cord = await self.cancel_make_ord(market)
            return cord
        except Exception as e:
            msg = (f'Error canceling order for {side} {market}, order\
                  with id {id}, order is probably no longer active so has already been cancelled, {e}')
            print(msg)
            self.logger.log(logging.ERROR,msg)
            return False 
        
    async def _break_lob_loop_check(self,timer0,ticker,remaining=None):
        """
        check if time is elapsed or lob queue has been filled
        """
        if not self.paper_trade:
            return (float(remaining) == 0 or await lob_helpers._elapsed_time(timer0))
        return (self.queue[ticker][5].queue_len <=0 or await lob_helpers._elapsed_time(timer0))
    
    async def _eval_jump_in_obook(self,id,ticker,obook,price,reducing_order,side_order,timer0):
        """
        check lob for jumps in price levels
        """
        side = self.queue[ticker][2]
        try:
            level0, _, queue_len = await lob_helpers.o_book_stats(obook, price, side,reducing_order,False)
            return {'level0':level0,'queue_len':queue_len}
        except Exception as e:
            print(f'possible jump in order for {ticker} , error: {e}')
            ord = await self.not_found_price(ticker,price,timer0,reducing_order,obook,side_order,id)
            return ord 
    
    async def wait_on_limit_ord(self, ticker, price, side, order_type_ml):
        """
        Wait on Orderbook for ticker to fill. Uses different logic for paper trade and live trade.
        """
        await self._initialization_of_ws()
        timer0, side_order, redn, rd = await self._initialize_order(ticker, price, side, order_type_ml)
        jump_hash = await self._init_obook_evaluation(ticker, side_order, price, redn, timer0)
        if 'level0' not in jump_hash :
            return jump_hash  
        level0, initial_queue_len, remaining, initial_obook = jump_hash['level0'], jump_hash['queue_len'], jump_hash['remaining'], jump_hash['initial_obook']
        total_initial_queue_len = await self._setup_queue_estimate(ticker, initial_queue_len)

        while ticker in self.queue:

            limit_or_cancel = await self.cancel_or_wait_limit_ord(ticker)
            if limit_or_cancel is not False:
                return limit_or_cancel
            
            jump_hash = await self._fetch_current_data(ticker, initial_obook, price, redn, side_order, timer0)
            if not level0 and isinstance(jump_hash, dict):
                return jump_hash  
            level1, current_o_book, remaining, current_trades = jump_hash['level0'], jump_hash['current_o_book'], jump_hash['remaining'], jump_hash['current_trades']

            if await self._should_market_execute(ticker,current_o_book,redn):
                await self.logger.log(logging.INFO, f'Executing market order for {ticker} based on LOB prediction')
                market_order_result = await self.decide_market(ticker, redn, side, id)
                return market_order_result
            
            await self.queue[ticker][5].estimate_place_in_queue(current_trades, timer0, price,redn,side\
                    ,ticker,level0,level1,current_o_book,initial_obook)
            
            msg = f'Evaluating orderbook fill for {ticker} , initital place in queue  is \
                    {total_initial_queue_len} and current_queue is {round(self.queue[ticker][5].queue_len,rd)} timer of 600s '
            print(msg)
            await self.logger.log(logging.INFO,msg)
            await self._evaluate_order_fill(ticker,id,timer0,price,redn,side_order,remaining)

            level0 = level1
            initial_obook = current_o_book
            total_initial_queue_len = self.queue[ticker][5].queue_len

    async def _initialization_of_ws(self):
        """
        Check if the order can be added based on the type and current queue state
        """
        if not self.init_channels:
            await self.ws_assistants_init()
            asyncio.create_task(self.ws_updates()) 

    async def _initialize_order(self, ticker, price, side, order_type_ml):
        """
        Initialize and place the order, return necessary details for further processing
        """
        timer0 = time.perf_counter()
        side_order = lob_helpers.side_helper_f(side, self.queue[ticker][3], True)
        redn = self.queue[ticker][3]
        size = self.queue[ticker][1]
        rd = self.queue[ticker][4]

        if not self.paper_trade:
            order = await self.hbot.place_order(ticker, side_order, size, price, order_type_ml, False, redn)
            await asyncio.sleep(0.3)
            id = order['order']['id']
            status = order['order']['status']
            asyncio.create_task(self.orderManager._start_order_monitoring(ticker, side_order, size, price, id, status, redn, timer0, order_type_ml))

        return timer0, side_order, redn, rd

    async def _init_obook_evaluation(self, ticker, side_order, price, reducing_order, timer0):
        """
        Evaluate the initial order book and return relevant data
        """
        initial_obook = self.orderbook_data[ticker]
        jump_hash = await self._eval_jump_in_obook(id,ticker,initial_obook,price,reducing_order,side_order,timer0)
        if not 'level0' in jump_hash and isinstance(jump_hash,dict):
            return jump_hash
        remaining = None if self.paper_trade else jump_hash
        jump_hash2 = {'remaining':remaining,'initial_obook':initial_obook}
        jump_hash.update(jump_hash2)
        return jump_hash

    async def _setup_queue_estimate(self, ticker, initial_queue_len):
        """
        Set up the queue estimate for the order
        """
        
        total_initial_queue_len = initial_queue_len * (self.queue_len_safety_factor) + self.queue[ticker][1]
        queue_estimate = QueueEstimate(self.logger, total_initial_queue_len)
        self.queue[ticker] += [queue_estimate]
        self.setup_queue_tickers[ticker] = True 
        await self.logger.log(logging.INFO, f"Queue estimate added for {ticker}")
        return total_initial_queue_len
    
    async def _fetch_current_data(self, ticker, current_o_book, price, reducing_order, side_order, timer0):
        """
        Fetches the current order book and trade data.
        """
        current_o_book = self.orderbook_data[ticker]
        current_trades = self.trades_data[ticker]
        jump_hash = await self._eval_jump_in_obook(id,ticker,current_o_book,price,reducing_order,side_order,timer0)
        if not 'level0' in jump_hash and isinstance(jump_hash,dict):
            return jump_hash
        remaining = None if self.paper_trade else jump_hash
        jump_hash2 = {'remaining':remaining,'current_trades':current_trades,'current_o_book':current_o_book}
        jump_hash.update(jump_hash2)
        return jump_hash

    async def _evaluate_order_fill(self, ticker,id,timer0,price,redn,side_order,remaining):
        """
        Evaluates if the order should be filled based on current order book state.
        """ 
        if await self._break_lob_loop_check(timer0,ticker,remaining):
            order = await self._finalize_order_decision(ticker, price, redn, side_order,id, timer0)
            return order

    async def _should_market_execute(self,ticker,current_o_book,reducing_order):
        pred = await lob_predictor.make_pred(ticker, self.hbot, current_o_book)
        pred = await self.side_for_obook_pred(pred, self.queue[ticker][2], ticker, reducing_order)
        return pred == 'market'
    
    async def _fetch_current_data(self, ticker, initial_obook, price, reducing_order, side_order, timer0):
        """
        Fetches the current order book and trade data.
        """
        side = self.queue[ticker][2]
        current_o_book = self.orderbook_data[ticker]
        current_trades = self.trades_data[ticker]
        jump_hash = await self._eval_jump_in_obook(ticker, initial_obook, price, side, reducing_order, side_order, timer0)
        level1 = jump_hash.get('level0') if 'level0' in jump_hash else None
        return current_o_book, current_trades, jump_hash, level1

    async def check_remaining(self, ticker, side_order, id, price, redn):
        """
        check remaining size for order
        """
        async def is_retry_event_set(id):
            if id in self.orderManager.retry_order_events:
                return self.orderManager.retry_order_events[id].is_set()
            return False 
        
        if ticker in self.orderManager.retry_tickers or await is_retry_event_set(id): 
            await self.orderManager.orders[id]['task']
            msg = (f'Task for {side_order} {ticker} with {id} done')
            self.logger.log(logging.INFO,msg)
            print(msg)
            await asyncio.sleep(1.5)

        # Check if the order is currently being processed
        if id in self.orderManager.orders:
            t0 = self.orderManager.orders[id]['t0'] 
            elapsed_time = await lob_helpers._elapsed_time(t0)
            print(f'order of {id} for {side_order} {ticker} still being processed')
            await asyncio.sleep(1.5)
            if elapsed_time < 601:
                return self.orderManager.orders[id]['remainingSize']
            await asyncio.sleep(1.5)

        # Refresh order info from either completed or expired orders
        if id in self.orderManager.completed_orders:
            await self.remove_setup_queue_tickers(ticker)
            ord_f = self.remove_order(ticker, redn, price)
            return ord_f
        
        if not redn and id in self.orderManager.expired_orders:
            msg = f"{ticker} order {id} was not successfully closed, fill was 0"
            self.logger.log(logging.INFO,msg)
            print(msg)
            c_ord = await self.cancel_ord(ticker,id)
            return c_ord
        
    async def _finalize_order_decision(self, ticker, price, redn, side_order,id, timer0):
        """
        Makes the final decision on the order based on the evaluation.
        """
        # check if order filled 1)if not redn add to active and remove from queue, 2) if redn remove from active 3)order expired cancel
        if not self.paper_trade:
            remaining = await self.check_remaining(ticker,side_order,id,price,redn)
            return remaining
        #paper trade logic
        if ticker not in self.setup_queue_tickers: #this means loop didnt start and possible jump because no setup_queu tickers
            if self.queue[ticker][5] <= 0 :
                order = await self.remove_order(ticker,redn,price,msg='')
                return order

        if (time.perf_counter() - timer0) >= 600  : 
            # this is fine because OrderManager only works for non paper trades so paper trades can bec canceled even if redn
            msg = f"{ticker} order {id} was not successfully closed"
            c_ord = await self.cancel_make_ord(ticker,msg)
            return c_ord
    
    async def decide_market(self, ticker, redn, side, id):
        """
        Decides to execute a market order based on the prediction.
        """
        size = self.queue[ticker][1]
        rd = self.queue[ticker][4]
        obook = self.orderbook_data[ticker]
        bid = float(obook['bids'][0]['price'])
        ask = float(obook['asks'][0]['price'])
        price = await lob_helpers._get_best_price_helper(bid, ask, redn, side)

        if not self.paper_trade:
            remaining = await self.check_remaining(ticker, side, id, price, redn)
            if isinstance(remaining, dict):
                return remaining
            if remaining and  0 < remaining <= size:
                try:
                    cord = await self.cancel_ord(ticker, id)  # real cancel
                except Exception as e:
                    print(f'Order {id} already canceled and not found , {e}')
                mord = await self.force_market_order(ticker, side, remaining, price, redn, side, rd)
                if redn:
                    price = mord['price_closed']
                else:
                    price = mord['price']
            
        return await self.remove_order(ticker, redn, price, msg='Converting to market order')

    async def not_found_price(self, ticker, price, timer0, redn, initial_obook, side_order, id):
        """
        Handles the scenario when the limit order price is no longer present in the order book.
        """
        side = self.queue[ticker][2]
        size = self.queue[ticker][1]
        if not redn:
            new_side = lob_helpers.side_helper_f(side, True, False)
        else:
            new_side = lob_helpers.side_helper_f(side,False,False)
        opp_obook = lob_helpers.order_obook(new_side, initial_obook)

        if self.paper_trade:
            if price in opp_obook:
                msg = f'Price {price} for ticker {ticker} has been filled '
                print(msg)
                self.logger.log(logging.INFO,msg)
                if ticker not in self.setup_queue_tickers:
                    self.queue[ticker] += [0]
                ord_f = await self._finalize_order_decision(ticker, price, redn, side_order,id, timer0)
                return ord_f
            cord = await self.cancel_make_ord(ticker)
            return cord
            
        # not paper trade logic
        remaining = await self.check_remaining(ticker, side_order, id, price, redn)
        if isinstance(remaining,dict): # order filled or expired
            return remaining 
        if remaining and not redn and remaining == size: # not filled if time not expired but orderbook jumped 
            #over 10 levels cancel to not waste time and move onto next ticker in analysis
            cord = await self.cancel_ord(ticker,id)
            return cord 
        if remaining and 0 < float(remaining) < size: ## this cant happen because if something is partially filled
            # it will automatically be taken care of in OrderManager
            msg = (f'Order for {side} {ticker} partially filled awaiting task to finish for {id}')
            print(msg)
            await self.logger.log(logging.INFO,msg)
            await self.orderManager[id]['task']
            return  await self.check_remaining(ticker, side_order, id, price, redn)



