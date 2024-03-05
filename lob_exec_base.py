from hbot51 import *
from sklearn.preprocessing import MinMaxScaler
import dill 
import xgboost 
from asynclog import * 
import random 
import time 
import datetime 
import builtins
import uuid 
from bson.binary import Binary, UUID_SUBTYPE
import lob_predictor
import asyncio
import lob_helpers
from abc import ABC, abstractmethod
from OrderManager import OrderManager

class make_best_trades(ABC):
    """
    Class for Managing and simulating Orderbook fills so that 
    the trading algorithm can better approximate real live trading
    """
    def __init__(self,hbot,logger,bal,trader):
        self.hbot = hbot
        self.orderManager = OrderManager(hbot)
        self.logger = logger
        self.bal = bal 
        self.trader = trader 
        self.max_slippage = 0.2/100
        self.queue = {}
        self.active_queue = {}
        self.logger = logger
        self.long_hash_orders = {2:'market',1:'limit',0:'limit'}
        self.short_hash_orders = {2:'limit',1:'market',0:'limit'}
        self.trades = {}
        self.margin_fees = {'maker':0.02/100,'taker':0.05/100}
        self.positions = {}
        self.lev = self.trader.lev
        self.stl_a = self.trader.stl_a
        self.position_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(9)
        self.queue_ws_channels = ['v3_trades','v3_accounts','v3_orderbook']
        self.active_queue_ws_channel = 'v3_markets'
        self.orderbook_data = {}
        self.paper_trade = True 
        
    @abstractmethod
    async def cancel_make_ord(self,ticker,msg):
        pass 

    @abstractmethod
    async def cancel_ord(self,market,id):
        pass 

    @abstractmethod
    async def check_remaining(self, ticker, side_order, id, price, redn):
        pass 

    @abstractmethod
    async def decide_market(self,ticker,redn,side,id):
        pass 

    @abstractmethod
    async def not_found_price(self,ticker,price,timer0,redn,initial_obook,side_order,id):
        pass 

    @abstractmethod
    async def add_ticker(self,ticker,expected_price,size,side,order_type_ml,redn,rd):
        pass 

    @abstractmethod
    async def wait_on_limit_ord(self,ticker,price,side,order_type_ml):
        pass

    async def current_value(self):
        value_tot = self.bal
        if len(self.positions.keys()) > 0:
            for ticker,v in self.positions.items():
                side = v[2]
                price_entry = v[0]
                size = v[1]
                margin = v[3]
                price_ticker = self.trader.positions[ticker].Close.iloc[-1]
                p_l, _ = await lob_helpers.calc_pl_w_fee(price_entry,price_ticker,size,side,(self.margin_fees))
                value_tot += (p_l + margin)
            return value_tot
        return value_tot
    
    async def print_queue_active_queues(self):
        await self.logger.log(logging.INFO,f' queue looks like : {self.queue.keys()}')
        await self.logger.log(logging.INFO,f'active queue is : {self.active_queue.keys()}')
        print(f' queue looks like : {self.queue.keys()}')
        print(f'active queue is : {self.active_queue.keys()}')

    async def update_bal_and_add_ticker(self,ticker,price_entry,size,side,fee,margin):
        if side.upper() == 'LONG':
            self.bal -= (fee + margin)
            self.positions[ticker] = [price_entry,size,side,margin]
        elif side.upper() == 'SHORT':
            self.bal -= (fee + margin)
            self.positions[ticker] = [price_entry,size,side,margin]

    async def side_for_obook_pred(self,pred,side,ticker,redn):
        if not redn:
            if side.upper() == 'SHORT':
                return self.short_hash_orders[pred]
            elif side.upper() == 'LONG':
                return self.long_hash_orders[pred]
        elif redn:
            if side.upper() == 'SHORT':
                return self.long_hash_orders[pred]
            elif side.upper() == 'LONG':
                return self.short_hash_orders[pred]
        print(f'Error in prediction for {side} {ticker}')
        await self.logger.log(logging.ERROR,f'Error in prediction for {side} {ticker}')
    
    def market_buff(self):
        all = list(self.queue.keys()) + list(self.active_queue.keys())
        return list(set(all))
    
    def count(self):
        h = self.market_buff()
        return len(h)
    
    async def trade_f(self,ticker,side,price_entry,price_exit,size):
        margin = self.positions[ticker][3]
        p_l, fee = await lob_helpers.calc_pl_w_fee(price_entry,price_exit,size,side,(self.margin_fees))
        self.bal += (p_l + margin - fee)
        id = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
        self.trades[id] = [price_entry,price_exit, size, side, p_l,ticker,self.bal]
        self.positions = {k:v for k,v in self.positions.items() if k != ticker}
        await lob_helpers.tickers_to_df(self.trades)
    
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
    
    async def remove_order(self,ticker,redn,price,msg=''):
        #switch prices in case of change
        if not redn:
            sidef = self.queue[ticker][2]
            await self.add_ticker(ticker,price,(self.queue[ticker][1]),(self.queue[ticker][2]),'active',redn,self.queue[ticker][4])
            await self.logger.log(logging.INFO,f'action order added to active queue succesfully  {ticker} at {price}\
                                   and order size {self.queue[ticker][1]}')
            await self.remove_ticker(ticker,'limit')

        
            return {'action':'order added to active succesfully ','price':price,'ticker':ticker,'side':sidef,'redn':redn}
        else:
            
            try:
                sidef = self.queue[ticker][2] 
                await self.logger.log(logging.INFO,f'action order removed from active and limit queue succesfully for {ticker} at\
                                       {price} and order size {self.queue[ticker][1]}')
                msg =  {'action':'order reduced successfully','price_closed':price,'ticker':ticker,'side':sidef,'redn':redn}
                await self.remove_ticker(ticker,'active')
                await self.print_queue_active_queues()
                return msg 
            except Exception as e:
                print(f'Failure to remove ticker {ticker} from active queue and close order, error is : {e} ')
                await self.logger.log(logging.ERROR,f'Failure to remove ticker {ticker} from active queue and close order, error is : {e} ')
    
    
    async def force_market_order(self,ticker,side,size,price_want,redn,side_order,rd):
        '''this fn takes care of all market orders using OrderManager Alone'''
        obook = self.orderbook_data[ticker]
        timer0 = time.perf_counter()
        order_type_ml = 'MARKET'
        best_bid = float(obook['bids'][0]['price'])
        best_ask = float(obook['asks'][0]['price'])
        if redn:
            if side.upper() == 'SHORT':
                price_want = best_bid
            if side.upper() == 'LONG':
                price_want = best_ask
        elif not redn:
            if side.upper() == 'SHORT':
                price_want = best_ask
            elif side.upper() == 'LONG':
                price_want = best_bid
        
        try:
            order = await self.hbot.place_order(ticker,side_order,size,price_want,order_type_ml,False,redn)
            await asyncio.sleep(1.5)
            id = order['order']['id']
            status = order['order']['status']
            if status != 'FILLED':
                task =  asyncio.create_task(self.orderManager._start_order_monitoring(ticker,side_order,size,price_want,id,\
                    status,redn,timer0,order_type_ml))
                await self.orderManager.orders[id]['task']
                price = round(self.orderManager._calculate_average_price(id,(self.orderManager.completed_orders)),rd)
                if redn:
                    key_price = 'price_closed'
                else:
                    key_price = 'price'
                if id in self.orderManager.completed_orders:
                
                    return {'action':f' Market Order for {ticker} added to active succesfully '\
                            ,key_price:price,'ticker':ticker,\
                                    'side':side,'redn':redn}
        except Exception as e:
            print(f"Exception while making order: {e}")
            await self.logger.log(logging.ERROR, f"Exception while making order for {ticker}: {e}")

    async def order_message(self,order_type,ticker,total_slippage_loss,max_allowable_loss_for_slippage,price_want,redn):
        msg = f'{order_type} order for {redn} for ticker {ticker}, has a estimated total_loss {total_slippage_loss} greater then max slippage\
                            allowed of {max_allowable_loss_for_slippage} so will make limit order or lob predictor\
                            predicted limit order or wait at {price_want}'
        print(msg)
        await self.logger.log(logging.INFO,msg)
                        
    async def _determine_order_price_type(self,pred, redn, best_bid, best_ask,ticker):
        if redn:
            side = self.active_queue[ticker][2]
            if side.upper() == 'SHORT':
                    price_want = best_bid
                    o_type = self.long_hash_orders[pred]
            if side.upper() == 'LONG':
                    o_type = self.short_hash_orders[pred]
                    price_want = best_ask
            return price_want, o_type 
        else:
            if side.upper() == 'SHORT':
                price_want = best_ask 
                o_type = self.short_hash_orders[pred]
            if side.upper() == 'LONG':
                price_want = best_bid 
                o_type = self.long_hash_orders[pred]
            return price_want, o_type 
    
    async def cancel_bc_slippage(self,ticker,total_slippage_loss,max_allowable_loss_for_slippage,price_want):
        msg = f'Order Cancelled, Estimated Total Slippage loss for {ticker} was : {total_slippage_loss} \
        at a execution_price of {price_want}, maximum_allowable_slippage_loss was : {max_allowable_loss_for_slippage}'
        await self.logger.log(logging.INFO,msg)
        return {'action':'Order Cancelled','status':msg}
        
    async def order_eval(self, side, pred, ticker, best_bid, best_ask, size, redn, total_slippage_loss,
                         max_allowable_loss_for_slippage, obook, rd):
        """
        Evaluate orders based on Predicted order type, Slippage, and if order needs to be reduced.
        """
        price_want, o_type = self._determine_order_price_type(side, pred, redn, best_bid, best_ask)
        await self.order_message(o_type,ticker,total_slippage_loss,max_allowable_loss_for_slippage,price_want,redn)
        if redn and total_slippage_loss <= max_allowable_loss_for_slippage:
            return await self._evaluate_reducing_order(ticker, price_want, size, rd, o_type,obook,pred,redn,best_bid,best_ask)
        if not redn and total_slippage_loss <= max_allowable_loss_for_slippage:
            return await self._evaluate_non_reducing_order(ticker, side, pred, price_want, size, rd, obook,redn)
        if total_slippage_loss >= max_allowable_loss_for_slippage:
            return await self.cancel_bc_slippage(ticker,total_slippage_loss,max_allowable_loss_for_slippage,price_want)

    async def _evaluate_reducing_order(self, ticker, price_want, size, rd, o_type,obook,pred,redn,best_bid,best_ask):
        side = self.active_queue[ticker][2]
        price_i = float(self.active_queue[ticker][0])
        price_want, o_type = self._determine_order_price_type(pred, redn, best_bid, best_ask,ticker)
        p_l,_ = lob_helpers.calc_pl_w_fee(price_i,price_want,size,side,(self.margin_fees))
        if -3 < p_l < 0:
            o_type = 'STOP_LIMIT'
        elif 0 > p_l > 10:
            o_type = 'TAKE_PROFIT'
        else:
            o_type = 'MARKET'     
        if o_type == 'MARKET':
            return await self._process_market_order(ticker, side,o_type, price_want,size, rd, redn)
        if o_type == 'STOP_LIMIT':
            return await self._process_limit_order(ticker, size, price_want, p_l, rd, obook, redn )
        if o_type == 'TAKE_PROFIT':
            return await self._process_limit_order(ticker, price_want,size,side,o_type, rd, redn)

    async def _evaluate_non_reducing_order(self, ticker, side, pred, price_want, size, rd, obook,redn):
        best_bid = float(obook['bids'][1]['price'])
        best_ask = float(obook['asks'][1]['price'])
        o_type = self. _determine_order_price_type(pred, redn, best_bid, best_ask,ticker)
        if o_type != 'MARKET':
            return await self._process_limit_order(ticker,price_want,size,side,o_type,rd,redn)
        return await self._process_market_order(ticker, side, o_type, price_want, size, rd,redn)

    async def _process_market_order(self,ticker, side, o_type, price_want, size, rd,redn):
        side_order = lob_helpers.side_helper_f(side,redn,True)
        if not redn:
            if self.paper_trade:
                add = await self.add_ticker(ticker,price_want,size,side,o_type,redn,rd)
                return add 
            mord = await self.force_market_order(ticker,side,size,price_want,redn,side_order,rd)
            price_executed = mord['price'] 
            add = await self.add_ticker(ticker,price_executed,size,side,o_type,redn,rd)
            return add 
        if redn:
            if self.paper_trade:
                rord = await self.remove_order(ticker,redn,price_want)
                return rord 
            mord = await self.force_market_order(ticker,side,size,price_want,redn,side_order,rd)
            price_executed = mord['price_closed']
            rord = await self.remove_order(ticker,redn,price_executed)
            return rord 
            
    async def _process_limit_order(self,ticker,price_want,size,side,o_type,rd,redn):
       # should go through wait_on lim ord  always since take_profit ,stop_limit, and limit are all lim orders
        order = await self.add_ticker(ticker,price_want,size,side,o_type,redn,rd) 
        return order 
        
    async def make_order_retry(self, ticker, side, size, pr_want, redn, rd, stl, tar, retry=10):
        # Check if the order already meets the stop loss or target
        if not redn  and pr_want in (stl, tar):
            await self.logger.log(logging.ERROR, f'{side} {ticker} at price {pr_want} is already at stl {stl} or target {tar}, cancelling order')
            return {'action': 'Order Cancelled', 'status': f'{side} {ticker} at price {pr_want} is already at stl {stl} or target {tar}, cancelling order'}

        retries = 0
        while retries < retry:
            try:
                order = await self.make_order(ticker,side,size,pr_want,redn,rd)
                print(f'Order for {ticker}: {order}')
                await self.logger.log(logging.INFO, f'Order for {ticker}: {order}')

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


    async def make_order(self,ticker,side,size,price_original,redn,rd):
        '''
        Router for make Order Class
        '''
        o_book= await self.hbot.get_orderbook(ticker)
        best_bid = float(o_book['bids'][0]['price'])
        best_ask = float(o_book['asks'][0]['price'])
        await self.logger.log(logging.INFO,f'Calculating Max Slippage and estimated_slippage for ticker {ticker} ')

        #look at slippages
        _, slippage = lob_helpers.estimate_slippage(o_book, size, side,redn,rd)
        incoming_slippage_loss = round(size*(lob_helpers.incoming_slippage(best_bid,best_ask,price_original,side,redn)),rd)
        price_original2 = lob_helpers.side_helper(side,best_bid,best_ask,redn)
        order_slippage_loss = round(lob_helpers.estimate_loss_to_slippage(slippage,side,price_original2,size,redn),rd)

        total_slippage_loss = incoming_slippage_loss + order_slippage_loss
        max_allowable_loss_for_slippage = round((lob_helpers.estimate_loss_to_slippage(self.max_slippage,side,price_original,size,redn)),rd)

        #Orderbook Predictions
        pred = await lob_predictor.make_pred(ticker,self.hbot,o_book)
        print(f'Orderbook predictor has predicted the following movement: {res_hash[pred]} for ticker {ticker}')
        await self.logger.log(logging.INFO,f'Orderbook predictor has predicted the following movement: {res_hash[pred]} for ticker {ticker}')
       
        ord = await self.order_eval(side, pred, ticker, best_bid, best_ask, size, redn, total_slippage_loss,
                         max_allowable_loss_for_slippage, o_book, rd)
        return ord 

    async def remove_ticker(self,ticker,queue_type,price=None):
        if len(list(self.queue.keys())) > 0 or len(list(self.active_queue.keys())) > 0:
            try:
                if queue_type != 'active':
                    self.queue = {k:v for k,v in self.queue.items() if k != ticker}
                else:
                    side = self.active_queue[ticker][2]
                    price_entry = self.active_queue[ticker][0]
                    size = self.active_queue[ticker][1]
                    if price is not None: #market order close
                        price_exit = price
                    else:#limit order close
                        price_exit = self.queue[ticker][0]
                        self.queue = {k:v for k,v in self.queue.items() if k != ticker}
                    await self.trade_f(ticker,side,price_entry,price_exit,size)
                    self.active_queue = {k:v for k,v in self.active_queue.items() if k != ticker}
            except Exception as e:
                print(f'Error removing ticker from trader helper for {ticker} with order type {queue_type}, error {e}')
                await self.logger.log(logging.ERROR,f'Error removing ticker from trader helper for {ticker} with order type {queue_type}, error {e}')
 


if __name__ == '__main__':
    ETHEREUM_ADDRESS = os.getenv("eth_add")
    pkey = os.getenv("pkey")
    DYDX_API_CREDENTIALS = {
        "key": os.environ.get("ek", ""),
        "secret": os.environ.get("es", ""),
        "passphrase": os.environ.get("epp", ""),
    }
    hstk = os.getenv("eck")
    hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'],DYDX_API_CREDENTIALS['secret'],DYDX_API_CREDENTIALS['passphrase'],'0',hstk,ETHEREUM_ADDRESS)
   
