import lob_helpers 
from OrderManager import OrderManager
import asyncio 
class LOBhelper:
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