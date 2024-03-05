import asyncio 
from asynclog import * 
from lob_helpers import * 

class QueueEstimate:
    '''Class Estimates place in queue based on order imbaalnce, change in depth and levels in orderbook and executed trades in orderbook'''
    '''Estimates add a fudget factor of 5% plus order'''
    def __init__(self,logger,initial_queue_len):
        self.logger = logger
        self.queue_len = initial_queue_len 
        
    async def estimate_place_in_queue(self,trades, t1, price_level_of_interest,redn,side,ticker,level0,level1,obook1,obook0):
        '''
        look at changes in len of orderbook and trades before a specified time to measure trade orderflow imbalance 
        
        '''
        #estimates place in queue by looking at 1) trades removed from orderbook on appropriate side 2) change in levels ahead of price and 3)change at price_level
        #this gives a rough estimate of when queue should work 

        side_n = side_helper_f(side,redn,False)

        size_of_price_levelt1 = [float(el['size']) for i,el in enumerate(obook1[side_n]) if float(el['price']) == price_level_of_interest ]
        size_of_price_levelt0= [float(el['size']) for i,el in enumerate(obook0[side_n]) if float(el['price']) == price_level_of_interest ]

        chg_size_at_level = (size_of_price_levelt1[0] - size_of_price_levelt0[0])
        if chg_size_at_level  < 0:
            new_orders_executed_or_cancelled = chg_size_at_level
            print(f'size inside level has changed by {new_orders_executed_or_cancelled} for {ticker}')
        else:
            new_orders_executed_or_cancelled = 0
        #changes in levels
        chg_in_queue = level1 - level0
        chg_q_len = 0
        if chg_in_queue > 0:
            #price has shifted down and you need to add more levels
            print(f'queue length has shifted down by {chg_in_queue} levels for {ticker} at {price_level_of_interest}')
            await self.logger.log(logging.INFO,f'queue length has shifted down by {chg_in_queue} levels for {ticker} at {price_level_of_interest}')

            for i in range(chg_in_queue):
                chg_q_len += float(obook1[side][i]['size'])
        if chg_in_queue < 0:
            # price has moved up and you need to subtract levels
            #chg_q_len -= float(
            print(f'queue length has shifted up by {chg_in_queue} levels for {ticker} at {price_level_of_interest}')
            await self.logger.log(logging.INFO,f'queue length has shifted up by {chg_in_queue} levels for {ticker} at {price_level_of_interest}')

            for i in range(abs(chg_in_queue)):
                chg_q_len -= float(obook0[side][i]['size'])
        
        await self.logger.log(logging.INFO,f' estimating place in queue for {ticker} at price_level {price_level_of_interest}')
        print(f' estimating place in queue for {ticker} at price_level {price_level_of_interest}')
        # Count the number of orders removed from both buy and sell sides at the price level of interest between snapshots
        order_removed = 0 
        side = side_helper_f(side,redn,True)


        for trade in trades:
            if parse_trade_time(trade['createdAt']) >= t1:
                if float(trade['price']) <= price_level_of_interest:
                    if trade['side'] == side:
                        order_removed += float(trade['size'])

        chg_q = chg_q_len - order_removed + new_orders_executed_or_cancelled

        if chg_q > 0:
            #growth scenario
            print(f'queue length has increased by {chg_q} for {ticker} at {price_level_of_interest}')
            await self.logger.log(logging.INFO,f'queue length has increased by {chg_q} for {ticker} at {price_level_of_interest}')
            self.queue_len += chg_q
        
        if chg_q < 0:
            #decline scenario 
            print(f'queue length has decreased by {chg_q} for {ticker} at {price_level_of_interest}')
            await self.logger.log(logging.INFO,f'queue length has decreased by {chg_q} for {ticker} at {price_level_of_interest}')
            self.queue_len += chg_q 
            

        if chg_q == 0:
            #original size being simulated has been executed at beginning of queue not totally 
            print(f'queue length is static by  for {ticker} at {price_level_of_interest}')
            await self.logger.log(logging.INFO,f'queue length is static for {ticker} at {price_level_of_interest}')
                                   #ticker,price,reducing_order,initial_obook)