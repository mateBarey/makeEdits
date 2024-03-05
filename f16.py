import time
import os
from asyncio import gather, get_event_loop
from dydx3 import Client
from dydx3.constants import API_HOST_GOERLI, API_HOST_MAINNET
from dydx3.constants import NETWORK_ID_GOERLI
from dydx3.constants import ORDER_SIDE_BUY
from dydx3.constants import ORDER_STATUS_OPEN
from dydx3.constants import ORDER_SIDE_SELL
from dydx3.constants import ORDER_TYPE_LIMIT
from dydx3.constants import TIME_IN_FORCE_GTT
from dydx3.constants import MARKET_BTC_USD
from bson.binary import Binary, UUID_SUBTYPE
from web3 import Web3
import pandas as pd 
from pprint import pprint
import talib as ta
import asyncio 
import numpy as np 
import datetime as dt 
from eval_trade import * 
import random 
from stack import * 
import httpx 
import uuid 
from risk_adj import * 
from savepymongo import write_df_to_mongoDB
from asynclog import *
from make_data_req import * 
import warnings

import lobhelperderivrevised
from hbot51 import *
import builtins
from lob_helpers import * 
from wsv_handler import wsAssistant

def print(*args, **kwargs):
    kwargs["flush"] = True
    builtins.print(*args, **kwargs)

# Ignore all warnings
warnings.filterwarnings('ignore')

eth_add2 = '0x14D7487c48DEE890396Bd66630fFd75Ba2A39194'
pkey2 = os.getenv("pkey2")
client = Client(host='https://api.dydx.exchange')
DYDX_API_CREDENTIALS = {
    "key": os.environ.get("ek2", ""),
    "secret": os.environ.get("es2", ""),
    "passphrase": os.environ.get("epp2", ""),
}
hstk = os.getenv("eck2")

#initalize needed vars
lock = asyncio.Lock()
hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'],DYDX_API_CREDENTIALS['secret'],DYDX_API_CREDENTIALS['passphrase'],'0',hstk,eth_add2,None)


#initialized_agents_hash
#files of df crypto params
#drv_ = r'C:\Users\grc\Downloads\tickers_train'
#files_ = os.listdir(drv_)
#tickers_ = [i.split('-')[0] for i in files_ ]
#init_agent_bools = [False for _ in tickers_]
#initialized_agents_hash = dict(zip(tickers_,init_agent_bools))

async def get_decision(candles,ticker):
    #global initialized_agents_hash
    '''
    data: is the dataframe with the ohlc klines candlesticks from hbot
    '''
    async with lock:
        retry = 0
        retries = 3
        while retry < retries:
            try:
                if not initialized_agents_hash.get(ticker):
                    init_resps = [get_resp((get_data(i,candles,ticker)),'POST','evaluate_trade') for i in range(19)]
                    await gather(*init_resps)
                    initialized_agents_hash[ticker] = True
                    order = await get_resp((get_data(-1,candles,ticker)),'POST','evaluate_trade')
                    print(order)
                    return order

                else:
                    order = await get_resp((get_data(-1,candles,ticker)),'POST','evaluate_trade')
                    print(order)
                    return order
            except Exception as e:
                print(f'Error with evaluatiing {ticker} with eval_data fn, {e}')
                retry += 1
        return {'status_code':911}
#instructions = initialize_VPN(area_input=['Mexico','Netherlands','Germany']) 
def filt_(df):
    #msg = 'this is df that will be filtered'
    #print(msg)
    df['Wins'] = df['Wins'].astype(str)
    df['Loss'] = df['Loss'].astype(str)
    new_df = df[(df.Wins != 'NaN') &  (df.Loss != 'NaN')]
    zdf = pd.DataFrame(new_df)
    
    return zdf

def filt_1(df):

    df = df.fillna(0)
    new_df = df[~((df.Wins == 0) &  (df.Loss == 0)) ]

    zdf = pd.DataFrame(new_df)
    return zdf

#trade logic function
def trade_(symb,j,typee,price,t_int,qt,p_l,action,strat):
    global df_records
    if typee == 'LONG':
        if action == 'ENTER':
            df_records.loc[j,'Symbol'] = symb
            df_records.loc[j,'price_entry'] = price
            df_records.loc[j,'type'] = typee
            df_records.loc[j,'t_int'] = t_int
            df_records.loc[j,'coins'] = qt
            df_records.loc[j,'strategy'] = strat
            time_ = get_time()
            df_records.loc[j,'time_entry'] = time_
            df_records.loc[j,'uuid'] = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            print(df_records)
       
            return df_records
        else:
            df_records.loc[j,'price_exit'] = price
            df_records.loc[j,'type'] = typee
            time_ = get_time()
            df_records.loc[j,'time_exit'] = time_ 
            if p_l < 0:
                df_records.loc[j,'Loss'] = p_l
                print(df_records)
                try:
                    write_df_to_mongoDB(df_records,'local','recs')
                except Exception as e:
                    print(f"Error writing Pymongo db {e}")

              
                return df_records
            else:
                df_records.loc[j,'Wins'] = p_l
                try:
                    write_df_to_mongoDB(df_records,'local','recs')
                except Exception as e:
                    print(f"Error writing Pymongo db {e}")
             
                print(df_records)
                return df_records
    else:
        if action == 'ENTER':
            df_records.loc[j,'Symbol'] = symb
            df_records.loc[j,'price_entry'] = price
            df_records.loc[j,'type'] = typee
            df_records.loc[j,'t_int'] = t_int
            df_records.loc[j,'coins'] = qt
            df_records.loc[j,'strategy'] = strat
            time_ = get_time()
            df_records.loc[j,'time_entry'] = time_
            df_records.loc[j,'uuid'] = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
 
            print(df_records)
            return df_records
        else:
            df_records.loc[j,'price_exit'] = price
            df_records.loc[j,'type'] = typee
            time_ = get_time()
            df_records.loc[j,'time_exit'] = time_ 
            if p_l < 0:
                df_records.loc[j,'Loss'] = p_l
                print(df_records)
                try:
                    write_df_to_mongoDB(df_records,'local','recs')
                except Exception as e:
                    print(f"Error writing Pymongo db {e}")
              
            else:
                df_records.loc[j,'Wins'] = p_l
                print(df_records)
                try:
                    write_df_to_mongoDB(df_records,'local','recs')
                except Exception as e:
                    print(f"Error writing Pymongo db {e}")

              
                return df_records

#significant figure logic for lot sizes that are small or big


def find_dec_places(pr):
    try:
        return str(pr)[::-1].find('.')
    except:
        return 0

def get_time():
    current_time = dt.datetime.now()
    formatted_time = f"{current_time.year}{current_time.month:02d}{current_time.day:02d}_{current_time.hour:02d}{current_time.minute:02d}{current_time.second:02d}"
    return formatted_time


def filt_buff(mark_buff,df):
    new_df = df[~df['symb'].isin(mark_buff)]
    removed_if_ne = set(df.symb.values.tolist()) - set(new_df.symb.values.tolist())
    print(f'Tickers removed {removed_if_ne}')
    return new_df

async def decision_parallel(df,hbot,logger,res):
    def make_dec(decision,sy):
        if decision['status_code'] == 200:
            decision = decision['evaluation_result']['action']
            return decision
        else:
            logger.log(logging.ERROR, f'Status Response failure status code was not 200\
                for decision evaluator for {sy}')
            print(f'Status Response failure status code was not 200\
                for decision evaluator for {sy}')
            print(f'Program did not receive correct response code for decision evaluation for {sy} removing from market buff ')
            logger.log(logging.ERROR, f'Program did not receive correct response code for decision evaluation for {sy}\
                                    removing from market buff ')
            return False 
    markets = df.symb.values.tolist()
    candles_coroutines = [hbot.get_candles(market,res) for market in markets]
    candles_a = await gather(*candles_coroutines)
    await asyncio.sleep(0.3)
    decision_coroutines = [ get_decision(candles.tail(20).reset_index(drop=True),markets[i]) for i,candles in enumerate(candles_a) ]
    decisions = await gather(*decision_coroutines)
    await asyncio.sleep(0.3)
    final_dec = [make_dec(decision,markets[i]) for i,decision in enumerate(decisions)]
    df['decision'] = final_dec
    return df

def calc_pslope(qt,side,rd,stl_a):
    if side.upper() == 'SHORT':
        p_slope = round(stl_a/qt,rd)
    if side.upper() == 'LONG':
        p_slope = round(-stl_a/qt,rd) 
    return p_slope 

class trademaker:
    def __init__(self,hbot,logger,trade_helper,stl_a,lev,num_traders):
        self.hbot = hbot 
        self.wsAssistant_market = wsAssistant(hbot,['v3_markets'],0)
        self.init_markets = False 
        self.logger = logger
        self.trade_helper = trade_helper
        self.positions = {}
        self.market_price_tickers = {}
        self.trades_tickers = {}
        self.obook_tickers = {}
        self.position_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(3)
        self.trades = {}
        self.stl_a = stl_a
        self.lev = lev
        self.num_traders = num_traders

            
    async def ws_assistants_init(self):
        await self.wsAssistant_market.initialize_data_instances()
        self.init_markets = True 

    
    async def update_and_get_info(self,ticker,key):
        #key = 'MARKET', 'TRADES', 'OBOOK'
        queues_ = {'MARKET':self.hbot.price_queues,'TRADES':self.hbot.trade_queues,'OBOOK':self.hbot.obook_queues}
        positions_ = {'MARKET':self.hbot.ws_market_positions,'TRADES':self.hbot.ws_obook_positions,'OBOOK':self.hbot.ws_obook_positions}
        tickers_to_updt = {'MARKET':self.market_price_tickers,'TRADES':self.trades_tickers,'OBOOK':self.obook_tickers}
        queue = await self.hbot.find_empty_queue(queues_[key])
        _tickers_info = await get_updt_q_info_for_ticker(positions_[key],queue)
        await asyncio.sleep(0.1)
        tickers_to_updt[key].update(_tickers_info)
        return tickers_to_updt[key][ticker]
            
    async def get_props_to_posns(self,ticker,price,rd):
        if ticker not in self.positions:
            self.positions[ticker] = [price]
            candles = self.hbot.get_candles(ticker,'1MIN')
            cslope1, cslope2, cslope = await self.calc_cslopes(self,candles,rd)
            return cslope1, cslope2, cslope
        else:
            self.positions[ticker].append(price)
            if len(self.positions[ticker]) >= 4:
                new_a = self.positions[ticker][3:] + [price]
                self.positions[ticker] = new_a
                cslope1 = new_a[-2] - new_a[-1]
                cslope2 = new_a[-4] - new_a[-3]
                cslope = cslope2 - cslope1 
                return cslope1, cslope2, cslope
            else:
                candles = self.hbot.get_candles(ticker,'1MIN')
                cslope1, cslope2, cslope = await self.calc_cslopes(self,candles,rd)
                return cslope1, cslope2, cslope
            
    async def finish_waiting_for_price_queue(self,queue_num):
        self.price_queues[queue_num].task_done()

    async def pro_l(self,price,price_i,qt,typee,rd):
        if typee == 'LONG':
            p_l = round(qt* ( price - price_i  ),rd)
        else:
            p_l = round(qt* ( price_i  - price),rd)
        
        return p_l 
    
    def count(self):
        return self.trade_helper.count()
    
    def market_buff(self):
        return self.trade_helper.market_buff()
    
    async def new_stlf(self,price_i,side,qt,rd):
        #changed stl 2.67 to 1.35 on 10/31/23
        if side.upper() == 'LONG':
            stl = round(-self.stl_a/qt + price_i,rd)
            return stl 
            
        elif side.upper() == 'SHORT':
            stl = round(self.stl_a/qt + price_i,rd)
            return stl         
        
 
    async def turbulencef(self,typee):
        candles = await self.hbot.get_candles(MARKET_BTC_USD,'4HOURS')
        #0 not being filled in for nans quick fix if nan make it 0 for percent_change
        candles = candles.fillna(0)
        turbulence = candles.percent_change.iloc[-2]*100
        if typee.lower() == 'SHORT':
            if turbulence > 5.8 :
                return True 
        if typee.upper() == 'LONG':
            if turbulence < -5.8 :
                return True 
        await self.logger.log(logging.INFO, f'turbulence acceptable {turbulence} for dirn {typee}')

        return False 
        
    async def decision_eval(self,decision,typee,ticker):
        try:
            if typee.upper() == 'SHORT' and decision == 2:
                print(f'Decision evaluated {decision} for {typee} {ticker}')
                await self.logger.log(logging.INFO, f'Decision evaluated {decision} for {typee} {ticker}')

                return True 
            if typee.upper() == 'LONG' and decision == 1:
                print(f'Decision evaluated {decision} for {typee} {ticker}')
                await self.logger.log(logging.INFO, f'Decision evaluated {decision} for {typee} {ticker}')
                return True 
        except Exception as e:
            self.logger.log(logging.ERROR,f'Failure to Evaluate decision for {typee} {ticker}, {e}')
            return False 

    async def else_cond(self,ord,sy,typee):
        if ord.upper() == 'CANCEL':
            print(f'Order Cancelled for {sy} total slippage loss was greater then maximum .2% slippage loss')
            await self.logger.log(logging.ERROR,f'Order Cancelled for {sy} total slippage loss was greater then maximum .2% slippage loss')
           
            return False 
        if ord.upper() == 'QUEUE':
            print('Program is running three trades and must wait for one queue to open')
            await self.logger.log(logging.ERROR, 'Program is running three trades and must wait for one queue to open')

            return False 
        if ord.upper() == 'CRITERIA':
            print(f'Price has moved away and order did not meet criteria for ticker : {sy} and {typee}')
            await self.logger.log(logging.ERROR, f'Price has moved away and order did not meet criteria for ticker : {sy} and {typee}')

            return False
        
    async def trade_condns(self,typee,condn_num,price,tar,p_l,cslope1,pslope,stl,sy,t0,qt,rd,strat,t_int,j,price_i,queue_num):
        global df_records
        msg_s = {1:f'target_reached: for {sy} at {price} for {typee}',2:f'taking early \
                profits because of possible change in direction , for {typee} {sy} at price {price}',3:f'stop_loss of {stl} \
                    reached for {typee} {sy} at {price}'}
        
        # negative for long , positive for short dpl/dpshort = -Q  so  -dPl/Q ~ dp_short so if p_l = -2.67 then -dPl/Q ~ 2.67/Q
        # which makes sense since we want a positive dP change in price in longs and a negative dP in shorts 
        if typee.upper() == 'LONG':
            condn_s = {1:(price >= tar),2:(p_l > 3.33 and cslope1 >= pslope),3:(p_l <= -1.67)}
            
        else:
            condn_s = {1:(price <= tar),2:(p_l > 3.33 and cslope1 <= pslope),3:(p_l <= -1.67)}
        if condn_s[condn_num] == True:
            df_records.loc[j,'duration'] = (str((dt.datetime.now() - t0).seconds))
            order_result = await self.trade_helper.make_order_retry(sy,typee,qt,price,True,rd)
            print(f'order result for {sy}: {order_result}')
            price = order_result['price_closed']

            self.hbot.ws_market_positions[ticker] = 'close'
            await self.finish_waiting_for_price_queue(self,queue_num)

            p_l = await self.pro_l(price,price_i,qt,typee,rd)
            msg = msg_s[condn_num]
            #self.positions = self.trade_helper.active_queue
            self.positions = {k:v for k,v in self.positions.items() if k != sy}
            self.trades = self.trade_helper.trades
            print(msg)
            await self.logger.log(logging.INFO,msg)
            trade_(sy,j,typee,price,t_int,qt,p_l,'EXIT',strat)
            return True
        
    async def calc_cslopes(self,candles,rd):
        cslope1 = round(candles.Close.iloc[-1] - candles.Close.iloc[-2],rd)
        cslope2 = round(candles.Close.iloc[-2] - candles.Close.iloc[-3],rd)
        cslope = round(cslope1 - cslope2,rd)
        return cslope1, cslope2, cslope


    async def trade_main(self,tar,sy, pr, qt ,j,t_int,typee,strat,pslope,rd,price_i,stl):
        global queue
        global df_records
        t0 = dt.datetime.now()

        old_stl = stl 
        candles = await self.hbot.get_candles(sy,'1MIN')
        price = round(candles.Close.iloc[-1],rd)
        cslope1, _, cslope = await self.calc_cslopes(candles,rd)
        while True:
            try:
                price_item = await self.update_and_get_info(sy,'MARKET')
                price = price_item[0]
                cslope1, _, cslope = await self.get_props_to_posns(sy,price,candles,rd)
                queue_num = price_item[1]
                p_l = await self.pro_l(price,price_i,qt,typee,rd)
                print(f' queue looks like : {self.trade_helper.queue.keys()}')
                await self.logger.log(logging.INFO, f' queue looks like : {self.trade_helper.queue.keys()}')
                print(f'active queue is : {self.trade_helper.active_queue.keys()}')
                await self.logger.log(logging.INFO, f'active queue is : {self.trade_helper.active_queue.keys()}')

                #rolling stop loss if profit exceeds 75% lock in profits with adjusted stop loss at ma8
                if typee.upper() == 'LONG':
                    if p_l >= 0.75*pr and cslope > 0 :
                        print(f'changed long stop loss from {stl}')
                        await self.logger.log(logging.INFO, f'changed long stop loss from {stl}')
                        new_stl = round(price*(1.005),rd)  #.5% above current price
                        stl = new_stl 
                        print(f'changed long stop loss to stl of {stl}')
                        await self.logger.log(logging.INFO, f'changed long stop loss to stl of {stl}')
                if typee.upper() == 'SHORT':
                    if p_l >= 0.75*pr and cslope < 0 :
                        print(f'changed short stop loss from {stl}')
                        await self.logger.log(logging.INFO, f'changed short stop loss from {stl}')
                        new_stl = round(price*(0.995),rd)
                        stl = new_stl
                        print(f'changed short stop loss to stl of {stl}')
                        await self.logger.log(logging.INFO, f'changed short stop loss to stl of {stl}')
                try:
                    if new_stl:
                        message = f'profit is : {p_l} +  at price of {price} and initial price of {price_i},qt: {qt} and stl of {old_stl}, new_stl: {stl} and pot_pr of {pr} for {typee} {sy},tar {tar}, with res: {t_int} and strat: {strat} , idx {j} at {dt.datetime.now().ctime()}'
                        await self.logger.log(logging.INFO, f'message: {message}')
                except:
                        message = f'profit is : {p_l} +  at price of {price} and initial price of {price_i},qt: {qt} and stl of {old_stl}, and pot_pr of {pr} for {typee} {sy}, tar {tar}, with res: {t_int} and strat: {strat} , idx {j} at {dt.datetime.now().ctime()}'
                        await self.logger.log(logging.INFO, f'message: {message}')
                print(message)
                if await self.trade_condns(typee, 1, price, tar, p_l, cslope1, pslope, stl, sy, t0, qt, rd, strat, t_int, j,price_i,queue_num):
                    return df_records
                elif await self.trade_condns(typee, 2, price, tar, p_l, cslope1, pslope, stl, sy, t0, qt, rd, strat, t_int, j,price_i,queue_num):
                    return df_records
                elif await self.trade_condns(typee, 3, price, tar, p_l, cslope1, pslope, stl, sy, t0, qt, rd, strat, t_int, j,price_i,queue_num):
                    return df_records
            except Exception as e:
                print(f'Error in main Trading function for {typee} {sy}, will sleep 1 sec and retry , {e}')
                self.logger.log(logging.ERROR,f'Error in main Trading function for {typee} {sy}, will sleep 1 sec and retry , {e}')
                await asyncio.sleep(1.1)
                continue 

    async def preprocess(self,tar,sy, pr, qt ,j,t_int,typee,strat,decision):
        global queue
            #jot down old_stl in case loop changes stl due to trailing stop_loss
        can_add = False 
        #Ensure ticker is not in market_buff array if so skip
        if sy in self.market_buff() :
            print(f'already trading {sy}, will run analysis to look for another ticker!')
            return False
        else:

            #get min tick size
            await asyncio.sleep(0)
            mark_info = await self.hbot.get_markets()
            mark_info1 = mark_info.data[sy]['tickSize']
            mark_info2 = mark_info.data[sy]['stepSize']

            rd = find_dec_places(float(mark_info1))
            step = find_dec_places(float(mark_info2))

            qt = round(qt,step)
            #get pslope for estimating terminal rate at which you can lose -2.7 which is stop_loss
            candles0 = await self.hbot.get_candles(sy,'1MIN')
            price_i = round(candles0.Close.iloc[-1],rd)
            pslope = calc_pslope(qt,typee,rd,self.stl_a) # ************changed 10/31/23  ** pslope is the critical slope that can cause you to start losing
            action = 'ENTER'

            #get turbulence in case of drawdown or drawup depending on position side and type  
            turbulence = await self.turbulencef(typee)

            #decision code 
            ticker = sy.split('-')[0]
            #await asyncio.sleep(3)
           
            eval_d = await self.decision_eval(decision,typee,ticker)
            #Check turbulence and decision here (non_lin_pred and crit check done in eval_trade, better to do decision here where
            # agent hash is local)
            if turbulence == False and eval_d == True and sy not in self.market_buff():
                if len(filt_1(df_records)) > 1:
                    risk = get_risk(df_records)
                    if risk == False:
                        await self.logger.log(logging.INFO, f'halved risk from qt : {qt} to qtnew: {round(qt/2,3)} ')
                        qt = round(qt/2,step) 
                    
                #Make sure only 3 trades running always 
                async with self.position_lock:
                    new_a = self.market_buff() + [sy]
                    new_a = list(set(new_a))
                    if len(new_a) <= 3:
                        can_add = True

                if can_add:

                    order_result = await self.trade_helper.make_order_retry(sy,typee,qt,price_i,False,rd)
                    print(f'order result for {sy}: {order_result}')
                    await self.logger.log(logging.INFO, f'order result for {sy}: {order_result}')

                    if 'price' in order_result:
                        price_i = order_result['price']
                        stl = await self.new_stlf(price_i,typee,qt,rd)
                        trade_(sy,j,typee,price_i,t_int,qt,0,action,strat)
                        
                        async with self.semaphore:
                            task = asyncio.create_task(self.wsAssistant.get_web_socket_feed(self.trade_helper.self.active_queue_ws_channel))
                            trade = await self.trade_main(tar,sy, pr, qt ,j,t_int,typee,strat,pslope,rd,price_i,stl)
                            return trade 
        
                        
                    ord = await self.else_cond('CANCEL',sy,typee)
                    return ord
                

                ord = await self.else_cond('QUEUE',sy,typee)
                return ord 

            ord = await self.else_cond('CRITERIA',sy,typee)
            return ord 


                


if __name__ == '__main__':
    queue = asyncio.Queue()
    res = None 
    #posn size then becomes 800/3*lev/price
    stl_a = 3.67
    lev = 5
    bal = 250
    trader = trademaker(hbotf,None,None,stl_a,lev,3)

    trade_helper = lobhelperderivrevised.LOBhelper(hbotf,None,bal,trader)
    df_records = pd.DataFrame([],columns=['Wins','Loss','Symbol','price_entry','price_exit','uuid','time_entry','time_exit','type','t_int','cumulative_balance','strategy'])


    async def produce(queues,async_logger):
        global df_records
        global queue
        global hbotf
        global trade_helper
        global trader 

        trader.logger = async_logger
        trade_helper.logger = async_logger
        trader.trade_helper = trade_helper
        hbotf.logger = async_logger
        while True:

            if len(df_records) > 0:
                name = f'algoorecords{dt.datetime.now().month}{dt.datetime.now().day}{dt.datetime.now().year}.csv'
                df_records.to_csv(name)
            if trader.count() <= 2 :

                res_s = ['1DAY','4HOURS', '1HOUR', '30MINS', '15MINS','5MINS','1MIN']
                xx = random.randint(0,4)
                res = res_s[xx]
                market_buff = trader.market_buff()
                print(f'trader helper total queue {market_buff} ')
                await async_logger.log(logging.INFO,f'trader helper total queue: {market_buff}')

                print(f"Before waiting for trades to finish, this is on producer side, count is {trader.count()}, ")
                await async_logger.log(logging.INFO,f"Before waiting for trades to finish, this is on producer side, count is {trader.count()}")

                ff = await get_filt(res,hbotf,async_logger,stl_a)
                print(ff)
                if ff['trade'] != None:
                    df = ff['df']
                    df = await decision_parallel(df,trader.hbot,async_logger,res)
                    if len(trader.market_buff()) > 0:
                        df = filt_buff(market_buff,df)
                    if len(df) > 0:
                    # put the item in the queue
                        res_arr = [df,res]
                        print(f'adding tickers found to queue , len of tickers is {len(df)}')
                        await queue.put(res_arr)
        
          
            else:
                print('Sleeping and continuing loop')
                xx = random.randint(0,6)
                await asyncio.sleep(xx)
                continue

          

    async def consume1(queues,j):
        global df_records
        global queue
        global trader
        while True:
            
            item = await queue.get()
            df = item[0]
            res = item[1]
            #'Close-0','market-1','Profitability-2','stop_loss-3','target-4','risk_reward-5','potential_profit-6',
            # 'potential_loss-7','criteria-8','side-9',qt-10,strat-11
            cryptof = df.values.tolist()
            loops = [trader.preprocess(cryptos[4],cryptos[1],cryptos[6],cryptos[10],(j1 +j),\
                                       res,cryptos[-4],cryptos[11],cryptos[12]) for j1,cryptos in enumerate(cryptof)]
            await gather(*loops)
            o1 = [i for i in loops if type(i) == type(df_records)]
            if len(o1) > 1 :
                print(f'this is df records before being filtered, {df_records}')
                df_records = pd.concat(o1).drop_duplicates().reset_index(drop=True)
                #df_records = filt_(df_records)
                print(f'thi is df records after filtering {df_records}')
            elif len(o1) == 1:
                df_records = o1[0]
            pd.set_option('display.max_rows',df_records.shape[0]+1)
            
            #df_records = filt_(df_records)
            print(df_records)
            j += len(df_records)

        
            queue.task_done()
    
    async def run12():
        global queue
        loop = asyncio.get_running_loop()
        async_logger = AsyncCSVLogger(filename=f'async_log{dt.datetime.now().month}{dt.datetime.now().day}{dt.datetime.now().year}.csv',\
                                       loop=loop)
        # Call setup_logger to configure the logger
        async_logger.setup_logger()

        await async_logger.__aenter__()

        try:
            consumer_tasks = [asyncio.create_task(consume1(queue, i)) for i in range(3)]
            await asyncio.gather(produce(queue, async_logger), *consumer_tasks)
        finally:
            await async_logger.__aexit__(None, None, None)
   
    data = asyncio.get_event_loop().run_until_complete(run12())
    

    print(data)
