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
from OrderManager import OrderManager
from eval_trade import *
import asyncio 
from wsv_handler55 import * 
from risk_adj import * 
from trade_recorder import Trade_recorder
from make_data_req import * 

init_decision_hash = None 
initialized_agents_hash = None 
decision_lock = asyncio.Lock()

eth_add2 =  os.getenv("eth_add2")
pkey2 = os.getenv("pkey2")
DYDX_API_CREDENTIALS = {
    "key": os.environ.get("ek2", ""),
    "secret": os.environ.get("es2", ""),
    "passphrase": os.environ.get("epp2", ""),
}
client = Client(host='https://api.dydx.exchange')

hstk = os.getenv("eck2")
hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'],DYDX_API_CREDENTIALS['secret'],DYDX_API_CREDENTIALS['passphrase'],'0',hstk,eth_add2,None)
res_sp = ['30MINS', '15MINS','5MINS','1MIN']

def calc_pslope(qt,side,rd,stl_a):
    if side.upper() == 'SHORT':
        p_slope = round(stl_a/qt,rd)
    if side.upper() == 'LONG':
        p_slope = round(-stl_a/qt,rd) 
    return p_slope 

def filt_1(df):
    df = df.fillna(0)
    new_df = df[~((df.Wins == 0) &  (df.Loss == 0)) ]
    zdf = pd.DataFrame(new_df)
    return zdf

async def remove_ticker_obook(ticker):
    if hbotf.ws_obook_positions:
        hbotf.ws_obook_positions = {k:v for k,v in hbotf.ws_obook_positions.items() if k!=ticker }

async def remove_ticker_market(ticker):
    if hbotf.ws_market_positions:
        hbotf.ws_market_positions = {k:v for k,v in hbotf.ws_market_positions.items() if k!=ticker }

async def side_helper_oman(side,redn):
    if not redn:
        side = 'SELL' if side.upper() == 'SHORT' else 'BUY'
    else:
        side = 'BUY' if side.upper() == 'SHORT' else 'SELL'
    return side 

async def decision_eval(decision,typee,ticker):
    try:
        #print(f'decision:{decision},type: {typee},ticker: {ticker}')
        if typee.upper() == 'SHORT' and decision == 2:
            print(f'Decision evaluated {decision} for {typee} {ticker}')
            #await self.logger.log(logging.INFO, f'Decision evaluated {decision} for {typee} {ticker}')

            return True 
        if typee.upper() == 'LONG' and decision == 1:
            print(f'Decision evaluated {decision} for {typee} {ticker}')
            #await self.logger.log(logging.INFO, f'Decision evaluated {decision} for {typee} {ticker}')
            return True 
    except Exception as e:
        #self.logger.log(logging.ERROR,f'Failure to Evaluate decision for {typee} {ticker}, {e}')
        print(f'Failure to Evaluate decision for {typee} {ticker}, {e}')
        return False 
    

async def initalize_decision_params():
    global initialized_agents_hash, init_decision_hash
    if not init_decision_hash:
        drv_ = r'C:\Users\grc\Downloads\tickers_train'
        files_ = os.listdir(drv_)
        tickers_ = [i.split('-')[0] for i in files_ ]
        init_agent_bools = [False for _ in tickers_]
        initialized_agents_hash = dict(zip(tickers_,init_agent_bools))
        init_decision_hash = True 

async def get_decision(candles,ticker):
    global init_decision_hash, initialized_agents_hash
    '''
    data: is the dataframe with the ohlc klines candlesticks from hbot
    '''
    if not init_decision_hash:
        await initalize_decision_params()
    async with decision_lock:
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



async def decision_parallel(res,market,side):
    def make_dec(decision,sy):
        if decision['status_code'] == 200:
            decision = decision['evaluation_result']['action']
            return decision
        else:
            msg =  f'Status Response failure status code was not 200\
                for decision evaluator for {sy}'
            #logger.log(logging.ERROR,msg)
            print(msg)
            msg2 = f'Program did not receive correct response code for decision evaluation for {sy} removing from market buff '
            print(msg2)
            #logger.log(logging.ERROR, msg2)
            return None  

    candles = await hbotf.get_candles(market,res)
    await asyncio.sleep(0.3)
    decision_coroutines = await get_decision(candles.tail(20).reset_index(drop=True),market) 

    await asyncio.sleep(0.3)
    final_dec = make_dec(decision_coroutines,market)
    eval_dec = await decision_eval(final_dec,side,market)
    return eval_dec 

async def turbulencef(typee):
    candles = await hbotf.get_candles('BTC-USD','4HOURS')
    candles2 = await hbotf.get_candles('ETH-USD','4HOURS')

    #0 not being filled in for nans quick fix if nan make it 0 for percent_change
    candles = candles.fillna(0)
    candles2 = candles2.fillna(0)
    turbulence = candles.percent_change.iloc[-2]*100
    turbulence2 = candles2.percent_change.iloc[-2]*100
    if typee.lower() == 'SHORT':
        if (turbulence  or turbulence2) > 5.8:
            return True 
    if typee.upper() == 'LONG':
        if (turbulence or turbulence2)  < -5.8 :
            return True 
    #await self.logger.log(logging.INFO, f'turbulence acceptable {turbulence} for dirn {typee}')
    return False 
    
class Trader:
    def __init__(self,hbot,logger):
        self.hbot = hbot 
        self.logger = logger 
        self.ticker_hash = {}
        self.ws_assistant_markets = wsAssistant(self.hbot,['v3_markets'],0)
        self.ws_assistant_accounts = wsAssistant(hbotf,['v3_accounts'],0)
        self.wsAssistant_orderbook = wsAssistant(self.hbot,['v3_orderbook'],4)
        self.trades_data = {}
        self.margin_fees = {'maker':0.02/100,'taker':0.05/100}
        self.bal = None 
        self.init_markets = False 
        self.orderManager = None 
        self.min_loss = -1.67
        self.stop_loss_prices = {}
        self.finalized_orders = {}
        self.account_data = {}
        self.orderbook_data = {}
        self.positions = {}
        self.completed_trades = {}
        self.test = False 
        self.ticks = {}
        self.steps = {}
    async def trader_test_pl(self):
        if self.test:
            return 1.11
        return 3.33
    async def update_bal_and_add_ticker(self,ticker,price_entry,size,side,lev):
        #margin is the collateral  in the account 
        margin = size * price_entry / lev 
        fee = size*price_entry*self.margin_fees['taker']
        if side.upper() == 'LONG':
            self.bal -= (fee + margin)
            self.positions[ticker] = [price_entry,size,side,margin]
        elif side.upper() == 'SHORT':
            self.bal -= (fee + margin)
            self.positions[ticker] = [price_entry,size,side,margin]

    async def remove_ticker(self,ticker,price):
        side = self.positions[ticker][2]
        price_entry = self.positions[ticker][0]
        size = self.positions[ticker][1]
        margin = self.positions[ticker][-1]
        await self.trade_f(ticker,side,price_entry,price,size,margin)

 

    async def trade_f(self,ticker,side,price_entry,price_exit,size,margin):

        p_l, fee = await lob_helpers.calc_pl_w_fee(price_entry,price_exit,size,side,(self.margin_fees))
        self.bal += (p_l + margin - fee)
        id = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
        self.completed_trades[id] = [price_entry,price_exit, size, side, p_l,ticker]
        self.positions = {k:v for k,v in self.positions.items() if k != ticker}
        print(f'completed trades : {self.completed_trades}')
        lob_helpers.tickers_to_df(self.completed_trades)

    async def pro_l(self,price,price_i,qt,typee,ticker):
        step = self.steps[ticker]
        qt = round(qt,step)
        rd = self.ticks[ticker]
        if typee == 'LONG':
            p_l = round(qt* ( price - price_i  ),rd)
        else:
            p_l = round(qt* ( price_i  - price),rd)
        
        return p_l 
    
    async def ws_updates(self):
        from datetime import datetime 
        if not self.init_markets:
            await self.ws_assistant_markets.initialize_data_instances()
            await self.ws_assistant_accounts.initialize_data_instances()
            await self.wsAssistant_orderbook.initialize_data_instances()

            self.init_markets = True 
        await asyncio.sleep(1.8)
        while hbotf.ws_market_positions or hbotf.ws_obook_positions :
            try:
                for ticker in hbotf.ws_market_positions: #data_need = [item for item in data if item[0] == ticker ]
                    
                    market_data  = await self.ws_assistant_markets.data_get(ticker)
                    account_data = await self.ws_assistant_accounts.data_get(ticker)
                    obook_data = await self.wsAssistant_orderbook.data_get(ticker)

                    print(f'account_data looks like: {account_data}')
                    #print(f'data looks like : {market_data}')
                    if market_data:
                        rd = self.ticks[ticker]
                        if 'indexPrice' in market_data[ticker]:
                            price =round(float(market_data[ticker]['indexPrice']),rd)
                            await self.receive_price_updates(ticker,price)

                    if account_data:
                        if account_data['market'] == ticker: 
                            await self.orderManager._receive_order_update(
                                account_data['id'],
                                account_data['status'],
                                account_data['remainingSize'],
                                account_data['price']
                            )
                            self.account_data[ticker] = account_data
                            self.orderManager._update_account_data(self.account_data)
                  
                    if obook_data:
                        self.orderbook_data[ticker] = obook_data
                        self.orderManager._update_obook_data(self.orderbook_data)
                            
            except Exception as e:
                print(f'Error with kafka retrieval, for {ticker} , {e}')

    async def receive_price_updates(self,ticker,price):
        print(f'this is how ticker hash looks {self.ticker_hash}')
        if len(self.ticker_hash[ticker]) > 3:
            self.ticker_hash[ticker] = self.ticker_hash[ticker][-3:] + [price]
        else:
            self.ticker_hash[ticker].append(price)
            print(f'this is how it looks after appending {self.ticker_hash}')
        
    async def calc_cslopes(self,candles,market):
        rd = self.ticks[market]
        cslope1 = round(candles.Close.iloc[-1] - candles.Close.iloc[-2],rd)
        cslope2 = round(candles.Close.iloc[-2] - candles.Close.iloc[-3],rd)
        cslope = round(cslope1 - cslope2,rd)
        return cslope1, cslope2, cslope

    async def get_props_to_posns(self,ticker):
        rd = self.ticks[ticker]
        print(f' this is how ticker hash looks {self.ticker_hash[ticker]}')
        if len(self.ticker_hash[ticker]) < 4:
            print(f'Ticker hasn not populated yet downloading candles, len of ticker hash is {len(self.ticker_hash[ticker])}')
            candles = await self.hbot.get_candles(ticker,'1MIN')
            cslope1, cslope2, cslope = await self.calc_cslopes(candles,ticker)
            return cslope1, cslope2, cslope
        else:
            if len(self.ticker_hash[ticker]) >= 4:
                print('ticker hash populated using websocket ')
                new_a = self.ticker_hash[ticker][-4:]
                cslope1 = round(new_a[-2] - new_a[-1],rd)
                cslope2 = round(new_a[-4] - new_a[-3],rd)
                cslope = round(cslope2 - cslope1,rd) 
                return cslope1, cslope2, cslope
       
            
    async def remove_stop_loss_for_ticker(self,ticker):
        self.stop_loss_prices = {k:v for k,v in self.stop_loss_prices.items() if k != ticker}

    async def trade_condns(self,ticker,side,price,tar,p_l,cslope1,pslope,qt,condn_num,id_trade):
        rd = self.ticks[ticker]
        msg_s = {1:f'target_reached: for {ticker} at {price} for {side}',\
                 2:f'taking early profits because of possible change in direction , for {side} {ticker} at price {price}',\
                 3:f'minimum stop loss exceeded',\
                 4:f'stop_loss price of {self.stop_loss_prices[ticker]} reached for {side} {ticker} at {price}'}
        p_l_ = await self.trader_test_pl()
        if side.upper() == 'LONG':
            condn_s = {1:(price >= tar),2:(p_l > p_l_ and cslope1 >= pslope),3:(p_l <= self.min_loss),4:(price <= self.stop_loss_prices[ticker])}
        else:
            condn_s = {1:(price <= tar),2:(p_l > p_l_ and cslope1 <= pslope),3:(p_l <= self.min_loss),4:(price >= self.stop_loss_prices[ticker])}
        if condn_s[condn_num] == True:
            msg = msg_s[condn_num]
            print(msg)
            redn = True
            side_order = await side_helper_oman(side,redn)
            order_type = 'LIMIT'
            price = round(price,rd)
            await remove_ticker_market(ticker)
            order = await self.hbot.place_order(ticker,side_order,qt,price,order_type,False,redn)
            await asyncio.sleep(1.4)
            order_id = order['order']['id']
            status = order['order']['status']
            t0 = time.perf_counter()
            hbotf.ws_obook_positions[ticker] = 'open'
            morder = await self.orderManager._start_order_monitoring(ticker,side_order,qt,price,order_id,status,redn,t0,order_type)
            await self.orderManager.orders[order_id]['task']
            order_f = self.orderManager.completed_orders[order_id]
            price = order_f['average_price']
            
            await remove_ticker_obook(ticker)
            await self.remove_stop_loss_for_ticker(ticker)
            await self.remove_ticker(ticker,price)
            self.finalized_orders[id_trade] = self.orderManager.completed_orders[order_id]
            return True 
                    
    async def check_change_stl(self,popr,cslope,side,p_l,price,ticker):
        change_stl = None
        rd = self.ticks[ticker]
        if side.upper() == 'LONG':
            if p_l >= 0.75*popr and cslope > 0 :
                msg_before_change = f'changed long stop loss from {self.stop_loss_prices[ticker]}'
                print(msg_before_change)
                #await self.logger.log(logging.INFO, f'changed long stop loss from {self.stop_loss_prices[ticker]}')
                new_stl = round(price*(1.005),rd)  #.5% above current price
                self.stop_loss_prices[ticker] = new_stl
                msg_after_change = f'changed long stop loss to stl of {self.stop_loss_prices[ticker]}'
                print(msg_after_change)
                #await self.logger.log(logging.INFO, msg_after_change)
                change_stl = True
        if side.upper() == 'SHORT':
            if p_l >= 0.75*popr and cslope < 0 :
                msg_before_change = f'changed long stop loss from {self.stop_loss_prices[ticker]}'
                #await self.logger.log(logging.INFO, msg_before_change)
                new_stl = round(price*(0.995),rd)
                self.stop_loss_prices[ticker] = new_stl
                msg_after_change = f'changed short stop loss to stl of {self.stop_loss_prices[ticker]}'
                print(msg_after_change)
                #await self.logger.log(logging.INFO, msg_after_change)
                change_stl = True 
        if change_stl:
            return change_stl
        return False
                
    async def trade(self,ticker,tar,qt,side,popr,res):
        #open_ws_ = asyncio.create_task(self.ws_updates())
        await asyncio.sleep(1.4)
        market_props = await self.hbot.get_markets()
        rd = self.ticks[ticker]
        stl = self.stop_loss_prices[ticker]
        pslope = calc_pslope(qt,side,rd,stl)
        price_i = self.ticker_hash[ticker][0]
        id_trade = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
        while True:
            print(f'ticker hash : {self.ticker_hash}')
            price = self.ticker_hash[ticker][-1]
            print(f'price: {price} and price_i : {price_i} rd: {rd}')
            p_l = await self.pro_l(price,price_i,qt,side,ticker)
            cslope1,_, cslope = await self.get_props_to_posns(ticker)
            if await self.check_change_stl(popr,cslope,side,p_l,price,ticker):
                stl = self.stop_loss_prices[ticker]
            message = f'profit is : {p_l} at price of {price} and initial price of {price_i},\
                qt: {qt} and stl of {self.stop_loss_prices[ticker]}, and pot_pr of {popr} for {side} {ticker},\
                tar {tar}, with res: {res} and strategy {side} at {dt.datetime.now().ctime()}'
            print(message)
            for i in range(1,5):
                if await self.trade_condns(ticker,side,price,tar,p_l,cslope1,pslope,qt,i,id_trade):
                    print(f'Order {id_trade} finalized for {side} {ticker} at {self.finalized_orders[id_trade]["average_price"]}')
                    return self.finalized_orders[id_trade]
                
            sleep_secs = random.randint(2,6)
            await asyncio.sleep(sleep_secs)
                

async def gen_tickers_(res,hbotf,async_logger,stl):
    ff = await get_filt(res,hbotf,async_logger,stl,True)
    if ff['trade']:
        df = ff['df']
        print(f'df found {df}')
        new_df = df.loc[df.potential_profit != 0]
        df = pd.DataFrame(new_df)
        #df = df[:1]
        headers = ['Close', 'symb', 'Profitability', 'stop_loss', 'target', 'risk_reward',\
                   
                    'potential_profit', 'potential_loss', 'criteria', 'side', 'qt','strategy']
        cryptof = df.values.tolist()
        redn = False 

        tickers_ = [cryptof[i][1] for i in range(len(cryptof)) ]
        sides_ = [cryptof[i][-4] for i in range(len(cryptof))]
        return df, tickers_, sides_ 
    
    print('Nothing found returning 3 nulls')
    return None, None, None

async def test_tickers(tickers,res,sides,cryptof,orderManager,async_logger):
    for i, ticker in enumerate(tickers):
        if await decision_parallel(res,ticker,(sides[i])):
           await order_and_trade(cryptof,i,orderManager,res,async_logger)
        
    print('no decisions made returning None')
            
async def get_tickers_and_sides(tickers,sides,res,hbotf,async_logger,stl):
    if not tickers:
        while True:
            df, tickers_, sides_ = await gen_tickers_(res,hbotf,async_logger,stl)
            if df and tickers_ and sides_ :
                break 
    if tickers_ and sides_ :
        return df, tickers_ , sides_ 
    
async def res_reset(res_arr):
    return (len(res_arr) == 0)

async def get_rand_res(res_arr):
    xx = random.randint(0,len(res_arr)-1)
    res = res_arr[xx]
    new_res_arr = [el for el in res_arr if el != res]
    print(f'res chosen is {res}')
    return res, new_res_arr 

async def gen_res(res_spp):
    if len(res_spp) > 1:
        res, new_res_arr = await get_rand_res(res_spp)

    elif len(res_spp) == 1:
        res = res_spp[0]
        new_res_arr = res_sp.copy()
    else: 
        res_s = res_sp.copy()
        res, new_res_arr = await get_rand_res(res_s)

    return res, new_res_arr 

async def order_and_trade(cryptof,dec_,orderManager,res,async_logger):
    
    tar = cryptof[dec_][4]
    stl = cryptof[dec_][3]
    order_type = 'LIMIT'
    market = cryptof[dec_][1]
    g = await hbotf.get_markets()
    #print(f'ticksize: {g[market]["tickSize"]}')
    step = find_dec_places(g[market]['stepSize']) #amount multiple of step
    rd = find_dec_places(g[market]['tickSize']) #price multiple of tick
    #print(f'this is rd {rd}')
    min_order_size = float(g[market]['minOrderSize'])
    print(f'step: {step} ,rd: {rd} for {market}')
    side = cryptof[dec_][-4]
    amount = round(cryptof[dec_][-3],step)
    print(f'amount rounded up by step {amount}')
    price = round(cryptof[dec_][0],rd) # rd is tick size 
    popr = cryptof[dec_][6]
    turbulence = await turbulencef(side)
    volume_check = await hbotf.check_volumes(market)
    redn = False
    if not turbulence and volume_check and (amount > min_order_size):
        side_order = await side_helper_oman(side,redn)



        order = await hbotf.place_order(market,side_order,amount,price,order_type,False,redn)
        await asyncio.sleep(1.5)
        id = order['order']['id']
        status = order['order']['status']
        hbotf.ws_obook_positions[market] = 'open'
        hbotf.ws_market_positions[market] = 'open'

        trader = Trader(hbotf,async_logger)
        trader.test = True
        trader.orderManager = orderManager
        open_ws_ = asyncio.create_task(trader.ws_updates())
        account = await hbotf.get_account()
        trader.bal = float(account['account']['freeCollateral'])
        t0 = time.perf_counter()
        morder = await  orderManager._start_order_monitoring(market,side,amount,price,id,status,redn,t0,order_type)
        print(f'morder monitoring')
        await orderManager.orders[id]['task']
        time_taken = (time.perf_counter() - t0)
        print(f'time taken: {time_taken}')
        if id in orderManager.completed_orders:
            trader.steps[market] = step 
            trader.ticks[market] = rd
            lev = amount* price /(trader.bal)
            price = float(orderManager.completed_orders[id]['average_price'])
            print(f'update bal and add ticker : {market,price,amount,side,lev}')
            await trader.update_bal_and_add_ticker(market,price,amount,side,lev)
            await remove_ticker_obook(market)
            price = float(orderManager.completed_orders[id]['average_price'])
            #trader.update_bal_and_add_ticker(market,price,amount,side,lev)
            print(f'completed morder ')
            trader.ticker_hash[market] = [price]
            trader.stop_loss_prices[market] = stl
            return await trader.trade(market,tar,amount,side,popr,res)

async def OmTest():
    #loop = asyncio.get_running_loop()
    loop = hbotf._loop
    async_logger = AsyncCSVLogger(filename=f'async_log{dt.datetime.now().month}{dt.datetime.now().day}{dt.datetime.now().year}.csv',\
                                    loop=loop)
    # Call setup_logger to configure the logger
    async_logger.setup_logger()

    await async_logger.__aenter__()
    tickers_ = None
    orderManager = OrderManager(hbotf)
    Trade_record = Trade_recorder(async_logger)
    
    stl = 0.47 # was 1.67 when using 3x 
    res_s = res_sp.copy()
    
    while True:
        res , res_s = await gen_res(res_s)
        
        df, tickers_, sides_ = await gen_tickers_(res,hbotf,async_logger,stl)
                
        if tickers_ and sides_:    
            cryptof = df.values.tolist()
            await test_tickers(tickers_,res,sides_,cryptof,orderManager,async_logger) # add cryptodf and then make fn to order and trade
        else:
            print('Nothing found!')
        

#asyncio.run(OmTest())
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    test = loop.run_until_complete(OmTest())