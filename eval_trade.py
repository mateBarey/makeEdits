from hbot51 import *
import asyncio
from asyncio import * 
from asynclog import * 
import ray 
import sys 
import os 
from collections import OrderedDict
#ray.init(num_cpus=int(ray.cluster_resources()['CPU'] / 2))
sys.path.append(r'C:\Users\grc\Downloads\moddedart\newDart')
ray.init(ignore_reinit_error=True,log_to_driver=False)
os.environ['RAY_DEDUP_LOGS'] = '0'
'''
HELPER METHODS 
'''

'''
get_fib method: 
runs fibonacci levels for a specific market with a period 
of 100 time intervals for a given time resolution
'''
#inital params 
bal_available = None
filt_l = OrderedDict()
filt_sh = OrderedDict()
market_rd = {}
market_step = {}

def get_fib(df,dirn,rd):
    fib_hash= {}
    #state = df.rsi.iloc[-1]
    if dirn.upper() == 'LONG':
        high = round(float(df.Close.max()),rd) 
        low = round(float(df.Close.min()),rd)
        diff = round(high - low,rd)
        level1 = round(low + 0.236 * diff,rd)
        level2 = round(low + 0.382 * diff,rd)
        level3 = round(low + 0.618 * diff,rd)
        target = round(low + 0.3469 *diff,rd)
        t2 = round((target + level1)/2,rd)
        fib_hash['0'] = low
        fib_hash['0.236'] = level1
        fib_hash['0.382'] = level2
        fib_hash['0.618'] = level3
        fib_hash['1'] = high
        fib_hash['target'] = target
        fib_hash['t2'] = t2
        #fib_hash['t2'] = t2

        return fib_hash
    elif dirn.upper() == 'SHORT': # for UPTREND
    
        Price_Max = round(float(df.High.max()),rd)
       
        low_val = round(df.ma10.min(),rd)
        diff = round(Price_Max - low_val,rd)
        level1 = round(Price_Max - 0.236 * diff,rd)
        level2 = round(Price_Max - 0.382 * diff,rd)
        level3 = round(Price_Max - 0.618 * diff,rd)
        target = round(Price_Max - 0.3469 *diff,rd)
        t2 = round((target + level1)/2,rd)
        fib_hash['0'] = Price_Max
        fib_hash['0.236'] = level1
        fib_hash['0.382'] = level2
        fib_hash['0.618'] = level3
        fib_hash['1'] = low_val
        fib_hash['target'] = target
        fib_hash['t2'] = target
        return fib_hash
    
async def new_stl(candles,side,qt,rd,stl_a):
    #change stl  10/31/23
    #print(f'stl:{stl_a} qt: {qt}, rd:{rd} ')
    if side.upper() == 'LONG':
        price_entry = round((candles.Close.iloc[-1] + candles.Low.iloc[-1])/2,rd)
        stl = round(-stl_a/qt*1.05 + price_entry,rd)
        return stl 
    elif side.upper() == 'SHORT':
        price_entry = round((candles.Close.iloc[-1] + candles.High.iloc[-1])/2,rd)
        stl = round(stl_a/qt*1.05 + price_entry,rd)

        #stl = round(price_entry + 2.67/qt,rd)
        return stl 
    
def find_dec_places(pr):
    try:
        if int(pr) == 1:
            return 0
        places = str(pr)[::-1].find('.')
        if places < 0:
            places = 1 
        return places 
    except:
        return 
    

@ray.remote
def xgboost_pred_parallel(df):
    print(os.getcwd())
    import stack
    return stack.xgboostpred(df)

def parallel_predictions(dataframes_list):
    # Parallelize the function call
    futures = [xgboost_pred_parallel.remote(df) for df in dataframes_list]
    
    # Retrieve results
    results = ray.get(futures)
    
    return results

async def non_lin_pr_pred(typee,candles,sy,rd,async_logger,pred):
   
    pr = round(candles.Close.iloc[-1],rd)

    if typee.upper() == 'SHORT':
       
        if pred < pr :
            #print(f'Predicted_price: {pred}, current_price is : {pr} for SHORT for {sy}')
            await async_logger.log(logging.INFO, f'Predicted_price: {pred}, current_price is : \
                                   {pr} for SHORT for {sy}')
            return True 
    if typee.upper() == 'LONG':
        
        if pred > pr :
            #print(f'Predicted_price: {pred}, current_price is : {pr} for LONG for {sy}')
            await async_logger.log(logging.INFO, f'Predicted_price: {pred}, current_price is :\
                                    {pr} for LONG for {sy}')
            return True 
        
    print(f'Xgboost Predictor did not find future to prediction to be within the bounds of current\
           {typee.upper()}\
           position for {sy}')
    await async_logger.log(logging.ERROR, f'Xgboost Predictor did not find future to prediction to be within\
                            the bounds of current {typee.upper()} position for {sy}')

    return False 

async def crit_eval(cc,borrow,dirn,npp,market,candles,stl,risk_reward,potential_profit,potential_loss,\
                    criteria,target,lev):
    global filt_l, filt_sh
    if cc != False and borrow != 0 and npp == True:
        #print(f'crit eval borrow: {borrow} , close : {candles.Close.iloc[-1]} for market {market}')
        if dirn == 'LONG':
            filt_l[market] = [candles.Close.iloc[-1],market,True,stl,target,risk_reward,potential_profit,\
                              potential_loss,criteria,'LONG',borrow,lev]

        else:
            filt_sh[market] = [candles.Close.iloc[-1],market,True,stl,target,risk_reward,potential_profit,\
                               potential_loss,criteria,'SHORT',borrow,lev]

def elim_candles(markets,candles):
    print(f'len of markets: {len(markets)}')
    def if_open_eq_close(candles):
        open = (candles.Open.values.tolist())[-3:]
        
        close = (candles.Close.values.tolist())[-3:]
        return any(a == b for a, b in zip(open, close))
    candles_need = [ candle for idx, candle in enumerate(candles) if not if_open_eq_close(candle)]
    markets_need = [ markets[idx] for idx, candle in enumerate(candles) if not if_open_eq_close(candle)]
    return candles_need, markets_need

async def crit_check(typee,candles,rd,fibs):
    #recalculate criteria with new candles....****
    pr = round(candles.Close.iloc[-1],rd)
    if typee.upper() == 'LONG':
        low = round(fibs['0'],rd)
        oth = round(fibs['0.236'],rd)
        cr = round((oth + low)/2,rd)
        if pr <= cr :
            return True 
        
    if typee.upper() == 'SHORT':
        high = round(fibs['0'],rd)
        oth = round(fibs['0.236'],rd)
        cr = round((high + oth)/2,rd)
        if  pr >= cr :
            return True 
    return False

async def get_fib_and_props(dirn,candles,rd,borrow,stl_a):
    if dirn.upper() == 'SHORT':
        fib = get_fib(candles,dirn,rd)
        price_max = fib['0']
        level_1 = fib['0.236']
        criteria = round((price_max + level_1)/2,rd)
        stl = await new_stl(candles,dirn,borrow,rd,stl_a)
        target = fib['t2']
        potential_profit = round(borrow*(criteria - target),rd)
        potential_loss = round(borrow*(stl - criteria),rd)
        risk_reward = abs(potential_profit/potential_loss)

    elif dirn.upper() == 'LONG':
        fib = get_fib(candles,dirn,rd)
        low = fib['0']
        level_1 = fib['0.236']
        criteria = round((level_1 + low)/2,rd)
        target = fib['t2']
        stl = await new_stl(candles,dirn,borrow,rd,stl_a)
        potential_profit = round(borrow*(target-criteria),rd)
        potential_loss = round(borrow*(criteria - stl),rd)
        risk_reward = abs(potential_profit/potential_loss)

    return fib, criteria, stl, target, potential_profit, potential_loss, risk_reward

async def calc_evalf(candles,borrow,stl_a,market,rd,async_logger,pred_n_m,lev):
    global market_rd, filt_l, filt_sh
    dirn = None
    if candles.rsi.iloc[-1] > 60 or (candles.macd.iloc[-3] < candles.macd_signal.iloc[-3]\
             and candles.macd.iloc[-1] > candles.macd_signal.iloc[-1] and (round(candles.Close.iloc[-1],rd)\
             < round(candles.ma30.iloc[-1],rd))):
        dirn = 'SHORT'

    elif candles.rsi.iloc[-1] <40 or (candles.macd_signal.iloc[-3] < candles.macd.iloc[-3] and\
         candles.macd_signal.iloc[-1] > candles.macd.iloc[-1] and (round(candles.Close.iloc[-1],rd) > \
                round(candles.ma30.iloc[-1],rd))):
        dirn = 'LONG'
    if dirn:
        fib, criteria, stl, target, potential_profit, potential_loss, risk_reward = await\
            get_fib_and_props(dirn,candles,rd,borrow,stl_a)
        cc = await crit_check(dirn,candles,rd,fib)
        if cc != False:
            npp = await non_lin_pr_pred(dirn,candles,market,rd,async_logger,pred_n_m)
            #print(f'borrow: {borrow}, close {candles.Close.iloc[-1]}')
            await crit_eval(cc,borrow,dirn,npp,market,candles,stl,risk_reward,potential_profit,potential_loss,\
                            criteria,target,lev)



async def make_dfs(async_logger,res):
    ff = {}
    long_df = pd.DataFrame.from_dict(filt_l, orient='index',columns=['Close','symb','Profitability',\
                                'stop_loss','target','risk_reward','potential_profit','potential_loss','criteria','side','qt','lev'])
    long_df = long_df.sort_values(by=['risk_reward'],ascending=[False])
    short_df = pd.DataFrame.from_dict(filt_sh, orient='index',columns=['Close','symb','Profitability',\
                                'stop_loss','target','risk_reward','potential_profit','potential_loss','criteria','side','qt','lev'])
    short_df = short_df.sort_values(by=['risk_reward'],ascending=[False])
    print(f'short_df: {short_df.symb.values}')
    if not short_df.empty and not long_df.empty:
        combined_df = pd.concat([short_df,long_df],ignore_index=True)
        ff['trade'] = 'combined'
        combined_df['strategy'] = 'agent'
        ff['df'] = combined_df
        print(f'found following tickers {combined_df.symb.values.tolist()} {len(combined_df.symb.values.tolist())} tickers in market analysis for res {res}')
        await async_logger.log(logging.INFO,f'found following tickers {combined_df.symb.values.tolist()} {len(combined_df.symb.values.tolist())} tickers in  market analysis for res {res}')
        return ff 
    elif not short_df.empty:
        short_df = pd.DataFrame((short_df.loc[short_df.qt != 0]))
        ff['trade'] = 'SHORT'
        short_df['strategy'] = 'agent'
        ff['df'] = short_df
        print(f'found following tickers {short_df.symb.values.tolist()} {len(short_df.symb.values.tolist())} tickers in short market analysis for res {res}')
        await async_logger.log(logging.INFO,f'found following tickers {short_df.symb.values.tolist()} {len(short_df.symb.values.tolist())} tickers in short market analysis for res {res}')
        return ff 
    elif not long_df.empty:
        long_df = pd.DataFrame((long_df.loc[long_df.qt != 0]))

        ff['trade'] = 'LONG'
        long_df['strategy'] = 'agent'
        ff['df'] = long_df
        print(f'found following tickers {long_df.symb.values.tolist()} {len(long_df.symb.values.tolist())} tickers in long market analysis for res {res}')
        await async_logger.log(logging.INFO,f'found following tickers {long_df.symb.values.tolist()} {len(long_df.symb.values.tolist())} tickers in long market analysis for res {res}')
        return ff 
    print(f'found no tickers in market analysis for res {res}')
    await async_logger.log(logging.INFO,f'found no tickers in market analysis for res {res}')
    ff['df'] = None 
    ff['trade'] = None 
    return ff 

async def gather_candles_all(markets_,res,hbotf):
    candles_hash = {}
    for market in markets_:
        candles_hash[market] = await hbotf.get_candles(market,res)
    return candles_hash

async def get_filt(res,hbot,async_logger,stl_a,test=False):
    '''
    Filter Markets by Agent Decision and Predicted immediate price and rank based on possible reward
    assumptions are 3x leverage at 333 balance with 3 trades 
    '''
    global market_rd, filt_l, filt_sh, market_step
    critical_bal = 70 # was 150
    min_leverage = 1
    account = await hbot.get_account()
    bal_a = float(account['account']['freeCollateral'])
    if bal_a >= critical_bal:
        markets = await  hbot.get_markets()
        markets_ = [i for i in list(markets.keys()) if 'LUNA-USD' != i ]
        candles_a = await gather_candles_all(markets_,res,hbot)
        await asyncio.sleep(0.3) 
        candles_a = list(candles_a.values())
        candles_a, markets_ = elim_candles(markets_,candles_a)
        #markets_ = await _factor_markets(markets_,candles_a)

        lev_ = [1/(float(markets[i]['initialMarginFraction']) + float(markets[i]['maintenanceMarginFraction']) +.05)  for i in markets_]
        initial_Marg_frac = [markets[i]['initialMarginFraction'] for i in markets_] 
        maint_marg_frac = [markets[i]['maintenanceMarginFraction'] for i in markets_] 
        incremental_marg_frac = [markets[i]['incrementalInitialMarginFraction'] for i in markets_] 
        base_posn_size = [markets[i]['baselinePositionSize'] for i in markets_]      
        inc_posn_size = [markets[i]['incrementalPositionSize'] for i in markets_]      


        tick_size = [markets[i]['tickSize'] for i in markets_]

        minOrderSize = [markets[i]['minOrderSize'] for i in markets_]

        pred_n_m = parallel_predictions(candles_a)
        rd_a = [find_dec_places(float(markets[market]['tickSize'])) for market in markets_] # all sizes are multiples of tick
        step_a = [find_dec_places(float(markets[market]['stepSize'])) for market in markets_] # all prices multiples of step
        #print(f'leverages: {lev_}')
        #print(f'market : {markets_}')
        #print(f'base_posn_size: {base_posn_size}')
        #print(f'inc_posn_size: {inc_posn_size}')
        #print(f'inc marg frac: {incremental_marg_frac}')
        #print(f'bal_a : {bal_a}')
        #print(f'initial_marg frac: {initial_Marg_frac}')
        #print(f'maint marg frac: {maint_marg_frac}')
        #max_test_borrow = [round(3*(minOrderSize[idx]),rd_a[idx]) for idx,lev in enumerate(lev_)]
        max_test_borrow = [round(120/candles_a[idx].Close.iloc[-1],rd_a[idx]) for idx,lev in enumerate(lev_)]
        #print(f'borrow: {set(max_test_borrow)}')
        max_borrow = [round(lev*bal_a/candles_a[idx].Close.iloc[-1],step_a[idx]) for idx,lev in enumerate(lev_)]
        if test :
            max_borrow = max_test_borrow
        eval_corountines = [calc_evalf(candles_a[idx],max_borrow[idx],\
            stl_a,markets_[idx],rd_a[idx],async_logger,pred_n_m[idx],lev) for idx,lev in enumerate(lev_) \
                if candles_a[idx].Close.iloc[-1] < 10000 and lev_[idx] > min_leverage  and max_borrow[idx] != 0]
        eval_a = await gather(*eval_corountines)

        await asyncio.sleep(0.5)
        ff = await make_dfs(async_logger,res)
        return ff 
    else:
        print(f'Current account balance of {bal_a} is currently less then  critical balance of {critical_bal} with minimum leverage of {min_leverage}')
        ff = {}
        ff['df'] = None 
        ff['trade'] = None 
        return ff
    
