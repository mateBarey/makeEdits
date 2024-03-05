import pandas as pd
import os 
import httpx 
import asyncio 
from asyncio import * 
import time 
import random 



#files of df crypto params
#drv_ = r'C:\Users\grc\Downloads\tickers_train'
#files_ = os.listdir(drv_)
#tickers_ = [i.split('-')[0] for i in files_ ]
#init_agent_bools = [False for _ in tickers_]
#initialized_agents_hash = dict(zip(tickers_,init_agent_bools))

#shorts and longs
short_df = pd.DataFrame([],columns=['ticker','Close'])
long_df = pd.DataFrame([],columns=['ticker','Close'])

def get_data(i,candles,ticker):
    """
    i: idx of candle
    candles: df holding candles data 
    ticker: symbol of interest
    """
    data = {
        'ticker':ticker,
        'trade_data': {
            'close' : candles['Close'].values[i],
            'volume': candles['Volume'].values[i],
            'open':  candles['Open'].values[i],
            'high': candles['High'].values[i],
            'low' : candles['Low'].values[i],
            'ma10' : candles['ma10'].values[i],
            'ma30' : candles['ma30'].values[i],
            'rsi' : candles['rsi'].values[i],
            'macd' : candles['macd'].values[i],
            'macd_signal' : candles['macd_signal'].values[i],
            'macd_hist' : candles['macd_hist'].values[i],
            'obv' : candles['Obv'].values[i]}
    }
    return data


async def get_resp(data,type,endpoint):
    '''
    data is json dumped data per index i 
    '''
    if type == 'POST':
        async with httpx.AsyncClient() as client:
            response = await client.post(f"http://localhost:8000/{endpoint}", json=data)
            return response.json()
    else:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:8000/{endpoint}")
            return response.json()
        

#####
timeout = 3600
t0 = time.perf_counter() 

#number of lots = stop_loss + risk
async def main():
    '''
    Main loop to test evaluate trade on 
    ALGO-USD, 15MINS ticker and resolution
    '''
    while (time.perf_counter() - t0) <= timeout:
        t_int = '15MINS'
        ticker = 'SOL-USD'
        candles = await get_candles_m(ticker,t_int)
        decision = await get_decision(candles,ticker)
        print(decision)
        if decision['status_code'] == 200:
            decision = decision['evaluation_result']
            print(decision)
        else:
            return 'Error'
        
        xx = random.randint(0,13)
        await asyncio.sleep(xx)


    # so problem is make_data req is not continously running so it runs 20x for each ticker
    # the intialized agents state needs to be saved in either the trading module or the fastapi module