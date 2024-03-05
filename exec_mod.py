import pandas as pd 
from sklearn.preprocessing import MinMaxScaler
import dill 
import numpy as np 
import os 
from agentmod import * 

def create_model_hash():
    with open(r"C:\Users\grc\Downloads\model4cryptos15m.pkl", 'rb') as fopen:
        model = dill.load(fopen)
    drv_ = r'C:\Users\grc\Downloads\tickers_train'
    files_ = os.listdir(drv_)
    def make_model_props(ticker):
        file_need = drv_ + f'\{[i for i in files_ if ticker in i][0]}'
        df = pd.read_csv(file_need)
        df = df.rename(columns={'usdVolume': 'Volume'})
        df = get_indicators(df)
        real_trend = df.Close.tolist()
        parameters = [df['Close'].to_list(), df['Volume'].to_list(),df['Open'].to_list(),df['High'].to_list(),df['Low'].to_list(),df['rsi'].to_list(),df['macd'].to_list(),df['macd_signal'].to_list(),df['macd_hist'].to_list(),df['ma10'].to_list(),df['ma30'].to_list(),df['Obv'].to_list()]
        minmax = MinMaxScaler(feature_range = (df.Close.min(), df.High.max())).fit(np.array(parameters).T)
        scaled_parameters = minmax.transform(np.array(parameters).T).T.tolist()
        initial_money = np.max(parameters[0]) * 3
        max_buy = 2
        agent = Agent(model = model,
                    timeseries = scaled_parameters,
                    skip = 1,
                    initial_money = initial_money,
                    real_trend = real_trend,
                    minmax = minmax,
                    max_buy = max_buy)
        return df, real_trend, parameters, minmax, scaled_parameters, initial_money, agent 

    tickers_ = [i.split('-')[0] for i in files_]
    model_props = [make_model_props(i) for i in tickers_]
    model_hash = dict(zip(tickers_,model_props))
    return model_hash
