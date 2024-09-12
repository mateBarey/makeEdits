import onnxruntime as ort
import numpy as np
import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from agent_mod_onnx import *

def create_model_hash():
    # Path to your ONNX model
    onnx_model_path ='./model.onnx'
    
    # Create an ONNX Runtime Inference session for the model
    ort_session = ort.InferenceSession(onnx_model_path)
    
    drv_ = './tickers_train'
    files_ = os.listdir(drv_)
    
    def make_model_props(ticker):
        file_need = os.path.join(drv_, next(i for i in files_ if ticker in i))
        df = pd.read_csv(file_need)
        df = df.rename(columns={'usdVolume': 'Volume'})
        df = get_indicators(df)  # Assuming this function is defined elsewhere
        real_trend = df.Close.tolist()
        parameters = [df['Close'].to_list(), df['Volume'].to_list(), df['Open'].to_list(), df['High'].to_list(), df['Low'].to_list(), df['rsi'].to_list(), df['macd'].to_list(), df['macd_signal'].to_list(), df['macd_hist'].to_list(), df['ma10'].to_list(), df['ma30'].to_list(), df['Obv'].to_list()]
        minmax = MinMaxScaler(feature_range=(df.Close.min(), df.High.max())).fit(np.array(parameters).T)
        scaled_parameters = minmax.transform(np.array(parameters).T).T.tolist()
        initial_money = np.max(parameters[0]) * 3
        max_buy = 2
        
        # Here we create an Agent with a 'None' model, assuming Agent can handle an ONNX session
        # You will need to adjust Agent's methods to use the ONNX session for predictions
        agent = Agent(model=ort_session, timeseries=scaled_parameters, skip=1, initial_money=initial_money, real_trend=real_trend, minmax=minmax, max_buy=max_buy)
        
        return df, real_trend, parameters, minmax, scaled_parameters, initial_money, agent
    
    tickers_ = [i.split('-')[0] for i in files_]
    model_props = [make_model_props(i) for i in tickers_]
    model_hash = dict(zip(tickers_, model_props))
    
    return model_hash
