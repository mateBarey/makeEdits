import asyncio
import pandas as pd 
import os 
from sklearn.preprocessing import MinMaxScaler
import dill 
import xgboost 
import random 
import time 
from datetime import timedelta
import datetime 


async def make_pred(ticker,hbotf,o_book_0):
    """
    Make predictions based on XGB classsifier model for Bid Ask Crossover
:ticker is symbol being evaluated
    :hbotf is the async class for interacting with trading api
    :o_book_0 is snapshot of orderbook at time 0
    """
    # Load scaler and model from disk
    #Load Pred Model properties
    scaler_path = r"C:\Users\grc\Downloads\lob\xgbmodelf\scaler.pkl"
    model_path = r"C:\Users\grc\Downloads\lob\xgbmodelf\best_model.pkl"
    scaler = dill.load(open(scaler_path, "rb"))
    loaded_model = dill.load(open(model_path, "rb"))
    o_book_1 = await hbotf.get_orderbook(ticker)
    df = pd.DataFrame(0, index=range(1), columns=['B_1', 'B_2', 'B_3', 'B_4', 'B_5', 'A_1', 'A_2', 'A_3', 'A_4', 'A_5',
   'B_1_lag1', 'B_2_lag1', 'B_3_lag1', 'B_4_lag1', 'B_5_lag1', 'A_1_lag1',
   'A_2_lag1', 'A_3_lag1', 'A_4_lag1', 'A_5_lag1', 'Bd', 'Ad', 'Pb', 'Pa',
   'Pb_lag1', 'Pa_lag1', 'Pbd', 'Pad', 'B_sum', 'A_sum', 'B_sum_lag1',
   'A_sum_lag1', 'Bsd', 'Asd'])
    #create l2 features
    for i in range(1,6):
        df[f'B_{i}'] = float(o_book_1['bids'][i-1]['size'])
        df[f'A_{i}'] = float(o_book_1['asks'][i-1]['size'])
        df[f'B_{i}_lag1'] = float(o_book_0['bids'][i-1]['size'])
        df[f'A_{i}_lag1'] = float(o_book_0['asks'][i-1]['size'])
    #create l3 features
    df['B_sum'] = df[[f'B_{i}' for i in range(1, 6)]].sum(axis=1)
    df['A_sum'] = df[[f'A_{i}' for i in range(1, 6)]].sum(axis=1)
    df['B_sum_lag1'] = df[[f'B_{i}_lag1' for i in range(1, 6)]].sum(axis=1)
    df['A_sum_lag1'] = df[[f'A_{i}_lag1' for i in range(1, 6)]].sum(axis=1)
    #create price features best bid and best ask at time t 
    df['Pb'] = float(o_book_1['bids'][0]['price'])
    df['Pa'] = float(o_book_1['asks'][0]['price'])
    df['Pb_lag1'] = float(o_book_0['bids'][0]['price'])
    df['Pa_lag1'] = float(o_book_0['asks'][0]['price'])
    # Imbalance Features
    df['Bd'] = df['B_1'] - df['B_1_lag1']
    df['Ad'] = df['A_1'] - df['A_1_lag1']
    df['Bsd'] = df['B_sum'] - df['B_sum_lag1']
    df['Asd'] = df['A_sum'] - df['A_sum_lag1']
    df['Pbd'] = df['Pb'] - df['Pb_lag1']
    df['Pad'] = df['Pa'] - df['Pa_lag1']

    df = df.reset_index(drop=True)
    X_pred = scaler.transform(df)
    y_pred = loaded_model.predict(X_pred)
    return y_pred.item()