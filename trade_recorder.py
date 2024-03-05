
import pandas as pd 
from bson.binary import Binary, UUID_SUBTYPE
import uuid 
import datetime as dt 
from savepymongo import write_df_to_mongoDB
import time 

def get_time():
    current_time = dt.datetime.now()
    formatted_time = f"{current_time.year}{current_time.month:02d}{current_time.day:02d}_{current_time.hour:02d}{current_time.minute:02d}{current_time.second:02d}"
    return formatted_time

class Trade_recorder:
    def __init__(self,logger):
        self.logger = logger
        self.df_records = pd.DataFrame([],columns=['Wins','Loss','Symbol','price_entry',\
        'price_exit','uuid','time_entry','time_exit','type','t_int','cumulative_balance','strategy'])

    async def trade_enter(self,symb,j,side,price,t_int,qt,p_l,strat):
        self.df_records.loc[j,'Symbol'] = symb
        self.df_records.loc[j,'price_entry'] = price
        self.df_records.loc[j,'type'] = side
        self.df_records.loc[j,'t_int'] = t_int
        self.df_records.loc[j,'coins'] = qt
        self.df_records.loc[j,'strategy'] = strat
        time_ = get_time()
        self.df_records.loc[j,'time_entry'] = time_
        self.df_records.loc[j,'uuid'] = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)

    async def exit_trade(self,j,price,p_l,t0):
        self.df_records.loc[j,'price_exit'] = price
        time_ = get_time()
        self.df_records.loc[j,'time_exit'] = time_ 
        await self.duration(j,t0)
        if p_l < 0:
            self.df_records.loc[j,'Loss'] = p_l
            print(self.df_records)
            try:
                write_df_to_mongoDB(self.df_records,'local','recs')
            except Exception as e:
                print(f"Error writing Pymongo db {e}")

            
            return self.df_records
        else:
            self.df_records.loc[j,'Wins'] = p_l
            try:
                write_df_to_mongoDB(self.df_records,'local','recs')
            except Exception as e:
                print(f"Error writing Pymongo db {e}")
            
            print(self.df_records)
            return self.df_records

    async def duration(self,j,t0):
        self.df_records.loc[j,'duration'] = (time.perf_counter() - t0)
