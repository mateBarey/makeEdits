import pandas as pd 
import uuid

def filt_(df):

    df = df.fillna(0)
    new_df = df[~((df.Wins == 0) &  (df.Loss == 0)) ]

    zdf = pd.DataFrame(new_df)
    return zdf

def get_risk(df):
    new_df = filt_(df)
    def count_muhh(obj):
        win = obj.Wins
        loss = obj.Loss
        symbol = obj.Symbol
            # Convert win to float

        try:
            win = float(win)
            loss = float(loss)
        except ValueError:
            print(f"Cannot convert win value: {win} or loss value: {loss} to float for symbol {symbol}")
            return None
        if win > 0:
            return 'Win'
        if loss < 0:
            return 'Loss'

    def find_risk(df):
        if df.shape[0] < 2: #not enough if data if less then 2 rows 
            return True  # or whatever default value you want when there's not enough data

        last_two = df[-2:]
        last_two['W_L'] = last_two.apply(lambda row: count_muhh(row), axis=1)

        num_wins = (last_two['W_L'] == 'Win').sum()
        num_losses = (last_two['W_L'] == 'Loss').sum()

        if num_wins == 2:
            return True
        elif num_losses == 2:
            return False
        else:
            # Recursively call find_risk with the dataframe excluding the last row
            return find_risk(df[:-1])
    
    if len(new_df) > 3:
        result = find_risk(new_df)
        return result 
    else:
        print('Insufficient Data')
        return True 
# base case 2 wins  or 2 loss  risk on risk half 

# or 1 win and 1 loss  risk on 