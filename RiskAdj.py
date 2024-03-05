import asyncio 
import pandas as pd 

class RiksAdj:
    def __init__(self,df):
        self.df = df 

    async def _filt(self):
        df = self.df.copy()
        df['Wins'] = self.df.Wins.astype(str)
        df['Loss'] = self.df.Loss.astype(str)
        new_df = df[(df.Wins != 'NaN') & (df.Loss != 'NaN')]
        return pd.DataFrame(new_df)
    
    async def _count(self,obj):
        win = obj.Wins
        loss = obj.Loss
        symbol = obj.Symbol
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
        
    async def _find_risk(self, df):
        if df.shape[0] < 2:
            return True  # Default value when there's not enough data

        last_two = df[-2:]
        last_two['W_L'] = last_two.apply(lambda row: await self._count(row), axis=1)

        num_wins = (last_two['W_L'] == 'Win').sum()
        num_losses = (last_two['W_L'] == 'Loss').sum()

        if num_wins == 2:
            return True
        elif num_losses == 2:
            return False
        else:
            # Recursively call _find_risk with the dataframe excluding the last row
            return await self._find_risk(df[:-1])