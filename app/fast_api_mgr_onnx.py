import os 
from fastapi import FastAPI, HTTPException, Request, Path 
from fastapi.responses import *
import asyncio
from asyncio import * 
from pydantic import BaseModel 
from LoggerC import Logger
from exec_mod_onnx import * 
import uvicorn

#create model_hash
model_hash = create_model_hash()

#set logs
log_directory = r'C:\Users\grc\Downloads\Logs Decision'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
log_file = os.path.join(log_directory, 'Decision_Logs.csv')
logger = Logger(log_file)

#initialize fastapi
app = FastAPI()

# Semaphore for concurrency control
sem: asyncio.Semaphore = asyncio.Semaphore()

class TradeData(BaseModel):
    close: float
    volume: float
    open: float
    high: float
    low: float
    rsi: float
    macd: float
    macd_signal: float
    macd_hist: float
    ma10: float
    ma30: float
    obv: float

class CryptoData(BaseModel):
    ticker: str
    trade_data: TradeData


@app.post("/evaluate_trade")
async def evaluate_trade(data: CryptoData):

    trade_data_list = [
        data.trade_data.close,
        data.trade_data.volume,
        data.trade_data.open,
        data.trade_data.high,
        data.trade_data.low,
        data.trade_data.rsi,
        data.trade_data.macd,
        data.trade_data.macd_signal,
        data.trade_data.macd_hist,
        data.trade_data.ma10,
        data.trade_data.ma30,
        data.trade_data.obv
    ]
  
      
    ticker = data.ticker.split('-')[0]
    
    if ticker not in model_hash:
        raise HTTPException(status_code=404, detail="No data found for the provided ticker")
    try:
        # Retrieve the agent and other properties for the ticker from model_hash
        _, _, _, _, _, _, agent = model_hash[ticker]
        evaluation_result = await agent.evaluate_trade(trade_data_list)
        return {"status_code": 200,"evaluation_result": evaluation_result}
    except Exception as e:
        print(f'Error acessing Agent Hash for ticker {ticker}')
        logger.log('Error', f'An error ocurred: {e}')
        return JSONResponse(content={"error": f"Error accessing Agent Hash for ticker {ticker}: {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    # Ensure your Agent class has a method to evaluate trades based on the incoming trade data
    


@app.get("/inventory/{ticker}")
async def get_inventory(ticker: str = Path(..., title="The Ticker", description="The ticker of the target cryptocurrency")):    
    if ticker not in model_hash:
        raise HTTPException(status_code=404, detail="No data found for the provided ticker")
    try:
        # Retrieve the agent and other properties for the ticker from model_hash
        _, _, _, _, _, _, agent = model_hash[ticker]
        inventory = agent._inventory
        return {"status_code": 200,"inventory": inventory}
    except Exception as e:
        print(f'Error acessing Agent Hash for ticker {ticker}')
        logger.log('Error', f'An error ocurred: {e}')
        return JSONResponse(content={"error": f"Error accessing Agent Hash for ticker {ticker}: {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@app.get("/queue/{ticker}")
async def queue(ticker: str = Path(..., title="The Queue for the current Ticker", description="The queue for the agent evaluating the current ticker")):    
    if ticker not in model_hash:
        raise HTTPException(status_code=404, detail="No data found for the provided ticker")
    try:
        # Retrieve the agent and other properties for the ticker from model_hash
        _, _, _, _, _, _, agent = model_hash[ticker]
        queue = agent._queue
        return {"status_code": 200,"queue": queue}
    except Exception as e:
        print(f'Error acessing Agent Hash for ticker {ticker}')
        logger.log('Error', f'An error ocurred: {e}')
        return JSONResponse(content={"error": f"Error accessing Agent Hash for ticker {ticker}: {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@app.get("/balance/{ticker}")
async def balance(ticker: str = Path(..., title="The Balance for the current Ticker", description="The balance for the agent evaluating the current ticker")):    
    if ticker not in model_hash:
        raise HTTPException(status_code=404, detail="No data found for the provided ticker")
    try:
        # Retrieve the agent and other properties for the ticker from model_hash
        _, _, _, _, _, _, agent = model_hash[ticker]
        balance = agent._capital
        return {"status_code": 200,"balance": balance}
    except Exception as e:
        print(f'Error acessing Agent Hash for ticker {ticker}')
        logger.log('Error', f'An error ocurred: {e}')
        return JSONResponse(content={"error": f"Error accessing Agent Hash for ticker {ticker}: {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/reset/{ticker}/{capital}")
async def reset(ticker: str = Path(..., title="Reset Agent to initial conditions for the current Ticker", description="Resetting the agent for the current ticker"),
                capital: str = Path(..., title="Reset Capital for Agent to initial conditions for the current Ticker", description="Resetting the agent for the current ticker")):    
    if ticker not in model_hash:
        raise HTTPException(status_code=404, detail="No data found for the provided ticker")

    try:
        # Convert capital to float and handle invalid input
        capital_float = float(capital)
    except ValueError:
        return JSONResponse(content={"error": "Invalid capital input, must be a valid number"}, status_code=status.HTTP_400_BAD_REQUEST)

    try:
        # Retrieve the agent and other properties for the ticker from model_hash
        _, _, _, _, _, _, agent = model_hash[ticker]
        agent.reset_capital(capital_float)
        return {"balance": agent._capital}  # return the updated capital
    except Exception as e:
        print(f'Error accessing Agent Hash for ticker {ticker}')
        # logger.log('Error', f'An error occurred: {e}')
        # Ensure logger is initialized and configured if you use it
        return JSONResponse(content={"error": f"Error accessing Agent Hash for ticker {ticker} or error with capital: {str(e)}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

logger.write_to_csv()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)