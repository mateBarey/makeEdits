import asyncio
import json
import time 
import math
from pprint import pprint
from functools import partial
import os
import pandas as pd 
import talib as ta
from wsv_handler import * 
from decimal import * 
from BatchRequests import BatchRequestProcessor
from dydx3 import Client
from dydx3.errors import DydxApiError
from dydx3.starkex.order import SignableOrder
from dydx3.helpers.request_helpers import generate_now_iso, remove_nones

BASE_URL = 'https://api.dydx.exchange'
def find_dec_places(pr):
    try:
        #if int(pr) == 1:
        #    return 0
        places =  str(pr)[::-1].find('.')
        #if places  < 0:
        #    places = 1

        return places 
    except:
        return 0
    
def quanitze_price_or_amount(price_amount):
    return Decimal(price_amount).quantize(Decimal("0.00001"))

def get_indicators(data):
    # Get MACD
    data["macd"], data["macd_signal"], data["macd_hist"] = ta.MACD(data['Close'])
    
    # Get MA10 and MA30
    data["ma10"] = ta.MA(data["Close"], timeperiod=10)
    data["ma30"] = ta.MA(data["Close"], timeperiod=30)
    
    # Get RSI
    data["rsi"] = ta.RSI(data["Close"], timeperiod=14)
    
    #OBV
    data["Obv"] = ta.OBV(data["Close"],data["Volume"])
    
    #atr
    data["atr"] = ta.ATR(data["High"].values,data["Low"].values,data["Close"].values,timeperiod=20)
    return data

def fpath(path,method):
    return f'{method} {path}'

def clamp(value, minvalue, maxvalue):
    return max(minvalue, min(value, maxvalue))

class DydxPerpetualAsyncAPIError(DydxApiError):
    def __init__(self, status_code, msg):
        self.status_code = status_code
        self.msg = msg
        self.response = None
        self.request = None


class DydxPerpetualClientWrapper:
    def __init__(self, api_key, api_secret, passphrase, account_number, stark_private_key, ethereum_address,logger):
        self._api_credentials = {'key': api_key,
                                 'secret': api_secret,
                                 'passphrase': passphrase}
        self.client = Client(host = BASE_URL,
                             api_key_credentials = self._api_credentials,
                             stark_private_key = stark_private_key)
        self._loop = asyncio.get_event_loop()
        self._ethereum_address = ethereum_address
        self._account_number = account_number
        self.logger = logger
        self.ws_market_positions = {}
        self.ws_obook_positions = {}
        self.active_programs = 0
        self.BatchProcessor = BatchRequestProcessor(0.05)
        self.batch_requests = {}
        self.rate_limits = {'get':175}
        self.ws_rate_limits = {'rate_limit':2,'window_sec':1,'ping':5}
        self.fees = {'maker':0.02/100,'taker':0.05/100}
        self.ws_trades = {} # not sure about this
        self.semaphore = asyncio.Semaphore(7)
        self._dydx_perpetual_api_key = api_key
        self._dydx_perpetual_api_secret = api_secret
        self._dydx_perpetual_passphrase = passphrase
        self._dydx_perpetual_ethereum_address = ethereum_address
        self._dydx_perpetual_stark_private_key = stark_private_key
        self._dydx_stark_private_key = (
            None if stark_private_key == "" else stark_private_key
        )

        self._dydx_client = None
    @property
    def dydx_client(self):
        if self._dydx_client is None:
            api_credentials = {
                "key": self._dydx_perpetual_api_key,
                "secret": self._dydx_perpetual_api_secret,
                "passphrase": self._dydx_perpetual_passphrase,
            }

            self._dydx_client = Client(
                host=BASE_URL,
                api_key_credentials=api_credentials,
                stark_private_key=self._dydx_stark_private_key,
            )
        return self._dydx_client
    #async def total_val(self):
        #f = await self.get_account().data['account']['equity']
        #return float(f)
    @property
    def api_credentials(self):
        return self._api_credentials

    @property
    def account_number(self):
        return self._account_number
    
    def get_order_signature(
        self,
        position_id: str,
        client_id: str,
        market: str,
        side: str,
        size: str,
        price: str,
        limit_fee: str,
        expiration_epoch_seconds: int,
    ) -> str:
        order_to_sign = SignableOrder(
            network_id=self.dydx_client.network_id,
            position_id=position_id,
            client_id=client_id,
            market=market,
            side=side,
            human_size=size,
            human_price=price,
            limit_fee=limit_fee,
            expiration_epoch_seconds=expiration_epoch_seconds,
        )
        order_signature = order_to_sign.sign(self._dydx_perpetual_stark_private_key)
        return order_signature
    
    async def rest_authenticate(self, request):
        ts = generate_now_iso()

        endpoint_url = request.url.replace(BASE_URL, "")
        if request.params is not None:
            request_path = generate_query_path(endpoint_url, request.params)
        else:
            request_path = endpoint_url

        data = request.data if request.data is not None else "{}"

        signature = self.dydx_client.private.sign(
            request_path=request_path,
            method=str(request.method),
            iso_timestamp=ts,
            data=remove_nones(json.loads(data)),
        )

        headers = {
            "DYDX-SIGNATURE": signature,
            "DYDX-API-KEY": self._dydx_perpetual_api_key,
            "DYDX-TIMESTAMP": ts,
            "DYDX-PASSPHRASE": self._dydx_perpetual_passphrase,
        }

        if request.headers is not None:
            headers.update(request.headers)

        request.headers = headers
        return request

    async def correct_channel_path(self,channel):
        if 'orders' in channel:
            return '/v3/orders'
        return f'v3/{channel}'
    
    async def setup_sig(self,channel,ticker=None):
        correct_channel_path_f = await self.correct_channel_path(channel)
        signature = self.client.private.sign(
            request_path = f'{correct_channel_path_f}',
            method = 'POST',
            iso_timestamp = generate_now_iso(),
            data = {},
            )
        return signature 

    async def place_order(self, market, side, amount, price, order_type, postOnly,reduceOnly):
        path = 'v3/orders'
        method = 'POST'
        path = fpath(path,method)
        async with self.semaphore:

            config_data = {'collateralAssetId': '0x02893294412a4c8f915f75892b395ebbf6859ec246ec365c3b1f56f47c3a0a5d',\
                        'collateralTokenAddress': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'defaultMakerFee': '0.0002',\
                        'defaultTakerFee': '0.0005', 'exchangeAddress': '0xD54f502e184B6B739d7D27a6410a67dc462D69c8', \
                        'maxExpectedBatchLengthMinutes': '720', 'maxFastWithdrawalAmount': '200000',\
                        'cancelOrderRateLimiting': {'maxPointsMulti': 3, 'maxPointsSingle': 9500, 'windowSecMulti': 10, \
                        'windowSecSingle': 10}, 'placeOrderRateLimiting': {'maxPoints': 1750, 'windowSec': 10, \
                        'targetNotional': 40000, 'minLimitConsumption': 4, 'minMarketConsumption': 20, 'minTriggerableConsumption': 100, \
                        'maxOrderConsumption': 100}}

            g = await self.get_markets()
            #rd = find_dec_places(g[market]['tickSize'])
            step = find_dec_places(g[market]['stepSize'])
            amount = round(amount,step)
            orderNotional = amount * price 
            if order_type == 'LIMIT':
                minOrderConsumption = config_data['placeOrderRateLimiting']['minLimitConsumption']
                fee = (self.fees['maker'])
            if order_type == 'MARKET' or  order_type == 'TAKE_PROFIT' or order_type == 'STOP_LIMIT':
                minOrderConsumption = config_data['placeOrderRateLimiting']['minMarketConsumption']
                fee = (self.fees['taker'])
            amount = str(quanitze_price_or_amount(amount))
            price = str(quanitze_price_or_amount(price))
            fee = str(round(orderNotional * fee,2))
            expiration = int(time.time()) + 600
            orderConsumption = clamp(math.ceil(config_data['placeOrderRateLimiting']['targetNotional'] / orderNotional),minOrderConsumption, config_data['placeOrderRateLimiting']['maxOrderConsumption'])
            rate_limit = (config_data['placeOrderRateLimiting']['maxPoints'] - orderConsumption ) 
            account = await self.get_account()

            time_in_force = 'IOC' if (order_type == 'MARKET' or reduceOnly) else 'GTT'
            trailing_percent = 0 if order_type == 'MARKET' else None
            if order_type == 'TAKE_PROFIT' or order_type == 'STOP_LIMIT':
                async def request_function():
                    return await self._loop.run_in_executor(None, partial(self.client.private.create_order,
                                                            position_id=account['account']['positionId'],
                                                            market=market,
                                                            side=side,
                                                            size=amount,
                                                            price = price,
                                                            trigger_price = price,
                                                            order_type=order_type,
                                                            post_only=postOnly,
                                                            limit_fee=fee,
                                                            expiration_epoch_seconds=expiration,
                                                            time_in_force=time_in_force,
                                                            trailing_percent=trailing_percent,
                                                            reduce_only=reduceOnly
                                                            ))
                
            else:
                async def request_function():
                    return await self._loop.run_in_executor(None, partial(self.client.private.create_order,
                                                            position_id=account['account']['positionId'],
                                                            market=market,
                                                            side=side,
                                                            size=amount,
                                                            price = price,
                                                            order_type=order_type,
                                                            post_only=postOnly,
                                                            limit_fee=fee,
                                                            expiration_epoch_seconds=expiration,
                                                            time_in_force=time_in_force,
                                                            trailing_percent=trailing_percent,
                                                            reduce_only=reduceOnly
                                                            ))
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            if reduceOnly:
                priority = True 
            else:
                priority = False 
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data 
        
    async def cancel_all_orders(self, market):
        path = 'v3/orders'
        method = 'DELETE'
        path = fpath(path,method)

        '''
        DELETE v3/orders requests are limited to 3 requests per 10 seconds per asset-pair.
        cancel_all_orders(market=MARKET_BTC_USD)
        market 	(Optional) Market of the orders being canceled.
        '''
        async with self.semaphore: 
            rate_limit = 3
            priority = False 
            reduceOnly = False 

            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.cancel_all_orders,
                                                     market=market))
                
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
           
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data 
        
    async def get_all_trades(self, market):
        path = 'v3/orders'
        method = 'GET'
        path = fpath(path,method)

        '''
        DELETE v3/orders requests are limited to 3 requests per 10 seconds per asset-pair.
        cancel_all_orders(market=MARKET_BTC_USD)
        market 	(Optional) Market of the orders being canceled.
        '''
        async with self.semaphore: 
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            rate_limit = self.rate_limits['get']

            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.public.get_trades,
                                                     market=market))
           
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data

    async def cancel_order(self,id):
        path = 'v3/orders/:id'
        method = 'DELETE'
        path = fpath(path,method)
        '''
        DELETE v3/orders/:id requests are limited to 250 requests per 10 seconds per asset-pair.
        client.private.cancel_order(order_id='0x0000')
        orderId 	Unique id of the order to be canceled.
        '''
        async with self.semaphore:
            rate_limit = 25
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            
            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.cancel_order,
                                                        order_id=id))
            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data

    async def cancel_active_orders(self,market,side=None,id=None):
        path = 'v3/active-orders'
        method = 'DELETE'
        '''
        DELETE Active-Orders Rate Limits
        market_side_orders = client.private.cancel_active_orders(
        market=MARKET_BTC_USD,
        side=ORDER_SIDE_SELL,
        )
        market 	Market of the order.
        side 	(Optional) Either BUY or SELL. This parameter is required if id is included.
        id 	(Optional) The unique id assigned by dYdX. Note, if id is not found, will return a 400.
        DELETE v3/active-orders/*

            425 points allotted per 10 seconds per market.
            1 point consumed if order id included.
            25 points consumed if order side included.
            50 points consumed otherwise.

        '''
        async with self.semaphore:
            #market is required , id and side are optional
            rate_limit = 425
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 

            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.cancel_active_orders,
                                                        id=id,
                                                        market = market,
                                                        side = side))
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data

    async def get_my_positions(self):
        path = 'v3/positions'
        method = 'GET'
        path = fpath(path,method)
        async with self.semaphore:
           
            rate_limit = self.rate_limits['get']
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 

            async def request_function():
                return await self._loop.run_in_executor(None, self.client.private.get_positions)
            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data
    

    async def get_active_orders(self, market,side=None,id=None):
        path = 'v3/active-orders'
        method = 'GET'
        path = fpath(path,method)
        '''
        GET v3/active-orders/*
        client.private.get_active_orders(
            market=MARKET_BTC_USD,
            side=ORDER_SIDE_SELL,
            id optional
        )
        175 points allotted per 10 seconds per market.
        1 point consumed if order id included.
        3 points consumed if order side included.
        5 points consumed otherwise.
        '''
        async with self.semaphore:
            self.active_programs += 1
            #market is required , id and side are optional
            rate_limit = 175         
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.get_active_orders,
                                                        market=market,
                                                        side=side,
                                                        id=id))
            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data
    


    async def get_order(self, id):
        path = 'v3/orders/:id'
        method = 'GET'
        path = fpath(path,method)
        '''
        get v3/orders/:id
        '''
        async with self.semaphore:
            rate_limit = self.rate_limits.get('get')
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.get_order_by_id,
                                                     order_id=id))
            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data
    

    async def get_markets(self):
        path = 'v3/markets'
        method = 'GET'
        path = fpath(path,method)
        async with self.semaphore:
            rate_limit = self.rate_limits['get']
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            async def request_function():
                return await self._loop.run_in_executor(None, self.client.public.get_markets)
            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data['markets']
            
        
    async def get_config(self):
        path = 'v3/config'
        method = 'GET'
        path = fpath(path,method)
        async with self.semaphore:
            rate_limit = self.rate_limits['get']
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            async def request_function():
                return await self._loop.run_in_executor(None, self.client.public.get_config)
            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data
        
    async def get_fills(self, exchange_order_id):
        path = 'v3/fills'
        method = 'GET'
        path = fpath(path,method)
        async with self.semaphore:
            rate_limit = self.rate_limits['get']
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.get_fills,
                                                     order_id=exchange_order_id,
                                                     limit=100))
            
            await self.BatchProcessor.add_request(uid, path, request_function,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data
        

    async def get_account(self):
        path = 'v3/accounts/:id'
        method = 'GET'
        path = fpath(path,method)
        async with self.semaphore:
                       
            rate_limit = self.rate_limits['get']
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.private.get_account,
                                                     ethereum_address=self._ethereum_address))            
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data
        
    async def get_candles(self,market,res):
        path = 'v3/candles/:market'
        method = 'GET'
        path = fpath(path,method)
        async with self.semaphore:
            rate_limit = self.rate_limits['get']

            async def request_function():
                return await self._loop.run_in_executor(None,partial(self.client.public.get_candles,market=market,resolution=res))
            uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
            priority = False 
            reduceOnly = False 
         
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            candles = pd.DataFrame(response.data['candles'])
            candles = candles.rename(columns={'close': 'Close','open':'Open','low':'Low','high':'High','usdVolume':'Volume'})
            candles["Close"] = candles.apply(lambda row: float(row.Close), axis=1)
            candles["Open"] = candles.apply(lambda row: float(row.Open), axis=1)
            candles["High"] = candles.apply(lambda row: float(row.High), axis=1)
            candles["Low"] = candles.apply(lambda row: float(row.Low), axis=1)
            candles["Volume"] = candles.apply(lambda row: float(row.Volume), axis=1)

            candles['percent_change'] = candles.Close.diff()
            candles['vol_change'] = candles.Volume.diff()
            candles = candles[::-1]
            candles = candles.reset_index()
            candles = get_indicators(candles)
            return candles 
                    
    async def get_orderbook(self,market):
        path = 'v3/orderbook/:market'
        method = 'GET'
        path = fpath(path,method)
        rate_limit = self.rate_limits['get']    
        uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
        priority = False 
        reduceOnly = False 
        async with self.semaphore:
            async def request_function():
                return await self._loop.run_in_executor(None, partial(self.client.public.get_orderbook,
                                                        market=market))
            await self.BatchProcessor.add_request(uid, path, request_function ,rate_limit,reduceOnly, priority)
            await self.BatchProcessor.execute()
            response = await self.BatchProcessor.get_response(uid)
            return response.data 
        

    async def check_volumes(self,market):
        vol_1_h = await self.get_candles(market,'1HOUR')
        return abs(vol_1_h.vol_change.iloc[-2]) > 40000
    
    async def get_minSize(self,ticker):
        markets = await  self.get_markets()
        markets_ = [i for i in list(markets.keys()) if 'LUNA-USD' != i ]
        idx = markets_.index(ticker)
        minOrderSize = [markets[i]['minOrderSize'] for i in markets_]
        return  minOrderSize[idx]
    


    def sign(self, request_path, method, timestamp, data):
        sign = self.client.private.sign(request_path=request_path,
                                        method=method,
                                        iso_timestamp=timestamp,
                                        data=data)
        return sign
    
if __name__ == '__main__':
    ETHEREUM_ADDRESS =  os.getenv("eth_add")
    eth_add2 =  os.getenv("eth_add2")
    pkey = os.getenv("pkey")
    pkey2 = os.getenv("pkey2")
    DYDX_API_CREDENTIALS = {
        "key": os.environ.get("ek2", ""),
        "secret": os.environ.get("es2", ""),
        "passphrase": os.environ.get("epp2", ""),
    }
    hstk = os.getenv("eck2")
    client = Client(host='https://api.dydx.exchange',stark_private_key=hstk)

    hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'],DYDX_API_CREDENTIALS['secret'],DYDX_API_CREDENTIALS['passphrase'],'0',hstk,eth_add2,None)
    ticker = 'RUNE-USD'

    async def get_stuff(ticker):
        obook = await hbotf.get_markets()
        print(obook)

    async def make_order(market):
        g = await hbotf.get_markets()
        rd = find_dec_places(g[ticker]['tickSize'])
        step = find_dec_places(float(g[ticker]['stepSize']))
        f = await hbotf.get_orderbook(market)
        price = round(float(f['asks'][3]['price']),rd)
        amount = round(100/float(f['asks'][3]['price']),step)
        reduceOnly = False
        print(market,'SELL',amount,price,'LIMIT',False,reduceOnly)
        order = await hbotf.place_order(market,'SELL',amount,price,'LIMIT',False,reduceOnly)
        await asyncio.sleep(1.5)
        id = order['order']['id']
        print(order)
        #get_active_ord = await hbotf.get_active_orders(market=market,side='SELL',id=id)

        #print(f'get_act_ord_resp {get_active_ord}')
        #make a cancel order
        order = await hbotf.cancel_active_orders(market=market,side='SELL',id=id)
        #await asyncio.sleep(0.1)
        return order
    
    async def get_candles_all():
        markets = await hbotf.get_markets()
        candles_hash = {}
        for market in  markets.keys():
            candles_hash[market] = await hbotf.get_candles(market,'1MIN')
        return candles_hash['BTC-USD']
    
    async def get_market(market,res):
        return await hbotf.get_candles(market,res)

    async def check_Vvolumes(ticker):
        return await hbotf.check_volumes(ticker)
    

    ticker = 'LTC-USD'
    res = '15MINS'
    data = asyncio.get_event_loop().run_until_complete(get_market(ticker,res))
    
    print(data.Close[-3:])
    print((data.Open[-3:]))