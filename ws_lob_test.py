from wsv_handler import *
import asyncio
from asyncio import *
import os 
from OrderManager import OrderManager

if __name__ == '__main__':
    ETHEREUM_ADDRESS = os.getenv('eth_add')
    eth_add2 = os.getenv('eth_add2')
    pkey = os.getenv("pkey")
    pkey2 = os.getenv("pkey2")
    DYDX_API_CREDENTIALS = {
        "key": os.environ.get("ek2", ""),
        "secret": os.environ.get("es2", ""),
        "passphrase": os.environ.get("epp2", ""),
    }
    hstk = os.getenv("eck2")
    hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'], DYDX_API_CREDENTIALS['secret'],
                                                DYDX_API_CREDENTIALS['passphrase'], '0', hstk, eth_add2, None)

    hbotf.ws_obook_positions['XMR-USD'] = 'open'
    OrderManagerf= OrderManager(hbotf)

    #webSassistant_obook = wsAssistant(hbotf,['v3_orderbook'],4)
    #webSassistant_trades = wsAssistant(hbotf,['v3_trades'],0)
    webSassistant_accounts = wsAssistant(hbotf,['v3_accounts'],0)


    async def setup_ws_feeds():
        #await webSassistant_obook.initialize_data_instances()
        #await webSassistant_trades.initialize_data_instances()
        await webSassistant_accounts.initialize_data_instances()    
        print('wait one sec for feed to run')
        await asyncio.sleep(3)

    async def place_limit_order(ticker):
        global OrderManagerf
        side = 'SELL'
        order_type = 'LIMIT'
        ticker = list(hbotf.ws_obook_positions.keys())[0]
        markets = await hbotf.get_markets()
        rd = find_dec_places(markets[ticker]['tickSize'])
        step = find_dec_places(float(markets[ticker]['stepSize']))
        orderbook = await hbotf.get_orderbook(ticker)
        price = round(float(orderbook['asks'][3]['price']),rd)
        amount = round(100/float(orderbook['asks'][3]['price']),step)
        reduceOnly = False  
        order = await hbotf.place_order(ticker,side,amount,price,order_type,False,reduceOnly)
        t0 = time.perf_counter()
        await asyncio.sleep(0.5)
        id = order['order']['id']
        status = order['order']['status']
        await OrderManagerf._start_order_monitoring(ticker,side,amount,price,id,status,False,t0)
        return id, order
        
    
    async def websocket_order_updates():
        global OrderManagerf
        while True:
            for ticker in hbotf.ws_obook_positions.keys():
                account_data = await webSassistant_accounts.get_data_by_channel_and_ticker('v3_accounts', ticker)
                if account_data:
                    ticker_account_data = await webSassistant_accounts.account_order_get(ticker, account_data)
                    await OrderManagerf.receive_order_update(
                        ticker_account_data['id'],
                        ticker_account_data['status'],
                        ticker_account_data['remainingSize']
                    )
            await asyncio.sleep(2)  # Adju

    async def main():
        global OrderManagerf
        await setup_ws_feeds()
        asyncio.create_task(websocket_order_updates())
        ticker = list(webSassistant_accounts.ws_obook_positions.keys())[0]
        id, order = await place_limit_order(ticker)
        while  OrderManagerf.orders:
             await asyncio.sleep(1)

    asyncio.run(main())