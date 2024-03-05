from wsv_handler55 import *
import asyncio
from asyncio import *
import os 

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

    #hbotf.ws_market_positions['XMR-USD'] = 'open'
    #hbotf.ws_market_positions['ETH-USD'] = 'open'
    hbotf.ws_obook_positions['FIL-USD'] = 'open'

    webSassistant_obook = wsAssistant(hbotf,['v3_orderbook'],4)
    webSassistant_trades = wsAssistant(hbotf,['v3_trades'],0)
    webSassistant_accounts = wsAssistant(hbotf,['v3_accounts'],0)
    webSassistant_markets = wsAssistant(hbotf,['v3_markets'],0)


    async def _process_data(item):
        print(f'order_book_data for {item[0]} {item[2]}th iteration: {item[1]} at time {item[3]}')

    async def main_obook():
        global webSassistant_obook

        await webSassistant_obook.initialize_data_instances()
        print('wait one sec for feed to run')
        await asyncio.sleep(3)
        while webSassistant_obook.ws_obook_positions:
            
            #initialize wsAssistant and get data
            try:
                obook_data = await webSassistant_obook.get_data_by_channel_and_ticker('v3_orderbook','RUNE-USD')
                if obook_data:
                    process_data_ = [_process_data(item) for item in obook_data]
                    process_data_ = await gather(*process_data_)
                    return process_data_
                await asyncio.sleep(3)
            except Exception as e:
                print(f'Error with Kafka handling')

    async def main_accounts():
        global webSassistant_accounts

        await webSassistant_accounts.initialize_data_instances()
        print('wait one sec for feed to run')
        await asyncio.sleep(3)
        while webSassistant_accounts.ws_obook_positions:
            
            #initialize wsAssistant and get data
            try:
                obook_data = await webSassistant_accounts.get_data_by_channel_and_ticker('v3_accounts','FIL-USD')
                if obook_data:
                    process_data_ = [_process_data(item) for item in obook_data]
                    process_data_ = await gather(*process_data_)
                await asyncio.sleep(3)
            except Exception as e:
                print(f'Error with Kafka handling')      
    

   
    data = asyncio.run(main_accounts())
    print(data)