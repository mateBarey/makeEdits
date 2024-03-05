import asyncio
import json
import uuid
from bson.binary import Binary, UUID_SUBTYPE
from Obook import OrderBook
from dydx3 import *
from dydx3.helpers.request_helpers import generate_now_iso
from hbot51 import *
from lob_helpers import *
from Wsconnection import *
from aiokafka import *
import random 
import anyio
from anyio import open_cancel_scope, get_cancelled_exc_class, move_on_after
ws_url = 'wss://api.dydx.exchange/v3/ws'


'''
Notes reason obook wsAssitant trades and obooks should be separate in case of dead lock 
and race conditions ... but if ws_obook posns change in queue in lob helper they should be cancellled separately but in both assistants
'''
class wsAssistant:
    def __init__(self, hbot, channels, num_consumers):
        self.hbot = hbot
        self.channels = channels
        self.num_orderbook_consumers = num_consumers
        self.ws_market_positions = self.hbot.ws_market_positions
        self.ws_obook_positions = self.hbot.ws_obook_positions
        self.safety_factor = 0.05
        self.ws_url = ws_url
        self.semaphore = asyncio.Semaphore(12) if len(
            self.channels) > 1 else asyncio.Semaphore(1)
        self.orderbooks_per_market = {}
        self.active_connections = {}
        self.orderbook_batch = None
        self.last_batch_send_time = None
        self.orderbook_consumers = []
        self.tickers_in_batch = set()
        self.cancellation_tickers_ = {}
        self.remove_tickers = False  
        self.add_tickers = False
        self.pause_events = {}
        self.addition_tickers = {}

    async def pause_kafka_components(self):
        '''Pause Kafka producers and consumers'''
        # Pause producers
        if hasattr(self, 'kafka_producer_orderbook'):
            await self.kafka_producer_orderbook.stop()  # Assuming stop() effectively pauses the producer
        if hasattr(self, 'kafka_producer_trades'):
            await self.kafka_producer_trades.stop()

        # Pause consumers
        if self.orderbook_consumers:
            for consumer in self.orderbook_consumers:
                await consumer.stop()
        else:
            await self.kafka_consumer.stop()

    async def restart_kafka_components(self):
        '''Restart Kafka producers and consumers'''
        # Restart producers
        if hasattr(self, 'kafka_producer_orderbook'):
            await self.kafka_producer_orderbook.start()
        if hasattr(self, 'kafka_producer_trades'):
            await self.kafka_producer_trades.start()

        # Restart consumers
        if self.orderbook_consumers:
            for consumer in self.orderbook_consumers:
                await consumer.start()
        else:
            await self.kafka_consumer.start()

    async def init_orderbook_consumers(self):
        if self.num_orderbook_consumers > 0:
            '''initialize n consumers for orderbook processing'''
            for i in range(self.num_orderbook_consumers):
                consumer = AIOKafkaConsumer(
                    *[f'{channel}_topic' for channel in self.channels],
                    enable_auto_commit=True,
                    auto_commit_interval_ms=200,
                    auto_offset_reset='latest',
                    bootstrap_servers='localhost:9092',
                    group_id='orderbook_group')  # Same group_id for parallel consumption
                self.orderbook_consumers.append(consumer)
                print(
                    f"Initialized and started consumer {consumer} for 'v3_orderbook_topic'")

    async def async_init(self):
        '''initialize aiokafka consumers and producers'''
        # Initialize Kafka Producers
        if 'v3_orderbook' in self.channels:
            self.kafka_producer_orderbook = AIOKafkaProducer(
                bootstrap_servers='localhost:9092')

            await self.kafka_producer_orderbook.start()

        if 'v3_trades' in self.channels:
            self.kafka_producer_trades = AIOKafkaProducer(
                bootstrap_servers='localhost:9092')
            await self.kafka_producer_trades.start()

        if 'v3_accounts' in self.channels:
            self.kafka_producer_accounts = AIOKafkaProducer(
                bootstrap_servers='localhost:9092')
            await self.kafka_producer_accounts.start()

        if 'v3_markets' in self.channels:
            self.kafka_producer_markets = AIOKafkaProducer(
                bootstrap_servers='localhost:9092')
            await self.kafka_producer_markets.start()

        # Initialize Kafka Consumer
        if 'v3_orderbook' in self.channels:
            await self.init_orderbook_consumers()
        else:
            # Start all consumers
            self.kafka_consumer = AIOKafkaConsumer(
                *[f'{channel}_topic' for channel in self.channels],
                bootstrap_servers='localhost:9092'
            )
    async def flush_producer(self):
        '''flush fn for purging current batches in case of add/removal of tickers within ws_obook or market posns'''
        if hasattr(self, 'kafka_producer_orderbook'):
            await self.kafka_producer_orderbook.flush()
        if hasattr(self, 'kafka_producer_trades'):
            await self.kafka_producer_trades.flush()

    async def shutdown(self):
        '''shutdown fn for aiokafka coroutines'''
        tasks = []

        if 'v3_trades' in self.channels:
            tasks.append(self.kafka_producer_trades.stop())
        if 'v3_accounts' in self.channels:
            tasks.append(self.kafka_producer_accounts.stop())
        if 'v3_markets' in self.channels:
            tasks.append(self.kafka_producer_markets.stop())
        if 'v3_orderbook' in self.channels:
            tasks.append(self.kafka_producer_orderbook.stop())

        if self.orderbook_consumers:
            for consumer in self.orderbook_consumers:
                tasks.append(consumer.stop())
        else:
            tasks.append(self.kafka_consumer.stop())
        await asyncio.gather(*tasks)

    async def _while_condn_for_channel(self,channel,ticker=None):
        channels_w_id = ['v3_trades','v3_orderbook']
        if channel in channels_w_id :
            return (ticker in self.ws_obook_positions)
        if channel == 'v3_accounts':
            return self.ws_obook_positions 
        elif channel == 'v3_markets':
            return self.ws_market_positions
        
    async def process_orderbook_message(self, ticker, message):
        '''process messages added to batch for sending from producer to v3_orderbook_topic'''
        if ticker not in self.ws_obook_positions:
            return 
        if self.orderbook_batch is None:
            self.orderbook_batch = self.kafka_producer_orderbook.create_batch()
            self.last_batch_send_time = time.perf_counter()
            print(f'New batch created')

        metadata = self.orderbook_batch.append(
            key=None, value=message, timestamp=None)
        current_time = time.perf_counter()
        self.tickers_in_batch.add(ticker)
        all_tickers_processed = self.tickers_in_batch == set(
            list(self.ws_obook_positions.keys()))
        if all_tickers_processed:
            print(f'All {len(self.tickers_in_batch)} tickers have been added to batch {self.tickers_in_batch}')
        time_limit_reached = (current_time - self.last_batch_send_time) <= 1

        if metadata is None or (all_tickers_processed and time_limit_reached):
            print(
                f"Sending batch: {self.orderbook_batch.record_count()} messages")
            await self.send_orderbook_batch()

    async def send_orderbook_batch(self):
        '''send batch created of orderbook data from producer to v3_orderbook_topic'''
        if self.orderbook_batch:
            partitions = await self.kafka_producer_orderbook.partitions_for('v3_orderbook_topic')
            partition = random.choice(tuple(partitions))
            print(f"Sending batch to partition {partition}")
            await self.kafka_producer_orderbook.send_batch(self.orderbook_batch, 'v3_orderbook_topic', partition=partition)
            self.orderbook_batch = None
            self.tickers_in_batch = set()

    def obook_select(self, obooks_, ticker):
        '''select obook for a certain ticker given a hash of initialized orderbooks'''
        if obooks_ != None:
            return obooks_[ticker]
        return None

    async def find_data(self, channel, res, ticker):
        '''data search fn for account analysis fn'''
        try:
            if 'markets' in res['contents']:
                data = res['contents']['markets']
                return data
            elif 'markets' not in res['contents'] and 'ETH-USD' in res['contents'].keys():
                return res['contents']

        except Exception as e:
            print(
                f'Error, Data feed for {channel} is sending back other data which doesnt align with keyvalue logic, {e}')
            return

    async def _account_ws_analyze(self, i, channel, res, ticker, obook=None):
        '''account analysis fn for ws feed within make_ws_req '''
        data = res['contents']['orders']
        if data:
            await self.kafka_producer_accounts.send_and_wait('v3_accounts_topic',
                                                             json.dumps([ticker, data]).encode('utf-8'))
        return

    async def _market_ws_analyze(self, i, channel, res, ticker, obook=None):
        '''market analysis fn for ws feed within make_ws_req '''
        data = await self.find_data(channel, res, ticker)
        ticker = list(self.ws_market_positions.keys())[0]
        if data:
            # print(f'data found for {ticker}: {data}')
            await self.kafka_producer_markets.send_and_wait('v3_markets_topic',
                                                            json.dumps([ticker, data]).encode('utf-8'))
        return

    async def _trade_ws_analyze(self, i, channel, res, ticker, obook=None):
        '''trade analysis fn for ws feed within make_ws_req '''
        if ticker not in self.cancellation_tickers_:
            trades = res['contents']
            # print(trades)
            if trades:
                await asyncio.sleep(0.1)
                await self.kafka_producer_trades.send_and_wait('v3_trades_topic',
                                                            json.dumps([ticker, trades]).encode('utf-8'))
            return

    async def _obook_ws_analyze(self, i, channel, res, ticker, obook):
        '''orderbook analysis fn for ws feed within make_ws_req '''
        if ticker not in self.cancellation_tickers_:
            #print(i,channel,res,ticker,obook)
            print(f'{i}th iteration')
            if i == 0:
                await obook.process_initial_response(res['contents'])
                self.orderbooks_per_market[ticker] = obook
                obook_dict = await obook.make_obook()
                # print(f' i: {i}, obook: {obook_dict} ,ticker {ticker}')

            elif ticker in self.orderbooks_per_market:
                obook_instance = self.orderbooks_per_market[ticker]
                await obook_instance.update_order_book(res['contents'])

                self.orderbooks_per_market[ticker] = obook_instance
                obook_dict = await obook_instance.make_obook()
                # print(f' i: {i}, obook: {obook_dict}, ticker {ticker}')z

            if obook_dict and ticker not in self.tickers_in_batch:
                print(f'sending ticker to batch: {ticker}')
                message = json.dumps(
                    [ticker, obook_dict, i, generate_now_iso()]).encode('utf-8')
                await self.process_orderbook_message(ticker, message)

            print(
                f'tickers in batch : {self.tickers_in_batch}, ticker_being_analyzed: {ticker}')
            return
        

    async def start_new_connection_for_ticker(self, ticker,channel):
        ''' Start new connections for both 'v3_trades' and 'v3_orderbook' channels'''
        await self._create_and_start_connection(channel, ticker) 

    async def _create_and_start_connection(self, channel, ticker=None):
        """Create and start connection for a specific ticker and channel."""
    
        i= 0
      
        tickers_, obooks_ = await self.channel_specific_params(channel)
        req = await self.setup_ws_feed(channel, ticker)
        uid = Binary(uuid.uuid4().bytes, UUID_SUBTYPE)
        ws_connection = WSConnection(ticker, channel)
      
        task = asyncio.create_task(self.make_ws_req(
            channel,req,i,tickers_,obooks_))
        self.active_connections[uid] = [ws_connection, req,task]

    async def stop_connection_for_ticker(self, ticker):
        '''Stop connections for both 'v3_trades' and 'v3_orderbook' channel '''
        uids_to_remove = [uid for uid, value in self.active_connections.items(
        ) if value[1].get('id') == ticker]
        await asyncio.gather(*[self._disconnect_and_remove(uid) for uid in uids_to_remove])

   
    async def _disconnect_and_remove(self, uid):
        """Disconnect and remove a connection and its associated subprocess."""
        ws_connection, _, task = self.active_connections.get(uid, [None, None, None])
        if ws_connection :
            await ws_connection.disconnect()

        print(f'cancelling uids {uid} for tg')
        
        #logic before task manager
        if task and not task.done():
            was_cancelled = task.cancel('Stop right now!')
            print(f'was the task cancelled: {was_cancelled}')
            await asyncio.sleep(0)
            print(f'cancelled: {task.cancelled()}')
        if uid in self.active_connections:
            del self.active_connections[uid]

    async def kill_connection_based_on_request(self,channel,ticker=None):
        # Iterate through active connections to find the matching request
        channels = ['v3_markets','v3_accounts']
        ticker_channels = ['v3_trades','v3_orderbook']
        kill_channels = {k:v for k,v in self.active_connections.items() if v[1]['channel'] == channel\
                          and channel in channels\
                         or  v[1]['channel'] == channel and channel in ticker_channels\
                              and v[1]['ticker'] == ticker }
        kill_tasks = [asyncio.create_task(self.active_connections[uid][0].disconnect())\
                       for uid in kill_channels.keys()]
        kill_tasks = await gather(*kill_tasks)
        self.active_connections = {k:v for k,v in self.active_connections.items()\
                                    if k not in list(kill_channels.keys())}

    async def kill_specific_feeds(self,channels_to_kill_list,list_of_tickers):
        kill_channel_feeds = [asyncio.create_task(self.kill_connection_based_on_request(channel,list_of_tickers[i])) \
                              for i,channel in\
                            enumerate(channels_to_kill_list)]
        kill_channel_feeds = await gather(*kill_channel_feeds)



    async def correct_channel_path(self, channel):
        '''select correct channel path for channels with tickers'''
        if 'accounts' in channel:
            return '/ws/accounts'
        return channel

    async def setup_ws_feed(self, channel, ticker=None):
        ''''setup the req hash for the web socket feed'''
        correct_channel_path_f = await self.correct_channel_path(channel)
        channels_w_id = ['v3_trades', 'v3_orderbook']
        signature = self.hbot.client.private.sign(
            request_path=f'{correct_channel_path_f}',
            method='GET',
            iso_timestamp=generate_now_iso(),
            data={},
        )
        if channel in channels_w_id:
            return {
                'type': 'subscribe',
                'channel': f'{channel}',
                'id': ticker,
                'accountNumber': '0',
                'apiKey': self.hbot.client.api_key_credentials['key'],
                'passphrase': self.hbot.client.api_key_credentials['passphrase'],
                'timestamp': generate_now_iso(),
                'signature': signature
            }
        else:
            return {
                'type': 'subscribe',
                'channel': f'{channel}',
                'accountNumber': '0',
                'apiKey': self.hbot.client.api_key_credentials['key'],
                'passphrase': self.hbot.client.api_key_credentials['passphrase'],
                'timestamp': generate_now_iso(),
                'signature': signature
            }

    async def make_req_(self, i, channel, res, ticker, obooks_):
        
        #obooks = [self.obook_select(obooks_, ticker) for ticker in tickers_]
        obook = self.obook_select(obooks_,ticker)
        print(f'evaluating ticker obook: {ticker}')
        eval_tickers =  await self.select_channel_analysis(i, channel, res, ticker, obook)
                        
        

    async def select_channel_analysis(self, i, channel, res, ticker, obook=None,):
        '''fn that the routing fn make_req uses to select the appropriate  analysis fn'''
        channel_analysis = {
            'v3_orderbook': self._obook_ws_analyze,
            'v3_trades': self._trade_ws_analyze,
            'v3_markets': self._market_ws_analyze,
            'v3_accounts': self._account_ws_analyze
        }
        # print(f'select channel analysis: {channel}')
        analysis_function = channel_analysis[channel]
        return await analysis_function(i, channel, res, ticker, obook)

    async def channel_specific_params(self, channel):
        '''fn used to initialize specific params based on the ws channel being subscribed to'''
        tickers_ = {'v3_orderbook': list(self.ws_obook_positions.keys()), 'v3_trades': list(self.ws_obook_positions.keys()),
                    'v3_markets': list(self.ws_market_positions.keys()), 'v3_accounts': list(self.ws_obook_positions.keys())}
        obooks_ = {
            'v3_trades': None,
            'v3_markets': None,
            'v3_accounts': None,
            'v3_orderbook': {ticker: OrderBook() for ticker in list(self.ws_obook_positions.keys())}
        }
        return tickers_[channel], obooks_[channel]

    async def add_ticker_channel_specific_params(self, channel,tickers_added):
        '''fn used to initialize specific params based on the ws channel being subscribed to'''
        
        obooks_ = {
            'v3_trades': None,
            'v3_markets': None,
            'v3_accounts': None,
            'v3_orderbook': {ticker: OrderBook() for ticker in tickers_added}
        }
        return tickers_added, obooks_[channel]
    
    async def _get_ticker_for_channel(self, req):
        '''fn used to get a specific ticker for a specific ws request'''
        return req['id'] if 'id' in req else None

    async def make_ws_req(self, channel, req, i, tickers_, obooks_):
        '''the control flow fn for the websocket feeds it routes feeds to analysis to producers and consumers'''
        uid = next(
            uid for uid, val in self.active_connections.items() if val[1] == req)
        
        async with self.semaphore:
            # Initialize WSConnection with the market if needed
            
            ticker = await self._get_ticker_for_channel(req)
            print(f'req for ticker {ticker}')
            ws_connection = self.active_connections[uid][0]
            await ws_connection.connect(ws_url)
            await ws_connection.send_json(req)
            
            try:
                while await self._while_condn_for_channel(channel,ticker):

                   
                    # Clear completed sub-tasks
                  
                    if ticker in self.cancellation_tickers_:
                        print(f'breaking out loop for {ticker}')
                        break  
                    print(f'positions :{self.ws_obook_positions if self.ws_obook_positions else self.ws_market_positions}')
                    try:
                        res = await ws_connection.receive_json()

                        if 'contents' in res:
                            # print(f'res: {res}')
                            await self.make_req_(i, channel, res, ticker, obooks_)
                            #sub_task = asyncio.create_task(self.make_req_(i, channel, res, tickers_, obooks_))
                            i += 1


                        print(f'status of channel {channel} connection is : {ws_connection.connection_state} for {ticker}')
                        await asyncio.sleep(1.4)
                    except json.JSONDecodeError as e:
                        print(
                            f"Received a non-JSON message or unable to parse the message: {e}")
                    await asyncio.sleep(0)
            except asyncio.CancelledError as e:
                print(f'Cancellation requetsd for {channel} and {ticker}, {e}')
            finally :
                print(f'Exiting make_ws_req loop for {channel} and {ticker}')
              


    async def handle_kafka_msg(self):
        '''aiokafka handling fn used to obtain all batch data from the consumers and send it outside the class or inside as needed'''
        all_data = []
        async def process_consumer(consumer):
            nonlocal all_data
            while True:
                # Define a time-out period for getmany() (in milliseconds)
                timeout_ms = 100

                # Fetch messages from all subscribed partitions
                messages = await consumer.getmany(timeout_ms=timeout_ms)
                #print(f'received_messages : {messages}')
                if not messages:
                    break

                for tp, msgs in messages.items():
                    for msg in msgs:
                        topic_data = json.loads(msg.value.decode('utf-8'))
                        all_data.append(topic_data)

        if self.orderbook_consumers:
            # Process each consumer concurrently
            consumer_tasks = [process_consumer(
                consumer) for consumer in self.orderbook_consumers]
            await asyncio.gather(*consumer_tasks)
        else:
            if 'v3_orderbook' not in self.channels:
                await process_consumer(self.kafka_consumer)
        return all_data


    async def get_web_socket_feed2(self,channel):
            # get ws params for channel
            i = 0
            channels_w_id = ['v3_trades','v3_orderbook']
            tickers_, obooks_ = await self.channel_specific_params(channel)
            # get request for channel
            #print(f'obooks: {obooks_}')
            req_ =  [await self.setup_ws_feed(channel,ticker) for ticker in tickers_ ]
            #print(f'req: {req_}')
            #for orderbook or trades need to subscribe to each ticker channel in req otherwise just 1 channel subscription
            uid = [Binary(uuid.uuid4().bytes, UUID_SUBTYPE) for _ in range(len(tickers_))]
            ws_connection = [WSConnection(ticker, channel) for ticker in tickers_]
            if channel in channels_w_id:
                tasks = [asyncio.create_task(self.make_ws_req(channel,req,i,tickers_,obooks_)) for req in req_]
                self.active_connections= {uid[i]:[ws_connection[i], req_[i],tasks[i]] for i,el in enumerate(req_)}
                await gather(*tasks)
                print(f'active_connections: {self.active_connections}')
            else:
                #make 1 req accounts and markets
                make_actual_req = await self.make_ws_req(channel,req_[0],i,tickers_,None)
                

    async def get_web_socket_feed(self, channel):
        '''fn used to start a specific ws feed from a specific channel'''
        i = 0
        channels_w_id = ['v3_trades', 'v3_orderbook']
        tickers_, _ = await self.channel_specific_params(channel)

        if channel in channels_w_id:
            tasks = [self._create_and_start_connection(channel,ticker) for ticker in tickers_] #mult reqs - orderbook or trades
            await gather(*tasks)
        
        else:
            make_actual_req =  asyncio.create_task(self._create_and_start_connection(channel)) #1 req - account or markets

 



async def elapsed_time_(t1):
    '''fns used for testing the class fns'''
    elapsed_ = (time.perf_counter() - t1)
    print(f'elapsed_time: {elapsed_}')
    return (elapsed_ >= 15 )

async def elapsed_time_f2(t1):
    '''fns used for testing the class fns'''
    elapsed_ = (time.perf_counter() - t1)
    print(f'elapsed_time: {elapsed_}')
    return (elapsed_ >= 35 )

if __name__ == '__main__':
    ETHEREUM_ADDRESS = os.getenv("eth_add")
    eth_add2 = os.getenv("eth_add2")
    print(f'eth_add2: {eth_add2}')
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
    hbotf.ws_obook_positions['RUNE-USD'] = 'open'
    hbotf.ws_obook_positions['ETH-USD'] = 'open'
    hbotf.ws_obook_positions['YFI-USD'] = 'open'

    webSassistant_ = wsAssistant(hbotf, ['v3_orderbook'], 4)

    async def _process_data(item):
        print(
            f'order_book_data for {item[0]} {item[2]}th iteration: {item[1]} at time {item[3]}')

    kill_channels = ['v3_orderbook',
                     'v3_orderbook']

    async def get_data_remove_ticker_test():
        '''test fn used to test the performance of the wsAssistant class for removing tickers from ws_obook_posns'''
        global webSassistant_
        t1 = time.perf_counter()
        # Start all consumers
        removed_tickers = ['ETH-USD','YFI-USD']
        for consumer in webSassistant_.orderbook_consumers:
            print(f'subscribed topics : {consumer.subscription()}')
            await consumer.start()
        elapsed_time = False
        elapsed_time2 = False 
        while webSassistant_.ws_obook_positions:

            if not elapsed_time and await elapsed_time_(t1)  :
                print('Time elapsed!!..removing tickers')
                webSassistant_.cancellation_tickers_ = {ticker:'close' for ticker in removed_tickers}
               
                webSassistant_.ws_obook_positions = {k:v for k,v in webSassistant_.ws_obook_positions.items() if k not in\
                        removed_tickers}
                try:
                    await webSassistant_.kill_specific_feeds(kill_channels,removed_tickers)
                except:
                    print('made it here ')
                    print(f'cancellation_tickers:{webSassistant_.cancellation_tickers_}')
                    if webSassistant_.cancellation_tickers_:
                        webSassistant_.cancellation_tickers_ = {}
                #await webSassistant_.monitor_tickers()
                await asyncio.sleep(2)
                

                #await webSassistant_.monitor_tickers()
                elapsed_time = True 
                print(f'Obook tickers are {webSassistant_.ws_obook_positions}')
                await asyncio.sleep(1)

            try:
                data = await webSassistant_.handle_kafka_msg()
                if data:
                    # print(f'received_data: {data}')
                    # Sort the collected data based on time_data (fourth element in each list)
                    process_data_ = [_process_data(item) for item in data]
                    process_data_ = await gather(*process_data_)
            except Exception as e:
                print(f'Error analysin obook {e}')

            #if not elapsed_time2  and await elapsed_time_f2(t1) :
             #   webSassistant_.ws_obook_positions['BTC-USD'] = 'open'
             #   await webSassistant_.monitor_tickers()
             #   elapsed_time2 = True 




    async def get_data_addition_ticker_test():
        '''test fn used to test the performance of the wsAssistant class for removing tickers from ws_obook_posns'''
        global webSassistant_
        t1 = time.perf_counter()
        # Start all consumers
        add_tickers = ['MATIC']
        for consumer in webSassistant_.orderbook_consumers:
            print(f'subscribed topics : {consumer.subscription()}')
            await consumer.start()
        elapsed_time = False
        elapsed_time2 = False 
        while webSassistant_.ws_obook_positions:

            if not elapsed_time and await elapsed_time_(t1)  :
                print('Time elapsed!!..removing tickers')
                webSassistant_.addition_tickers = {ticker:'open' for ticker in add_tickers}
               
                webSassistant_.ws_obook_positions = webSassistant_.ws_obook_positions.update(webSassistant_.addition_tickers)

                await asyncio.sleep(2)
                

                #await webSassistant_.monitor_tickers()
                elapsed_time = True 
                print(f'Obook tickers are {webSassistant_.ws_obook_positions}')
                await asyncio.sleep(1)

            try:
                data = await webSassistant_.handle_kafka_msg()
                if data:
                    # print(f'received_data: {data}')
                    # Sort the collected data based on time_data (fourth element in each list)
                    process_data_ = [_process_data(item) for item in data]
                    process_data_ = await gather(*process_data_)
            except Exception as e:
                print(f'Error analysin obook {e}')

            #if not elapsed_time2  and await elapsed_time_f2(t1) :
             #   webSassistant_.ws_obook_positions['BTC-USD'] = 'open'
             #   await webSassistant_.monitor_tickers()
             #   elapsed_time2 = True 


    # 1st wsAssistant test

    async def main():
        global webSassistant_

        await webSassistant_.async_init()
        await asyncio.sleep(1)

        # Combine the creation of feeds and the testing function
        asyncio.create_task(webSassistant_.get_web_socket_feed(webSassistant_.channels[0]))
        await get_data_remove_ticker_test()
        
        await webSassistant_.shutdown()

    # Instead of manually getting the loop and setting debug mode, just call asyncio.run()
    asyncio.run(main())