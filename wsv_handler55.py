import asyncio
from asyncio import *
from Obook import *
import json 
from dydx3.helpers.request_helpers import generate_now_iso
from hbot51 import *
#from web3 import Web3
import os 
import time 
from lob_helpers import * 
from Wsconnection import * 
import datetime as dt
import uuid
from bson.binary import Binary, UUID_SUBTYPE
from aiokafka import *
import random

ws_url = 'wss://api.dydx.exchange/v3/ws'


class wsAssistant:
    def __init__(self, hbot, channels,num_consumers):
        self.hbot = hbot
        self.channels = channels
        self.safety_factor = 0.05
        self.ws_url = ws_url
        self.ws_market_positions = self.hbot.ws_market_positions
        self.ws_obook_positions = self.hbot.ws_obook_positions
        self.semaphore = asyncio.Semaphore(12) 
        self.orderbooks_per_market = {}
        self.active_connections = {}
        self.orderbook_batch = None
        self.last_batch_send_time = None
        self.num_orderbook_consumers = num_consumers
        self.kafka_producer_orderbook = None
        self.kafka_producer_trades = None
        self.orderbook_consumers = []
        self.cancellation_tickers = {}
        self.addition_tickers = {}
        self.init_orderbook = False 
        self.init_trades = False 
        self.init_accounts = False 
        self.init_markets = False 
        self.start_feed = False
        self.kafka_producer = None 
        self.start_up = True
        self.batch_data = {
            'v3_orderbook_topic': {
                'batch': None,
                'last_send_time': 0,
                'tickers_in_batch': set()
            },
            'v3_trades_topic': {
                'batch': None,
                'last_send_time': 0,
                'tickers_in_batch': set()
            }
            # Initialize other topics as needed
        }

    async def init_orderbook_consumers(self):
        if self.num_orderbook_consumers > 0:
            for i in range(self.num_orderbook_consumers):
                consumer = AIOKafkaConsumer(
                    *[f'{channel}_topic' for channel in self.channels],
                    enable_auto_commit=True,
                    auto_commit_interval_ms=100,
                    auto_offset_reset='latest',
                    bootstrap_servers='localhost:9092',
                    group_id='orderbook_group')  # Same group_id for parallel consumption
                self.orderbook_consumers.append(consumer)
                print(f"Initialized and started consumer {consumer} for 'v3_orderbook_topic'")

    async def async_init(self):

        # Initialize Kafka Producers
        if 'v3_orderbook' in self.channels:
            self.kafka_producer_orderbook = AIOKafkaProducer(bootstrap_servers='localhost:9092')
            await self.kafka_producer_orderbook.start()
            self.kafka_producer = self.kafka_producer_orderbook
            print("Orderbook Kafka producer initialized.")

        if 'v3_trades' in self.channels:
            self.kafka_producer_trades = AIOKafkaProducer(bootstrap_servers='localhost:9092')
            await self.kafka_producer_trades.start()
            self.kafka_producer = self.kafka_producer_trades
            print("Trades Kafka producer initialized.")

        if 'v3_accounts' in self.channels:
            self.kafka_producer_accounts = AIOKafkaProducer(bootstrap_servers='localhost:9092')
            await self.kafka_producer_accounts.start()

        if 'v3_markets' in self.channels:
            self.kafka_producer_markets = AIOKafkaProducer(bootstrap_servers='localhost:9092')
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
            print(f'Consumer iniitialized : {self.kafka_consumer}')
            
    async def init_messages(self,wsAssistant_type,key):
        hash_messages = {
            'prods_cons_msg':
            f'Initializing {wsAssistant_type} Producers and Consumers',
            'start_msg':f'Starting {wsAssistant_type} Consumer/s',
            'final_msg': f'All {wsAssistant_type} Producers Consumers Started and intiialized'
        }    
        return hash_messages[key]
    
    async def _init_orderbook(self):
        if not self.init_orderbook:
            await self.async_init()
            await self.init_messages('Orderbook','prods_cons_msg')
            await asyncio.sleep(0.5)
            for consumer in self.orderbook_consumers:
                print(f'subscribed topics : {consumer.subscription()}')
                await consumer.start()
                await asyncio.sleep(0.5)
            await self.init_messages('Orderbook','start_msg')
            self.init_orderbook = True
            await self.init_messages('Orderbook','final_msg')

    async def _init_trades(self):
        if not self.init_trades:
            await self.async_init()
            await self.init_messages('Trades','prods_cons_msg')
            await asyncio.sleep(0.5)
            await self.kafka_consumer.start()
            await self.init_messages('Trades','start_msg')
            print(f'subscribed topics : {self.kafka_consumer.subscription()}')
            await asyncio.sleep(0.5)
            self.init_trades = True 
            await self.init_messages('Trades','final_msg')


    async def _init_accounts(self):
        if not self.init_accounts:
            await self.async_init()
            await self.init_messages('Accounts','prods_cons_msg')
            await asyncio.sleep(0.5)
            await self.kafka_consumer.start()
            await self.init_messages('Accounts','start_msg')
            await asyncio.sleep(0.5)
            self.init_accounts = True 
            await self.init_messages('Accounts','final_msg')

    async def _init_markets(self):
        if not self.init_markets:
            await self.async_init()
            await self.init_messages('Markets','prods_cons_msg')
            await asyncio.sleep(0.5)
            await self.kafka_consumer.start()
            await self.init_messages('Markets','start_msg')
            await asyncio.sleep(0.5)
            self.init_markets = True 
            await self.init_messages('Markets','final_msg')

    async def initialize_data_instances(self): # all you need is this and get_data in whole class like in the test module
        #print(f'channels: {self.channels}')
        if 'v3_orderbook' in self.channels:
            if not self.init_orderbook:
                await self._init_orderbook()
                self.init_orderbook = True
        elif 'v3_trades' in self.channels:
            if not self.init_trades:
                await self._init_trades()
                self.init_trades = True
        elif 'v3_accounts' in self.channels:
            if not self.init_accounts:
                await self._init_accounts()
                self.init_accounts = True
        elif 'v3_markets' in self.channels:
            if not self.init_markets:
                await self._init_markets()
                self.init_markets = True 
        #start relevant ws_feed
        get_ws_feed = asyncio.create_task(self.get_web_socket_feed())
        print(f'starting {self.channels[0]} ws feed... wait 1s')
        self.start_feed = True 

    async def get_data_by_channel_and_ticker(self, channel, ticker):
        """Retrieves data for the specified ticker from the appropriate Kafka topic."""
        tickers_ = {'v3_orderbook':self.ws_obook_positions,'v3_trades':self.ws_obook_positions,\
                    'v3_markets':self.ws_market_positions, 'v3_accounts':self.ws_obook_positions}
        if not self.start_feed:
            print(f'Call initialize data instances to start {channel} {ticker} ws feed before get data fn')
            return 
        if ticker in tickers_[channel]:
            print(f'current tickers in orderboook {tickers_[channel]}')
            while tickers_[channel]:
                try:
                    data = await self.handle_kafka_msg()
                    if data:
                        data_need = [item for item in data if item[0] == ticker ]
                        if data_need :
                            return data_need
                    await asyncio.sleep(1)

                except Exception as e:
                    print(f'Error with kafka data retrieval ,or, {e}')
                finally:
                    self.shutdown()
                
  

    async def shutdown(self):
        tasks = []
        
        if 'v3_accounts' in self.channels:
            tasks.append(self.kafka_producer_accounts.stop())
        if 'v3_markets' in self.channels:
            tasks.append(self.kafka_producer_markets.stop())
        if 'v3_trades' in self.channels:
            tasks.append(self.kafka_producer_trades.stop())
        if 'v3_orderbook' in self.channels:
            tasks.append(self.kafka_producer_orderbook.stop())

        if self.orderbook_consumers:
            for consumer in self.orderbook_consumers:
                tasks.append(consumer.stop())
        else :
            tasks.append(self.kafka_consumer.stop())
        await asyncio.gather(*tasks)

    async def flush_producer(self):
        '''Flush Kafka producers for all topics.'''
        for topic, batch_info in self.batch_data.items():
            if batch_info['batch']:
                await self.send_batch(topic)

      
    async def switch_kafka_producer(self):
        original_producer = self.kafka_producer
        if 'v3_orderbook' in self.channels:
            self.kafka_producer = self.kafka_producer_orderbook
        elif 'v3_trades' in self.channels:
            self.kafka_producer = self.kafka_producer_trades
        print(f"Switched Kafka producer from {original_producer} to {self.kafka_producer} for channel {self.channels[0]}.")


    async def process_message(self, topic, ticker, message):
        if not self.kafka_producer:
            await self.switch_kafka_producer()
        if not hasattr(self, 'kafka_producer'):
            print("Error: Kafka producer not initialized.")
            print(f'kafka_producer: {self.kafka_producer}')

            return
        if ticker not in self.ws_obook_positions:
            return 

        # Ensure the topic's batch data is initialized
        if topic not in self.batch_data:
            self.batch_data[topic] = {
                'batch': None,
                'last_send_time': 0,
                'tickers_in_batch': set()
            }

        batch_info = self.batch_data[topic]

        # Create a new batch if needed
        if batch_info['batch'] is None:
            batch_info['batch'] = self.kafka_producer.create_batch()
            batch_info['last_send_time'] = time.perf_counter()
            print(f"New batch created for {ticker} on topic {topic}")

        metadata = batch_info['batch'].append(key=None, value=message, timestamp=None)
        current_time = time.perf_counter()
        batch_info['tickers_in_batch'].add(ticker)

        all_tickers_processed = batch_info['tickers_in_batch'] == set(list(self.ws_obook_positions.keys()))
        time_limit_reached = (current_time - batch_info['last_send_time']) <= 5

        if metadata is None or (all_tickers_processed and time_limit_reached):
            print(f"Sending batch: {batch_info['batch'].record_count()} messages on topic {topic}")
            await self.send_batch(topic)


    async def send_batch(self, topic):
        
        batch_info = self.batch_data.get(topic)
        
        if batch_info and batch_info['batch']:
            partitions = await self.kafka_producer.partitions_for(topic)
            partition = random.choice(tuple(partitions))
            print(f"Sending batch to partition {partition} on topic {topic}")

            
            await self.kafka_producer.send_batch(batch_info['batch'], topic, partition=partition)
            batch_info['batch'] = None
            batch_info['tickers_in_batch'].clear()


    def obook_select(self,obooks_,ticker):
        if obooks_ != None:
            return obooks_[ticker]
        return None 
            
    async def find_data(self,channel,res,ticker):
        try:
            if 'markets' in res['contents'] :
                data = res['contents']['markets']
                return data 
            elif 'markets' not in res['contents'] and 'ETH-USD' in res['contents'].keys():
                return res['contents']
            
        except Exception as e:
            print(f'Error, Data feed for {channel} is sending back other data which doesnt align with keyvalue logic, {e}')
            return 
        
    async def _account_ws_analyze(self,i,channel,res,ticker,obook=None):
        from pprint import pprint 
        print(res)
        data = res['contents']['orders']
        if data:
            #print(f'account: {data}')
            await self.kafka_producer_accounts.send_and_wait('v3_accounts_topic', \
                json.dumps([ticker, data,i,generate_now_iso()]).encode('utf-8'))
        return 

    async def _market_ws_analyze(self,i,channel,res,ticker,obook=None):
        data = await self.find_data(channel,res,ticker)
        ticker = list(self.ws_market_positions.keys())[0]
        if data:
            await self.kafka_producer_markets.send_and_wait('v3_markets_topic',\
             json.dumps([ticker, data,i,generate_now_iso()]).encode('utf-8'))
        return 
    
    async def _trade_ws_analyze(self,i,channel,res,ticker,obook=None):
        if ticker not in self.cancellation_tickers:
            trades = res['contents']
            #print(trades)
            if trades and ticker not in self.batch_data["v3_trades_topic"]["tickers_in_batch"]:
                
                
                data = json.dumps([ticker, trades,i,generate_now_iso()]).encode('utf-8')
                await self.process_message('v3_trades_topic', ticker, data)
            return 
   
                    
    async def _obook_ws_analyze(self,i,channel,res,ticker,obook):
        #print(i,channel,res,ticker,obook)
        if ticker not in self.cancellation_tickers:
            #print(f'{i}th iteration for ticker {ticker}')
            if i == 0:
                await obook.process_initial_response(res['contents'])
                self.orderbooks_per_market[ticker] = obook
                obook_dict = await obook.make_obook()
                #print(f' i: {i}, obook: {obook_dict} ,ticker {ticker}')

            elif ticker in self.orderbooks_per_market:
                obook_instance = self.orderbooks_per_market[ticker]
                await obook_instance.update_order_book(res['contents'])

                self.orderbooks_per_market[ticker] = obook_instance
                obook_dict = await obook_instance.make_obook()
                #print(f' i: {i}, obook: {obook_dict}, ticker {ticker}')z

            if obook_dict and ticker not in self.batch_data['v3_orderbook_topic']['tickers_in_batch']:
                #print(f"Sending to Kafka for {ticker}: {obook_dict}")
                message = json.dumps([ticker, obook_dict,i,generate_now_iso()]).encode('utf-8')
                await self.process_message('v3_orderbook_topic',ticker,message)
                print("Data sent to Kafka")

            print(
                f'tickers in batch : {self.batch_data["v3_orderbook_topic"]["tickers_in_batch"]}, ticker_being_analyzed: {ticker}')
            
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
        # Stop connections for both 'v3_trades' and 'v3_orderbook' channels
        uids_to_remove = [uid for uid, value in self.active_connections.items() if value[1].get('id') == ticker]
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

    async def correct_channel_path(self,channel):
        if 'accounts' in channel:
            return '/ws/accounts'
        return channel
    
    async def setup_ws_feed(self,channel,ticker=None):
        correct_channel_path_f = await self.correct_channel_path(channel)
        channels_w_id = ['v3_trades','v3_orderbook']
        signature = self.hbot.client.private.sign(
            request_path = f'{correct_channel_path_f}',
            method = 'GET',
            iso_timestamp = generate_now_iso(),
            data = {},
            )
        if channel in channels_w_id:
            return {
                    'type': 'subscribe',
                    'channel': f'{channel}',
                    'id':ticker,
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

    async def make_req_(self,i,channel,res,ticker,obooks_):
        obook = self.obook_select(obooks_,ticker)

        await self.select_channel_analysis(i,channel,res,ticker,obook)
      
        
    async def select_channel_analysis(self,i,channel,res,ticker,obook=None,):
        channel_analysis = {
            'v3_orderbook': self._obook_ws_analyze,
            'v3_trades': self._trade_ws_analyze,
            'v3_markets': self._market_ws_analyze,
            'v3_accounts': self._account_ws_analyze
        }
        #print(f'select channel analysis: {channel}')
        analysis_function = channel_analysis[channel]
        return await analysis_function(i,channel, res, ticker, obook)

    async def channel_specific_params(self,channel):
        #key (markets : active_queues), (trades,obook: queues), (accounts: queues)
        tickers_ = {'v3_orderbook':list(self.ws_obook_positions.keys()),'v3_trades':list(self.ws_obook_positions.keys()),\
                    'v3_markets':list(self.ws_market_positions.keys()), 'v3_accounts':list(self.ws_obook_positions.keys())}
        if self.addition_tickers:
            obooks_ = {
                    'v3_trades':None,
                    'v3_markets':None,
                    'v3_accounts':None,
                    'v3_orderbook':{ticker:OrderBook() for ticker in list(self.addition_tickers.keys())}
                    }
        else:
            obooks_ = {
                    'v3_trades':None,
                    'v3_markets':None,
                    'v3_accounts':None,
                    'v3_orderbook':{ticker:OrderBook() for ticker in list(self.ws_obook_positions.keys())}
                    }
        return tickers_[channel], obooks_[channel]
        
    async def _get_ticker_for_channel(self,req):
        return req['id'] if 'id' in req else None
    
    async def monitor_tickers(self):
        '''Monitor tickers within ws_obook_posns for addition and removal.'''
        if self.channels[0] not in ['v3_markets','v3_accounts']:
            current_tickers = {v[1]['id'] for k, v in self.active_connections.items() if 'id' in v[1]}
            required_tickers = set(self.ws_obook_positions.keys())
            new_tickers = required_tickers - current_tickers
            removed_tickers = current_tickers - required_tickers
            #print(f'removed tickers: {removed_tickers}')
            #print(f'active connections : {self.active_connections}')
            #print(f'new_tickers :{new_tickers}')
            if new_tickers:
                await self.flush_producer()
                # Clear batch data for all topics
                if 'v3_orderbook' in self.channels:
                    topic = 'v3_orderbook_topic'
                else:
                    topic = 'v3_trades_topic'
                    self.batch_data[topic]['batch'] = None
                    self.batch_data[topic]['tickers_in_batch'].clear()

                tasks = [self.start_new_connection_for_ticker(ticker, self.channels[0]) for ticker in new_tickers]
                await asyncio.gather(*tasks)
                self.addition_tickers = {}
                print(f'new tickers {new_tickers} added to ws feeds')

            if removed_tickers:
                if not self.cancellation_tickers:
                    self.cancellation_tickers_ = {ticker:'close' for ticker in removed_tickers}
                await self.flush_producer()
                # Clear batch data for all topics
                if 'v3_orderbook' in self.channels:
                    topic = 'v3_orderbook_topic'
                else:
                    topic = 'v3_trades_topic'
                    self.batch_data[topic]['batch'] = None
                    self.batch_data[topic]['tickers_in_batch'].clear()

                removal_tasks = [self.stop_connection_for_ticker(ticker) for ticker in removed_tickers]
                await asyncio.gather(*removal_tasks)
                self.cancellation_tickers = {}
                print(f'removed tickers {removed_tickers} removed from ws connections')

    async def _while_condn_for_channel(self,channel,ticker=None):
        channels_w_id = ['v3_trades','v3_orderbook']
        if channel in channels_w_id :
            return (ticker in self.ws_obook_positions)
        if channel == 'v3_accounts':
            return self.ws_obook_positions 
        elif channel == 'v3_markets':
            return self.ws_market_positions
        

    async def cleanup_active_connections(self):
        if self.start_up and self.active_connections:
            print('Cleaning up connections found for fresh start up')
            uids = list(self.active_connections.keys())  # Collect UIDs to avoid modification during iteration
            for uid in uids:
                try:
                    await self._disconnect_and_remove(uid)
                except Exception as e:
                    print(f"Error cleaning up connection {uid}: {e}")
            self.active_connections.clear()  # Clear all connections after cleanup
            self.start_up = False
        else:
            print('No active connections in instance, launching ws feed!')
            self.start_up = False

        
    async def make_ws_req(self, channel, req, i, tickers_, obooks_):
        uid = next(
            uid for uid, val in self.active_connections.items() if val[1] == req)

        async with self.semaphore:
            # Get the ticker from the request
            ticker = await self._get_ticker_for_channel(req)

            # Create a new WSConnection instance for each ticker
            ws_connection = self.active_connections[uid][0]

            # Connect the WebSocket
            await ws_connection.connect(ws_url)

            # Send the subscription request
            await ws_connection.send_json(req)
            print(f'uid {uid} for {ticker} in {channel}')
                # Continue receiving messages while the channel condition is met
            try:
                while await self._while_condn_for_channel(channel, ticker):

                    if ticker in self.cancellation_tickers:
                        break 
                    try:
                        # Receive a message from the WebSocket
                        res = await ws_connection.receive_json()
                        #print(f'res: {res}')
                        # Process the message if it contains 'contents'
                        if res is not None and 'contents' in res:
                            await self.make_req_(i, channel, res, ticker, obooks_)
                            i += 1
                        await self.monitor_tickers()

                        await asyncio.sleep(3)
                    except json.JSONDecodeError as e:
                        print(f"Received a non-JSON message or unable to parse the message: {e}")

            except Exception as e:
                print(f'{ticker} {channel} loop broken, {e}')
                print(f'double check producer {self.kafka_producer}')

            finally:
                # Ensure the WebSocket connection is closed
                print(f'Loop broken for {ticker} and {channel} performing cleanup')
                await self._disconnect_and_remove(uid)
        

    async def handle_kafka_msg(self):
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
            consumer_tasks = [process_consumer(consumer) for consumer in self.orderbook_consumers]
            await asyncio.gather(*consumer_tasks)
        else:
            if 'v3_orderbook' not in self.channels:
                await process_consumer(self.kafka_consumer)
        return all_data

    async def get_web_socket_feed(self):
        # get ws params for channel
        channels_w_id = ['v3_trades','v3_orderbook']
        tickers_, obooks_ = await self.channel_specific_params(self.channels[0])
       
        #await self.cleanup_active_connections()
        if self.channels[0] in channels_w_id:
            tasks = [self._create_and_start_connection(self.channels[0],ticker) for ticker in tickers_] 
            await gather(*tasks)
        else:
            #make 1 req accounts and markets
            make_actual_req = await self._create_and_start_connection(self.channels[0])

    async def data_get(self,ticker): 
        #remember no ticker in v3_accounts, or v3_markets only orderbook and trades have a ticker
        if self.channels[0] in ['v3_accounts','v3_markets']:
            ticker = None 
        data_accounts_markets = [item[1] for item in await self.handle_kafka_msg() ] # there is no item 0 for accounts or markets
        #print(f'data markets {data_accounts_markets}')
        data = [item[1] for item in await self.handle_kafka_msg() if ticker == item[0]]
        if 'v3_accounts' in self.channels and data_accounts_markets:
            return [order for order in data_accounts_markets if ticker == order['market'] ][0]
        if 'v3_markets' in self.channels and data_accounts_markets:
            return data_accounts_markets[0]
        if 'v3_orderbook' in self.channels and data:
            return data[0]
        if 'v3_trades' in self.channels and data:
            return data[0]
                        
async def elapsed_time_(t1):
    elapsed_ = (time.perf_counter() - t1)
    print(f'elapsed_time: {elapsed_}')
    return (elapsed_  >= 16)

if __name__ == '__main__':
    ETHEREUM_ADDRESS = '0xC66980969Ec9b7c17a81d2e2BCcd5C4E1221e638'
    eth_add2 = '0x14D7487c48DEE890396Bd66630fFd75Ba2A39194'
    pkey = os.getenv("pkey")
    pkey2 = os.getenv("pkey2")
    DYDX_API_CREDENTIALS = {
        "key": os.environ.get("ek2", ""),
        "secret": os.environ.get("es2", ""),
        "passphrase": os.environ.get("epp2", ""),
    }
    hstk = os.getenv("eck2")

    hbotf = DydxPerpetualClientWrapper(DYDX_API_CREDENTIALS['key'],DYDX_API_CREDENTIALS['secret'],\
                                       DYDX_API_CREDENTIALS['passphrase'],'0',hstk,eth_add2,None)
    
    hbotf.ws_obook_positions['ETH-USD'] = 'open'
    #hbotf.ws_obook_positions['RUNE-USD'] = 'open'
    #hbotf.ws_obook_positions['ETH-USD'] = 'open'
    #hbotf.ws_obook_positions['YFI-USD'] = 'open'
    #hbotf.ws_market_positions['BTC-USD'] = 'open'
    #webSassistant_ = wsAssistant(hbotf,['v3_orderbook'],4)
    #ebSassistant_trades = wsAssistant(hbotf,['v3_trades'],0)
    webSassistant_accounts = wsAssistant(hbotf,['v3_accounts'],0)
    #webSassistant_markets = wsAssistant(hbotf,['v3_markets'],0)
            
    async def _process_data(item):
        print(f'order_book_data for {item[0]} {item[2]}th iteration: {item[1]} at time {item[3]}')

    async def get_data_removal_test_obook():
        global webSassistant_
         # Start all consumers
        for consumer in webSassistant_.orderbook_consumers:
            print(f'subscribed topics : {consumer.subscription()}')
            await consumer.start()
        elapsed_time = False
        t1 = time.perf_counter()
        remove_tickers = ['ETH-USD','YFI-USD']
        while webSassistant_.ws_obook_positions :

            if not elapsed_time and await elapsed_time_(t1)  :
                print('Time elapsed!!..removing tickers')
                #webSassistant_.cancellation_tickers_ = {ticker:'close' for ticker in remove_tickers}
                webSassistant_.ws_obook_positions = {k:v for k,v in webSassistant_.ws_obook_positions.items() if k not in\
                        remove_tickers}

                #await webSassistant_.monitor_tickers()
                await asyncio.sleep(2)
                elapsed_time = True 
                print(f'Obook tickers are {webSassistant_.ws_obook_positions}')
                await asyncio.sleep(1)

            data = await webSassistant_.handle_kafka_msg()
            if data:
                process_data_ = [_process_data(item) for item in data]
                process_data_ = await gather(*process_data_)
          
        

    async def get_data_main_addition_test_obook():
        global webSassistant_
         # Start all consumers
        for consumer in webSassistant_.orderbook_consumers:
            print(f'subscribed topics : {consumer.subscription()}')
            await consumer.start()
        elapsed_time = False
        t1 = time.perf_counter()
        add_tickers = ['MATIC-USD','AVAX-USD']
        while webSassistant_.ws_obook_positions :

            if not elapsed_time and await elapsed_time_(t1)  :
                print('Time elapsed!!..adding tickers')
                webSassistant_.addition_tickers = {ticker:'close' for ticker in add_tickers}
                
                webSassistant_.ws_obook_positions.update(webSassistant_.addition_tickers)
                print(f'obook posns: {webSassistant_.ws_obook_positions}')
                await webSassistant_.monitor_tickers()
                await asyncio.sleep(1)
                elapsed_time = True 
                print(f'Obook tickers are {webSassistant_.ws_obook_positions}')

            data = await webSassistant_.handle_kafka_msg()
            if data:
                process_data_ = [_process_data(item) for item in data]
                process_data_ = await gather(*process_data_)
          

    async def get_data_removal_test_trades():
        global webSassistant_trades
         # Start all consumers
        await webSassistant_trades.kafka_consumer.start()
        print(f'subscribed topics : {webSassistant_trades.kafka_consumer.subscription()}')
        elapsed_time = False
        t1 = time.perf_counter()
        remove_tickers = ['ETH-USD','YFI-USD']
        print(f'trades posns: {webSassistant_trades.ws_obook_positions}')
        while webSassistant_trades.ws_obook_positions :

            try:
                if not elapsed_time and await elapsed_time_(t1)  :
                    print('Time elapsed!!..removing tickers')
                    webSassistant_trades.cancellation_tickers_ = {ticker:'close' for ticker in remove_tickers}
                    
                    webSassistant_trades.ws_obook_positions = {k:v for k,v in webSassistant_trades.ws_obook_positions.items() if k not in\
                            remove_tickers.keys()}
                    
                    await webSassistant_trades.monitor_tickers()
                    await asyncio.sleep(2)
                    elapsed_time = True 
                    print(f'Obook tickers are {webSassistant_trades.ws_obook_positions}')
                    await asyncio.sleep(1)

                data = await webSassistant_trades.handle_kafka_msg()
                if data:

                    process_data_ = [_process_data(item) for item in data]
                    process_data_ = await gather(*process_data_)
            except Exception as e:
                print(f'Data not ready yet , {e}')
          

    async def get_data_main_addition_test_trades():
        global webSassistant_trades
         # Start all consumers
        await webSassistant_trades.kafka_consumer.start()
        print(f'subscribed topics : {webSassistant_trades.kafka_consumer.subscription()}')
        elapsed_time = False
        t1 = time.perf_counter()
        add_tickers = ['MATIC-USD','AVAX-USD']
        while webSassistant_trades.ws_obook_positions :

            if not elapsed_time and await elapsed_time_(t1)  :
                print('Time elapsed!!..adding tickers')
                webSassistant_trades.addition_tickers = {ticker:'close' for ticker in add_tickers}
                
                webSassistant_trades.ws_obook_positions.update(webSassistant_trades.addition_tickers)
                print(f'obook posns: {webSassistant_trades.ws_obook_positions}')
                await webSassistant_trades.monitor_tickers()
                await asyncio.sleep(1)
                elapsed_time = True 
                print(f'Obook tickers are {webSassistant_trades.ws_obook_positions}')

            data = await webSassistant_trades.handle_kafka_msg()
            if data:

                process_data_ = [_process_data(item) for item in data]
                process_data_ = await gather(*process_data_)


    async def get_data_main_markets():
        global webSassistant_markets
        # Start all consumers
        await webSassistant_markets.kafka_consumer.start()
        print(f'subscribed topics : {webSassistant_markets.kafka_consumer.subscription()}')
        while webSassistant_markets.ws_market_positions :
            try:
                data = await webSassistant_markets.handle_kafka_msg()
                if data:
                    process_data_ = [_process_data(item) for item in data]
                    process_data_ = await gather(*process_data_)
            except Exception as e:
                print(f'Error processing data or handling with kafka , {e}')


    async def get_data_main_accounts():
        global webSassistant_accounts
        # Start all consumers
        #await webSassistant_accounts.kafka_consumer.start()
        print(f'subscribed topics : {webSassistant_accounts.kafka_consumer.subscription()}')
        while webSassistant_accounts.ws_obook_positions :
            try:
                data = await webSassistant_accounts.handle_kafka_msg()
                if data:
                    process_data_ = [_process_data(item) for item in data]
                    process_data_ = await gather(*process_data_)
            except Exception as e:
                print(f'Error processing data or handling with kafka , {e}')

    #1st wsAssistant test
    async def main():
        global webSassistant_accounts
        await webSassistant_accounts.initialize_data_instances()
        await asyncio.sleep(3)
        await get_data_main_accounts()
        await webSassistant_accounts.shutdown()

    asyncio.run(main())
