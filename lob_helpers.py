import asyncio
import datetime 
import builtins
from asyncio import * 
import pandas as pd 
import time 

async def _elapsed_time(t0):
    return (time.perf_counter() - t0) > 601

def print(*args, **kwargs):
    kwargs["flush"] = True
    builtins.print(*args, **kwargs)

def find_dec_places(pr):
    try:
        #if int(pr) == 1:
        #    return 0
        places = str(pr)[::-1].find('.')
        #if places < 0:
        #    places = 1 
        return places 
    except:
        return 0

def midpoint(price1,price2,rd):
    return round((price1 + price2)/2,rd)

def parse_trade_time(time):
    return datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ')

#movement hash for predictor
res_hash = {2:"shifting_right",1:"shifting_left",0:'wait or do nothing'}

def slippage_calc(avg_execution_price,expected_price, order_type,redn):
    """
    Calculate slippage based on average execution price, expected price, order type, and whether it's a reducing order.
    """
    if not redn :
        # kind of like p_l_long = size*(pf - po) p_l_short = size*(po - p)
        slippage = (avg_execution_price - expected_price) if order_type.upper() == 'LONG' else (expected_price - avg_execution_price)
    else:
        slippage = (avg_execution_price - expected_price) if order_type.upper() == 'SHORT' else (expected_price - avg_execution_price)
    return slippage

def estimate_slippage(order_book, order_size, order_type,redn,rd):
    """
    Estimate slippage and average execution price based on the order book.
    """
    total_size = 0
    total_cost = 0.0
    
    if order_type.upper() == 'LONG':
        side = 'bids' if redn else 'asks'
        expected_price = float(order_book[side][0]['price'])
    elif order_type.upper() == 'SHORT':
        side = 'asks' if redn else 'bids'
        expected_price = float(order_book[side][0]['price'])
    else:
        raise ValueError("Order type must be either 'LONG' or 'SHORT'")
        
    for order in order_book[side]:
        price = float(order['price'])
        size = float(order['size'])
        available_size = min(order_size - total_size, size)
        total_cost += available_size * price
        total_size += available_size
        
        if total_size >= order_size:
            break
            
    if total_size < order_size:
        raise Exception("Not enough liquidity to fulfill order")
    
    avg_execution_price = round(total_cost / total_size,rd)
    slippage = slippage_calc(avg_execution_price,expected_price, order_type,redn)

    return avg_execution_price, slippage
        
#function side helper
def side_helper(side,best_bid,best_ask,redn):
    if not redn:
        if side.upper() == 'SHORT':
            return best_ask
        return best_bid
    else:
        if side.upper() == 'SHORT':
            return best_bid
        return best_ask

def side_helper_f(side,redn,trade_or_queue):
    if redn:
        if side.upper() == 'SHORT':
            side = 'BUY' if trade_or_queue else 'bids'
        if side.upper() == 'LONG':
            side = 'SELL' if trade_or_queue else 'asks'
    else:
        if side.upper() == 'SHORT':
            side = 'SELL' if trade_or_queue else 'asks'
        if side.upper() == 'LONG':
            side = 'BUY' if trade_or_queue else 'bids'
        
    return side 

#if add status
#only thing that needs one is limit order 
def get_ord(order):
    if order != None:
        return order
    return False 

def get_ord_by_id(active_orders,id):
    return [i for i in active_orders['orders'] if i['id'] == id ][0]

def order_obook(side,book):
    return [float(el['price']) for el in book[side]]

async def calc_pl_w_fee(price_entry,price_exit,size,side,margin_fees):
    if side.upper() == 'SHORT':
        p_l = size * (price_entry - price_exit)
    else:
        p_l = size * (price_exit - price_entry)
    fee = size*price_exit*margin_fees['taker']
    return p_l, fee
    
def tickers_to_df(trades):
    if len(list(trades.keys())) > 0:
        df = pd.DataFrame.from_dict(trades,orient='index',columns=['price_entry','price_exit','size','side','p_l','ticker'])
        df.to_csv(f'traderhelpefralgorecords{datetime.datetime.now().month}{datetime.datetime.now().day}\
                    {datetime.datetime.now().year}.csv')
        
async def o_book_stats(obook,price,side,redn,trade_or_queue):
    side = side_helper_f(side,redn,trade_or_queue)
    o_book_modded = {float(el['price']):float(el['size']) for i,el in enumerate(obook[side]) if (side == 'bids' \
                and float(el['price']) >= price) or (side == 'asks' and  float(el['price']) <= price) }
    queue_len = sum(list(o_book_modded.values()))
    level = len(list(o_book_modded.keys())) - 1
    size = o_book_modded[price]
    return level, size, queue_len

def estimate_loss_to_slippage(slippage,side,price_original,size,redn):
    #slippage means your losing profit by buying higher in the dirn of trade
    #where red means reduce order 
    if not redn:
        if side.upper() == 'LONG':
            pf = price_original*(1 + slippage)
            slippage_f = pf - price_original
            loss = size * slippage_f
        else:
            pf = price_original*(1 - slippage)
            slippage_f = price_original  - pf 
            loss = size* slippage_f
    else:
        if side.upper() == 'LONG':
            pf = price_original*(1 - slippage)
            slippage_f = price_original  - pf 
            loss = size* slippage_f
        else:
            pf = price_original*(1 + slippage)
            slippage_f = pf - price_original
            loss = size * slippage_f
    return loss 

def incoming_slippage(best_bid,best_ask,price_original,side,redn):
    if side.upper() == 'SHORT':
        if redn:
            incoming_slippage = (best_ask - price_original)

        else:
            incoming_slippage = (price_original - best_bid)
    else:
        if redn:
            incoming_slippage = (price_original - best_bid)
        else:
            incoming_slippage = (best_ask - price_original)
    return incoming_slippage


async def get_updt_q_info_for_ticker(positions,queues):
    #this should work for obook, trades, and price_queues
    tickers_ = list(positions.keys())
    item_coroutines = [pq.get() for pq in queues]
    _items = await gather(*item_coroutines)
    _items = [i for i in _items if i != None]
    _items = [(item + [i]) for i,item in enumerate(_items) if item[0] in tickers_]
    _tickers_info = {item[0]:item[1:] for item in _items}
    return _tickers_info

