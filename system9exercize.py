import aiohttp
import asyncio
import pandas as pd
import json

async def depth_snapshot(order_book):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000') as resp:
                    #print(resp.status)
                    resp = await resp.json()
                    order_book['lastUpdateId'] = resp['lastUpdateId']
                    order_book['bids'] = resp['bids']
                    order_book['asks'] = resp['asks']

                    for i in order_book['bids']:
                        for j in i:
                            j = float(j)

                    for i in order_book['asks']:
                        for j in i:
                            j = float(j)

                    print(order_book)

                    break
        except Exception as e:
            print(e)


async def queue_stream(q, stream):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(stream) as ws:
                    while True:
                        
                        msg = await ws.receive()
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await q.put(msg.json())
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
                        
        except Exception as e:
            print(e)
            continue
        
async def add_diff(q, order_book):
    while q.qsize == 0: continue
    await depth_snapshot(order_book)
    while True:
        item = await q.get()
        print(order_book['lastUpdateId'], item['u'], item['U'])
        if item['u'] <= order_book['lastUpdateId']: continue
        
        if item['U'] <= order_book['lastUpdateId'] + 1 and item['u'] >= order_book['lastUpdateId'] + 1:
            for level in item['b']:
                bid_set = False
        
                for i in order_book['bids']:
                    if float(level[0]) == float(i[0]):
                        i[1] = float(level[1])
                        bid_set = True
                        break

                if not bid_set:
                    order_book['bids'].append([float(level[0]), float(level[1])])

            for level in item['a']:
                ask_set = False
                
                for i in order_book['asks']:
                    if float(level[0]) == float(i[0]):
                        i[1] = float(level[1])
                        ask_set = True
                        break

                if not ask_set:
                    order_book['asks'].append([float(level[0]), float(level[1])])

            order_book['lastUpdateId'] = item['u']

            order_book['bids'] = sorted(order_book['bids'], key = lambda x: x[0])
            order_book['asks'] = sorted(order_book['asks'], key = lambda x: x[0], reverse=True)
            break
        else:
            continue
        
    
    while True:
        
        item = await q.get()
        if item['u'] <= order_book['lastUpdateId']: continue
        
        for level in item['b']:
            
            bid_set = False
    
            for i in order_book['bids']:
                print(order_book)
                print(level[0], i[0])
                if float(level[0]) == float(i[0]):
                    print(i[1], level[1])
                    i[1] = float(level[1])
                    bid_set = True
                    break

            if not bid_set:
                order_book['bids'].append([float(level[0]), float(level[1])])

        for level in item['a']:
            ask_set = False
            
            for i in order_book['asks']:
                if float(level[0]) == float(i[0]):
                    i[1] = float(level[1])
                    ask_set = True
                    break

            if not ask_set:
                order_book['asks'].append([float(level[0]), float(level[1])])
                    
        order_book['lastUpdateId'] = item['u']

        order_book['bids'] = sorted(order_book['bids'], key = lambda x: x[0])
        order_book['asks'] = sorted(order_book['asks'], key = lambda x: x[0], reverse=True)
        print(order_book)
        #await asyncio.sleep(1)

async def main():
    stream = 'wss://stream.binance.com:9443/ws/bnbbtc@depth'
    q = asyncio.Queue()
    order_book = {'bids':[], 'asks': [], 'lastUpdateId': 0}
    await asyncio.gather(queue_stream(q, stream), add_diff(q, order_book))
    #await q.join()

if __name__ == '__main__':
    asyncio.run(main())
        
