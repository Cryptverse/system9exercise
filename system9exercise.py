import aiohttp
import asyncio

async def depth_snapshot(order_book, q):
    while q.qsize() == 0: await asyncio.sleep(0.0000001)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000') as resp:
                    resp = await resp.json()
                    order_book['lastUpdateId'] = resp['lastUpdateId']
                    order_book['bids'] = [[float(x) for x in y] for y in  resp['bids']]
                    order_book['asks'] = [[float(x) for x in y] for y in  resp['asks']]
                    print(order_book)
                    await add_diff(q, order_book)
        except Exception as e:
            print(e)


async def queue_stream(q):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect('wss://stream.binance.com:9443/ws/bnbbtc@depth') as ws:
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
    while True:
        item = await q.get()
        if item['u'] <= order_book['lastUpdateId']: continue
        
        if item['U'] <= order_book['lastUpdateId'] + 1 and item['u'] >= order_book['lastUpdateId'] + 1:
            for level in item['b']:
                bid_set = False
        
                for i in order_book['bids']:
                    if float(level[0]) == i[0]:
                        i[1] = float(level[1])
                        bid_set = True
                        break
                    
                if not bid_set: order_book['bids'].append([float(level[0]), float(level[1])])

            for level in item['a']:
                ask_set = False
                
                for i in order_book['asks']:
                    if float(level[0]) == i[0]:
                        i[1] = float(level[1])
                        ask_set = True
                        break

                if not ask_set:order_book['asks'].append([float(level[0]), float(level[1])])

            order_book['lastUpdateId'] = item['u']
            order_book['bids'] = sorted([x for x in order_book['bids'] if x[1] != 0], key = lambda x: x[0])
            order_book['asks'] = sorted([x for x in order_book['asks'] if x[1] != 0], key = lambda x: x[0], reverse=True)
            break
        else:
            continue
        
    
    while True:
        
        item = await q.get()
        if item['u'] <= order_book['lastUpdateId']: continue
        
        for level in item['b']:
            bid_set = False
    
            for i in order_book['bids']:
                if float(level[0]) == i[0]:
                    i[1] = float(level[1])
                    bid_set = True
                    break

            if not bid_set: order_book['bids'].append([float(level[0]), float(level[1])])

        for level in item['a']:
            ask_set = False
            
            for i in order_book['asks']:
                if float(level[0]) == i[0]:
                    i[1] = float(level[1])
                    ask_set = True
                    break

            if not ask_set:order_book['asks'].append([float(level[0]), float(level[1])])
                    
        order_book['lastUpdateId'] = item['u']
        order_book['bids'] = sorted([x for x in order_book['bids'] if x[1] != 0], key = lambda x: x[0])
        order_book['asks'] = sorted([x for x in order_book['asks'] if x[1] != 0], key = lambda x: x[0], reverse=True)
        print(order_book)

async def main():
    q = asyncio.Queue()
    order_book = {'bids':[], 'asks': [], 'lastUpdateId': 0}
    await asyncio.gather(queue_stream(q), depth_snapshot(order_book, q))
    await q.join()

if __name__ == '__main__':
    asyncio.run(main())
        
