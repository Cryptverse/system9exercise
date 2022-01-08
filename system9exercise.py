import aiohttp
import asyncio

def cancel_tasks() -> None:
    for task in asyncio.all_tasks():
        task.cancel()

async def depth_snapshot(q: asyncio.Queue, orderbook: dict) -> None:
    #Only snap the orderbook once the update stream starts running.
    #Otherwise, the updates may not be subsequent, and hence, the 
    #update conditions will not be met.
    while q.empty(): await asyncio.sleep(0.0000001)
    #API calls 
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000') as resp:
                    resp = await resp.json()
                    orderbook['lastUpdateId'] = resp['lastUpdateId']
                    orderbook['bids'] = [[float(x) for x in y] for y in  resp['bids']]
                    orderbook['asks'] = [[float(x) for x in y] for y in  resp['asks']]
                    #add_diff is called from this function because updates should only be processed once snapshot is made.
                    await add_diff(q, orderbook)
            q.join()
            #Break out of loop as this call only needs to be made once per consistent web stream.
            break
        except Exception as e:
            print('Order book snapshot error:', e)

async def queue_stream(q: asyncio.Queue) -> None:
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect('wss://stream.binance.com:9443/ws/btcusdt@depth') as ws:
                    while True:
                        msg = await ws.receive()

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await q.put(msg.json())
                            continue
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            print('Websocket closed:', msg)
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print('Websocket error: ', msg)
                            break
                        else:
                            print(msg.type)
                            break

                await q.join()
                print('Queue joined.')
                cancel_tasks()
                #Websocket retry loop has to be broken since a new socket will be opened from main function.
                break

        except Exception as e:
            print('Websocket error:', e)

async def add_diff(q: asyncio.Queue, orderbook: dict) -> None:
    def update_orderbook() -> None:
            for level in item['b']:
                #Variable used to decide if price level should be appended if absent or updated if present
                bid_set = False

                for i in orderbook['bids']:
                    if float(level[0]) == i[0]:
                        i[1] = float(level[1])
                        bid_set = True
                        break

                if not bid_set: orderbook['bids'].append([float(level[0]), float(level[1])])

            for level in item['a']:
                ask_set = False

                for i in orderbook['asks']:
                    if float(level[0]) == i[0]:
                        i[1] = float(level[1])
                        ask_set = True
                        break

                if not ask_set:orderbook['asks'].append([float(level[0]), float(level[1])])

            #Last update ID has to be recorded for update logic and orderbook consistency
            orderbook['lastUpdateId'] = item['u']
            #Sort bids from highest to lowest and asks from lowest to highest. Drop zero-volume price levels.
            orderbook['bids'] = sorted([x for x in orderbook['bids'] if x[1] != 0], key = lambda x: x[0], reverse=True)
            orderbook['asks'] = sorted([x for x in orderbook['asks'] if x[1] != 0], key = lambda x: x[0])
            q.task_done()
            print(orderbook)
        
    #Keep looping until getting an event that intersects local orderbook
    while True:
        item = await q.get()
        
        #If last update ID of event is smaller than local orderbook ID, drop event. Update has already been recorded
        if item['u'] <= orderbook['lastUpdateId']:
            q.task_done()
            continue
        
        #Only process event if its update IDs overlap the last update ID of local orderbook.
        if item['U'] <= orderbook['lastUpdateId'] + 1 and item['u'] >= orderbook['lastUpdateId'] + 1:
            update_orderbook()
            break

    while True:
        item = await q.get()
        if item['u'] <= orderbook['lastUpdateId']:
            q.task_done()
            continue

        update_orderbook()

async def main() -> None:
    print('Program started.')
    q = asyncio.Queue()
    orderbook = {'bids':[], 'asks': [], 'lastUpdateId': 0}
    try:
        await asyncio.gather(queue_stream(q), depth_snapshot(q, orderbook))
    except asyncio.CancelledError as e:
        print('Tasks cancelled.', e)

    #Loop is expected to throw a cancellation error if web socket is interrupted. In that case,
    #loop is restarted.
    while True:
        try:
            print('Program restarted.')
            await asyncio.gather(queue_stream(q), depth_snapshot(q, orderbook))
        except asyncio.CancelledError as e:
            print('Tasks cancelled.', e)

if __name__ == '__main__':
    asyncio.run(main())
