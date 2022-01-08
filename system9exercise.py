import aiohttp
import asyncio

def cancel_tasks() -> None:
    for task in asyncio.all_tasks():
        task.cancel()

async def depth_snapshot(q: asyncio.Queue, orderbook: dict) -> None:
    while q.empty(): await asyncio.sleep(0.0000001)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000') as resp:
                    resp = await resp.json()
                    orderbook['lastUpdateId'] = resp['lastUpdateId']
                    orderbook['bids'] = [[float(x) for x in y] for y in  resp['bids']]
                    orderbook['asks'] = [[float(x) for x in y] for y in  resp['asks']]
                    await add_diff(q, orderbook)
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

        except Exception as e:
            print('Websocket error:', e)

async def add_diff(q: asyncio.Queue, orderbook: dict) -> None:
    def update_orderbook() -> None:
            for level in item['b']:
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

            orderbook['lastUpdateId'] = item['u']
            orderbook['bids'] = sorted([x for x in orderbook['bids'] if x[1] != 0], key = lambda x: x[0], reverse=True)
            orderbook['asks'] = sorted([x for x in orderbook['asks'] if x[1] != 0], key = lambda x: x[0])
            q.task_done()
            print(orderbook)
        
    #Keep looping until getting an event that intersects local orderbook
    while True:
        item = await q.get()
        
        #if last update ID of event is smaller than local orderbook ID, drop event
        if item['u'] <= orderbook['lastUpdateId']:
            q.task_done()
            continue
        
        #Only process event if its update IDs overlap the update ID of local orderbook
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

    while True:
        try:
            print('Program restarted.')
            await asyncio.gather(queue_stream(q), depth_snapshot(q, orderbook))
        except asyncio.CancelledError as e:
            print('Tasks cancelled.', e)

if __name__ == '__main__':
    asyncio.run(main())
