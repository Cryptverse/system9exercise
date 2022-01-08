import aiohttp
import asyncio

def cancel_tasks() -> None:
    for task in asyncio.all_tasks():
        task.cancel()


async def depth_snapshot(q: asyncio.Queue, order_book: dict) -> None:
    while q.empty(): await asyncio.sleep(0.0000001)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000') as resp:
                    resp = await resp.json()
                    order_book['lastUpdateId'] = resp['lastUpdateId']
                    order_book['bids'] = [[float(x) for x in y] for y in  resp['bids']]
                    order_book['asks'] = [[float(x) for x in y] for y in  resp['asks']]
                    await add_diff(q, order_book)
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

async def add_diff(q: asyncio.Queue, order_book: dict) -> None:
    #Keep looping until getting an event that intersects local orderbook
    while True:
        item = await q.get()
        #if last update ID of event is smaller than local orderbook ID, drop event
        if item['u'] <= order_book['lastUpdateId']: q.task_done()
        #Only process event if its update IDs overlap the update ID of local orderbook
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
            order_book['bids'] = sorted([x for x in order_book['bids'] if x[1] != 0], key = lambda x: x[0], reverse=True)
            order_book['asks'] = sorted([x for x in order_book['asks'] if x[1] != 0], key = lambda x: x[0])
            q.task_done()
            print(order_book)
            break

    while True:

        item = await q.get()
        if item['u'] <= order_book['lastUpdateId']: q.task_done()

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
        order_book['bids'] = sorted([x for x in order_book['bids'] if x[1] != 0], key = lambda x: x[0], reverse=True)
        order_book['asks'] = sorted([x for x in order_book['asks'] if x[1] != 0], key = lambda x: x[0])
        q.task_done()
        print(order_book)

async def main() -> None:
    print('Program started.')
    q = asyncio.Queue()
    order_book = {'bids':[], 'asks': [], 'lastUpdateId': 0}
    try:
        await asyncio.gather(queue_stream(q), depth_snapshot(q, order_book))
    except asyncio.CancelledError as e:
        print('Tasks cancelled.', e)

    while True:
        try:
            print('Program restarted.')
            await asyncio.gather(queue_stream(q), depth_snapshot(q, order_book))
        except asyncio.CancelledError as e:
            print('Tasks cancelled.', e)

if __name__ == '__main__':
    asyncio.run(main())
