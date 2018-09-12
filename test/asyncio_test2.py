import time
import datetime
import asyncio
import aiohttp

domain = 'http://integralist.co.uk'
a = '{}/foo?run={}'.format(domain, time.time())
b = '{}/bar?run={}'.format(domain, time.time())


async def get(url):
    print('GET: ', url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            t = '{0:%H:%M:%S}'.format(datetime.datetime.now())
            print('Done: {}, {} ({})'.format(t, response.url, response.status))


loop = asyncio.get_event_loop()
tasks = [
    asyncio.ensure_future(get(a)),
    asyncio.ensure_future(get(b))
]
loop.run_until_complete(asyncio.wait(tasks))