import asyncio
import time

urls = ('http://python.org', 'http://google.com', 'http://') # 예닐곱 개 정도의 URL을 준비한다.

async def test_urls(co, urls):
    s = time.time()
    fs = {co(url) for url in urls}
    for f in asyncio.as_completed(fs):
        url, body, t = await f
        print
        print(f'Resonse from: {url}, {len(body)}Bytes - {f}sec')
        print(f'{time.time() - s:0.3f}sec')

loop = asyncio.get_event_loop()
loop.run_until_complete(test_urls(get_url_data, urls))