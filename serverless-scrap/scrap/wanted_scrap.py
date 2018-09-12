import asyncio
import json
from aiohttp import ClientSession
import urllib.request


############
# 1. total 값을 구한다.  get_total_value
# 2. elasticache 에서 저장된 id를 들고온다. get_job_id_elasticache
############
# 3. job id 를 구하기 위해 total 값까지 반복문을 통해 요청할 task 를 생성하여 job id 를 구한다. fetch_job_id
############
# 4. 구한 job id 와 elasticache 에 저장되어있던 id 와 비교하여 없는 id 를 구별한다. compare_id
############
# 5. 그 id 를 통해 job data 를 구한다. bound_fetch_job_data, fetch_job_data
############
# 6. 구해진 job data 는 aws rds 에 저장한다. insert_job_data_rds
# 7. 새로운 job id 는 elasticache 에 저장한다  insert_job_id_elasticache
############


def get_job_id_elasticache():
    pass


def compare_id():
    pass


def insert_job_data_rds():
    pass


def insert_job_id_elasticache():
    pass


def get_total_value(url: str) -> int:
    print("start get_total_value", url)
    res_body = json.loads(urllib.request.urlopen(url).read().decode("utf-8"))
    return res_body['data']['jobs']['total']


async def fetch_job_data(url: str, session: ClientSession) -> int:
    async with session.get(url) as response:
        # 가끔씩 text/html 로 응답이 와서 ContentTypeError 가 발생. content_type 에 None 대입.
        res_body = await response.json(content_type=None)
        print("end get_job_data", url)
        return res_body['id']


async def bound_fetch_job_data(sem: asyncio.Semaphore, url: str, session: ClientSession) -> int:
    # Getter function with semaphore.
    async with sem:
        return await fetch_job_data(url, session)


async def fetch_job_id(url: str) -> list:
    print("get_job_id start : ", url)
    async with ClientSession() as session:
        async with session.get(url) as response:
            res_job_list = (await response.json())['data']['jobs']['data']
            job_id_list = [data['id'] for data in res_job_list]
            print("get_job_id end : ", url)
            return job_id_list


async def scrap_job_ids() -> list:
    limit = 12
    url = 'http://www.wanted.co.kr/api/v3/search?' \
          'status=active&limit=12&tag_type_id=518&job_sort=-confirm_time' \
          '&offset={0}'
    total = get_total_value(url.format(0))
    print("total : ", total)
    tasks = [
        asyncio.ensure_future(fetch_job_id(url.format(offset)))
        for offset in range(0, total, limit)
    ]
    result = await asyncio.gather(*tasks)
    return [y for x in result for y in x]  # flatten list


async def scrap_job_datas(job_id_list: list):
    url = "https://www.wanted.co.kr/api/v1/jobs/{0}?lang=ko"
    sem = asyncio.Semaphore(800)

    async with ClientSession() as session:
        tasks = [
            asyncio.ensure_future(bound_fetch_job_data(sem, url.format(job_id), session))
            for job_id in job_id_list
        ]
        result = await asyncio.gather(*tasks)
        print("scrap_job_data : ", result)
        print("job_data_len", result.__len__())


def init_scrap(event, context):
    print("create loop")
    loop = asyncio.get_event_loop()

    print("start scrap_job_ids routine")
    job_id_list = loop.run_until_complete(scrap_job_ids())
    print("job_id_list len", job_id_list.__len__())
    print("end scrap_job_ids routine")

    print("start scrap_job_datas_routine")
    loop.run_until_complete(scrap_job_datas(job_id_list))
    print("end scrap_job_data_routine")

    loop.close()
    print("close loop")
