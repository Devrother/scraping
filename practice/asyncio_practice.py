import asyncio
import json

import aiohttp
import urllib.request


############
# async_1
# 1. total 값을 구한다.  get_total_value
# 2. elasticache 에서 저장된 id를 들고온다. get_job_id_elasticache
############
# async_2
# 3. job id 를 구하기 위해 total 값까지 반복문을 통해 요청할 task 를 생성하여 job id 를 구한다. get_job_id
############
# 4. 구한 job id 와 elasticache 에 저장되어있던 id 와 비교하여 없는 id 를 구별한다. compare_id
############
# async_3
# 5. 그 id 를 통해 job data 를 구한다. get_job_data
############
# async_4
# 6. 구해진 job data 는 aws rds 에 저장한다. insert_job_data_rds
# 7. 새로운 job id 는 elasticache 에 저장한다  insert_job_id_elasticache
############

def get_job_id_elasticache():
    return []


def compare_id(jl, jle):
    pass


def insert_job_data_rds():
    pass


def insert_job_id_elasticache():
    pass


async def get_job_data(session, url, job_id_list):
    return []


def get_total_value(url: str) -> int:
    print("start get_total_value")
    print("url : ", url)
    res_body = json.loads(urllib.request.urlopen(url).read().decode("utf-8"))
    return res_body['data']['jobs']['total']


async def get_job_id(url: str) -> list:
    print("get_job_id start : ", url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            res_body = response.json()
            print(res_body)
            res_job_list = res_body['data']['jobs']['data']
            job_id_list = []
            for data in res_job_list:
                job_id_list.append(data['id'])
            print(job_id_list)
            return job_id_list


async def main():
    url = 'https://www.wanted.co.kr/api/v3/search?' \
          'status=active&limit=12&tag_type_id=518&job_sort=-confirm_time' \
          '&offset={0}'
    # job_id_list_elasticache = get_job_id_elasticache()
    total = get_total_value(url.format(0))
    print("total : ", total)
    limit = 12

    futures = []

    for offset in range(0, total, limit):
        task = asyncio.ensure_future(get_job_id(url.format(offset)))
        futures.append(task)

    result = await asyncio.gather(*futures)
    print(result)
    # loop.run_until_complete(asyncio.wait(futures))  # TODO: job id list (return 값)을 어떻게 받지?

    # async with aiohttp.ClientSession() as session:
    #     print("getting job id...")
    #     job_id_list = await get_job_id(session, url.format(0))
    #     print(job_id_list.__len__())

    print("compare...")
    # compare_id(job_id_list, job_id_list_elasticache)

    # async with aiohttp.ClientSession() as session:
    #     job_data = await get_job_data(session, url, job_id_list)


def init_scrap():
    print("create loop")
    loop = asyncio.get_event_loop()
    print("start main routine")
    loop.run_until_complete(main())
    print("end main routine")
    loop.close()
    print("close loop")

init_scrap()