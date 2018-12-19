from utils.aws.db.dynamodb_client import DynamodbClient
from utils.aws.sqs.sqs_client import SqsClient
from utils.aws.db.structure import job_ids_key

import asyncio
import aiobotocore

from itertools import chain
from aiohttp import ClientSession

# 1. dynamoDB 에서 job_id를 들고온다.
# 2. total 값을 구하고 이 값을 이용하여 job id 를 scrap 한다.
# 3. DB 에서 들고온 id 랑 비교하여 scrap 되지 않은 job_id 를 구한다.
# 4. 새롭게 scrap 한 job_id 를 저장한다.
# 5. scrap 되지않은 job_data 구한다.
# 6. scrap 한 데이터를 파싱한다.
# 7. company data 와 job data 로 구분한다.
# 8. 이미지 url 은 sqs 에 전달.
# 9. 파싱한 job_data 를 저장한다.


GET_JOB_ID_URL = 'http://www.wanted.co.kr/api/v3/search?' \
                 'status=active&limit=12&tag_type_id=518&job_sort=-confirm_time' \
                 '&offset={0}'
GET_JOB_DATA_URL = 'https://www.wanted.co.kr/api/v1/jobs/{0}?lang=ko'
LIMIT = 12
SEMA = asyncio.Semaphore(10)
STRINGS_TO_PARSE_DATA = ['company_id', 'id', 'company_name', 'position', 'jd', 'create_time', 'company_info',
                         'location', 'logo_thumb_img']

QUEUE_NAME = ''
DY_ID_TABLE = ''
DY_DATA_TABLE = ''
REGION = 'ap-northeast-2'
SQS_MESSAGE = 'company logo image url'


def main(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scrap_init(loop))


async def scrap_init(loop):
    aws_session = aiobotocore.get_session(loop=loop)
    async with ClientSession(loop=loop) as session, \
            aws_session.create_client('sqs', region_name=REGION) as sqs_cli, \
            aws_session.create_client('dynamodb', region_name=REGION) as db_cli:
        dy_client = DynamodbClient(db_cli)
        sqs_client = SqsClient(sqs_cli)

        # Get total value
        total = await get_total_value(session, GET_JOB_ID_URL.format(0))

        # Get job ids using total value
        tasks = [
            asyncio.ensure_future(get_job_id(session, GET_JOB_ID_URL.format(offset)))
            for offset in range(0, total, LIMIT)
        ]
        result = await asyncio.gather(*tasks)
        job_ids = list(chain(*result))  # flatten list
        job_ids_in_dynamodb = (await dy_client.get_data(DY_ID_TABLE, job_ids_key))['Item']['ids']['NS']
        compared_ids = [str(id) for id in job_ids if str(id) not in job_ids_in_dynamodb]
        put_job_ids = job_ids_in_dynamodb + compared_ids

        # Get job datas using job ids
        queue_url = await sqs_client.get_queue_url(QUEUE_NAME)
        fs = [get_job_data(session, GET_JOB_DATA_URL.format(job_id), SEMA) for job_id in compared_ids]
        for future in asyncio.as_completed(fs, loop=loop):
            data = await future
            parsed_data = parse_data(data)
            logo_thumb_img_url = parsed_data['logo_thumb_img']
            filename = parsed_data['company_id']

            await sqs_client.send_message(queue_url, SQS_MESSAGE, logo_thumb_img_url, filename)
            await dy_client.insert_data(parsed_data, DY_DATA_TABLE)

        await dy_client.insert_data(put_job_ids, DY_ID_TABLE)


async def get_job_data(session, url, sem):
    async with sem:
        async with session.get(url) as response:
            return await response.json(content_type=None)


def parse_data(data):
    return {s: data[s] for s in STRINGS_TO_PARSE_DATA if s in data}


async def get_total_value(session, url):
    async with session.get(url) as response:
        return (await response.json(content_type=None))['data']['jobs']['total']


async def get_job_id(session, url):
    async with session.get(url) as response:
        res_job_list = (await response.json(content_type=None))['data']['jobs']['data']
        return [data['id'] for data in res_job_list]
