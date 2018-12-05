import asyncio
import json
import aiobotocore

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
STRINGS_TO_PARSE_DATA = ['company_id', 'id', 'company_name', 'position', 'jd', 'create_time', 'company_info', 'location', 'logo_thumb_img']

QUEUE_NAME = ''
DYNAMODB_TABLE_NAME = ''

def main(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scrap_init(loop))


async def scrap_init(loop):
    aws_session = aiobotocore.get_session(loop=loop)
    async with ClientSession(loop=loop) as session, \
            aws_session.create_client('sqs', region_name='ap-northeast-2') as sqs_client:
        # Get total value
        total = await get_total_value(session, GET_JOB_ID_URL.format(0))

        # Get job ids using total value
        tasks = [
            asyncio.ensure_future(get_job_id(session, GET_JOB_ID_URL.format(offset)))
            for offset in range(0, total, LIMIT)
        ]
        result = await asyncio.gather(*tasks)
        job_ids = [y for x in result for y in x]  # flatten list
        job_ids_in_dynamodb = await get_dynamo_job_id(aws_session)
        compared_ids = [str(id) for id in job_ids if str(id) not in job_ids_in_dynamodb]
        put_job_ids = job_ids_in_dynamodb + compared_ids
        await push_dynamo_compared_job_id(aws_session, put_job_ids)

        # Get job datas using job ids
        queue_url = (await sqs_client.get_queue_url(QueueName=QUEUE_NAME))['QueueUrl']
        fs = [get_job_data(session, GET_JOB_DATA_URL.format(job_id), SEMA) for job_id in compared_ids]
        for future in asyncio.as_completed(fs, loop=loop):
            data = await future
            parsed_data = parse_data(data)
            logo_thumb_img_url = parsed_data['logo_thumb_img']
            filename = parsed_data['company_id']
            await sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody='company logo image url',
                MessageAttributes={
                    'logo_thumb_img_url': {
                        'DataType': 'String',
                        'StringValue': logo_thumb_img_url
                    },
                    'filename': {
                        'DataType': 'String',
                        'StringValue': str(filename)
                    }

                }
            )
            # await save_company_data
            # await save_job_data
            print("######")
            print("data: ", json.dumps(parsed_data, ensure_ascii=False))

        # tasks2 = [
        #     asyncio.ensure_future(get_job_data(session, GET_JOB_DATA_URL.format(job_id), SEM))
        #     for job_id in job_ids
        # ]
        # (done, pending) = await asyncio.wait(tasks2, timeout=250)
        # if pending:
        #     print("there is {} tasks not completed".format(len(pending)))
        # for f in pending:
        #     for fr in f.get_stack():
        #         print(fr.f_locals)
        #     f.cancel()
        # for f in done:
        #     x = await f
        #     print("job data :", x)


async def get_job_data(session, url, sem):
    async with sem:
        # print("get_job_data start : ", url)
        async with session.get(url) as response:
            return await response.json(content_type=None)


def parse_data(data):
    parsed_data = {}

    for s in STRINGS_TO_PARSE_DATA:
        parsed_data[s] = data[s]

    return parsed_data


async def get_total_value(session, url):
    async with session.get(url) as response:
        return (await response.json(content_type=None))['data']['jobs']['total']


async def get_job_id(session, url):
    # print("get_job_id start : ", url)
    async with session.get(url) as response:
        res_job_list = (await response.json(content_type=None))['data']['jobs']['data']
        # print("get_job_id end : ", url)
        return [data['id'] for data in res_job_list]


# Get job ids from dynamodb
async def get_dynamo_job_id(aws_session):
    async with aws_session.create_client('dynamodb', region_name='ap-northeast-2') as dynamo_client:
        response = await dynamo_client.get_item(
            TableName=DYNAMODB_TABLE_NAME,
            Key={'job_ids': {'S': 'job_ids'}}
        )
        return response['Item']['ids']['NS']


# Push compared_id into dynamodb
async def push_dynamo_compared_job_id(aws_session, put_job_ids):
    async with aws_session.create_client('dynamodb', region_name='ap-northeast-2') as dynamo_client:
        table_name = DYNAMODB_TABLE_NAME
        print('Writing to dynamo')
        request_items = create_batch_write_structure(table_name, put_job_ids)
        print("request_items: ", request_items)
        response = await dynamo_client.batch_write_item(
            RequestItems=request_items
        )


def create_batch_write_structure(table_name, put_job_ids):
    return {
        table_name: [
            {
                'PutRequest': {
                    'Item': {
                        "job_ids": {
                            "S": "job_ids"
                        },
                        "ids": {
                            "NS": put_job_ids
                        }
                    }
                }
            }
        ]
    }