from urllib.parse import urlparse, parse_qs
import requests


def job_ids_scrapping(url):
    """
    scrap dev job id.
    :param url: url to scrap
    :return: job_id_list:
    :rtype list
    """
    params = dict(
        status='active',
        limit='12',
        tag_type_id='518',
        job_sort='-confirm_time'
    )
    job_id_list = []
    search_api = '/api/v3/search'

    for i in range(3):
        resp = requests.get(url=url + search_api, params=params).json()
        for job_data in resp['data']['jobs']['data']:
            job_id_list.append(job_data['id'])

        next_api = resp['links']['next']
        print(job_id_list.__len__())

        offset = parse_qs(urlparse(next_api).query)['offset'][0]
        params['offset'] = offset

    return job_id_list


def job_data_scrapping(job_id_list, url):
    """
    Scrap job data using job id.
    :param job_id_list:
    :param url: url to scrap
    """
    jobs_api = '/api/v1/jobs/'
    job_data_list = []

    for job_id in job_id_list:
        resp = requests.get(url=url + jobs_api + str(job_id)).json()
        job_data = dict()
        job_data['jd'] = resp['jd']
        job_data_list.append(job_data)
        print(job_data)


def init_scraping(event, context):
    wanted_url = 'https://www.wanted.co.kr'
    job_data_scrapping(job_ids_scrapping(wanted_url), wanted_url)
