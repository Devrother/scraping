from urllib.parse import urlparse, parse_qs
import requests

wanted_url = 'https://www.wanted.co.kr'
search_api = '/api/v3/search'

params = dict(
    status = 'active',
    limit = '12',
    tag_type_id = '518',
    job_sort = '-confirm_time'
)

id_list = []
# while True:
    # resp = requests.get(url = wanted_url + search_api, params = params)
    # search_api_res_data = resp.json()

    # for d in search_api_res_data['data']['jobs']['data']:
    #     id_list.append(d['id'])

    # next_api = search_api_res_data['links']['next']
    # print(id_list.__len__())
    # if next_api is None:
    #     break
    # offset = parse_qs(urlparse(next_api).query)['offset'][0]
    # params['offset'] = offset

for i in range(10):
    resp = requests.get(url = wanted_url + search_api, params = params)
    search_api_res_data = resp.json()

    for d in search_api_res_data['data']['jobs']['data']:
        id_list.append(d['id'])

    next_api = search_api_res_data['links']['next']
    print(id_list.__len__())

    offset = parse_qs(urlparse(next_api).query)['offset'][0]
    params['offset'] = offset
    

# f = open('id_list.txt','w')
# f.write(id_list.__str__())
# f.close()

jobs_api = '/api/v1/jobs/'
res_list = []

for id in id_list:
    jobs_resp = requests.get(url=wanted_url + jobs_api + str(id))
    jobs_data = jobs_resp.json()
    res = dict()

    res['jd'] = jobs_data['jd']
    # res['location'] = jobs_data['location']
    # res['create_time'] = jobs_data['create_time']
    # res['company_name'] = jobs_data['company_name']
    # res['due_time'] = jobs_data['due_time']
    # res['id'] = jobs_data['id']

    res_list.append(res)
    print(res)


# f2 = open('data.txt','w')
# f2.write(res_list.__str__())
# f2.close()

# json.dumps(res_list)



# print (json.dumps(res_list, indent=2, sort_keys=True, ensure_ascii=False))