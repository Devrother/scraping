
job_ids_key = {
    "job_ids": {
        "S": "job_ids"
    }
}


def create_dynamodb_write_structure(table_name, data, method):
    item = None
    if table_name == 'job_ids':
        item = _get_job_ids_item(data)
    elif table_name == 'job_and_company_datas':
        item = _get_job_and_company_datas_item(data)

    return {table_name: [{method: item}]}


def _get_job_ids_item(data):
    return {
        'Item': {
            "job_ids": {"S": "job_ids"},
            "ids": {"NS": data}
        }
    }


def _get_job_and_company_datas_item(data):
    return {
        'Item': {
            "company_id": {"N": str(data["company_id"])},
            "job_id": {"N": str(data["id"])},
            "meta": {
                "M": {
                    "company_info": {"S": str(data["company_info"])},
                    "company_name": {"S": str(data["company_name"])},
                    "jd": {"S": str(data["jd"])},
                    "job_data_created_at": {"S": str(data["create_time"])},
                    "location": {"S": str(data["location"])},
                    "logo_img": {"S": str(data["logo_thumb_img"])},
                    "position": {"S": str(data["position"])}
                }
            }
        }
    }

