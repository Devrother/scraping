from utils.aws.db.structure import *


class DynamodbClient:
    def __init__(self, client):
        self.client = client

    async def insert_data(self, data, table_name, method='PutRequest'):
        await self.client.batch_write_item(
            RequestItems=create_dynamodb_write_structure(table_name, data, method)
        )

    async def get_data(self, table_name, key):
        return (await self.client.get_item(
            TableName=table_name,
            Key=key
        ))
