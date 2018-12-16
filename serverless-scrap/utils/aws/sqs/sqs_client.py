from utils.aws.sqs.structure import *

class SqsClient:
    def __init__(self, client):
        self.client = client

    async def get_queue_url(self, queue_name):
        queue_url = (await self.client.get_queue_url(QueueName=queue_name))['QueueUrl']
        return queue_url

    async def send_message(self, queue_url, message, img_url, filename):
        message_attr_structure = create_message_attr_structure(img_url, filename)
        await self.client.send_message(
            QueueUrl=queue_url,
            MessageBody=message,
            MessageAttributes=message_attr_structure
        )



