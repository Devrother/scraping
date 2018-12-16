def create_message_attr_structure(img_url, filename):
    return {
        'logo_thumb_img_url': {
            'DataType': 'String',
            'StringValue': img_url
        },
        'filename': {
            'DataType': 'String',
            'StringValue': str(filename)
        }
    }
