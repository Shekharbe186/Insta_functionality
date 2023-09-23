import boto3
import base64
import json
import uuid
from datetime import datetime
import time
import logging

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def enqueue_thumbnail_request(file_path):
    try:
        queue_url = 'your_sqs_queue_url'
        message = {'file_path': file_path}
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        return response
    except Exception as e:
        logger.error(f"Error enqueueing thumbnail request: {e}")
        return None

def is_valid_object_key(key):
    return len(key) > 0 and all(c.isalnum() or c in ['-', '_'] for c in key)

def generate_unique_file_path(filename):
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    unique_identifier = str(uuid.uuid4())[:8]  # getting only first 8 characters
    return f'images/{current_timestamp}_{unique_identifier}_{filename}'

def upload_image_and_enqueue_thumbnail(event):
    max_retries = 3
    retry_delay = 2  # Initial delay between retries in seconds

    for retry_attempt in range(max_retries):
        try:
            if 'body' not in event or 'filename' not in event:
                raise ValueError("Invalid request format. 'body' and 'filename' are required.")

            file_content = base64.b64decode(event['body']) # converting to binary form

            if not event['filename']:
                raise ValueError("Empty filename provided.")

            filename = event['filename']
            file_path = generate_unique_file_path(filename)

            if not is_valid_object_key(file_path):
                raise ValueError("Invalid file path.")

            s3.put_object(Bucket='your-bucket-name', Key=file_path, Body=file_content)

            response = enqueue_thumbnail_request(file_path)
            if response is None:
                raise Exception("Error enqueueing thumbnail request.")

            return {
                'statusCode': 200,
                'body': 'Image uploaded successfully!'
            }
        except Exception as e:
            logger.error(f"Error in Lambda function: {e}")
            if retry_attempt < max_retries - 1:
                # Exponential backoff
                time.sleep(retry_delay * (2 ** retry_attempt))
            else:
                return {
                    'statusCode': 500,
                    'body': 'Server Error'
                }
def lambda_handler(event, context):
    try:
        return upload_image_and_enqueue_thumbnail(event)
    except Exception as e:
        logger.error(f"Error in Lambda function: {e}")
        return {
            'statusCode': 500,
            'body': 'Server Error'
        }
