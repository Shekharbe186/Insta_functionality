import boto3
import base64
import json
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

def is_filename_present(bucket_name, filename):
    try:
        response = s3.head_object(Bucket=bucket_name, Key=filename)
        return True
    except Exception as e:
        return False

def upload_image_and_enqueue_thumbnail(event):
    try:
        if 'body' not in event or 'filename' not in event:
            raise ValueError("Invalid request format. 'body' and 'filename' are required.")

        file_content = base64.b64decode(event['body']) # converting to binary form

        if not event['filename']:
            raise ValueError("Empty filename provided.")

        filename = event['filename']
        bucket_name = 'your-bucket-name'

        if is_filename_present(bucket_name, filename):
            raise ValueError(f"Filename '{filename}' already exists in the bucket.")

        if not is_valid_object_key(filename):
            raise ValueError("Invalid file path.")

        s3.put_object(Bucket=bucket_name, Key=filename, Body=file_content)

        response = enqueue_thumbnail_request(filename)
        if response is None:
            raise Exception("Error enqueueing thumbnail request.")

        return {
            'statusCode': 200,
            'body': 'Image uploaded successfully!'
        }
    except Exception as e:
        logger.error(f"Error in Lambda function: {e}")
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
