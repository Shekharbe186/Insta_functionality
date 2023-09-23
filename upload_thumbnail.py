import boto3
from PIL import Image
from io import BytesIO
import logging
import time

s3 = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def generate_and_upload_thumbnail(source_bucket, source_key, destination_key):
    max_retries = 3
    retry_delay = 2  # Initial delay between retries in seconds
    
    for retry_attempt in range(max_retries):
        try:
            response = s3.get_object(Bucket=source_bucket, Key=source_key)
            image_content = response['Body'].read()
            image = Image.open(BytesIO(image_content))

            thumbnail = image.resize((100, 100))

            thumbnail_io = BytesIO()
            thumbnail.save(thumbnail_io, format='JPEG')
            thumbnail_io.seek(0)

            s3.upload_fileobj(thumbnail_io, source_bucket, destination_key)

            return f"Thumbnail '{destination_key}' uploaded successfully."
        except Exception as e:
            logger.error(f"Error generating thumbnail: {e}")
            if retry_attempt < max_retries - 1:
                # Exponential backoff
                time.sleep(retry_delay * (2 ** retry_attempt))
            else:
                raise Exception(f"Error generating thumbnail: {e}")


def process_thumbnail(event):
    try:
        filename = event['Records'][0]['body']
        source_bucket = 'your-bucket-name'
        source_key = f'images/{filename}'
        destination_key = f'thumbnails/{filename}'

        # Generate and upload thumbnail
        result = generate_and_upload_thumbnail(source_bucket, source_key, destination_key)

        return {
            'statusCode': 200,
            'body': result
        }
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': 'Server Error'
        }

def lambda_handler(event, context):
    return process_thumbnail(event)

