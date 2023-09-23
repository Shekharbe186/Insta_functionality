import boto3
import json
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def generate_presigned_url(bucket_name, object_key, expiration=3600):
    try:
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': object_key
            },
            ExpiresIn=expiration
        )
        return url
    except ClientError as e:
        error_message = f"An error occurred while generating presigned URL"
        raise Exception(error_message)

def download_image(event):
    try:
        if 'filename' not in event:
            raise ValueError("Invalid request format. 'filename' is required.")

        filename = event['filename']
        bucket_name = 'your-bucket-name'
        object_key = 'images/' + filename

        response = s3.head_object(Bucket=bucket_name, Key=object_key)

        presigned_url = generate_presigned_url(bucket_name, object_key)

        return {
            'statusCode': 200,
            'body': presigned_url
        }
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return {
                'statusCode': 404,
                'body': 'Image not found'
            }
        else:
            return {
                'statusCode': 500,
                'body': 'There was a problem fetching the image. Please try again'
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': 'There was a problem fetching the image. Please try again'
        }

def lambda_handler(event, context):
    return download_image(event)
