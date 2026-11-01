import boto3
import os
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url = 'http://localhost:8080',
    aws_access_key_id = os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key = os.getenv("S3_SECRET_KEY"),
    region_name = 'default'
)

s3.create_bucket(Bucket='owning-the-stack')