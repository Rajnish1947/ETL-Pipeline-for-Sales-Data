import boto3
from utils.config import *
print(" Uploading raw data to S3...")

s3 = boto3.client('s3', region_name=AWS_REGION)
s3.upload_file("data/sales_data.csv", S3_BUCKET, S3_RAW_PATH)
print(f" Uploaded to s3://{S3_BUCKET}/{S3_RAW_PATH}")
