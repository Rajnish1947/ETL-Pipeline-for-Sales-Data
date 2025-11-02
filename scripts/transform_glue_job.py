import pandas as pd
import boto3
from io import StringIO
from utils.config import *

s3 = boto3.client('s3', region_name=AWS_REGION)
print(" Transforming data...")

# Read from S3
raw_obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_RAW_PATH)
df = pd.read_csv(raw_obj['Body'])

# Clean & Transform
df['total_value'] = df['price'] * df['quantity']
df['date'] = pd.to_datetime(df['date'])

# Save to S3 (transformed folder)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3.put_object(Bucket=S3_BUCKET, Key=S3_TRANSFORMED_PATH, Body=csv_buffer.getvalue())

print(f" Transformed file saved to s3://{S3_BUCKET}/{S3_TRANSFORMED_PATH}")
