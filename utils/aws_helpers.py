import boto3
import pandas as pd
import io

# Initialize S3 client (uses credentials from AWS CLI or environment variables)
s3_client = boto3.client('s3')


def upload_to_s3(dataframe, bucket_name, file_key):
    """
    Upload a pandas DataFrame as a CSV file to S3
    """
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
    print(f" Uploaded {file_key} to bucket {bucket_name}")


def download_from_s3(bucket_name, file_key):
    """
    Download CSV file from S3 and return as a pandas DataFrame
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(content))
    print(f" Downloaded {file_key} from bucket {bucket_name}")
    return df

