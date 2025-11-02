import psycopg2
from utils.config import *

# Connect to Redshift
conn = psycopg2.connect(
    dbname=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD,
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT
)
cur = conn.cursor()

copy_sql = f"""
COPY {REDSHIFT_TABLE}
FROM 's3://{S3_BUCKET}/{S3_TRANSFORMED_PATH}'
IAM_ROLE 'arn:aws:iam::your-account-id:role/RedshiftCopyRole'
CSV IGNOREHEADER 1;
"""

print(" Loading data into Redshift...")
cur.execute(copy_sql)
conn.commit()
print(" Data successfully loaded into Redshift!")

cur.close()
conn.close()
