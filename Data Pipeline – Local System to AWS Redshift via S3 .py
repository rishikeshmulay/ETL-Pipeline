#!/usr/bin/env python
# coding: utf-8

# # Step 1: Upload data from Local system to S3

# In[ ]:


pip install boto3

import boto3

try:
    #create session
    session = boto3.Session(
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY',
        region_name='ap-south-1'  # Region e.g., Mumbai for India
    )
    
    #assign AWS S3 Resource
    s3 = session.resource('s3')

    local_file_path = 'path/to/your/local/file.csv'
    bucket_name = 'your-s3-bucket-name'
    s3_key = 'folder/in/bucket/file.csv'
    
    #upload file to s3
    s3.meta.client.upload_file(Filename=local_file_path, 
                               Bucket=bucket_name, 
                               Key=s3_key)

    print('File uploaded successfully to S3.')

except Exception as e:
    print(f'Error uploading file: {e}')


# # Step 2: Load data from S3 into Redshift

# In[ ]:


pip install psycopg2

import psycopg2

try:
    con = psycopg2.connect(
        dbname='dev',
        host='your-redshift-cluster.amazonaws.com',
        port=5439,
        user='your_username',
        password='your_password'
    )
except Exception as e:
    print("Error connecting to Redshift:", e)
    raise

con.autocommit = True
cur = con.cursor()

schema_name = 'your_schema'
table_name = 'your_table'
s3_location = 's3://your-bucket/path/to/yourfile.csv'
iam_role_arn = 'arn:aws:iam::account-id:role/your-redshift-role'

copy_query = f"""
COPY {schema_name}.{table_name}
FROM '{s3_location}'
IAM_ROLE '{iam_role_arn}'
FORMAT AS CSV
IGNOREHEADER 1;
"""

try:
    cur.execute(copy_query)
    print(f"✅ Data copied successfully into {schema_name}.{table_name} from {s3_location}")
except Exception as e:
    print("❌ Error executing COPY command:", e)
finally:
    cur.close()
    con.close()

