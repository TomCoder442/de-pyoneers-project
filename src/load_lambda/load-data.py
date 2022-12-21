from pg8000.native import Connection
import boto3
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


#1. Connect to destination database
user = 'project_team_2'
host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
database = 'postgres'
password = 'asVWTyV5kP53PRw'
port = '5432'
conn = Connection(user=user, host=host, database=database, password=password, port=port)
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

#2. list buckets
def list_s3_bucket():
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    print("List of buckets")
    for bucket in response['Buckets']:
        print(f' {bucket["Name"]}')

list_s3_bucket()   

#3. access bucket
def list_files():
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket='pyoneers-processed-data-test-2022-12-15')
    for obj in objects['Contents']:
        print(obj['Key'])
        read_files(obj['Key'])


#4. read the parquet files 
def read_files(tbl):
    buffer = io.BytesIO()
    s3 = boto3.resource('s3')
    object = s3.Object('pyoneers-processed-data-test-2022-12-15', f'{tbl}')
    object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    print(df.head())


list_files()


# def load_data():
#     try:
#         user = 'project_team_2'
#         host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
#         database = 'postgres'
#         password = 'asVWTyV5kP53PRw'
#         port = '5432'
#         db_string = f"postgresql://{user}:{password}@{host}:5432/{database}"
#         engine = create_engine(db_string)
#         Session = scoped_session(sessionmaker(bind=engine))
#         df = pd.read_sql_query(f"SELECT * FROM sales_order" , engine)
#     except: 
#         return None, None

# load_data()
