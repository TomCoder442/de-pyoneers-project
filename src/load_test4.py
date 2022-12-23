import boto3
import io
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
PROCESSED_BUCKET='processed-data-bucket-2022-12-21-1617'


try:
    user = 'project_team_2'
    host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    database = 'postgres'
    password = 'asVWTyV5kP53PRw'
    port = '5432'
    db_string = f"postgresql://{user}:{password}@{host}:5432/{database}"
    engine = create_engine(db_string)
    Session = scoped_session(sessionmaker(bind=engine))
    dest_tables = ['dim_counterparty','dim_currency','dim_date','dim_design','dim_location','dim_payment_type','dim_staff','dim_transaction','fact_payment','fact_purchase_orders', 'fact_sales_orders']
except Exception as e:
    print("Data load error: " + str(e))

def lambda_handler(event, context):
    #1. access bucket
    def list_files():
        s3_client = boto3.client('s3')
        objects = s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)
        for obj in range(0, len(objects['Contents'])):
            print(objects['Contents'][obj]['Key'])
            read_files(objects['Contents'][obj]['Key'], dest_tables[obj])


    #2. read the parquet files 
    def read_files(tbl_parquet, table_name):
        buffer = io.BytesIO()
        s3 = boto3.resource('s3')
        object = s3.Object('pyoneers-processed-data-test-2022-12-15', tbl_parquet)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
        df.to_sql(table_name, engine,index=False,if_exists='append')
        engine.execute(f"SELECT * FROM {table_name} limit 5").fetchall()
        print(df.head())

    list_files()