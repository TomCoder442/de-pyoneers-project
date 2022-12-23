import json
import pandas as pd
import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import io
from datetime import datetime 
import json
dt = datetime.now()
MAX_DATE_BUCKET= 'max-date-bucket-2022-12-21-1617'
EXTRACT_BUCKET = 'ingestion-bucket-2022-12-21-1617'


def lambda_handler(event, context):
    #1.0 extract all data
    def extract():
        try:
            user = 'project_user_2'
            host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
            database = 'totesys'
            password = 'paxjekPK3hDXu2aXcJ9xyuBS'
            port = '5432'
            db_string = f"postgresql://{user}:{password}@{host}:5432/{database}"
            engine = create_engine(db_string)
            Session = scoped_session(sessionmaker(bind=engine))
            src_tables = ['counterparty','currency','department','design','staff','sales_order','address','payment','purchase_order','payment_type','transaction']
    
            max_created_dates, max_updated_dates = load_last_created_updated()
            if max_created_dates == None and max_updated_dates == None:
                print("Fetching all data from Totesys database")
                for tbl in src_tables:
                    df = pd.read_sql_query(f"SELECT * FROM {tbl}" , engine)
                    print(df.head())
                    load_to_s3(df, tbl, EXTRACT_BUCKET)
            else:
                print("Fetching updated data from Totesys database")
                for tbl in src_tables:
                    df = pd.read_sql_query(f"SELECT * FROM {tbl} where created_at >'{max_created_dates[tbl]}' OR last_updated >'{max_updated_dates[tbl]}'" , engine)
                    print(df.head())
                    if len(df)>0:
                        load_to_s3(df, tbl, EXTRACT_BUCKET)
            #store max dates in json file
        except Exception as e:
            print("Data extract error: " + str(e))
    
    #2.0 Load to s3
    def load_to_s3(df, tbl, EXTRACT_BUCKET):
        try:
            rows_imported = 0
            print(f'importing rows {rows_imported} to {rows_imported +len(df)}... for table {tbl}')
            #save to s3
            file_path = f"{dt}/{tbl}.csv"
    
            s3_client = boto3.client('s3')
    
            with io.StringIO() as csv_buffer:
                df.to_csv(csv_buffer, index=False)
    
                response  = s3_client.put_object(
                    Bucket=EXTRACT_BUCKET, Key=file_path, Body=csv_buffer.getvalue()
                )
    
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
                if status == 200:
                    print(f"successful s3 put_object response. Status - {status}")
                else:
                    print(f"Unsuccessful s3 put_object response. Status - {status}")
                rows_imported +=len(df)
                print("Data imported successfully")
                grab_last_created_updated_dates()
        except Exception as e:
            print("Data load to s3 error: " + str(e))
    
    #Established latest data from the data just loaded to s3 and store as JSON file
    def grab_last_created_updated_dates():
        max_created_dates = {}
        max_updated_dates = {}
        try:
            user = 'project_user_2'
            host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
            database = 'totesys'
            password = 'paxjekPK3hDXu2aXcJ9xyuBS'
            port = '5432'
            db_string = f"postgresql://{user}:{password}@{host}:5432/{database}"
            engine = create_engine(db_string)
            Session = scoped_session(sessionmaker(bind=engine))
            src_tables = ['counterparty','currency','department','design','staff','sales_order','address','payment','purchase_order','payment_type','transaction']
            for tbl in src_tables:
                max_create_df = pd.read_sql_query(f"SELECT max(created_at) FROM {tbl}", engine)
                max_created_dates[tbl] = str(max_create_df["max"].iloc[0])
                max_updated_df = pd.read_sql_query(f"SELECT max(last_updated) FROM {tbl}", engine)
                max_updated_dates[tbl] = str(max_updated_df["max"].iloc[0])  
            print(max_created_dates, max_updated_dates)
    
        except Exception as e:
            print("Data extract error: " + str(e))
    
        s3_client = boto3.client('s3')
        response  = s3_client.put_object(Bucket=MAX_DATE_BUCKET, Key='max_created_dates.json', Body=json.dumps(max_created_dates))    
        response2  = s3_client.put_object(Bucket=MAX_DATE_BUCKET, Key='max_updated_dates.json', Body=json.dumps(max_updated_dates))
        
        return max_created_dates, max_updated_dates
    
    #Load 
    def load_last_created_updated():
        try: 
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(MAX_DATE_BUCKET)
            date_list = {}
            for obj in bucket.objects.all():
                date_list[obj.key]=json.loads(obj.get()['Body'].read())
            return date_list["max_created_dates.json"], date_list["max_updated_dates.json"]
        except: 
            return None, None
    
    extract()