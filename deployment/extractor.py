from pg8000.native import Connection
import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import io
from datetime import datetime 
from deployment import EXTRACT_BUCKET
dt = datetime.now()


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
        
        # max_created_dates, max_updated_dates = grab_last_created_date()

        src_tables = ['counterparty','currency','department','design','staff','sales_order','address','payment','purchase_order','payment_type','transaction']
        for tbl in src_tables:
            df = pd.read_sql_query(f"SELECT * FROM {tbl}" , engine)
            # df = pd.read_sql_query(f"SELECT * FROM {tbl} where created_at ='{max_created_dates[tbl]}'" , engine)

            print(df.head())
            load_to_s3(df, tbl, EXTRACT_BUCKET)
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
    
    
    except Exception as e:
        print("Data load to s3 error: " + str(e))


def grab_last_created_date():
        max_created_dates = {}
        max_updated_dates = {}
        try:
            user = 'project_user_2'
            host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
            database = 'totesys'
            password = 'paxjekPK3hDXu2aXcJ9xyuBS'
            port = '5432'
            conn = Connection(user=user, host=host, database=database, password=password, port=port)

            

            src_tables = ['counterparty','currency','department','design','staff','sales_order','address','payment','purchase_order','payment_type','transaction']
            for tbl in src_tables:
                max_date = conn.run(f'SELECT max(created_at) FROM {tbl}')
                max_created_dates[tbl] = str(max_date[0][0])
                print(max_date)

            for tbl in src_tables:
                max_date = conn.run(f'SELECT max(last_updated) FROM {tbl}')
                max_updated_dates[tbl] = str(max_date[0][0])
                print(max_date[0][0])
                
                
               

        except Exception as e:
            print("Data extract error: " + str(e))


        return max_created_dates, max_updated_dates


extract()