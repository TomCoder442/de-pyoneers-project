import json
import boto3
import io
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

def lambda_handler(event, context):
    
    s3_client = boto3.client('s3')
    DESTINATION_BUCKET = 'processed-data-bucket-2022-12-21-1617'
    
    try:
        user = 'project_team_2'
        host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
        database = 'postgres'
        password = 'asVWTyV5kP53PRw'
        port = '5432'
        db_string = f'postgresql://{user}:{password}@{host}:5432/{database}'
        engine = create_engine(db_string)
        Session = scoped_session(sessionmaker(bind=engine))
    except Exception as e:
        print('Data load error: ' + str(e))
    
    
    #1. access bucket
    def list_files():
        objects = s3_client.list_objects_v2(Bucket=DESTINATION_BUCKET)
        print(objects)
        try:
            for obj in range(0, len(objects['Contents'])):
                print(objects['Contents'][obj]['Key'])
                read_files(objects['Contents'][obj]['Key'], objects['Contents'][obj]['Key'].split(".")[0], len(objects['Contents']))
        except Exception as e:
            print("Destination bucket is empty: " + str(e))
    
    #2. read the parquet files
    def read_files(tbl_parquet, table_name, number_of_files):
        buffer = io.BytesIO()
        s3 = boto3.resource('s3')
        object = s3.Object(DESTINATION_BUCKET, tbl_parquet)
        object.download_fileobj(buffer)
        conn = engine.connect()
        result = conn.execute(f'SELECT * FROM {table_name}')
        database_data = pd.DataFrame(result.fetchall())
        df = pd.read_parquet(buffer)
    
        print(database_data)
        print(df)
    
        if number_of_files != 11:
            df.to_sql(table_name, con=engine, if_exists='append', schema='project_team_2', index=False)
    
        else:
            if database_data.empty:
                df.to_sql(table_name, con=engine, if_exists='append', schema='project_team_2', index=False)
            else:
                unique_key_columns = {
                    'dim_counterparty': 'counterparty_id',
                    'dim_currency': 'currency_id',
                    'dim_date': 'date_id',
                    'dim_design': 'design_id',
                    'dim_location': 'location_id',
                    'dim_payment_type': 'payment_type_id',
                    'dim_staff': 'staff_id',
                    'dim_transaction': 'transaction_id',
                    'fact_payment': 'payment_id',
                    'fact_purchase_order': 'purchase_order_id',
                    'fact_sales_order': 'sales_order_id',
                }
                unique_key_column = unique_key_columns[table_name]
                if table_name == 'dim_date':
                    df[unique_key_column] = pd.to_datetime(df[unique_key_column])
                    database_data[unique_key_column] = pd.to_datetime(database_data[unique_key_column])
    
                # Merge df and database_data and add an indicator column
                merged_data = pd.merge(df, database_data, on=[unique_key_column], how='outer', indicator='duplicate', suffixes=['', '_database'])
                print(merged_data)
    
                # Keep only the rows that are unique to df
    
                unique_data = merged_data[merged_data['duplicate'] == 'left_only']
    
                # Drop the indicator column and insert the resulting data into the table in the database
                unique_data = unique_data.drop(columns=['duplicate'])
                if table_name== 'dim_transaction':
                        unique_data['sales_order_id'] = unique_data['sales_order_id'].fillna(0)
                        unique_data['purchase_order_id'] = unique_data['purchase_order_id'].fillna(0)
                        unique_data['sales_order_id'] = unique_data['sales_order_id'].astype(int)
                        unique_data['purchase_order_id'] = unique_data['purchase_order_id'].astype(int)
                if table_name == 'fact_payment':
                        unique_data['payment_id'] = unique_data['payment_id'].astype(int)
                        unique_data['transaction_id'] = unique_data['transaction_id'].astype(int)
                        unique_data['counterparty_id'] = unique_data['counterparty_id'].astype(int)
                        unique_data['currency_id'] = unique_data['currency_id'].astype(int)
                        unique_data['payment_type_id'] = unique_data['payment_type_id'].astype(int)
                if table_name== 'fact_purchase_order':
                        unique_data['staff_id'] = unique_data['staff_id'].astype(int)
                        unique_data['counterparty_id'] = unique_data['counterparty_id'].astype(int)
                        unique_data['item_quantity'] = unique_data['item_quantity'].astype(int)
                        unique_data['currency_id'] = unique_data['currency_id'].astype(int)
                        unique_data['agreed_delivery_location_id'] = unique_data['agreed_delivery_location_id'].astype(int)
                if table_name == 'fact_sales_order':
                        unique_data['sales_staff_id'] = unique_data['sales_staff_id'].astype(int)
                        unique_data['counterparty_id'] = unique_data['counterparty_id'].astype(int)
                        unique_data['units_sold'] = unique_data['units_sold'].astype(int)
                        unique_data['currency_id'] = unique_data['currency_id'].astype(int)
                        unique_data['design_id'] = unique_data['design_id'].astype(int)
                        unique_data['agreed_delivery_location_id'] = unique_data['agreed_delivery_location_id'].astype(int)
                    
                print(unique_data)
                for index, row in unique_data.iterrows():
                    columns =df.columns
                    values = [f"'{row[i]}'" for i in range(len(df.columns))]
                    column_list = ",".join(columns)
                    print(column_list)
                    values_list = ",".join(values)
                    insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({values_list})"
                    print(insert_query)
                    engine.execute(insert_query)
    
    list_files()
    
    # List all objects in the bucket
    objects = s3_client.list_objects_v2(Bucket=DESTINATION_BUCKET)
    
    # Extract the keys of the objects
    try:
        keys = [{'Key': obj['Key']} for obj in objects['Contents']]
        print(keys)
        print({'Objects': keys})
        # Delete all objects in the bucket
        s3_client.delete_objects(Bucket=DESTINATION_BUCKET, Delete={'Objects': keys})
        print('all objects deleted')
    except Exception as e:
        print("Destination bucket is empty: " + str(e))
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }