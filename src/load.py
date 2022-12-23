import boto3
import io
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
def lambda_handler():
    try:
        user = 'project_team_2'
        host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
        database = 'postgres'
        password = 'asVWTyV5kP53PRw'
        port = '5432'
        db_string = f'postgresql://{user}:{password}@{host}:5432/{database}'
        engine = create_engine(db_string)
        Session = scoped_session(sessionmaker(bind=engine))
        dest_tables = ['dim_counterparty','dim_currency','dim_date','dim_design','dim_location','dim_payment_type','dim_staff','dim_transaction','fact_payment','fact_purchase_order', 'fact_sales_order']
    except Exception as e:
        print('Data load error: ' + str(e))
    #1. access bucket
    def list_files():
        s3_client = boto3.client('s3')
        objects = s3_client.list_objects_v2(Bucket='processed-data-bucket-2022-12-21-1617')
        for obj in range(0, len(objects['Contents'])):
            print(objects['Contents'][obj]['Key'])
            read_files(objects['Contents'][obj]['Key'], dest_tables[obj])
    #2. read the parquet files
    def read_files(tbl_parquet, table_name):
        buffer = io.BytesIO()
        s3 = boto3.resource('s3')
        object = s3.Object('processed-data-bucket-2022-12-21-1617', tbl_parquet)
        object.download_fileobj(buffer)
        conn = engine.connect()
        result = conn.execute(f'SELECT * FROM {table_name}')
        database_data = pd.DataFrame(result.fetchall())
        df = pd.read_parquet(buffer)

        unique_key_columns = {
            'dim_counterparty': 'counterparty_id',
            'dim_currency': 'currency_id',
            'dim_date': 'date_id',
            'dim_design': 'design_id',
            'dim_location': 'location_id',
            'dim_payment_type': 'payment_type_id',
            'dim_staff': 'staff_id',
            'dim_transaction': 'transaction_id',
            'fact_payment': 'payment_record_id',
            'fact_purchase_order': 'purchase_order_id',
            'fact_sales_order': 'sales_order_id',
        }

        if database_data.empty:
            df.to_sql(table_name, engine, index=False, if_exists='append')
        else:
            unique_key_column = unique_key_columns[table_name]
            if table_name == 'dim_date':
                # df[unique_key_column] = pd.to_datetime(df[unique_key_column])
                database_data[unique_key_column] = pd.to_datetime(database_data[unique_key_column])

            # Merge df and database_data and add an indicator column
            print(df)
            print('hi')
            print(database_data)
            merged_data = pd.merge(df, database_data, on=[unique_key_column], how='outer', indicator='duplicate', suffixes=['', '_database'])
            print(merged_data)

            # Keep only the rows that are unique to df
            unique_data = merged_data[merged_data['duplicate'] == 'left_only']

            # Drop the indicator column and insert the resulting data into the table in the database
            unique_data = unique_data.drop(columns=['duplicate'])
            print('yo')
            print(unique_data)
            if table_name== 'dim_transaction':
                    unique_data['sales_order_id'] = unique_data['sales_order_id'].fillna(0)
                    unique_data['purchase_order_id'] = unique_data['purchase_order_id'].fillna(0)
                    unique_data['sales_order_id'] = unique_data['sales_order_id'].astype(int)
                    unique_data['purchase_order_id'] = unique_data['purchase_order_id'].astype(int)
            if table_name== 'fact_purchase_order':
                    unique_data['purchase_record_id'] = unique_data['purchase_record_id'].astype(int)
                    unique_data['staff_id'] = unique_data['staff_id'].astype(int)
                    unique_data['counterparty_id'] = unique_data['counterparty_id'].astype(int)
                    unique_data['item_quantity'] = unique_data['item_quantity'].astype(int)
                    unique_data['currency_id'] = unique_data['currency_id'].astype(int)
                    unique_data['agreed_delivery_location_id'] = unique_data['agreed_delivery_location_id'].astype(int)
            if table_name == 'fact_sales_order':
                    unique_data['sales_record_id'] = unique_data['sales_record_id'].astype(int)
                    unique_data['sales_staff_id'] = unique_data['sales_staff_id'].astype(int)
                    unique_data['counterparty_id'] = unique_data['counterparty_id'].astype(int)
                    unique_data['units_sold'] = unique_data['units_sold'].astype(int)
                    unique_data['currency_id'] = unique_data['currency_id'].astype(int)
                    unique_data['design_id'] = unique_data['design_id'].astype(int)
                    unique_data['agreed_delivery_location_id'] = unique_data['agreed_delivery_location_id'].astype(int)
                    
            
            for row in unique_data.iterrows():
                # Get the column names and values as a list
                columns = df.columns
                values = [f"'{row[column]}'" for column in columns]
                # Convert the list of values to a string with comma-separated values
                values_str = ", ".join(values)
                
                insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_str})"
                print(insert_query)
                engine.execute(insert_query)

        engine.execute(f'SELECT * FROM {table_name} limit 5').fetchall()
    list_files()


            # for index, row in unique_data.iterrows():
            #     columns = df.columns
            #     set_clause = ""
            #     # Iterate through the column names and add the column name and value to the SET clause
            #     for column in columns:
            #         set_clause += f"{column} = '{row[column]}', "
            #     # Remove the last comma and space from the SET clause
            #     set_clause = set_clause[:-2]
                
            #     update_query = f"UPDATE {table_name} SET {set_clause} WHERE {unique_key_column} = '{row[unique_key_column]}'"
            #     print(update_query)
            #     engine.execute(update_query)



