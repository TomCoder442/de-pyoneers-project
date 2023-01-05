import json
    
    
def lambda_handler(event, context):
    import pandas as pd
    import boto3
    import io
    
    # set variables for the two s3 buckets
    SOURCE_BUCKET = 'ingestion-bucket-2022-12-21-1617'
    DESTINATION_BUCKET = 'processed-data-bucket-2022-12-21-1617'
    # Connect to AWS using the credentials
    s3 = boto3.client('s3')
    
    # IMPORTING THE CSV FILES FROM THE SOURCE S3 BUCKET:
    SOURCE_BUCKET_OBJECTS = s3.list_objects(Bucket=SOURCE_BUCKET)
    try:
        objects = SOURCE_BUCKET_OBJECTS['Contents']
    except Exception as e:
        print("Ingestion bucket is empty: " + str(e))
        objects = {}
    
    #### CONVERTING ALL THE INGESTION BUCKET CSVs INTO PANDAS DATAFRAMES. PUTTING THESE DATAFRAMES INTO ONE DICTIONARY ####
    #create an empty dictionary to store the source dataframes
    SOURCE_BUCKET_DATAFRAMES ={}
    list_of_names = []
    #loop through each CSV file and create a Dataframe
    for obj in objects:
        #download the csv file from the S3 bucket
        file = s3.get_object(Bucket=SOURCE_BUCKET, Key=(obj['Key']))
        df = pd.read_csv(file['Body'])
        #Name the dataframe using the csv file name (slice off the timestamp at the front, and the .csv at the end)
        df_name = (obj['Key'])[27:-4]
        df.__Name__ = df_name
        list_of_names.append(df_name)
        
        # store the dataframe in the dictionary 
        SOURCE_BUCKET_DATAFRAMES[df_name] = df
    
    #### HAVE 10 OF 11 DATAFRAMES. CREATING LAST ONE 'dim_date' ####
    print(list_of_names)
    if len(list_of_names) >= 10:        
        #creating a template dataFrame with a date range of mid 2022 to mid 2023
        date= pd.DataFrame({'Date': pd.date_range('2022-06-06', '2023-06-05')})
        date['year'] = date.Date.dt.year
        date['month'] = date.Date.dt.month
        date['day'] = date.Date.dt.day
        date['day_of_week'] = date.Date.dt.dayofweek
        date['day_name'] = date['Date'].dt.day_name()
        date['month_name'] = date['Date'].dt.month_name()
        date['quarter'] = date.Date.dt.quarter
        #### HAVE 11 OF 11 REQUIRED DATAFRAMES. PUTTING ALL 11 INTO ONE DICTIONARY ####
        # add the last dataframe into the dictionary of dataframes
        SOURCE_BUCKET_DATAFRAMES['date'] = date
        list_of_names.append('date')
    
    

    # EMPTY LIST TO STORE OUR TRANSFORMED DATAFRAMES IN:
    transformed_dfs_list = []
    
    
    #### TRANSFORMING THE DATA IN THE 8 DIM TABLES ####
    
    # DIMENSION TABLE - dim_transaction:
    if 'transaction' in list_of_names:
        print('updated dim_transaction')
        # set variable, delete unneeded columns, set primary key, view the dataframe
        dim_transaction = SOURCE_BUCKET_DATAFRAMES['transaction']
        dim_transaction = dim_transaction.drop(columns=['created_at', 'last_updated'])
        dim_transaction.set_index = dim_transaction['transaction_id']
        # give the dataframe a name attribute
        dim_transaction.name = "dim_transaction"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_transaction)
    
    
    # DIMENSION TABLE - dim_payment_type:
    if 'payment_type' in list_of_names:
        print('updated dim_payment_type')
        # if payment_type in list source bucket dataframes do this:
        # set variable, delete unneeded columns, set primary key, view the dataframe
        dim_payment_type = SOURCE_BUCKET_DATAFRAMES['payment_type']
        dim_payment_type = dim_payment_type.drop(columns=['created_at', 'last_updated'])
        dim_payment_type.set_index = dim_payment_type['payment_type_id']
        # give the dataframe a name attribute
        dim_payment_type.name = "dim_payment_type"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_payment_type)
    
    
    # DIMENSION TABLE - dim_currency:
    if 'currency' in list_of_names:
        print('updated dim_currency')
        # set variable, delete unneeded columns, set primary key, view the dataframe 
        # (plus create dictionary and function for long form currency names)
        dim_currency = SOURCE_BUCKET_DATAFRAMES['currency']
        currency_names = {
            'GBP': 'British Pound',
            'USD': 'US Dollar',
            'EUR': 'Euro',
            'CHF': 'Swiss Franc'
        }
        dim_currency['currency_name'] = dim_currency['currency_code'].apply(lambda x: currency_names[x])
        dim_currency = dim_currency.drop(columns=['created_at', 'last_updated'])
        dim_currency.set_index = dim_currency['currency_id']
        # give the dataframe a name attribute
        dim_currency.name = "dim_currency"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_currency)
    
    
    # DIMENSION TABLE - dim_staff:
    if 'staff' in list_of_names:
        print('updated dim_staff')
        # set variable, delete unneeded columns, set primary key, view the dataframe
        # (and merge with 'department' dataframe to aquire 'department_name' and 'location')
        dim_staff = SOURCE_BUCKET_DATAFRAMES['staff']
        dim_staff = dim_staff.drop(columns=['created_at', 'last_updated'])
        dim_staff.set_index = dim_staff['staff_id']
        dim_staff = pd.merge(dim_staff, SOURCE_BUCKET_DATAFRAMES['department'], on='department_id', how='left')
        dim_staff = dim_staff.drop(columns=['created_at', 'last_updated', 'manager', 'department_id'])
        dim_staff = dim_staff.reindex(columns=['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address'  ])
        # give the dataframe a name attribute
        dim_staff.name = "dim_staff"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_staff)
    
    
    # DIMENSION TABLE - dim_location:
    if 'address' in list_of_names:
        print('updated dim_location')
        # set variable, delete unneeded columns, set primary key, view the dataframe
        # (plus rename a column)
        dim_location = SOURCE_BUCKET_DATAFRAMES['address']
        dim_location = dim_location.drop(columns=['created_at', 'last_updated'])
        dim_location = dim_location.rename(columns={"address_id": "location_id"})
        dim_location.set_index = dim_location['location_id']
        # give the dataframe a name attribute
        dim_location.name = "dim_location"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_location)
    
    
    # DIMENSION TABLE - dim_counterparty:
    if 'counterparty' in list_of_names:
        print('updated dim_counterparty')
        # set variable, delete unneeded columns, set primary key, view the dataframe
        # (plus merge with 'dim_location' dataframe to aquire lots of address columns)
        # (plus rename some columns)
        dim_counterparty = SOURCE_BUCKET_DATAFRAMES['counterparty']
        dim_counterparty = pd.merge(dim_counterparty, dim_location, how='left', left_on='legal_address_id', right_on='location_id')
        dim_counterparty = dim_counterparty.drop(columns=[ 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated', 'location_id'])
        dim_counterparty.set_index = dim_counterparty['counterparty_id']
        dim_counterparty = dim_counterparty.rename(columns={"address_line_1": "counterparty_legal_address_line_1", "address_line_2": "counterparty_legal_address_line_2", "district": "counterparty_legal_district", "city": "counterparty_legal_city", "postal_code": "counterparty_legal_postal_code" , "country": "counterparty_legal_country", "phone": "counterparty_legal_phone_number"})
        # give the dataframe a name attribute
        dim_counterparty.name = "dim_counterparty"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_counterparty)
    
    
    # DIMENSION TABLE - dim_date:
    if 'date' in list_of_names:  
        # set variable, delete unneeded columns, set primary key, view the dataframe
        dim_date = date
        dim_date = dim_date.rename(columns={"Date": "date_id"})
        dim_date.set_index = dim_date['date_id']
        # give the dataframe a name attribute
        dim_date.name = "dim_date"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_date)
    
    
    # DIMENSION TABLE - dim_design:
    if 'design' in list_of_names:
        print('updated dim_design')
        # set variable, delete unneeded columns, set primary key, view the dataframe
        dim_design = SOURCE_BUCKET_DATAFRAMES['design']
        dim_design = dim_design.drop(columns=['created_at', 'last_updated'])
        dim_design.set_index = dim_design['design_id']
        # give the dataframe a name attribute
        dim_design.name = "dim_design"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(dim_design)
    
    
    #### CREATING THE 3 FACT TABLES ####
    
    # FACT TABLE - fact_payment:
    if 'payment' in list_of_names:
        print('updated fact_payment')
        # set variable 
        fact_payment = SOURCE_BUCKET_DATAFRAMES['payment']
        # convert the 'created_at' column to datetime objects and put into separate columns
        fact_payment = fact_payment.astype({'created_at': 'datetime64[ns]'})
        fact_payment['created_at'] = pd.to_datetime(fact_payment['created_at'])
        fact_payment['created_date'] = fact_payment['created_at'].dt.date
        fact_payment['created_time'] = fact_payment['created_at'].dt.time
        # convert the 'created_at' column to datetime objects and put into separate columns
        fact_payment = fact_payment.astype({'last_updated': 'datetime64[ns]'})
        fact_payment['last_updated'] = pd.to_datetime(fact_payment['last_updated'])
        fact_payment['last_updated_date'] = fact_payment['last_updated'].dt.date
        fact_payment['last_updated_time'] = fact_payment['last_updated'].dt.time
        # delete unneeded columns
        fact_payment = fact_payment.drop(columns=[ 'company_ac_number', 'counterparty_ac_number', 'created_at', 'last_updated'])
        # Reorder the columns in fact_payment
        fact_payment = fact_payment.reindex(columns=[ 'payment_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'transaction_id', 'counterparty_id', 'payment_amount', 'currency_id', 'payment_type_id', 'paid', 'payment_date' ])
        # give the dataframe a name attribute
        fact_payment.name = "fact_payment"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(fact_payment)
    
    
    # ##### FACT TABLE: fact_sales_order
    if 'sales_order' in list_of_names:
        print('updated fact_sales_order')
        # set variable 
        fact_sales_order = SOURCE_BUCKET_DATAFRAMES['sales_order']
        # convert the 'created_at' column to datetime objects and put into separate columns
        fact_sales_order['created_at'] = pd.to_datetime(fact_sales_order['created_at'])
        fact_sales_order['created_date'] = fact_sales_order['created_at'].dt.date
        fact_sales_order['created_time'] = fact_sales_order['created_at'].dt.time
        # convert the 'last_updated' column to datetime objects and put into separate columns
        fact_sales_order['last_updated'] = pd.to_datetime(fact_sales_order['last_updated'])
        fact_sales_order['last_updated_date'] = fact_sales_order['last_updated'].dt.date
        fact_sales_order['last_updated_time'] = fact_sales_order['last_updated'].dt.time
        # delete unneeded columns
        fact_sales_order = fact_sales_order.drop(columns=[ 'created_at', 'last_updated'])
        # Reorder the columns
        fact_sales_order = fact_sales_order.reindex(columns=[ 'sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id' ])
        # rename one column
        fact_sales_order = fact_sales_order.rename(columns={"staff_id": "sales_staff_id"})
        # give the dataframe a name attribute
        fact_sales_order.name = "fact_sales_order"
        transformed_dfs_list.append(fact_sales_order)
    
    
    # ##### FACT TABLE: fact_purchase_order
    if 'purchase_order' in list_of_names:
        print('updated fact_purchase_order')
        # set variable 
        fact_purchase_order = SOURCE_BUCKET_DATAFRAMES['purchase_order']
        # convert the 'created_at' column to datetime objects
        fact_purchase_order['created_at'] = pd.to_datetime(fact_purchase_order['created_at'])
        # extract the date and time values into separate columns
        fact_purchase_order['created_date'] = fact_purchase_order['created_at'].dt.date
        fact_purchase_order['created_time'] = fact_purchase_order['created_at'].dt.time
        # convert the 'last_updated' column to datetime objects
        fact_purchase_order['last_updated'] = pd.to_datetime(fact_purchase_order['last_updated'])
        # extract the date and time values into separate columns
        fact_purchase_order['last_updated_date'] = fact_purchase_order['last_updated'].dt.date
        fact_purchase_order['last_updated_time'] = fact_purchase_order['last_updated'].dt.time
        # delete unneeded columns
        fact_purchase_order = fact_purchase_order.drop(columns=[ 'created_at', 'last_updated'])
        # Reorder the columns in fact_purchase_order
        fact_purchase_order = fact_purchase_order.reindex(columns=[ 'purchase_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id' ])
        # give the dataframe a name attribute
        fact_purchase_order.name = "fact_purchase_order"
        # add dataframe to transformed_df_list
        transformed_dfs_list.append(fact_purchase_order)
        #print(fact_purchase_order.__name__)
    
    
    # ### CONFIRM I HAVE ALL 11 TRANSFORMED DATAFRAMES IN transformed_dfs_list 
    for df in transformed_dfs_list:
        print(df.name)

    def create_and_upload_parquet_file(dataframe, filename, bucket):
        # Write the DataFrame to a buffer in memory
        with io.BytesIO() as buffer:
            dataframe.to_parquet(buffer, index=False)
            # Seek to the beginning of the buffer
            buffer.seek(0)
            # Upload the buffer to S3
            s3.upload_fileobj(buffer, bucket, filename)
        

    # Loop through the list of dataframes
    for df in transformed_dfs_list:
        # Get the name of the dataframe
        df_name = df.name
        
    # Create and upload the parquet file
        create_and_upload_parquet_file(df, f"{df_name}.parquet", DESTINATION_BUCKET )
    
    # # List all objects in the bucket
    objects = s3.list_objects(Bucket=SOURCE_BUCKET)
    
    # Extract the keys of the objects
    try:
        keys = [{'Key': obj['Key']} for obj in objects['Contents']]
        print(keys)
        print({'Objects': keys})
        # Delete all objects in the bucket
        s3.delete_objects(Bucket=SOURCE_BUCKET, Delete={'Objects': keys})
        print('all objects deleted')
    except Exception as e:
        print('Source bucket is empty')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }