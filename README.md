# Multinational Retail Data Centralisation Project

The scenario is based on a multinational company that sells various products globally. The current state is that their sales data is spread across multiple data sources; making it not easily accesible and cannot be analysed by current members of the team. The purpose will be to ensure that all of the sales data is accessible from one centralised location. Therefore, the first task will be to ensure all data is retrieved from the multiple data sources, cleaned and uploaded to a central database. This will then allow the company members to query the data to get up-to-date metrics for the business. 

## Milestone 2

The aim of milestone 2 is to write python scripts which will connect to the data sources, extract this data and clean the extracted data. This data once cleaned will be uploaded to a postgres SQL database so that the data can be queried. 
 
 - database_utils.py script: This script is used to connect and upload the extracted data to the database. The DataExtractor Class contains methdos which will extract data from various data sources and output a pabdas dataframe.
    - libraries used: pandas, yaml, sqlalchemy, psycopg2
 ```python
 class DatabaseConnector:
    '''
    This class will be used to connect and upload the extracted data to the database.
    '''

    # the read_db_creds() method will read the credentials yaml file and return the dictionary of the credentials
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            self.credentials = yaml.safe_load(f)

    # the init_db_engine() method will read the credentials from the return of the read_db_creds() method and initialise and return an sqlalchemy dastabase engine
    def init_db_creds(self):
        self.DATABASE_TYPE = 'postgresql'
        self.DBAPI = 'psycopg2'
        self.HOST = self.credentials['RDS_HOST']
        self.USER = self.credentials['RDS_USER']
        self.PASSWORD = self.credentials['RDS_PASSWORD']
        self.DATABASE = self.credentials['RDS_DATABASE']
        self.PORT = self.credentials['RDS_PORT']
        self.engine = sqlalchemy.create_engine(f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
        return self.engine
        
    # the list_db_tables() method will list all the tables in the database so you know which tables you can extract data from
    def list_db_tables(self):
        self.inspector = sqlalchemy.inspect(self.engine)
        self.tables = self.inspector.get_table_names()
        print(self.tables)
    
    # the upload_to_db method will take in a Pandas dataframe and upload it to a table in a database:
    def upload_to_db(self, upload_df, table_name):
        # engine_url = 'postgresql+psycopg2://username:password@host:port/database'
        # Replacing the values of username, password, host, port and database with required database details.
        engine_url = 'postgresql+psycopg2://postgres:password@localhost:5432/Sales_Data'
        engine = sqlalchemy.create_engine(engine_url)
        upload_df.to_sql(name=table_name, con=engine, if_exists='replace', index=True)
 ```
 - data_extraction.py script: This script is used to help extract data from different data sources such as AWS RDS database, AWS S3 bucket, RESTful API, JSON, CSV files. The DatabaseConnector Class has methods which will connect and upload data to the database.
    - libraries used: pandas, tabula, requests, boto3, database_utils (created for project)
 ```python
 class DataExtractor:
    '''
    This class will work as a utility class, in this class, methods will be created that will help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, including AWS RDS database, AWS S3 bucket, RESTful API, JSON, CSV files.
    '''

    # The read_rds_table() method will extract the database table to a pandas dataframe. It will take an instance of the DatabaseConnector Class and the table name as an argument and return a pandas dataframe
    def read_rds_table(self, table_name, db_connector):
        engine = db_connector.engine
        users = pd.read_sql_table(table_name,engine)
        return users

    # The retrieve_pdf_data method will take in a link as an argument and return a pandas dataframe:
    def retrieve_pdf_data(self, pdf_url):
        # Tabula package reads the pdf and we want all pages read, since it splits each page into different dfs we want to ensure all pages of the pdf are read and returned as a list of dataframes
        self.df_cards_list = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)
        # We then return a single pd dataframe which concatenates all the lists of data frames from above:  
        return pd.concat(self.df_cards_list)

    # The list_number_of_stores method will return the number of stores to extract - it should take in the number of stores endpoint and header dictionary as an argument:
    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        self.num_stores = response.json()['number_stores']
        return self.num_stores

    # The retrieve_stores_data method will take the retrieve a store endpoint 
    def retrieve_stores_data(self, endpoint, header):
        num_stores = self.num_stores
        stores_data = []
        for store_number in range(0,num_stores):
            current_endpoint = endpoint.format(store_number)
            response = requests.get(current_endpoint,headers=header)
            store = response.json()
            stores_data.append(store)
        return pd.DataFrame(stores_data)

    # The extract_from_s3 method will use the boto3 package to download and extract the information returning a pandas dataframe, it will take in the S3 address as an argument:
    def extract_from_s3(self, s3_address): # s3://data-handling-public/products.csv
        contents = s3_address.split('/')
        s3 = boto3.client('s3')
        s3.download_file(contents[2], contents[3], contents[3])
        df_products = pd.read_csv(contents[3])
        return df_products
 ```
 - data_cleaning.py script: This script is used to clean the data that is extracted from the various data sources listed above. The DataCleaning Class contains methods which will take the pandas dataframe, clean it from Null values, incorrect information, alter data types and correct any erroneous data and return the final pandas dataframe that will be uploaded to the postgres SQL database.
    - libraries used: pandas, boto3, database_utils (created for project), data_extraction (created for project)
```python
class DataCleaning:
    '''
    This class will include methods to clean data from each of the data sources.
    '''

    # The clean_user_data() method will perform the cleaning of the user data. Look out for NULL values, errors with dates, incorrectly typed values and rows filled with incorrect information.
    def clean_user_data(self, table_name, db_connector):
        extractor = data_extraction.DataExtractor()
        # Ensuring that all columns are displayed:
        pd.set_option('display.max_columns', None)
        # Assigning the extracted table to a variable named df_user_data
        self.df_user_data = extractor.read_rds_table(table_name, db_connector)
        # Setting the index of this table as index from the data table
        self.df_user_data.set_index('index', inplace=True)
        # Did an initial value_counts on the 'country' column and realised that there are 3 countries and 21 NULL values and 15 incorrect information 
        # Used the incorrect information values from the value_counts output to create a new df: incorrect_info and see if all the entire row is incorrect and it was all incorect information
        self.incorrect_info = self.df_user_data[(self.df_user_data['country'] == 'GMRBOMI0O1') | (self.df_user_data['country'] == '7ZNO5EBALT') | (self.df_user_data['country'] == '3518UD5CE8') | (self.df_user_data['country'] == 'RQRB7RMTAD') | (self.df_user_data['country'] == 'PNRMPSYR1J') | (self.df_user_data['country'] == '5EFAFD0JLI') | (self.df_user_data['country'] == 'YOTSVPRBQ7') | (self.df_user_data['country'] == '50KUU3PQUF') | (self.df_user_data['country'] == 'EWE3U0DZIV') | (self.df_user_data['country'] == 'XN9NGL5C0B') | (self.df_user_data['country'] == 'S0E37H52ON') | (self.df_user_data['country'] == 'XGI7FM0VBJ') | (self.df_user_data['country'] == 'AJ1ENKS3QL') | (self.df_user_data['country'] == 'I7G4DMDZOZ') | (self.df_user_data['country'] == 'T4WBZSW0XI')]
        # Dropped these rows from the main dataframe using the index:
        self.df_user_data = self.df_user_data.drop(self.incorrect_info.index)
        # Created another df: null_info to check if all values of the row contain 'NULL' or just the counry column and it was all rows:
        self.null_info = self.df_user_data[(self.df_user_data['country']=='NULL')]
        # Dropped all 21 rows from the main dataframe using the index:
        self.df_user_data = self.df_user_data.drop(self.null_info.index)
        # converting a typo error where GB has been written as GGB in the country_code column:
        self.df_user_data['country_code'] = self.df_user_data['country_code'].replace('GGB', 'GB')
        # Converting columns: 'country' and 'country_code' and 'company' as dtype=category to save memory:
        self.df_user_data['country'] = self.df_user_data['country'].astype('category')
        self.df_user_data['country_code'] = self.df_user_data['country_code'].astype('category')
        self.df_user_data['company'] = self.df_user_data['company'].astype('category')
        # Correcting the incorrectly typed phone numbers which includes 'x' and '.' and '-' in a lot of the numbers:
        self.df_user_data['phone_number'] = self.df_user_data['phone_number'].str.replace('x','')
        self.df_user_data['phone_number'] = self.df_user_data['phone_number'].str.replace('.','')
        self.df_user_data['phone_number'] = self.df_user_data['phone_number'].str.replace('-','')
        # Converting the 'date_of_birth' column to ISO format, it has dates in 3 different formats, so creating a consistent date ISO date format column:
        self.df_user_data['date_of_birth'] = pd.to_datetime(self.df_user_data['date_of_birth'], infer_datetime_format=True, errors='coerce').dt.date
        # Converting the 'join_date' column to ISO format, it has dates in 3 different formats, so creating a consistent date ISO date format column:
        self.df_user_data['join_date'] = pd.to_datetime(self.df_user_data['join_date'], infer_datetime_format=True, errors='coerce').dt.date
        return self.df_user_data
        # self.df_user_data.to_csv('legacy_data_update.csv',index='False')

    # The clean_card_data method will clean the data to remove any erroneous values, NULL values or errors in formatting:
    def clean_card_data(self):
        extractor = data_extraction.DataExtractor()
        # Ensuring that all columns are displayed:
        pd.set_option('display.max_columns', None)
        # Assigning the extracted table to a variable named df_user_data
        self.df_card_data = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        # resseting the index column and dropping original index
        self.df_card_data = self.df_card_data.reset_index(drop=True)
        # converting the card_provider column to category dtype:
        self.df_card_data['card_provider'] = self.df_card_data['card_provider'].astype('category')
        # locating all rows where card_number has NULL values and there are 11 which entire row is NULL
        null_values_df = self.df_card_data[(self.df_card_data['card_number']=='NULL')]
        # dropping the above rows from the df
        self.df_card_data = self.df_card_data.drop(null_values_df.index)
        # Used the incorrect information values from the value_counts output to create a new df: incorrect_cards_info and see if all the entire row is incorrect and it was all incorect information
        incorrect__cards_info = self.df_card_data[(self.df_card_data['card_provider'] == 'OGJTXI6X1H') | (self.df_card_data['card_provider'] == 'UA07L7EILH') | (self.df_card_data['card_provider'] == 'XGZBYBYGUW') | (self.df_card_data['card_provider'] == 'BU9U947ZGV') | (self.df_card_data['card_provider'] == 'WJVMUO4QX6') | (self.df_card_data['card_provider'] == 'DE488ORDXY') | (self.df_card_data['card_provider'] == '5CJH7ABGDR') | (self.df_card_data['card_provider'] == 'JCQMU8FN85') | (self.df_card_data['card_provider'] == 'JRPRLPIBZ2') | (self.df_card_data['card_provider'] == 'DLWF2HANZF') | (self.df_card_data['card_provider'] == '1M38DYQTZV') | (self.df_card_data['card_provider'] == 'TS8A81WFXV') | (self.df_card_data['card_provider'] == 'OGJTXI6X1H') | (self.df_card_data['card_provider'] == '5MFWFBZRM9') | (self.df_card_data['card_provider'] == 'NB71VBAHJE')]
        # Dropped these rows from the main dataframe using the index:
        self.df_card_data = self.df_card_data.drop(incorrect__cards_info.index)
        # changing the dates to ISO format using infer_datetime_format as the column had multiple formats:
        self.df_card_data['date_payment_confirmed'] = pd.to_datetime(self.df_card_data['date_payment_confirmed'], infer_datetime_format=True, errors='coerce').dt.date
        # converting the expiry_date column to datetime and since cards expire on th last day of each month, added the day for that:
        # self.df_card_data['expiry_date'] = (pd.to_datetime(self.df_card_data['expiry_date'], format='%m/%y', errors='coerce') + pd.offsets.MonthEnd(0)).dt.date
        return self.df_card_data
    
    # The clean_stores_data will clean the data retrieved from the API and return a pandas DataFrame:
    def clean_store_data(self):
        retrieve_store_endpoint_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"
        api_key = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        extractor = data_extraction.DataExtractor()
        num_stores_endpoint_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        extractor.list_number_of_stores(num_stores_endpoint_api,api_key)
        # Assigning the extracted data from the API to a variable called df_stores_data:
        df_stores_data = extractor.retrieve_stores_data(retrieve_store_endpoint_api, api_key)
        # Ensuring that all columns are displayed:
        pd.set_option('display.max_columns', None)
        # Setting the index column to the index column:
        df_stores_data = df_stores_data.set_index('index')
        # locating all rows where country_code has NULL values and there are 3 which entire row is NULL
        null_values_df = df_stores_data[(df_stores_data['country_code']=='NULL')]
        # dropping the above rows from the df
        df_stores_data = df_stores_data.drop(null_values_df.index)
        # Used the incorrect information values from the value_counts output to create a new df: incorrect_stores_info and see if all the entire row is incorrect and it was all incorect information
        incorrect__stores_info = df_stores_data[(df_stores_data['country_code'] == 'B3EH2ZGQAV') | (df_stores_data['country_code'] == 'F3AO8V2LHU') | (df_stores_data['country_code'] == 'FP8DLXQVGH') | (df_stores_data['country_code'] == 'HMHIFNLOBN') | (df_stores_data['country_code'] == 'OH20I92LX3') | (df_stores_data['country_code'] == 'OYVW925ZL8') | (df_stores_data['country_code'] == 'YELVM536YT')]
        # Dropped these rows from the main dataframe using the index:
        df_stores_data = df_stores_data.drop(incorrect__stores_info.index)
        # The below uses the regular expression '\D+' to identify any non-numeric characters with an empty string:
        df_stores_data['staff_numbers'] = df_stores_data['staff_numbers'].str.replace(r'\D+', '')
        # Converting the staff_numbers into int:
        df_stores_data['staff_numbers'] = df_stores_data['staff_numbers'].astype('int64')
        # The below replaces the 'ee' prefix of certain rows of the continent column and replaces with empty string:
        df_stores_data['continent'] = df_stores_data['continent'].str.replace('ee','')
        # Converting the locality, store_type, country_code and continent columns to category dtype:
        df_stores_data['locality'] = df_stores_data['locality'].astype('category')
        df_stores_data['store_type'] = df_stores_data['store_type'].astype('category')
        df_stores_data['country_code'] = df_stores_data['country_code'].astype('category')
        df_stores_data['continent'] = df_stores_data['continent'].astype('category')
        # Changing the dates to ISO format using infer_datetime_format as the column had multiple formats:
        df_stores_data['opening_date'] = pd.to_datetime(df_stores_data['opening_date'], infer_datetime_format=True, errors='coerce').dt.date
        return df_stores_data

    # The convert_product_weights method will take in the products_data dataframe and convert all values as float which will be in kg
    def convert_product_weights(self, df_products):
        # renaming the index column:
        df_products = df_products.rename(columns={'Unnamed: 0': 'index'})
        # setting index to index column:
        df_products = df_products.set_index('index')
        # Used the incorrect information values from the value_counts output to create a new df: incorrect_stores_info and see if all the entire row is incorrect and it was all incorect information
        incorrect__products_info = df_products[(df_products['weight'] == '9GO9NZ5JTL') | (df_products['weight'] == 'Z8ZTDGUZVU') | (df_products['weight'] == 'MX180RYSHX')]
        # dropping the above rows from the df
        df_products = df_products.drop(incorrect__products_info.index)
        # Dropping values in the weight column which have Null values (entire row is NULL):
        df_products = df_products.dropna(subset=['weight'])
        # Iterating through each row of the 'weight' column:
        for i, row in df_products.iterrows():
            weight = row['weight']
            # if 'kg' in the weight string, remove the kg and convert to float
            if 'kg' in weight:
                weight_kg = float(weight.replace('kg', ''))
            # if 'g' in weight string:
            elif 'g' in weight:
                # some weights are calculated by number of packs by weight per pack, so splitting the 2 items, then removing 'g' and converting to kg
                if 'x' in weight:
                    num_packets, packet_weight = weight.split(' x ')
                    weight_kg = float(num_packets) * float(packet_weight.replace('g','')) / 1000
                # some weights had a mistype of '.' so removing that and calculating weight in kg
                elif '.' in weight:
                    weight_g = weight.replace(' .','')
                    weight_kg = float(weight_g.replace('g','')) / 1000
                # otherwise remove 'g' and calculate weight in kg
                else:
                    weight_kg = float(weight.replace('g', '')) / 1000
           # converting the weights in ml to kg         
            elif 'ml' in weight:
                weight_kg = float(weight.replace('ml', '')) / 1000
            # converting weights in oz to kg    
            elif 'oz' in weight:
                weight_kg = float(weight.replace('oz','')) / 35.274
            # reaplacing the caulcated weight in kg per iteration:
            df_products.at[i, 'weight'] = weight_kg
        # converting the weight column to float datatype     
        df_products['weight'] = df_products['weight'].astype('float64')
        # converting the name of the column so users know the weight is in kg
        df_products = df_products.rename(columns={'weight': 'weight_kg'}) 
        return df_products

    # The clean_products_data method will clean the resulting above products_data further of any additional erroneous values:
    def clean_products_data(self, df_products_data):
        # The below removes the £ sign for the product price column, changes it to data type float and then changes the name of the column to let user know price is in £:
        df_products_data['product_price'] = df_products_data['product_price'].str.replace('£', '')
        df_products_data['product_price'] = df_products_data['product_price'].astype('float64')
        df_products_data = df_products_data.rename(columns={'product_price': 'product_price_£'})
        # Changing the dates to ISO format using infer_datetime_format as the column had multiple formats:
        df_products_data['date_added'] = pd.to_datetime(df_products_data['date_added'], infer_datetime_format=True, errors='coerce').dt.date
        # changing the category and removed columns to date type category:
        df_products_data['category'] = df_products_data['category'].astype('category')
        df_products_data['removed'] = df_products_data['removed'].astype('category')
        return df_products_data

    # The clean_orders_table will clean the data within the orders table extracted from the RDS table: 'orders_table':
    def clean_orders_table(self):
        connector = database_utils.DatabaseConnector()
        connector.read_db_creds()
        db_connector = connector.init_db_creds()
        extractor = data_extraction.DataExtractor()
        df_orders = extractor.read_rds_table('orders_table', db_connector)
        # Removing the following columns: level_0, first_name, last_name, 1:
        df_orders.drop(['level_0', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        # setting index column as index:
        df_orders = df_orders.set_index('index')
        return df_orders

    # The clean_date_events method will clean the data from the extracted data events data and return a dataframe: source: 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    def clean_date_events(self):
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'date_details.json', 'date_details.json')
        df_date_events = pd.read_json('date_details.json')
        # Checking if rows with Null values in the time_period column has Null values for entire row:
        null_values_df = df_date_events[(df_date_events['time_period']=='NULL')]
        # Dropping these rows from the original dataframe:
        df_date_events = df_date_events.drop(null_values_df.index)
        # Used the incorrect information values from the value_counts output to create a new df: incorrect_stores_info and see if all the entire row is incorrect and it was all incorect information
        incorrect_date_events_df = df_date_events[(df_date_events['time_period'] == '5MUU1NKRED') | (df_date_events['time_period'] == 'IXNB2XXEKB') | (df_date_events['time_period'] == 'SYID3PBQLP') | (df_date_events['time_period'] == 'DZC37NLW4F') | (df_date_events['time_period'] == 'JMW951JPZC') | (df_date_events['time_period'] == 'YRYN6Y8SPJ') | (df_date_events['time_period'] == 'QA65EOIBX4') | (df_date_events['time_period'] == 'KQVJ34AINL') | (df_date_events['time_period'] == '1PZDMCME1C') | (df_date_events['time_period'] == 'SSF9ANE440') | (df_date_events['time_period'] == '7RR8SRXQAW') | (df_date_events['time_period'] == '7DNU2UWFP7') | (df_date_events['time_period'] == 'EOHYT5T70F') | (df_date_events['time_period'] == 'ALOGCWS9Y3') | (df_date_events['time_period'] == 'SQX52VSNMM') | (df_date_events['time_period'] == '1JCRGU3GIE') | (df_date_events['time_period'] == '5OQGE7K2AV') | (df_date_events['time_period'] == 'CM5MTJKXMH') | (df_date_events['time_period'] == 'GT3JKF575H') | (df_date_events['time_period'] == '1Z18F4RM05') | (df_date_events['time_period'] == 'OEOXBP8X6D') | (df_date_events['time_period'] == 'DXBU6GX1VC') | (df_date_events['time_period'] == 'MZIS9E7IXD')]
        # Dropped these rows from the main dataframe using the index:
        df_date_events = df_date_events.drop(incorrect_date_events_df.index)
        # Changing the time-period column to category data type:
        df_date_events['time_period'] = df_date_events['time_period'].astype('category')
        # Adding a column for the combined date in iso format:
        df_date_events['iso_date'] = pd.to_datetime(df_date_events[['year','month','day']]).dt.date
        return df_date_events
```

## Milestone 3

The aim of milestone 3 is to develop a star based schema for the database and convert the columns in the tables of the database to the correct datatype.

With the orders_table, converted:
- the date_uuid and user_uuid columns from text to uuid data type
- the card_number, store_code and product_code columns from text to varchar(x) where x represents the maximum number of characters of the column
- the product_quantity from bigint to smallint to save memory

With the dim_users table, converted:
- the user_uuid column from text to uuid data type
- the first_name and last_name from text to varchar(255)
- the country_code from text to varchar(2)
- the date_of_birth and join_date columns were already in the date data type from the data cleaning process so it was uncessary to convert these columns

The image shows the Sales_Data database. SQL queries were used in PGAdmin 4 to construct a star-schema within this database, along with altering data types as required.

<p align="center">
<img src="./images/PostgreSQL_Sales_Data_DB.png" alt="Screenshot of Sales_Data database in PGAdmin4" width="300" height="400" />

This is made possible by using PGAdmin 4 to interact with a PostreSQL server.

## Milestone 4

In the final phase of the project, the aim is to obtain up-to-date business metrics to aid in the company's business decision making. SQL queries have been designed to extract specific metrics requested by the management team so that they can make more data-driven decisions and get a better understanding of the company's sales. 

An example query is provided below: How quickly is the company making sales?
In this case, we are looking to determine the average time taken between each sale grouped by year.
```SQL
-- creating a comman table expression for entire timestamp by concatenating the iso_date and timestamp columns
WITH cte AS (
SELECT
	CAST(CONCAT(iso_date,' ',timestamp) AS timestamp) AS datetimes, year -- we label this as datetimes for the entire datetime and also pull out year
FROM dim_date_times)
	
SELECT -- here we want to present the final results in the format required so we extract the hour, mins etc from the avg_time_between_sales and assign it to thhe description and concatenate it all
  year,
  CONCAT(
    '"hours": ', EXTRACT(HOUR FROM avg_time_between_sales)::text,
    ', "minutes": ', EXTRACT(MINUTE FROM avg_time_between_sales)::text,
    ', "seconds": ', ROUND(EXTRACT(SECOND FROM avg_time_between_sales),0)::text,
    ', "milliseconds": ', ROUND(EXTRACT(MILLISECOND FROM avg_time_between_sales),0)::text
  ) AS average_time_taken_between_sales
FROM (
  SELECT -- use the calculated time_between_sales to find the avergae time between sales for each year
	year,
    AVG(time_between_sales) AS avg_time_between_sales
  FROM (
    SELECT -- calculate the time_between_sales using LEAD to calculate time for each interval between sales partitioned by year
      year,
      LEAD(datetimes) OVER (PARTITION BY year ORDER BY datetimes) - datetimes AS time_between_sales 
    FROM cte
  ) AS sales
	GROUP BY year -- grouping by year as we want to see the rate of sale by year
) AS avg_sales;
``` 

The output of the above is presented below:

<p align="center">
<img src="./images/Query_Output.png" alt="Screenshot of one of the query outputs" width="300" height="400" />

## Conclusions

The Multinational Retail Data Centralization project was highly successful. It began by developing Python scripts to connect with various data sources, extracting valuable information, and then rigorously cleaning and centralizing this data in a PostgreSQL database using PGAdmin4 as the management tool. The creation of a star-based schema enabled efficient data querying and analysis.

The primary goal was to empower our management team with data-driven insights to maintain our competitive edge in the market.

However, there are areas for improvement:
    - Enhance code efficiency by modularising data cleaning processes for better maintainability.
    - Implement real-time data updates from various sources to ensure continuous data-driven decision-making.
    - Explore opportunities to acquire data from competitors through legal channels to gain a competitive advantage against market leaders." 