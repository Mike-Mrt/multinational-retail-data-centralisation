import pandas as pd
import data_extraction
import database_utils
import boto3
# from datetime import datetime

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


# testing = data_extraction.DataExtractor()
# df_products = testing.extract_from_s3('s3://data-handling-public/products.csv')
# cleaning = DataCleaning()
# df_products = cleaning.convert_product_weights(df_products)
# df_products_data = cleaning.clean_products_data(df_products)
# uploading = database_utils.DatabaseConnector()
# uploading.upload_to_db(df_products_data,'dim_products')
testing = DataCleaning()
