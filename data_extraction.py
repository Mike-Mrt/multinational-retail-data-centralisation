import pandas as pd
import tabula
import database_utils
import requests
import boto3

# Had to downgradre to SQLAlchemy version 1.4.46 in order to not attain the following error: AttributeError: 'OptionEngine' object has no attribute 'execute'

class DataExtractor:
    '''
    This class will work as a utility class, in this class, methods will be created that will help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, includingAWS RDS database, AWS S3 bucket, RESTful API, JSON, CSV files.
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


# testing=DataExtractor()
# df = testing.extract_from_s3('s3://data-handling-public/products.csv')
# print(df.head())
# api_key = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# num_stores_endpoint_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# num_stores = testing.list_number_of_stores(num_stores_endpoint_api,api_key)
# print(num_stores)
# retrieve_store_endpoint_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"
# df_stores = testing.retrieve_stores_data(retrieve_store_endpoint_api,api_key)
# print(df_stores.info())
# print(df_stores)




