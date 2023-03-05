import pandas as pd
import tabula
import database_utils
import requests 

# Had to downgradre to SQLAlchemy version 1.4.46 in order to not attain the following error: AttributeError: 'OptionEngine' object has no attribute 'execute'

class DataExtractor:
    '''
    This class will work as a utility class, in this class, methods will be created that will help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, including CSV files, an API and an S3 bucket.
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
        self.response = requests.get(endpoint, headers=header)
        self.response_dict = self.response.json()
        return self.response_dict.get("number_stores")


testing = DataExtractor()
api_key = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
endpoint_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
print(testing.list_number_of_stores(endpoint_api,api_key))

