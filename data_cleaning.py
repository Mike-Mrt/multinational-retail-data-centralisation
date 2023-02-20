import pandas as pd
import data_extraction
import database_utils
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
        self.df_user_data.set_index('index')
        # Convert columns 'date_of_birth' and 'join_date' to ISO format:
        # self.df_user_data['date_of_birth'] = pd.to_datetime(self.df_user_data['date_of_birth'], format="%Y-%m-%d")
        # self.df_user_data['date_of_birth'] = pd.to_datetime(self.df_user_data['date_of_birth'], format="%Y %B %d")

        # self.df_user_data['join_date'] = pd.to_datetime(self.df_user_data['join_date'], format="%Y-%m-%d")
        # self.df_user_data['join_date'] = pd.to_datetime(self.df_user_data['date_of_birth'], format="%Y %B %d")

        print(self.df_user_data.head(5))
        print(self.df_user_data.dtypes)




testing = DataCleaning()
db_connector = database_utils.DatabaseConnector()
db_connector.read_db_creds()
db_connector.init_db_creds()
testing.clean_user_data('legacy_users', db_connector)
