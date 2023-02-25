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
        # Did an initial value_counts on the 'country' column and realised that there are 3 countries and 21 NULL values and 15 incorrect information 
        # Used the incorrect information values from the value_counts output to create a new df: incorrect_info and see if all the entire row is incorrect and it was all incorect information
        self.incorrect_info = self.df_user_data[(self.df_user_data['country'] == 'GMRBOMI0O1') | (self.df_user_data['country'] == '7ZNO5EBALT') | (self.df_user_data['country'] == '3518UD5CE8') | (self.df_user_data['country'] == 'RQRB7RMTAD') | (self.df_user_data['country'] == 'PNRMPSYR1J') | (self.df_user_data['country'] == '5EFAFD0JLI') | (self.df_user_data['country'] == 'YOTSVPRBQ7') | (self.df_user_data['country'] == '50KUU3PQUF') | (self.df_user_data['country'] == 'EWE3U0DZIV') | (self.df_user_data['country'] == 'XN9NGL5C0B') | (self.df_user_data['country'] == 'S0E37H52ON') | (self.df_user_data['country'] == 'XGI7FM0VBJ') | (self.df_user_data['country'] == 'AJ1ENKS3QL') | (self.df_user_data['country'] == 'I7G4DMDZOZ') | (self.df_user_data['country'] == 'T4WBZSW0XI')]
        # Dropped these rows from the main dataframe using the index:
        self.df_user_data = self.df_user_data.drop(self.incorrect_info.index)
        # 

testing = DataCleaning()
db_connector = database_utils.DatabaseConnector()
db_connector.read_db_creds()
db_connector.init_db_creds()
testing.clean_user_data('legacy_users', db_connector)
