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
        # self.df_user_data.to_csv('legacy_data_update.csv',index='False')

    # The clean_card_data method will clean the data to remove any erroneous values, NULL values or errors in formatting:
    def clean_card_data(self):
        extractor = data_extraction.DataExtractor()
        # Ensuring that all columns are displayed:
        pd.set_option('display.max_columns', None)
        # Assigning the extracted table to a variable named df_user_data
        self.df_card_data = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        return self.df_card_data

testing = DataCleaning()
df_test = testing.clean_card_data()
print(df_test.head())