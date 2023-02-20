import pandas as pd
import sqlalchemy
import database_utils

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

testing = DataExtractor()
db_connector = database_utils.DatabaseConnector()
db_connector.read_db_creds()
db_connector.init_db_creds()
users = testing.read_rds_table('legacy_users', db_connector)
print(users.head())