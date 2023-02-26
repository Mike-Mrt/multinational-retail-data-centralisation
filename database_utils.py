import yaml
import sqlalchemy
import psycopg2
import pandas as pd

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




