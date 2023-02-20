import yaml 

class DatabaseConnector:
    '''
    This class will be used to connect and upload the extracted data to the database.
    '''

    # the read_db_creds() method will read the credentials yaml file and return the dictionary of the credentials
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            self.credentials = yaml.safe_load(f)

    