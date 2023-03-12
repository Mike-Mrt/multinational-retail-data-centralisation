# Multinational Retail Data Centralisation Project

The scenario is based on a multinational company that sells various products globally. The current state is that their sales data is spread across multiple data sources; making it not easily accesible and cannot be analysed by current members of the team. The purpose will be to ensure that all of the sales data is accessible from one centralised location. Therefore, the first task will be to ensure all data is retrieved from the multiple data sources, cleaned and uploaded to a central database. This will then allow the company members to query the data to get up-to-date metrics for the business. 

## Milestone 2

The aim of milestone 2 is to write python scripts which will connect to the data sources, extract this data and clean the extracted data. This data once cleaned will be uploaded to a postgres SQL database so that the data can be queried. 
 
 - database_utils.py script: This script is used to connect and upload the extracted data to the database. The DataExtractor Class contains methdos which will extract data from various data sources and output a pabdas dataframe.
 - data_extraction.py script: This script is used to help extract data from different data sources such as CSV files, an API and an S3 bucket. The DatabaseConnector Class has methods which will connect and upload data to the database.
 - data_cleaning.py script: This script is used to clean the data that is extracted from the various data sources listed above. The DataCleaning Class contains methods which will take the pandas dataframe, clean it from Null values, incorrect information, alter data types and correct any erroneous data and return the final pandas dataframe that will be uploaded to the postgres SQL database.

 The scripts can be found with detailed explanations on my GitHub folder.

## Milestone 3

The aim of milestone 3 is to develop a star based schema for the database and convert the columns in the tables of the database to the correct datatype.

Starting with the orders table, converted:
- the date_uuid and user_uuid columns from text to uuid data type
- the card_number, store_code and product_code columns from text to varchar(x) where x represents the maximum number of characters of the column
- the product_quantity from bigint to smallint to save memory


> Insert screenshot of what you have built working.

## Milestone 4

Introduce aims of Milestone 4 and describe the aims and outcomes

```python``` # add code here

> Insert screenshot of what you have built working.

## Conclusions

Concluding comments, was the project successful
What does the project accomplish
What could be improved etc 
