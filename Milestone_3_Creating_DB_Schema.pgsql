-- This document will outline the postgresSQL queries run on the tables that have been uploaded to the Sales_Data database for Milestone 3, which will create the database schema:

-- M3 - T1:
-- Selects all rows and columns from the orders_table:
SELECT * FROM orders_table;

-- Using the statement below and replacing it with the colimn names outputs the maximum length of the data in the column:
SELECT MAX(LENGTH(product_code)) FROM orders_table;

-- Using the below query, we are casting the columns to the required data types using the '::datatype' cast operator:
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
ALTER COLUMN card_number TYPE varchar(19) USING card_number::varchar(19),
ALTER COLUMN store_code TYPE varchar(12) USING store_code::varchar(12),
ALTER COLUMN product_code TYPE varchar(12) USING product_code::varchar(11),
ALTER COLUMN product_quantity TYPE smallint USING product_quantity::smallint;

-- M3 - T2:
-- Selects all rows and columns from the dim_users:
SELECT * FROM dim_users;

-- Using the statement below to find maximum length of the country_code column:
SELECT MAX(LENGTH(country_code)) FROM dim_users;

-- The query below converts the columns below to the required datatypes: (To note, the date_of_birth and join_date columns were already in date format but if required to change use the same logic as below):
ALTER TABLE orders_table
ALTER COLUMN first_name TYPE varchar(255) USING first_name::varchar(255),
ALTER COLUMN last_name TYPE varchar(12) USING last_name::varchar(12),
ALTER COLUMN country_code TYPE varchar(12) USING country_code::varchar(11),
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

-- M3 - T3:
-- Selects all rows and columns from the dim_store_details:
SELECT * FROM dim_store_details;

-- use the query below to find max length of store_code, country_code
SELECT MAX(LENGTH(store_code)) FROM dim_store_details;

-- First, since there are 2 columns representing latitude which are lat and latitude, we have to merge this into another column:
-- create new column merged_lat:
ALTER TABLE dim_store_details
ADD COLUMN merged_lat text;

-- update merged_lat column with values from lat and latitude columns
UPDATE dim_store_details
SET merged_lat = COALESCE(lat,latitude);

-- drop the lat and latitude columns:
ALTER TABLE dim_store_details
DROP COLUMN lat,
DROP COLUMN latitude;

-- rename the merged_lat column to latitude:
ALTER TABLE dim_store_details
RENAME COLUMN merged_lat TO latitude;

-- casting the columns to required data types: (opening_date already converted to date in cleaning phase):
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE float USING longitude::float,
ALTER COLUMN locality TYPE varchar(255) USING locality::varchar(255),
ALTER COLUMN store_code TYPE varchar(12) USING store_code::varchar(12),
ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint,
ALTER COLUMN store_type TYPE varchar(255) USING store_type::varchar(255),
ALTER COLUMN store_type DROP NOT NULL,
ALTER COLUMN latitude TYPE float USING latitude::float,
ALTER COLUMN country_code TYPE varchar(2) USING country_code::varchar(2),
ALTER COLUMN continent TYPE varchar(255) USING continent::varchar(255);

-- M3 - T4:
-- Selects all rows and columns from the dim_products:
SELECT * FROM dim_products;

-- product_price column has been updated to product_price_£ in cleaning phase and £ removed from every value

-- adding the weight_class column:
ALTER TABLE dim_products
ADD COLUMN weight_class text;

-- updating values in weight_class column using weight_kg column:
UPDATE TABLE dim_products
 SET weight_class =
 	CASE 
 		WHEN weight_kg <=2 THEN 'Light'
		WHEN weight_kg > 2 AND weight_kg <= 40 THEN 'Mid_Sized'
		WHEN weight_kg > 40 AND weight_kg <= 140 THEN 'Heavy'
		WHEN weight_kg > 140 THEN 'Truck_Required'
	END;

-- M3 - T5:
-- Selects all rows and columns from the dim_products:
SELECT * FROM dim_products;

-- changing the name of the removed column to still_available:
ALTER TABLE
RENAME COLUMN removed TO still_available;

-- checking the distinct values of the still_available column:
SELECT DISTINCT(still_available) FROM dim_products;

-- output is Still_avaliable and removed, now can change the values of this column to true or false:
UPDATE TABLE dim_products
	SET still_available =
		CASE
			WHEN still_available = 'Still avaliable' THEN true
			ELSE false
		END;

-- checking max length of the columns: EAN, product_code
SELECT MAX(LENGTH("EAN")) FROM dim_products;

-- casting the column data types as required: (date_added already in date type):
ALTER TABLE dim_products
ALTER COLUMN product_price_£ TYPE float USING product_price_£::float,
ALTER COLUMN weight_kg TYPE float USING weight_kg::float,
ALTER COLUMN "EAN" TYPE varchar(17) USING "EAN"::varchar(17),
ALTER COLUMN product_code TYPE varchar(11) USING product_code::varchar(11),
ALTER COLUMN "uuid" TYPE uuid USING "uuid":uuid,
ALTER COLUMN still_available TYPE bool USING still_available::bool,
ALTER COLUMN weight_class TYPE varchar(14) USING weight_class::varchar(14);

-- M3 - T6:
-- Seeing all rows and columns from the dim_date_times:
SELECT * FROM dim_date_times;

-- checking max length of month, year and day:
SELECT MAX(LENGTH("month")) FROM dim_date_times;

-- casting the columns to required data types:
ALTER TABLE dim_date_times
ALTER COLUMN "month" TYPE char(2) USING "month"::char(2),
ALTER COLUMN "year" TYPE char(4) USING "year"::char(4),
ALTER COLUMN "day" TYPE char(2) USING "day"::char(2),
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- M3 - T7
-- Seeing all rows and columns from the dim_card_details:
SELECT * FROM dim_card_details;

-- checking max length of card_number, expiry_date:
SELECT MAX(LENGTH("card_number")) FROM dim_card_details;

-- initially card_number was 23 but realised that thhis was due to 1 row where '???' was a prefixx to the card number, so replaced the '???' with ''
UPDATE dim_card_details
SET card_number = REPLACE(card_number, '?', '')

-- casting the columns to required data types: (date_payment_confirmed already in date type):
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE varchar(19) USING card_number::varchar(19),
ALTER COLUMN expiry_date TYPE varchar(5) USING expiry_date::varchar(5);

-- M3 - T8
/* The tables are now of correct data types. Each table will serve the orders_table which will be the single source of truth for the orders.
This task will focus on making the columns within each table related to the orders_table to be primary keys */
-- Each PK identified is uniquely related to a column in the orders table:

ALTER TABLE dim_users
	ADD CONSTRAINT dim_users_pk PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
	ADD CONSTRAINT dim_store_details_pk PRIMARY KEY (store_code);

ALTER TABLE dim_products
	ADD CONSTRAINT dim_products_pk PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
	ADD CONSTRAINT dim_date_times_pk PRIMARY KEY (date_uuid); 

ALTER TABLE dim_card_details
	ADD CONSTRAINT dim_card_details_pk PRIMARY KEY (card_number);

-- M3 - T9:
/* With the primary keys created in the tables prefixed with dim we can now create the foreign keys in the orders_table to reference the 
primary keys in the other tables. This makes the star-based database schema complete. */

ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_user_fk FOREIGN KEY (user_uuid)
    REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_store_fk FOREIGN KEY (store_code)
    REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_products_fk FOREIGN KEY (product_code)
    REFERENCES dim_products(product_code);

ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_date_fk FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_card_fk FOREIGN KEY (card_number)
    REFERENCES dim_card_details(card_number);


