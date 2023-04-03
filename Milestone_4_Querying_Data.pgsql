/* This document will outline the postgresSQL queries run on the Sales_Data database. The aim will be to get up-to-date metrics of the data.
This will permit the business to start making more data-drive decisions and get a better understanding of its sales. This Milestone will, outline
business questions which will be answered through creating queries for the Sales_Data database: */

-- M4 - T1:
/* How many stores does the business have and in which countries? The Operations team would like to know which countries we currently operate in 
and which country now has the most stores. */

SELECT country_code, COUNT(*) AS total_no_stores -- the COUNT(*) function is used to count the number of rows (i.e., the number of stores) in each group defined by country_code
FROM dim_store_details -- indicates which table we are referring to in the previous row
WHERE country_code <> 'NA' -- since there is web portal store, we include this as it's country code is NA
GROUP BY country_code -- we are grouping by country_code as we want to see total_no_store per country
ORDER BY total_no_stores DESC; -- we then organise the data in descending order from the country with the highest number of stores to the lowest

-- M4 - T2:
/* Which locations currently have the most stores? The business stakeholders would like to know which locations currently have the most stores.
They would like to close some stores before opening more in other locations. Find out which locations have the most stores currently.*/

SELECT locality, COUNT(*) AS total_no_stores -- the COUNT(*) function is used to count the number of rows (i.e., the number of stores) in each group defined by locality
FROM dim_store_details -- indicates which table we are referring to in the previous row
GROUP BY locality -- we are grouping by locality as we want to see total_no_store per locality
ORDER BY total_no_stores DESC -- we then organise the data in descending order from the locality with the highest number of stores to the lowest
LIMIT 7; -- limit the output to the top 7 localities 

-- M4 - T3:
/* Which months produce the average highest cost of sales typically?
Query the database to find out which months typically have the most sales. */

SELECT dd.month, ROUND(SUM(dp.product_price_£ * oo.product_quantity)::numeric, 2) AS total_sales -- selects the month column from dim_date_times and sum of sales which is product price times product_quantity, casting this as numeric and rounding to 2 dp.
FROM orders_table AS oo -- from the orders_table joining thhe dim_date_times and dim_products using their primary and foreign keys assigned earlier
    JOIN dim_date_times AS dd ON oo.date_uuid = dd.date_uuid
    JOIN dim_products AS dp ON oo.product_code = dp.product_code
GROUP BY dd.month -- grouping by monthh as we want total sales by month
ORDER BY total_sales DESC -- ordering this in descending order by total price 
LIMIT 6; -- limiting the number of rows to 6 

-- M4 - T4:
/* How many sales are coming from online?
The company is looking to increase its online sales. They want to know how many sales are 
happening online vs offline. Calculate how many products were sold and the amount of sales made 
for online and offline purchases. */

SELECT -- the CASE is used to create a location column according to the store_type in the dim_store_details table. 
    CASE 
        WHEN dsd.store_type = 'Web Portal' THEN 'Online' -- labels store_type Web Portal as Online 
        ELSE 'Offline' -- and the rest as offline
    END AS location, -- renaming this column now as location
    COUNT(dd.*) AS num_sales, -- dim_date_times shows all transations so counting this will provide total number of sales
    SUM(oo.product_quantity) AS total_quantity -- summing the product_quantity column from the orders_table provides total quantity of products sold
FROM orders_table AS oo -- joining the dim_date_times table and dim_store_details table to the orders_table
    JOIN dim_date_times AS dd ON oo.date_uuid = dd.date_uuid
    JOIN dim_store_details AS dsd ON oo.store_code = dsd.store_code
GROUP BY location; -- grouping by the newly formed location column which has 2 categories

-- M4 - T5:
/* What percentage of sales come through each store type?
The sales team wants to know which of the different store types is generated the most revenue
so they know where to focus. Find out the total and percentage of sales coming from
each of the different store types.*/

SELECT
	dsd.store_type, -- selects the store_type column from the dim_store_details table
    ROUND(SUM(dp.product_price_£ * oo.product_quantity)::numeric, 2) AS total_sales, -- creates a total_sales column for the sum of total sales 
    ROUND(((SUM(dp.product_price_£ * oo.product_quantity)::numeric /SUM(SUM(dp.product_price_£ * oo.product_quantity)::numeric) OVER())*100), 2) AS percentage_total -- calculates the % of total sales per store_type, OVER function is used to compute the overall sum of total_sales
FROM orders_table AS oo -- joining the dim_products and dim_store_details to thhe order_table
    JOIN dim_products AS dp ON oo.product_code = dp.product_code
    JOIN dim_store_details AS dsd ON oo.store_code = dsd.store_code
GROUP BY dsd.store_type; -- grouping by store_type
