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

