-- SETUP
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486408.zoomcamp.yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-julian11/yellow_tripdata_2019-*.csv.gz', 
          'gs://kestra-zoomcamp-julian11/yellow_tripdata_2020-*.csv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486408.zoomcamp.green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-julian11/green_tripdata_2019-*.csv.gz', 
          'gs://kestra-zoomcamp-julian11/green_tripdata_2020-*.csv.gz']
);

SELECT column_name, data_type 
FROM `kestra-sandbox-486408.zoomcamp.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'green_tripdata'
ORDER BY ordinal_position;

-- homework
-- Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.
-- Which zone had the highest revenue?

SELECT COUNT(1)
FROM `kestra-sandbox-486408.dbt_prod.fct_monthly_zone_revenue`;

SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
FROM `kestra-sandbox-486408.dbt_prod.fct_monthly_zone_revenue`
WHERE EXTRACT(YEAR FROM revenue_month) = 2020 AND service_type = 'Green'
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 10;

-- Question 5. Q5: Total trips for Green taxis in October 2019?

SELECT SUM(total_monthly_trips) as trips
FROM `kestra-sandbox-486408.dbt_prod.fct_monthly_zone_revenue`
WHERE revenue_month BETWEEN '2019-10-01' AND '2019-10-31' AND service_type = 'Green';

-- Not: below query processes 1.6GB while the one on the aggregated fcts table processes jsut a few KB
SELECT COUNT(1)
FROM `kestra-sandbox-486408.dbt_prod.fct_trips`
WHERE pickup_datetime >= '2019-10-01' 
    AND pickup_datetime < '2019-11-01' 
    AND service_type = 'Green';


--  Load the FHV trip data for 2019 into your data warehouse (first into bucket)

CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486408.zoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-julian11/fhv_tripdata_2019-*.csv.gz']
);

-- count rows of new staging table

SELECT COUNT(1)
FROM `kestra-sandbox-486408.dbt_prod.stg_fhv_tripdata`;













