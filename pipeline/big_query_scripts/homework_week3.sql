# SETUP
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486408.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://kestra-zoomcamp-julian11/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE kestra-sandbox-486408.zoomcamp.yellow_tripdata_non_partitioned AS
SELECT * FROM kestra-sandbox-486408.zoomcamp.external_yellow_tripdata;

# question 1
SELECT COUNT(1) FROM `zoomcamp.external_yellow_tripdata`;

# question 2
SELECT DISTINCT PULocationID FROM `zoomcamp.external_yellow_tripdata`;

SELECT DISTINCT PULocationID FROM `zoomcamp.yellow_tripdata_non_partitioned`;

# question 3
SELECT PULocationID FROM `zoomcamp.yellow_tripdata_non_partitioned`;
SELECT PULocationID, DOLocationID FROM `zoomcamp.yellow_tripdata_non_partitioned`;

# question 4

SELECT COUNT(1)
FROM `zoomcamp.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;

# question 5
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `zoomcamp.yellow_tripdata_non_partitioned`;

#question 6 Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? (1 point) 

SELECT DISTINCT VendorID 
FROM `zoomcamp.yellow_tripdata_partitoned`
WHERE Date(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID 
FROM `zoomcamp.yellow_tripdata_non_partitioned`
WHERE Date(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

#question 9
SELECT COUNT(*) FROM `zoomcamp.yellow_tripdata_partitoned`;


