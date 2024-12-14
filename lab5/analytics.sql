-- Create the database if it does not exist
CREATE DATABASE IF NOT EXISTS hivedb;

-- Database creation step
SELECT 'Database creation step completed';

-- Switch to the new database
USE hivedb;

-- Switched to hivedb
SELECT 'Switched to hivedb';

-- Create external table if it does not exist
CREATE EXTERNAL TABLE IF NOT EXISTS user_hive (
  user_id string,
  name string,
  email string,
  created_at string -- Changed to string from timestamp
)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES (
  "dynamodb.table.name" = "user",
  "dynamodb.column.mapping" = "user_id:user_id,name:name,email:email,created_at:created_at"
);

-- External table creation step
SELECT 'External table creation step completed';

-- Verify data in the external table
SELECT * FROM user_hive LIMIT 10;

-- Data verification step
SELECT 'Data verification step completed';

-- Create a partitioned table in Parquet format
CREATE TABLE IF NOT EXISTS user_parquet (
    user_id STRING,
    name STRING,
    email STRING,
    created_at TIMESTAMP
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://soumil-dev-bucket-1995/tmp/user_parquet/';


-- Partitioned table creation step
SELECT 'Partitioned table creation step completed';

-- Set the execution engine to MapReduce for stability
SET hive.execution.engine = mr;


-- Execution engine set to MR
SELECT 'Execution engine set to MR';

-- Enable non-strict dynamic partition mode
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Dynamic partition mode set to nonstrict
SELECT 'Dynamic partition mode set to nonstrict';

-- Insert data into the partitioned Parquet table
INSERT INTO TABLE user_parquet PARTITION (year, month, day)
SELECT user_id, name, email,
       from_unixtime(unix_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss')) AS created_at,
    YEAR(from_unixtime(unix_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss'))) AS year,
    MONTH(from_unixtime(unix_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss'))) AS month,
    DAY(from_unixtime(unix_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss'))) AS day
FROM user_hive;
-- Data insertion step
SELECT 'Data insertion step completed';

-- Count users by month and year
SELECT year, month, COUNT(*) AS user_count
FROM user_parquet
GROUP BY year, month
ORDER BY year, month;
-- User count by month and year step
SELECT 'User count by month and year step completed';

-- Find the total number of users
SELECT COUNT(*) AS total_users
FROM user_parquet;
-- Total users step
SELECT 'Total users step completed';

-- Find the average number of users per day
SELECT AVG(user_count) AS avg_users_per_day
FROM (
         SELECT DAY(from_unixtime(unix_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss'))) AS day, COUNT(*) AS user_count
         FROM user_parquet
         GROUP BY DAY(from_unixtime(unix_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss')))
     ) AS daily_users;
-- Average users per day step
SELECT 'Average users per day step completed';

-- Find the top 5 names by frequency
SELECT name, COUNT(*) AS name_count
FROM user_parquet
GROUP BY name
ORDER BY name_count DESC
    LIMIT 5;

-- Top 5 names by frequency step
SELECT 'Top 5 names by frequency step completed';
