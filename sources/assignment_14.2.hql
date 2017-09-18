-- to know which DB is being used currently
set hive.cli.print.current.db=true;

-- create a database named custom on HDFS
CREATE DATABASE IF NOT EXISTS `assignment_14_2`;

-- switch to the database custom which we created
USE assignment_14_2;

-- create a table called temperature_data_temp
-- this will be used to load the data. It is required to create this
-- temp table because date field cannot be read directly
CREATE TABLE IF NOT EXISTS `temperature_data_temp`(
	temperature_date string,
	zip_code int,
	temperature float
)
row format delimited
fields terminated by ',';
 
-- this is the actual table into which the data will be loaded
CREATE TABLE IF NOT EXISTS `temperature_data`(
	temperature_date date,
	zip_code int,
	temperature float
);

-- load the data to the temp table
LOAD DATA 
LOCAL INPATH '/home/arvind/hive/acadgild/assignments/assignment_14.2/input/input.txt' OVERWRITE
INTO TABLE temperature_data_temp;

-- load the data to the actual table from the temp table
-- IN this process convert the date in string format to date format
INSERT INTO temperature_data 
SELECT TO_DATE(from_unixtime(unix_timestamp(temperature_date, 'dd-MM-yyyy'))), zip_code, temperature FROM temperature_data_temp;

-- drop the temp table
DROP TABLE IF EXISTS temperature_data_temp;

-- problem statment:
-- Fetch date and temperature from temperature_data where zip code is greater than
-- 300000 and less than 399999.
-- store the output in a directory in the local file system
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_14.2/output/task_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' 
SELECT * FROM temperature_data WHERE zip_code>300000 AND zip_code<399999;

-- problem statment:
-- Calculate maximum temperature corresponding to every year from temperature_data table.
-- store the output in a directory in the local file system
-- YEAR function is used to get the year component out of the DATE field
-- So GROUP BY YEAR(temperature_date) and find the max of temperature for each group
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_14.2/output/task_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
SELECT YEAR(temperature_date) AS YEAR, MAX(temperature) AS max_temperature FROM temperature_data GROUP BY YEAR(temperature_date);

-- problem statment:
-- Calculate maximum temperature from temperature_data table corresponding to those
-- years which have at least 2 entries in the table.
-- the inner query filters out those years who have a count of at least 2(i.e who have
-- at least 2 entries). Then we use an IN clause with the main table to filter out those years
-- and using GROUP BY find the MAX temperature for all such years
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_14.2/output/task_3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
SELECT YEAR(temperature_date), MAX(temperature) FROM
temperature_data 
WHERE YEAR(temperature_date) IN (SELECT YEAR(temperature_date) AS year FROM temperature_data GROUP BY YEAR(temperature_date) HAVING COUNT(1) >= 2) 
GROUP BY YEAR(temperature_date);

-- problem statment:
-- Create a view on the top of last query, name it temperature_data_vw.
-- same query as the last one. But we are creating a view out of the query
CREATE VIEW IF NOT EXISTS temperature_data_vw AS 
SELECT YEAR(temperature_date), MAX(temperature) FROM
temperature_data 
WHERE YEAR(temperature_date) IN (SELECT YEAR(temperature_date) AS year FROM temperature_data GROUP BY YEAR(temperature_date) HAVING COUNT(1) >= 2) 
GROUP BY YEAR(temperature_date);

-- problem statment:
-- Export contents from temperature_data_vw to a file in local file system, such that each
-- file is '|' delimited.
-- select the data from th eview and store it in a file in local file system
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_14.2/output/task_5'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
SELECT * FROM temperature_data_vw;
