# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Data Source

The available data consists of 2 parts:
* a directory of JSON logs on user activity on the app (which songs were requested by users, the log timestamp, some users information ) --> operational database which gets update by users transactions
* a directory with JSON metadata on the songs in their app (songs information as well as artists) --> static database which is updated in case the startup increases its catalogue

Two datasets that reside in S3. Below are the S3 links for each:
* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data
* Log data json path: s3://udacity-dend/log_json_path.json

# Project Description

I will apply the skills learnt from the data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. I will be loading data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. I will apply the learnt optimization techniques to copy the dimensions tables on each cluster and distribute the fact table on the constructed clusters which provide fast response to queries from my DWH.

![Architecture](https://github.com/dinaAbdelrahman/DataEngineering.P3.Data-Warehourse-on-Redshift/blob/master/architecture_AWS.GIF)

# Overview for the structure of the tables

Below shows the star schema I will building from the 2 datasets, details of the dimesions table creation can be found in sql_queries.py

![image.png](attachment:image.png)

Below shows the my fact table I will building from the 2 datasets, all fields are taken from the log_data however the song_id and artist_id were obtained from the join between the log_data and song_data. Details of the table creation can be found in sql_queries.py

![image.png](attachment:image.png)

# Project Steps:

## Create Table Schemas

1. Design schemas for the fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in sql_queries.py
3. Complete the logic in create_tables.py to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist.
5. Launch a redshift cluster and create an IAM role that has read access to S3. I use the IAC method which was more easy to debug and troubleshoot, details can be found in IAC.ipynb
6. Add redshift database and IAM role info to dwh.cfg.
7. Test by running create_tables.py and checking the table schemas in your redshift database. test was successful and the cluster was up in the console as well as the schemas and tables

## Build ETL Pipeline

1. Implement the logic in etl.py to load data from S3 to the 2 staging tables on Redshift.
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift to construct the star schema.
3. Test by running etl.py after running create_tables.py and running the analytic queries on the Redshift database.
4. Delete the redshift cluster after successful load of the data.
