# Sparkify Cloud ETL Pipeline: From S3 to Redshift

## Introduction

Sparkify, a rapidly growing music streaming startup, is looking to leverage the cloud to handle its expanding user base and song database. 
This repository contains the necessary tools and pipelines to extract Sparkify's data from Amazon S3, stage it on Amazon Redshift, and transform it into a structured format suitable for analytics purposes.

## Project Description

This project aims to construct an ETL (Extract, Transform, Load) pipeline for Sparkify's data that is currently hosted in Amazon S3. 
The data in S3 is organized into two main directories:

1. **User Activity Logs:** JSON logs that detail user activities on the Sparkify app.
2. **Song Metadata:** JSON metadata that contains information about the songs available on the Sparkify platform.

The main objectives of this project are:

1. Extract the data from S3.
2. Stage the extracted data in Redshift.
3. Transform the staged data into a set of dimensional tables, optimized for the analytics team's queries about user song plays.

## Getting Started

### Prerequisites

- AWS account with access to S3 and Redshift services.
- Python 3.x
- Libraries: `psycopg2`, `boto3`
- Redshift cluster with necessary IAM roles for S3 read access.

### Configuration

1. Clone this repository:
   ```
   git clone https://github.com/nadyavoynich/DataEngineering-ND-CloudETL-Redshift
   cd Sparkify-CloudETL-Pipeline
   ```

2. Create a configuration file named `dwh.cfg` in the root directory with the following format:

   ```
   [CLUSTER]
   REGION='us-west-2'
   
   [DWH]
   HOST=YOUR_REDSHIFT_ENDPOINT
   DB_NAME=YOUR_DB_NAME
   DB_USER=YOUR_DB_USER
   DB_PASSWORD=YOUR_DB_PASSWORD
   DB_PORT=5439
   
   [IAM_ROLE]
   ARN=YOUR_ARN

   [S3]
   LOG_DATA='s3://udacity-dend/log_data'
   LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
   SONG_DATA='s3://udacity-dend/song_data'
   ```

   Replace placeholders with your actual AWS and Redshift details.

### Execution Steps

1. Create tables (or drop and recreate tables):
   ```
   python create_tables.py
   ```

2. Run the ETL pipeline:
   ```
   python etl.py
   ```

After running these commands, your Redshift database will have the dimensional tables loaded with data from S3, ready for analytics queries.

3. Check data quality by simple counts:
   ```
   data_quality_check.py
   ```

## Database Schema

The database is structured into fact and four dimension tables optimized for song play analyses.
The primary fact table is `songplays`, and the dimension tables are `users`, `songs`, `artists`, and `time`.

## Further work
* Create a dashboard for analytic queries on the database.

## Acknowledgments

This project is part of the Data Engineering Nanodegree Program provided by Udacity.
