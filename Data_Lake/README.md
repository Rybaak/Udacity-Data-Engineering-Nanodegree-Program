# Sparkify ETL process

This is the 4 project in Udacity Data Engineering Nanodegree Program - "Project: Data Lake". Project is about define fact and dimension tables for a star schema and write ETL pipeline with Python and Apache Spark on AWS Cloud.

# Business problem
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The data we receive is in JSON files on S3, and our job is to create automate pipline on single machine to extract data from file, make schema on the fly, and load data to fact and dimension into Amazon RedShift on AWS. The Sparkify analytics team is particularly interested in understanding what songs users are listening to.

# Data example
* Song Sataset: 

    Example file mask: `s3://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json`
    Example JSON row: `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`


* Log Dataset

    Example file mask: `s3://udacity-dend/log_data/2018/11/2018-11-12-events.json`
    Example JSON row: `{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}`

# STAR Schema
FACT Table: 

`songplays` - (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) -> records in log data associated with song plays i.e. records with page "NextSong".

DIMENSION Tables: 

`users` - (user_id, first_name, last_name, gender, level) -> users in the app

`songs` - (song_id, title, artist_id, year, duration) -> songs in music database

`artists` - (artist_id, name, location, lattitude, longitude) -> artists in music database

`time` - (start_time, hour, day, week, month, year, weekday) -> timestamps of records in songplays broken down into specific units

# Usage
1. Put your AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY in dl.cfg .
2. python etl.py - This script will find all file in S3 buckets and run Spark application to process data.
* s3://udacity-dend/song_data
* s3://udacity-dend/log_data

    read, transform data and load into star schema:
    * song_data -> song, artist
    * log_data -> time, user, songplay
    
# Output
Data will be stored in your output data S3 bucket in:
* songs_table
* artists_table
* users_table
* time_table
* songplays_table
    
# Project struct
* dl.cfg - Config of your Amazon S3 on AWS such as [AWS_ACCESS_KEY_ID]/[AWS_SECRET_ACCESS_KEY]
* etl.py - python file to ETL data from S3 into S3 with transformation in Apache Spark
* README.md - Just README

# Metadata
* etl.py generates output in comand line, which SQL comman is actualy running.

# Quick start:
* Put your configuration into dl.cfg
* etl.py