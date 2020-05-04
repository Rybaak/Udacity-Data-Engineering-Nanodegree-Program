# Sparkify ETL process

This is the first project in Udacity Data Engineering Nanodegree Program - "Project: Data Modeling with Postgres". Project is about define fact and dimension tables for a star schema and write ETL pipeline with Python.

# Business problem
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The data we receive is in JSON files, and our job is to create automate pipline on single machine to extract data from file, make schema on the fly, and load data to fact and dimension. The Sparkify analytics team is particularly interested in understanding what songs users are listening to.

# Data example
* Song Sataset: 

    Example file mask: `song_data/A/B/C/TRABCEI128F424C983.json`
    Example JSON row: `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`


* Log Dataset

    Example file mask: `log_data/2018/11/2018-11-12-events.json`
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
1. python create_tables.py - This script will create database sparkifydb if not exists, and above tables if they not exists also.

2. python etl.py - This script will find all file in directory [data/song_data, data/log_data], read, transform data and load into:
    * song_data -> song, artist
    * log_data -> time, user, songplay
    
# Project struct
* data - catalog with source data files
* create_tables.py - python file to create database and tables
* etl.ipynb - interactive notebook to create test pipeline
* etl.py - python file to ETL data from data/
* README.md - Just README
* sql_queries.py - set of sql'q to create and insert
* test.ipynb - interactive notebook to select data by SQL.

# Metadata
* etl.py generates files names and inserted row count ex. :

    1) "Process /home/workspace/data/song_data/A/A/A/TRAAABD128F429CF47.json file. song_count = 1, artist_count = 1"
    
    2) "Process /home/workspace/data/log_data/2018/11/2018-11-29-events.json file. time_count = 319, user_count = 27, songplay = 319"