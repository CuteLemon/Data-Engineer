## Project: data warehouse
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I will buildan ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

### How to produce the data
We have two types of tables in database.One is fact table,the other is dimension table.
Fact data such as songs' information and users' behavior will be recorded in S3.First, I will load them to the Redshift as staging table.Then use the SQL queries to get the information as dimension table. 

### create tables 

```shell
python create_tables.py
```
You can run this command to create tables.

### load and transform data

```shell
python etl.py
```
You can run this command to load the data from S3 to Redshift and transform them into the fact & dimension tables.

### Fact Table
#### songplays 
- records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
### Dimension Tables
#### users 
- users in the app
    user_id, first_name, last_name, gender, level
    
#### songs 
- songs in music database
    song_id, title, artist_id, year, duration
    
#### artists 
- artists in music database
    artist_id, name, location, lattitude, longitude
    
#### time 
- timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
    
### How to get the sample of data

```
python etl.py -sample
```
You can check the data with this command.