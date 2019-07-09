## Project Introduction: Data Lake 
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I'm going to dealing the data hosted in S3. I will make a ETL pipeline that extracts data from S3, processes them using Spark, and load the data back into S3 as a set of dimensions tables.


### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
users - users in the app
- user_id, first_name, last_name, gender, level
songs - songs in music database
- song_id, title, artist_id, year, duration
artists - artists in music database
- artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

### how to run the script

```
python etl.py
```