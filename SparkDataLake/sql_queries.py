


songplay_sql= """
    SELECT 
        distinct ts,
        month(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as month,
        year(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as year,
        userId as user_id,
        level,song_id,
        artist_id,
        artist_name,
        duration,
        sessionId as session_id,
        location,
        userAgent as user_agent 
    FROM log l left join song s 
        ON l.song=s.title 
    WHERE page='NextSong' 
        and userId is not NULL
"""

user_sql = """
    SELECT 
        DISTINCT userId as user_id, 
        firstName as first_name, 
        lastName as last_name, 
        gender, 
        level
    from log
    where page = 'NextSong'
        and userId is not NULL
"""

song_sql = """
    SELECT 
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration 
    FROM song
"""

artist_sql = """
    select 
        DISTINCT artist_id, 
        artist_name as name, 
        artist_location as location, 
        artist_latitude as latitude, 
        artist_longitude aslongitude
    from song
"""

time_sql = """
    select 
        from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss') as start_time,
        hour(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as hour,
        day(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as day,
        weekofyear(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as week,
        month(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as month,
        year(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as year,
        dayofweek(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')) as weekday
    from 
        log
    where page = 'NextSong'
        

"""