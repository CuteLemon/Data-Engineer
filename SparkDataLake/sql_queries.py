


songplay_sql= """
    SELECT 
        distinct ts,
        userId as user_id,
        level,song_id,
        artist_id,
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