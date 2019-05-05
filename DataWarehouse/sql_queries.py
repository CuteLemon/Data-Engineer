import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN= config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
    artist varchar(200),
    auth varchar(200),
    firstName varchar(200),
    gender varchar(200),
    itemInSession int,
    lastName varchar(200),
    length varchar(200),
    level varchar(200),
    location varchar(200),
    method varchar(200),
    page varchar(200),
    registration varchar(200),
    sessionId int,
    song varchar(200),
    status varchar(200),
    ts bigint,
    userAgent varchar(200),
    userId int 
)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
    artist_id varchar(100),
    artist_latitude float,
    artist_location varchar(200),
    artist_longitude float,
    artist_name varchar(200),
    duration float,
    num_songs int,
    song_id varchar(100),
    title varchar(500),
    year int
)
""")

songplay_table_create = ("""
create table if not exists songplay (
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL,
    level varchar(20),
    song_id varchar(100) NOT NULL,
    artist_id varchar(100) NOT NULL, 
    session_id int, 
    location varchar(200),
    user_agent varchar(200)

)
""")

user_table_create = ("""
create table if not exists users (
    user_id int PRIMARY KEY,
    first_name varchar(100),
    last_name varchar(100),
    gender varchar(10),
    level varchar(20)
)
""")

song_table_create = ("""
create table if not exists songs (
    song_id varchar(50) PRIMARY KEY,
    title varchar(500),
    artist_id varchar(100) NOT NULL,
    year int,
    duration float
)
""")

artist_table_create = ("""
create table if not exists artists  (
    artist_id varchar(100) PRIMARY KEY,
    name varchar(200),
    location varchar(200),
    lattitude float,
    longitude float
)
""")

time_table_create = ("""
create table if not exists time  (
    start_time timestamp PRIMARY KEY,
    hour int, day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data' 
    credentials 'aws_iam_role={}' 
    JSON 'auto';
""").format(DWH_ROLE_ARN)
#  compupdate off region 'us-west-2';
staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data' 
    credentials 'aws_iam_role={}' 
    JSON 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(
            start_time,
            user_id, 
            level, 
            song_id, 
            artist_id,
            session_id, 
            location, 
            user_agent) 
(
    select distinct  to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')  as start_time,
            userId as user_id, 
            level, 
            song_id, 
            artist_id,
            sessionId as session_id, 
            location, 
            userAgent as user_agent
    from staging_events e left join staging_songs s
    on e.song = s.title
    where page = 'NextSong'
        and userId is not NULL

)
               
""")

user_table_insert = ("""
INSERT INTO users(
    select distinct userId as user_id, 
            firstName as first_name, 
            lastName as last_name, 
            gender, 
            level
    from staging_events
    where page = 'NextSong'
        and userId is not NULL
)

""")

song_table_insert = ("""
INSERT INTO songs (
    select distinct song_id,
            title,
            artist_id,
            year,
            duration 
    from staging_songs
)

                 
""")

artist_table_insert = ("""
INSERT INTO artists (
    select distinct artist_id, 
            artist_name as name, 
            artist_location as location, 
            artist_latitude as latitude, 
            artist_longitude aslongitude
    from staging_songs
)

""")

time_table_insert = ("""
INSERT INTO time (
    select  distinct 
            to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS') as start_time,
            extract(hour from to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')) as hour,
            extract(day from to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')) as day,
            extract(week from to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')) as week,
            extract(month from to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')) as month,
            extract(year from to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')) as year,
            extract(weekday from to_timestamp('epoch'::date + e.ts/1000 * interval '1 s','YYYY-MM-DD HH24:MI:SS')) as weekday
    from staging_events e
    where page = 'NextSong'
)

""")

# GET SAMPLE DATA
staging_events_sample = ("""
    select * 
    from staging_events
    limit 5
""")

staging_songs_sample = ("""
    select * 
    from staging_songs
    limit 5
""")

songplay_sample = ("""
    select * 
    from songplay
    limit 5
""")

users_sample = ("""
    select * 
    from users
    limit 5
""")

songs_sample = ("""
    select * 
    from songs
    limit 5
""")

artists_sample = ("""
    select * 
    from artists
    limit 5
""")

time_sample = ("""
    select * 
    from time
    limit 5
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
get_sample_table_queries = [staging_events_sample,staging_songs_sample,users_sample,songplay_sample,songs_sample,artists_sample,time_sample]