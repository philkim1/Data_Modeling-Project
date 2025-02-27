import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

logdata = config.get("S3", "LOG_DATA")
song_data = config.get("S3", "SONG_DATA")
dwh_role_arn = config.get("IAM_ROLE", "ARN")
log_jsonpath = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "drop table if exists events;"
staging_songs_table_drop = "drop table if exists songs;"
songplay_table_drop = "drop table if exists fact_songplay;"
user_table_drop = "drop table if exists dim_user;"
song_table_drop = "drop table if exists dim_song;"
artist_table_drop = "drop table if exists dim_artist;"
time_table_drop = "drop table if exists dim_times;"

# CREATE TABLES for staging and the database

staging_events_table_create= ("""create table events (\
    artist            varchar(150),\
    auth              varchar(10) not null,\
    firstName         varchar(15),\
    gender            varchar(1),\
    itemInSession     integer not null,\
    lastName          varchar(15),\
    length            double precision,\
    level             varchar(4),\
    location          varchar(50),\
    method            varchar(3) not null,\
    page              varchar(20) not null,\
    registration      bigint,\
    sessionId         integer not null,\
    song              varchar(175),\
    status            integer not null,\
    ts                timestamp not null,\
    userAgent         varchar(150),\
    userId            integer);    
""")

staging_songs_table_create = ("""create table songs (\
    num_songs            integer not null,\
    artist_id            varchar(20) not null,\
    artist_latitude      double precision,\
    artist_longitude     double precision,\
    artist_location      varchar(225),\
    artist_name          varchar(200) not null,\
    song_id              varchar(20) not null,\
    title                varchar(175) not null,\
    duration             double precision not null,\
    year                 integer not null);
""")

songplay_table_create = ("""create table fact_songplay (\
    songplay_id    bigint distkey,\
    start_time     time not null,\
    user_id        integer,\
    level          varchar(4),\
    song_id        varchar(20) not null,\
    artist_id      varchar(20) not null,\
    session_id     integer not null,\
    location       varchar(50),\
    user_agent     varchar(150));
""")

user_table_create = ("""create table dim_user (\
    user_id     integer sortkey,\
    first_name  varchar(15),\
    last_name   varchar(15),\
    gender      varchar(1),\
    level       varchar(4))
diststyle all;
""")

song_table_create = ("""create table dim_song (\
    song_id    varchar(20) not null sortkey,\
    title      varchar(175) not null,\
    artist_id  varchar(20) not null,\
    year       integer not null,\
    duration   double precision not null)
diststyle all;
""")

artist_table_create = ("""create table dim_artist (\
    artist_id   varchar(20) not null sortkey,\
    name        varchar(200) not null,\
    location    varchar(225),\
    latitude    double precision,\
    longitude  double precision)
diststyle all;
""")

time_table_create = ("""create table dim_times (\
    start_time   time not null,\
    hour         integer not null,\
    day          integer not null,\
    week         integer not null,\
    month        integer not null,\
    year         integer not null,\
    weekday      boolean not null);
""")

# loading the data in S3 into the STAGING TABLES

staging_events_copy = ("""
    copy events from '{}'\
    credentials 'aws_iam_role={}'\
    json '{}'\
    region 'us-west-2'\
    timeformat as 'epochmillisecs'
""").format(logdata,dwh_role_arn,log_jsonpath)

staging_songs_copy = ("""
    copy songs from '{}'\
    credentials 'aws_iam_role={}'\
    json 'auto'\
    region 'us-west-2'\
""").format(song_data,dwh_role_arn)

# FINAL TABLES for the database

songplay_table_insert = ("""insert into fact_songplay (songplay_id, start_time, user_id, level, song_id, artist_id,                                                     session_id, location, user_agent)
    select e.registration,
           e.ts::time as start_time,
           e.userId,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionId,
           e.location,
           e.userAgent
    from events e
    join songs s on (s.artist_name=e.artist)
    where e.page='NextSong';
""")

user_table_insert = ("""insert into dim_user (user_id, first_name, last_name, gender, level)
    select e.userId,
           e.firstName,
           e.lastName,
           e.gender,
           e.level
    from events e;
""")

song_table_insert = ("""insert into dim_song (song_id, title, artist_id, year, duration)
    select s.song_id,
           s.title,
           s.artist_id,
           s.year,
           s.duration
    from songs s;
""")

artist_table_insert = ("""insert into dim_artist (artist_id, name, location, latitude, longitude)
    select s.artist_id,
           s.artist_name,
           s.artist_location,
           s.artist_latitude,
           s.artist_longitude
    from songs s;
""")

time_table_insert = ("""insert into dim_times (start_time, hour, day, week, month, year, weekday)
    select ts::time as start_time,
           extract(hour from ts) as hour,
           extract(day from ts) as day,
           extract(week from ts) as week,
           extract(month from ts) as month,
           extract(year from ts) as year,
           case when extract(DOW from ts) in (6,0) then false else true end as weekday
    from events;
""")

# QUERY LISTS for the python scripts create_tables.py and etl.py to use

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
