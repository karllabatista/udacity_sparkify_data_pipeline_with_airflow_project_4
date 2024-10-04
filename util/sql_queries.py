import configparser

# CONFIG
'''
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY           = config.get('AWS','KEY')
SECRET        = config.get('AWS','SECRET')
LOG_DATA      = config.get('S3','LOG_DATA')
LOG_JSONPATH  = config.get('S3','LOG_JSONPATH')
SONG_DATA     = config.get('S3','SONG_DATA')
REGION        = config.get('S3','REGION')
'''
# DROP TABLES

staging_events_table_drop = "DROP TABLE  IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE  IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE  IF EXISTS songplays;"
user_table_drop = "DROP TABLE  IF EXISTS users;"
song_table_drop = "DROP TABLE  IF EXISTS songs;"
artist_table_drop = "DROP TABLE  IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist VARCHAR(500) ,
        auth VARCHAR(200),
        first_name VARCHAR(500),
        gender CHAR(1),
        itemInSession SMALLINT,
        last_name VARCHAR(500) ,
        length DECIMAL(6),
        level CHAR(20),
        location VARCHAR(500),
        method CHAR(3),
        page VARCHAR(100),
        registration  BIGINT,
        sessionId INTEGER,
        song VARCHAR(500),
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR(500),
        userID INTEGER                                                                                      

    )
    DISTSTYLE ALL;
                               
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id VARCHAR(300),
        num_songs INTEGER,
        title VARCHAR (500),
        artist_name VARCHAR (500),
        artist_latitude VARCHAR,
        year INTEGER,
        duration VARCHAR,
        artist_id VARCHAR,
        artist_longitude VARCHAR,
        artist_location VARCHAR(500)
                              
    )
    DISTSTYLE ALL;
                                                          
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
    
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL DISTKEY,
    level CHAR(20),
    song_id VARCHAR(500) NOT NULL,
    artist_id VARCHAR(500) NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR(500),
    user_agent VARCHAR(500)                    
                         
    )
    COMPOUND SORTKEY (start_time, user_id);
  
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
    (                 
        user_id INTEGER PRIMARY KEY SORTKEY ,
        first_name VARCHAR(500) NOT NULL,
        last_name  VARCHAR(500) NOT NULL,
        gender VARCHAR(20) ,
        level VARCHAR(20) 
    )
    DISTSTYLE ALL;               
    
                     
""")

song_table_create = ("""
    CREATE TABLE  IF NOT EXISTS songs
    (                 
        song_id VARCHAR(500) PRIMARY KEY NOT NULL SORTKEY,
        title VARCHAR(500) NOT NULL,
        artist_id  VARCHAR(500) NOT NULL,
        year INTEGER,
        duration  FLOAT 
    )
    DISTSTYLE ALL;               
    
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR(500) NOT NULL SORTKEY,
        location VARCHAR(500),
        latitude FLOAT,
        longitude FLOAT
        
    )
    DISTSTYLE ALL;

""")
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS TIME
    (
        start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER NOT NULL,
        weekday VARCHAR
                                 
    )
    
    DISTSTYLE ALL; 
""")

# STAGING TABLES
'''
staging_events_copy = ("""
    COPY staging_events
    FROM  {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    JSON {}
    REGION {};

""").format(LOG_DATA,KEY,SECRET,LOG_JSONPATH,REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM  {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    FORMAT as JSON 'auto'    
    REGION {};
                      
""").format(SONG_DATA,KEY,SECRET,REGION)
'''         
# FINAL TABLES

songplay_table_insert = ("""
                         
INSERT INTO SONGPLAYS(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' AS start_time,
        se.userID AS user_id,
        se.level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId as session_id,
        se.location AS location,
        se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss on ss.artist_name = se.artist
WHERE se.page ='NextSong';

""")

user_table_insert = ("""
INSERT INTO USERS(user_id,first_name,last_name,gender,level)
SELECT  DISTINCT  COALESCE(NULLIF(sev.userID,0)) as user_id,
        COALESCE(NULLIF(sev.first_name, ''), 'N/A'),
        COALESCE(NULLIF(sev.last_name, ''), 'N/A'),
        COALESCE(NULLIF(sev.gender, ''), 'N/A'),
        sev.level
FROM staging_events as sev
WHERE sev.page ='NextSong';
""")

song_table_insert = ("""
INSERT INTO SONGS(song_id,title,artist_id,year,duration)
SELECT  DISTINCT ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        CAST(ss.duration AS FLOAT) AS duration
FROM staging_songs as ss;

""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,name,location,latitude,longitude)
SELECT  DISTINCT ss.artist_id AS artist_id,
        COALESCE(NULLIF(ss.artist_name,''),'Unknown')  AS name,
        ss.artist_location AS location,
        CAST(ss.artist_latitude AS FLOAT) AS latitude,
        CAST(ss.artist_longitude AS FLOAT) AS longitude
FROM staging_songs as ss;
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) as hour,
    EXTRACT(day FROM start_time) as day,
    EXTRACT(week FROM start_time) as week,
    EXTRACT(month FROM start_time) as month,
    EXTRACT(year FROM start_time) as year ,
    EXTRACT(weekday FROM start_time) as weekday
FROM staging_events AS sev
WHERE sev.page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


