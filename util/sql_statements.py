SQL_LOAD_FACT_TABLE = """

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
"""

SQL_TRUNCATE_TABLE ="TRUNCATE {table}"

SQL_LOAD_USER_DIM_TABLE="""
INSERT INTO USERS(user_id,first_name,last_name,gender,level)
SELECT  DISTINCT  COALESCE(NULLIF(sev.userID,0)) as user_id,
        COALESCE(NULLIF(sev.first_name, ''), 'N/A'),
        COALESCE(NULLIF(sev.last_name, ''), 'N/A'),
        COALESCE(NULLIF(sev.gender, ''), 'N/A'),
        sev.level
FROM staging_events as sev
WHERE sev.page ='NextSong';
"""


SQL_LOAD_SONGS_DIM_TABLE="""
INSERT INTO SONGS(song_id,title,artist_id,year,duration)
SELECT  DISTINCT ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        CAST(ss.duration AS FLOAT) AS duration
FROM staging_songs as ss;
""" 
SQL_LOAD_ARTISTS_DIM_TABLE="""
INSERT INTO artists(artist_id,name,location,latitude,longitude)
SELECT  DISTINCT ss.artist_id AS artist_id,
        COALESCE(NULLIF(ss.artist_name,''),'Unknown')  AS name,
        ss.artist_location AS location,
        CAST(ss.artist_latitude AS FLOAT) AS latitude,
        CAST(ss.artist_longitude AS FLOAT) AS longitude
FROM staging_songs as ss;
"""
SQL_LOAD_TIME_DIM_TABLE="""
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
"""