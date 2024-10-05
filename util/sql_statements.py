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



