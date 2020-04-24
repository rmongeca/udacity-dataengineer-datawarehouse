"""
SQL Queries Script.
This script contains the queries that are executed in the other scripts,
together with wrapper functions around the psycopg2 functions to handle the DB
which control error exceptions. Finally, the script also contains other
constants used in other scripts, such as the configuration parameters in
dwh.cfg.
"""
import configparser
import psycopg2


# Config file
config = configparser.ConfigParser()
config.read("dwh.cfg")

# Tables list
TABLES = ["staging_events", "staging_songs", "songplays", "users", "songs",
          "artists", "time"]

# Create table statements
STAGING_EVENTS_CREATE = """
CREATE TABLE staging_events (
    staging_event_id int identity(0,1) PRIMARY KEY,
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession smallint,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId smallint,
    song varchar,
    status smallint,
    ts bigint,
    userAgent varchar,
    userId int
)
"""

STAGING_SONGS_CREATE = """
CREATE TABLE staging_songs (
    staging_song_id int identity(0,1) PRIMARY KEY,
    num_songs smallint,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int
)
"""

SONGPLAYS_CREATE = """
CREATE TABLE songplays (
    songplay_id int identity(0,1) PRIMARY KEY,
    start_time timestamp,
    user_id int NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id smallint,
    length numeric,
    location varchar,
    user_agent varchar
)
"""
USERS_CREATE = """
CREATE TABLE users (
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
"""
SONGS_CREATE = """
CREATE TABLE songs (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration numeric
)
"""
ARTISTS_CREATE = """
CREATE TABLE artists (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric
)
"""
TIME_CREATE = """
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
"""

# Staging tables COPY statements
STAGING_EVENTS_COPY = ("""
COPY staging_events
FROM {}
JSON {}
IAM_ROLE {}
REGION 'us-west-2'
""").format(
    config["S3"]["LOG_DATA"], config["S3"]["LOG_JSONPATH"],
    config["IAM_ROLE"]["ARN"]
)

STAGING_SONGS_COPY = ("""
COPY staging_songs
FROM {}
JSON 'auto'
IAM_ROLE {}
REGION 'us-west-2'
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])


# Final tables INSERT statements
SONGPLAYS_INSERT = ("""
INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id,
    length, location, user_agent)
SELECT
    (timestamp 'epoch' + se.ts/1000 * interval '1 s') AS start_time,
    se.userId AS user_id,
    s.song_id,
    a.artist_id,
    se.sessionId AS session_id,
    se.length,
    se.location,
    se.userAgent AS user_agent
FROM staging_events se
LEFT JOIN songs s ON se.song = s.title
LEFT JOIN artists a ON se.artist = a.name
WHERE se.userId IS NOT NULL
""")

USERS_INSERT = ("""
INSERT INTO users
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
""")

SONGS_INSERT = ("""
INSERT INTO songs
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

ARTISTS_INSERT = ("""
INSERT INTO artists
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
""")

TIME_INSERT = ("""
INSERT INTO time
SELECT DISTINCT
    (timestamp 'epoch' + ts/1000 * interval '1 s') AS start_time,
    EXTRACT(hour
        FROM (timestamp 'epoch' + ts/1000 * interval '1 s')
    ) AS hour,
    EXTRACT(day
        FROM (timestamp 'epoch' + ts/1000 * interval '1 s')
    ) AS day,
    EXTRACT(week
        FROM (timestamp 'epoch' + ts/1000 * interval '1 s')
    ) AS week,
    EXTRACT(month
        FROM (timestamp 'epoch' + ts/1000 * interval '1 s')
    ) AS month,
    EXTRACT(year
        FROM (timestamp 'epoch' + ts/1000 * interval '1 s')
    ) AS year,
    EXTRACT(weekday
        FROM (timestamp 'epoch' + ts/1000 * interval '1 s')
    ) AS weekday
FROM staging_events
""")

# Query lists
CREATE_QUERIES = [
    STAGING_EVENTS_CREATE, STAGING_SONGS_CREATE,
    SONGPLAYS_CREATE, USERS_CREATE, SONGS_CREATE, ARTISTS_CREATE, TIME_CREATE]

COPY_QUERIES = [STAGING_EVENTS_COPY, STAGING_SONGS_COPY]

INSERT_QUERIES = [ARTISTS_INSERT, SONGS_INSERT, USERS_INSERT, TIME_INSERT,
                  SONGPLAYS_INSERT]


# Psycopg2 wrapper function
def connect(connection_string, autocommit=False):
    """
    Connect to DB.
    Wrapper around try/except block for connection to DB and retrieving a
    cursor to execute queries.
    Args:
        - connection_string: string with all information necessary to connect
            to Redshift database.
        - autocommit: boolean to toggle session autocommit. Defaults to True.
    Returns:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    """
    try:
        # Open connection
        connection = psycopg2.connect(connection_string)
        connection.set_session(autocommit=True)
        # Get cursor
        cursor = connection.cursor()
    except psycopg2.Error as e:
        print("ERROR: Could not OPEN connection or GET cursor DB")
        print(e)
        cursor, connection = None, None
    return cursor, connection


def disconnect(cursor, connection):
    """
    Disconnect from DB.
    Wrapper around try/except block for disconnecting from the DB.
    Args:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    """
    try:
        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        print("ERROR: Issue disconnecting from DB")
        print(e)


def execute(query, cursor, data=None):
    """
    Execute function.
    Wrapper around try/except block for executing SQL queries with an open
    connection to a DB and a cursor.
    Args:
        - query: string with query to be executed.
        - cursor: cursor object to connected DB.
    Returns:
        - _: boolean with success/fail.
    """
    try:
        if data is None:
            cursor.execute(query)
        else:
            cursor.execute(query, data)
    except (psycopg2.Error, UnicodeEncodeError) as e:
        print(f"ERROR: Issue executing query:\n{query}\n")
        print(e)
        return False

    return True


def fetch(cursor):
    """
    Fetch results.
    Function which fetches resutls and returns them.
    Args:
        - cursor: cursor object to connected DB.
    Returns:
        - result: result for fetched query, or None if error.
    """
    try:
        result = cursor.fetchall()
    except psycopg2.Error as e:
        print("ERROR: Issue fetching results.")
        print(e)
        result = None
    return result


def drop(table, cursor):
    """
    Drop table.
    Wrapper around try/except block for dropping specified table with an open
    connection to a DB and a cursor.
    Args:
        - table: string with table to drop.
        - cursor: cursor object to connected DB.
    """
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    except psycopg2.Error as e:
        print(f"ERROR: Issue dropping table {table}.")
        print(e)
