import sqlite3
import time
from datetime import date, timedelta
from os import path


class HealthData():
    """
    This provides access to all health data. It relies on two databases:
    health.sqlite, and daily.sqlite. First one is exported from the Pebble app,
    the second one needs to be generated with PebbleHealthLib.construct_db.
    """

    def __init__(self):
        raise NotImplementedError


def construct_db(file_in="health.sqlite", file_out="daily_health.sqlite",
                 overwrite=False):
    """
    Given an sqlite database exported from the Pebble app this constructs a
    custom database that stores totals and averages for each day so that these
    don't need to be computed every time.
    """

    # Check if healt.sqlite has been provided
    if not path.exists(file_in):
        raise Exception("No {} file in the working directory. This needs to be obtained as an export from the Pebble app".format(file_in))

    # Create a database connection
    with sqlite3.connect(file_in) as in_conn, sqlite3.connect(file_out) as out_conn:
        in_conn.row_factory = sqlite3.Row
        in_c = in_conn.cursor()
        out_c = out_conn.cursor()

        # Check if the output databse exists already
        out_c.execute("""SELECT
                             name
                         FROM sqlite_master
                         WHERE
                             type='table'
                             AND name='days_summary'""")
        if out_c.fetchone() is not None and not overwrite:
            raise Exception("Database {} has already been configured. Pass overwrite=True as an argument to replace it.".format(file_out))
        else:
            out_c.execute("""DROP TABLE IF EXISTS 'days_summary'""")
            out_c.execute("""CREATE TABLE 'days_summary' (
                                 'id'  INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                                 'date_unix' INTEGER UNIQUE,
                                 'date_text' TEXT UNIQUE,
                                 'step_count' REAL,
                                 'distance_m' REAL,
                                 'active_minutes' INTEGER,
                                 'sleep' INTEGER,
                                 'deep_sleep' INTEGER,
                                 'nap' INTEGER,
                                 'deep_nap' INTEGER,
                                 'active_gcal' REAL,
                                 'resting_gcal' REAL,
                                 'avg_movement_vmc' REAL,
                                 'avg_light' REAL
                             )""")
            out_conn.commit()

        # Establish the first and last days stored in the db
        # Minute samples seem to be stored only for the last year...
        in_c.execute("""SELECT
                         MIN(date_local_secs) AS min_date,
                         MAX(date_local_secs) AS max_date
                     FROM minute_samples""")
        row1 = in_c.fetchone()
        # ...but activity sessions are stored forever.
        # Combining both allows establishing the whole range of dates stored.
        in_c.execute("""SELECT
                         MIN(start_local_secs) AS min_date,
                         MAX(end_local_secs) AS max_date
                     FROM activity_sessions""")
        row2 = in_c.fetchone()
        date_min = row1["min_date"] if row1["min_date"] < row2["min_date"] else row2["min_date"]
        date_max = row1["max_date"] if row1["max_date"] > row2["min_date"] else row2["max_date"]
        date_min = _unix_to_date(date_min)
        date_max = _unix_to_date(date_max)

        # Go through all dates and collect aggregate data
        date_i = date_min
        data = []
        while date_i <= date_max:
            date_i_next = date_i + timedelta(days=1)

            # First compile the minute data into some sums and averages
            in_c.execute("""SELECT
                              SUM(step_count) AS step_count,
                              SUM(distance_mm) AS distance_mm,
                              SUM(active_minutes) AS active_minutes,
                              SUM(active_gcal) AS active_gcal,
                              SUM(resting_gcal) AS resting_gcal,
                              AVG(vmc) AS avg_movement_vmc,
                              AVG(light) AS avg_light
                          FROM minute_samples
                          WHERE
                              date_local_secs > ?
                              AND date_local_secs <= ?
            """, (_datetime_to_unix(date_i), _datetime_to_unix(date_i_next)))
            row_minutes = in_c.fetchone()

            distance_m = None if row_minutes["distance_mm"] is None else row_minutes["distance_mm"]*1e-3
            data.append((_datetime_to_unix(date_i),
                         date_i.strftime("%d-%m-%Y"),
                         row_minutes["step_count"],
                         distance_m,
                         row_minutes["active_minutes"],
                         row_minutes["active_gcal"],
                         row_minutes["resting_gcal"],
                         row_minutes["avg_movement_vmc"],
                         row_minutes["avg_light"]))

            date_i = date_i_next
        # Commit all the details to the out db
        out_c.executemany("""INSERT INTO days_summary(
                                 'date_unix',
                                 'date_text',
                                 'step_count',
                                 'distance_m',
                                 'active_minutes',
                                 'active_gcal',
                                 'resting_gcal',
                                 'avg_movement_vmc',
                                 'avg_light'
                             ) VALUES ("""
                          + ",".join(["?"]*len(data[0])) + ")", data)
        out_conn.commit()


def _unix_to_date(unix):
    return date.fromtimestamp(unix)


def _datetime_to_unix(dt):
    return int(time.mktime(dt.timetuple()))
