import sqlite3
from datetime import date
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
    with sqlite3.connect('health.sqlite') as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        # Establish the first and last days stored in the db
        c.execute("""SELECT
                         MIN(date_local_secs) AS min_date,
                         MAX(date_local_secs) AS max_date
                     FROM minute_samples""")
        row1 = c.fetchone()
        c.execute("""SELECT
                         MIN(start_local_secs) AS min_date,
                         MAX(end_local_secs) AS max_date
                     FROM activity_sessions""")
        row2 = c.fetchone()
        date_min = row1["min_date"] if row1["min_date"] < row2["min_date"] else row2["min_date"]
        date_max = row1["max_date"] if row1["max_date"] > row2["min_date"] else row2["max_date"]
        date_min = _unix_to_date(date_min)
        date_max = _unix_to_date(date_max)

        print(date_min, date_max)


def _unix_to_date(unix):
    return date.fromtimestamp(unix)
