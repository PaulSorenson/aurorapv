#!/usr/local/bin/python3.6

"""
Reads Victorian Energy Compare csv as emailed via
https://energyeasy.ue.com.au/
Click on reports and select:
    "Victorian Energy Compare Data"

From this source the filename is attached as:
    'Victorian Energy Compare Data.csv'

Note that the files I have seem to only go back 2 years
so you might want to keep old copies around and merge
them.
"""

import pandas as pd
import pytz
import sqlite3 as sqlite

from .config import SQLITE_DB

utc = pytz.utc
mel = pytz.timezone("Australia/Melbourne")


def get_vicenergy(csvname: str) -> pd.DataFrame:
    df = pd.read_csv(csvname, parse_dates=["DATE"], dayfirst=True)
    # the column names from 5 are time of day offsets so convert
    # them to time delta objects
    df.columns = list(df.columns[:5]) + [
        pd.Timedelta(f"{c[:5]}:00") for c in df.columns[5:]
    ]
    # remove partial days
    df = df.loc[df["ESTIMATED?"] == "No"]

    df = pd.melt(
        df,
        id_vars=["DATE", "NMI", "METER SERIAL NUMBER", "CON/GEN", "ESTIMATED?"],
        value_name="energy",
    ).rename(
        columns={
            "StartTime": "localtime",
            "DATE": "date",
            "NMI": "nmi",
            "METER SERIAL NUMBER": "serial",
            "CON/GEN": "direction",
            "ESTIMATED?": "estimated",
        }
    )

    df["localtime"] = df.date + df.variable
    df = df.set_index(["localtime"]).sort_index()

    return df


def get_pyaurora(dbname: str = SQLITE_DB) -> pd.DataFrame:
    with sqlite.connect(dbname) as db:
        au = pd.read_sql("select * from samples", db)
    au.utc = pd.to_datetime(au.utc, format="%Y-%m-%d %H:%M:%S", exact=False)

    au["localtime"] = au.utc.apply(
        lambda x: utc.localize(x).astimezone(mel).replace(tzinfo=None)
    )

    # use local time for index to merge with UE data
    return au.set_index("localtime", drop=False).sort_index()
