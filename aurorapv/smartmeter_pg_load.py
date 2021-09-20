#!/usr/bin/env python3

import asyncio
import asyncpg as apg
import getpass
import glob
import gzip
import logging
import pandas as pd
import requests
from .aurora_data import get_vicenergy
from .aurora_pg_load import get_common_args, get_connection
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from dateutil import tz
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO, Tuple

log = logging.getLogger("pg_load")
logging.basicConfig(level="INFO")

tz_utc = timezone.utc
tz_local_name = "Australia/Melbourne"
tz_local = tz.gettz(tz_local_name)

"""
Load vicenergy data into postgres. These are 30 minute samples of generated and consumed
energy.

To retrieve this data:
- login to https://energyeasy.ue.com.au/
- access https://energyeasy.ue.com.au/electricityView/downloadVecReport for CSV file.

The file has 2 years of data and historically I have saved them as:
`Victorian Energy Compare Data-yyyy-mm-dd.csv` with the most recent
`Victorian Energy Compare Data.csv`

Summarising the logic in `process_raw_data.ipynb`:
- iterate over the glob to match above
- convert to long form (melt) with get_vicenergy()
- concatenate the dataframes
- drop duplicates (the files overlap)

After doing this we have a dataframe:
localtime (index) ein eout

Notes
- the notebook uses the left end of the interval, consider using
the right side.
- use timestamptz field and replace the Null timezone from CSV data with
Australia/Melbourne.

The Aurora data is resampled to 30 minute blocks and joined with this.

For backward compatibility could write pv30.csv and pvdaily.csv with legacy time alignments
so that the notebook is unchanged.
"""

UE_URL = "https://energyeasy.ue.com.au/"
# requires login
VIC_DATA = "https://energyeasy.ue.com.au/electricityView/downloadVecReport"

SMARTMETER_RAW = "smartmeter_samples"
TIME_FIELD = "sm_time"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SMARTMETER_RAW} (
    {TIME_FIELD} TIMESTAMPTZ NOT NULL,
    from_grid DOUBLE PRECISION,
    to_grid DOUBLE PRECISION,
    PRIMARY KEY({TIME_FIELD})
);
"""


async def create_tables(conn: apg.connection.Connection):
    await conn.execute(CREATE_SQL)
    await conn.execute(
        f"SELECT create_hypertable('{SMARTMETER_RAW}', '{TIME_FIELD}', if_not_exists => TRUE);"
    )


def get_args() -> Namespace:
    ap = get_common_args()
    ap.add_argument("--data-dir", default="./data/")
    ap.add_argument("--pattern", default="Victorian Energy Compare Data*.csv")
    ap.add_argument("--dump-csv", help="Specify CSV file to dump massaged dataframe.")
    ap.add_argument(
        "--download-data",
        action="store_true",
        help="Retrieve Victorian engergy data"
        " from United Energy site. Note for this to work you have to have be logged in.",
    )
    opt = ap.parse_args()
    log.debug(opt)
    return opt


async def get_smartmeter_data():
    r = requests.get(VIC_DATA, timeout=1.0)
    print(r.text[:400])
    with open("vic_smartmeter_data.html", "w") as vic_data:
        vic_data.write(r.text)


async def get_last_db_date(conn: apg.connection.Connection) -> datetime:
    last_db_date = await conn.fetchval(
        query=f"SELECT MAX({TIME_FIELD}) FROM {SMARTMETER_RAW};"
    )
    return last_db_date


async def get_smartmeter_csv(data_dir_path: Path, csv_pattern: str) -> pd.DataFrame:
    paths = [p for p in sorted(glob.iglob(str(data_dir_path / csv_pattern)))]
    log.info("\n".join(paths))
    # lots of overlapping data here but let pandas do heavy lifting
    # @todo: optimize given last date in db
    sm = pd.concat(get_vicenergy(p) for p in paths)
    sm = (
        sm[["energy", "direction"]]
        .reset_index()
        .drop_duplicates()
        .set_index(["localtime", "direction"])
        .sort_index()
        .unstack("direction")
    )
    sm.columns = ["from_grid", "to_grid"]
    sm.reset_index(inplace=True)
    return sm


async def main():
    opt = get_args()
    numeric_level = getattr(logging, opt.log_level.upper(), None)
    if isinstance(numeric_level, int):
        log.setLevel(numeric_level)
    data_dir_path = Path(opt.data_dir).expanduser()
    if not data_dir_path.exists():
        log.error(f"non-existent data directory: '{data_dir_path}'")
        return
    # find last time index in table
    conn = await get_connection(
        user=opt.user, database=opt.db, host=opt.host, password=opt.password
    )
    await create_tables(conn)
    last_db_date = await get_last_db_date(conn)
    log.info(f"last timestamp in database: '{last_db_date}'")

    if opt.download_data:
        # @todo: figure out login details
        # smartmeter_data = get_smartmeter_data()
        _ = get_smartmeter_data()

    sm = await get_smartmeter_csv(data_dir_path, opt.pattern)

    # print(sm.head())
    # uenergy smartmeter data ignores dst so I will assume it is +10 offset
    # until I can find out authoritatively.
    # 10 hours to shift back to UTC and 30 minutes to align with right
    # side of interval.
    sm.localtime += pd.Timedelta(value=-10, unit="H") + pd.Timedelta(value=30, unit="T")
    sm.rename(columns={"localtime": TIME_FIELD}, inplace=True)
    sm[TIME_FIELD] = pd.to_datetime(sm[TIME_FIELD], utc=True)
    sm.set_index(TIME_FIELD, inplace=True)

    sm_new = sm.loc[sm.index > last_db_date]
    if len(sm_new):
        log.info(
            f"first row in dataframe: {sm_new.index[0]}, last row in db: {last_db_date}"
        )
        log.info(f"new records from Vic data: {len(sm_new)}")
        if opt.dump_csv:
            log.info(f"Dumping input data to: {opt.dump_csv}")
            sm_new.to_csv(opt.dump_csv)
        log.info(f"Writing {len(sm_new)} records to db.")
        await conn.copy_records_to_table(
            table_name=SMARTMETER_RAW, records=sm_new.itertuples(index=True)
        )
    else:
        log.info("The database is already up to date.")


if __name__ == "__main__":
    asyncio.run(main())
