#!/usr/bin/env python3

import asyncio
import asyncpg as apg
import glob
import gzip
import keyring
import logging
import os
import shutil
from argparse import ArgumentParser, Namespace
from asyncpg.exceptions import UniqueViolationError
from datetime import datetime, timezone
from dateutil import tz
from getpass import getpass, getuser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, TextIO, Tuple, Union

log = logging.getLogger("aurora_pg_load")
logging.basicConfig(level="INFO")

tz_utc = timezone.utc
tz_local = tz.gettz("Australia/Melbourne")


r"""
Load pyaurora CSV files into timescaledb table. This is a bulk load, once the table
is bootstrapped, data should be streamed here in real time.

Note:
- postgres setup - default to localhost
- user setup, convenient to setup unix user name with create permissions.
- ```
create role pms;
alter role pms with LOGIN;
alter role pms with CREATEDB;
alter role pms with CREATEROLE;
```

```
test_db=> \dg
                                   List of roles
 Role name |                         Attributes                         | Member of
-----------+------------------------------------------------------------+-----------
 pms       | Create role, Create DB                                     | {}
 postgres  | Superuser, Create role, Create DB, Replication, Bypass RLS | {}
```
- pyaurora postgres db setup:
    'createdb pyaurora`

- timescaledb installed.
- configure postgresql to use timescale: ```
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```
"""

DB = "timeseries"
TABLE_RAW = "aurora_samples"
TIME_FIELD = "aurora_time"


def get_create_sql(table_name: str = TABLE_RAW, time_field: str = TIME_FIELD) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {time_field} TIMESTAMPTZ NOT NULL,
            gridPowerAll DOUBLE PRECISION NOT NULL,
            powerPeakToday DOUBLE PRECISION NOT NULL,
            dailyEnergy DOUBLE PRECISION NOT NULL,
            weeklyEnergy DOUBLE PRECISION NOT NULL,
            partialEnergy DOUBLE PRECISION NOT NULL,
            getEnergy10 DOUBLE PRECISION NOT NULL,
            frequencyAll DOUBLE PRECISION NOT NULL,
            gridVoltageAll DOUBLE PRECISION NOT NULL,
            gridVoltageAverage DOUBLE PRECISION NOT NULL,
            gridCurrentAll DOUBLE PRECISION NOT NULL,
            bulkVoltageDcDc DOUBLE PRECISION NOT NULL,
            in1Voltage DOUBLE PRECISION NOT NULL,
            in1Current DOUBLE PRECISION NOT NULL,
            in2Voltage DOUBLE PRECISION NOT NULL,
            in2Current DOUBLE PRECISION NOT NULL,
            pin1All DOUBLE PRECISION NOT NULL,
            pin2All DOUBLE PRECISION NOT NULL,
            iLeakDcDc DOUBLE PRECISION NOT NULL,
            iLeakInverter DOUBLE PRECISION NOT NULL,
            boosterTemp DOUBLE PRECISION NOT NULL,
            PRIMARY KEY({time_field})
        );
        """


async def get_connection(
    host: str,
    password: str,
    user: Optional[str] = None,
    database: str = DB,
):
    if password is None:
        raise Exception("database password must not be None")
    if host is None:
        raise Exception("database host must not be None")
    user = getuser() if user is None else user
    log.info(f"user: {user} db: {database} host: {host}")
    return await apg.connect(user=user, database=database, host=host, password=password)


async def create_tables(
    conn: apg.connection.Connection,
    table_name: str = TABLE_RAW,
    time_field: str = TIME_FIELD,
):
    await conn.execute(get_create_sql(table_name=table_name, time_field=time_field))
    await conn.execute(
        f"SELECT create_hypertable('{table_name}', '{time_field}', if_not_exists => TRUE);"
    )


def compose_insert_dict(d: Dict, table_name: str = TABLE_RAW) -> str:
    fields = list(d.keys())
    values = [f"${i + 1}" for i in range(len(d))]
    insert = f"""
    INSERT INTO {table_name}(
        {", ".join(fields)}
    )
    VALUES(
        {", ".join(values)}
    )
    """
    return insert


def compose_insert(field_names: Sequence, table_name: str) -> str:
    fields = ", ".join(field_names)
    placeholders = ", ".join([f"${i+1}" for i in range(len(field_names))])
    sql = f"INSERT INTO {table_name} ({fields}) values ({placeholders})"
    return sql


def to_datetime(s: str) -> datetime:
    # going into timestamptz field with the strings as UTC
    # postgres will correctly store these as UTC, pandas might
    # need tz_convert but postgres should use local timezone
    # as defined in postgresql.conf
    return datetime.fromisoformat(s).replace(tzinfo=tz_utc)


formatters = {
    TIME_FIELD: to_datetime,
}


async def pg_insert_raw(conn: apg.connection.Connection, sql: str, pv_dict: Dict) -> None:
    """
    insert record into postgres.
    """
    log.info(f'insert "{sql}"')
    pg_data = tuple(pv_dict.values())
    log.info(pg_data)
    await conn.execute(sql, *pg_data)


async def pg_insert(conn: apg.connection.Connection, sql: str, pv_dict: Dict) -> None:
    """
    insert record into postgres.
    """
    print("insert", sql)
    pg_data = tuple(formatters.get(c, float)(v) for c, v in pv_dict.items())
    print(tuple(pg_data))
    await conn.execute(sql, *pg_data)


def copy_data(
    columns: List[str], csv_in: TextIO
) -> Generator[Tuple[Union[datetime, float], ...], None, None]:
    for line in csv_in:
        values = line.strip().split(",")
        yield tuple(formatters.get(c, float)(v) for c, v in zip(columns, values))


def get_common_args() -> ArgumentParser:
    # use keyring command prior eg: keyring set lan luke-postgres
    user = getuser()
    # pwd = keyring.get_password("pms", "postgres")
    # if not pwd:
    #     pwd = getpass("enter password: ")

    ap = ArgumentParser()
    ap.add_argument("--db", default=DB, help="Name of database.")
    ap.add_argument("--host", help="Postgres remote host.")
    ap.add_argument("--user", default=user, help="Override unix username.")
    ap.add_argument(
        "--password",
        # default=pwd,
        help="Supply database password on command line. This is not recommended.",
    )
    ap.add_argument(
        "--aurora-table",
        default=TABLE_RAW,
        help="Overrided table name, normally only for test purposes.",
    )
    ap.add_argument("--log-level", default="INFO", help="Override log level")
    return ap


def get_args() -> Namespace:
    ap = get_common_args()
    ap.add_argument("--data-dir", default="~pms/src/pyaurora/output")
    ap.add_argument("--pattern", default="aurora_*.gz")
    ap.add_argument(
        "--zip",
        default=False,
        action="store_true",
        help="If files don't end in gz, use this to gzip them after. "
        "This can be handy for processing only new files.",
    )
    opt = ap.parse_args()
    log.debug(opt)
    return opt


async def main():
    opt = get_args()
    numeric_level = getattr(logging, opt.log_level.upper(), None)
    if isinstance(numeric_level, int):
        log.setLevel(numeric_level)
    data_dir_path = Path(opt.data_dir).expanduser()
    if not data_dir_path.exists():
        log.error(f"non-existent data directory: '{data_dir_path}'")
        return
    conn = await get_connection(
        user=opt.user, database=opt.db, host=opt.host, password=opt.password
    )
    log.debug(repr(conn))
    await create_tables(conn, table_name=opt.aurora_table)

    for csv_path in sorted(glob.iglob(str(data_dir_path / opt.pattern))):
        log.debug(csv_path)
        if csv_path.endswith("gz"):
            _open = gzip.open
        else:
            _open = open
        with _open(csv_path, "rt") as csv_in:
            try:
                columns = next(csv_in).strip().split(",")
                time_ix = columns.index("utc")
                columns[time_ix] = TIME_FIELD
            except StopIteration:
                log.error(f"failed to read headers: '{csv_path}'. File may be empty.")
                continue
            log.debug(columns)
            try:
                await conn.copy_records_to_table(
                    table_name=opt.aurora_table, records=copy_data(columns, csv_in)
                )
            except UniqueViolationError as uve:
                log.error(f"duplicate records would result from '{csv_path}'\n{uve}")
            if opt.zip and _open == open:
                with open(csv_path, "rb") as csv_in:
                    gzip_name = f"{csv_path}.gz"
                    log.debug(f"archiving: {gzip_name}")
                    with gzip.open(gzip_name, "wb") as gz:
                        shutil.copyfileobj(csv_in, gz)
                os.unlink(csv_path)


if __name__ == "__main__":
    asyncio.run(main())
