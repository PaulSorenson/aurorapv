#!/usr/bin/env python3

import asyncio
import json
import logging
import pprint
import socket
from argparse import ArgumentParser, Namespace
from collections import OrderedDict
from datetime import datetime
from functools import partial
from getpass import getpass, getuser
from logging.config import dictConfig
from logging.handlers import SysLogHandler
from typing import Any, Dict, Optional, Sequence

import asyncio_mqtt as mq
import asyncpg as apg
import keyring
from asyncpg.exceptions import DatabaseDroppedError

from . import mockpv as mpv
import pyaurora as pv
from aioconveyor import AioGenConveyor, Event
from . import aurora_pg_load as apl

logging.basicConfig(level="INFO")
log = logging.getLogger(__file__)

r"""
************
:mod:`aurora`
************

.. moduleauthor:: paul sorenson

Monitoring software for Aurora (now ABB) inverter via 'affable'
RS-485 to WiFi interface.

The kit came with a CD with windows software: Power 1 Aurora which
seems to work pretty well.  Since I don't want to run windows 24/7
to monitor the PV it is not a monitoring option.

From curtronics.com we have open source aurora software written in C.
It expects a serial device and can be made to work indirectly using socat:

``socat PTY,link=$HOME/dev/solar,raw TCP4:192.168.1.140:8899``

A sample aurora command looks like so:

``aurora -a2 -A -Y8 -w25 -M5 -d0 -D -j -U 50 -s -t -n  ~/dev/solar``

I found I had to use the -U option to specify a delay otherwise the reads
were unreliable.

I inspected the auroramon-1.8.8 code as well as running it in verbose mode
to inspect the data.

.. note::
    The Wifi adapter will happily allow you to connect even if the 
    inverter is powered off (ie the sun not shining).  So you need to 
    make a decision on how to deal with socket timeouts.

    The model I used initially which may not be optimal but works is to 
    use ``circusd`` to respawn the process and included a backoff timer
    when a socket timeout error occurs (the symptoms seen when the inverter
    is offline).


PostgreSQL Notes
================

```
# psql -U postgres -h localhost

=> --- CREATE USER pms with PASSWORD 'secret';
=> CREATE DATABASE pms;
=> GRANT ALL PRIVILEGES ON DATABASE pms to pms;
=> \q
```

Here is a cheat to write out the SQL create statement from a CSV
file we prepared earlier.

```
# csvsql -i postgresql --tables aurora.samples aurora_2015-10-22.csv > aurora-create.sql

# psql -U pms -h localhost -d pms
--- make sure you are in the right database
--- => \c pms
=> CREATE SCHEMA aurora;
=> \dn
=> \i aurora-create.sql

--- Load a single file to test
=> \copy aurora.samples from 'aurora_2015-10-22.csv' csv header
```

Try inserting all files from the command line using `csvkit`:
```
csvstack aurora_2015-*.csv | csvsql --no-create --db postgresql://pms:secret@localhost/pms --table samples --db-schema aurora --insert
```
Note that this is much slower than using ```csvstack``` followed by ```postgresql \copy```.

"""


dictConfig(pv.logconfig)
log = logging.getLogger("aurora")


operations = (
    #'getTime',
    #'getFirmwareRel',
    "gridPowerAll",
    "powerPeakToday",
    "dailyEnergy",
    "weeklyEnergy",
    #'last7Energy',
    "partialEnergy",
    "getEnergy10",
    "frequencyAll",
    "gridVoltageAll",
    "gridVoltageAverage",
    "gridCurrentAll",
    "bulkVoltageDcDc",
    "in1Voltage",
    "in1Current",
    "in2Voltage",
    "in2Current",
    "pin1All",
    "pin2All",
    "iLeakDcDc",
    "iLeakInverter",
    "boosterTemp",
)
"""The inverter operations to be polled in each cycle."""


def get_args() -> Namespace:
    ap = apl.get_common_args()
    ap.add_argument("--data-dir", default="~pms/src/pyaurora/data/")
    ap.add_argument("--pattern", default="Victorian Energy Compare Data*.csv")
    ap.add_argument("--dump-csv", help="Specify CSV file to dump massaged dataframe.")
    ap.add_argument(
        "--download-data",
        action="store_true",
        help="Retrieve Victorian engergy data"
        " from United Energy site. Note for this to work you have to have be logged in.",
    )
    # @todo: we have host and pv-host args where host maybe should be db host.
    ap.add_argument(
        "--pv-host",
        default="aurora.home.metrak.com",
        help="WiFi (Aurora) adapter address (%(default)s).",
    )
    ap.add_argument(
        "--pv-port",
        type=int,
        default=8899,
        help="Inverter WiFi adapter port (%(default)s).",
    )
    ap.add_argument(
        "--inv-addr", type=int, default=2, help="Inverter address (%(default)s)"
    )
    ap.add_argument(
        "--read-delay",
        type=float,
        default=0.05,
        help="Time between command and read (%(default)f).",
    )
    ap.add_argument(
        "--connect-timeout",
        type=float,
        default=60.0,
        help="Specify a timeout in seconds for connecting. "
        "This is the timeout to adjust fore when the inverter is offline. "
        "Don't set it too short otherwise supervisord might give up before the "
        "sun comes up the next day - it should have a retry count where "
        "retry-count * connect-timeout is more than 12 hours or so.",
    )
    ap.add_argument(
        "--loop-interval",
        type=int,
        default=10,
        help="""Time between inverter polls.  If set to zero, the inverter
is polled a single time and the process exits (%(default)s).
Note that changing this may affect the relevance of some commands eg
"energy in last 10 seconds".""",
    )
    ap.add_argument(
        "--default-timeout",
        type=float,
        default=60,
        help="""Sleep time after a socket timeout error (%(default)s).  The inverter
    goes offline each night and requests result in socket timeout and subsequent process
    exit.  This timeout is intended to reduce the frequency of spawning processes during
    the night.""",
    )
    ap.add_argument(
        "--csv",
        help="""Optionally write CSV to file.  The name
may contain `strftime` format strings.  If the string is "stdout" the CSV
output will be directed to `sys.stdout`.""",
    )
    ap.add_argument(
        "--pickle",
        help="Provide a filename to write some records out to a pickle file. "
        "It will be opened in binary mode. After collecting a predefined number "
        "of records it will close the file.",
    )
    ap.add_argument(
        "--pv-mock",
        action="store_true",
        help="fake data, useful for testing when the sun don't shine.",
    )
    gr_wr = ap.add_argument_group("writers", "must have at least one writer/consumer.")
    gr_wr.add_argument("--db-write", action="store_true", help="write records to db.")
    gr_wr.add_argument("--pp-write", action="store_true", help="pretty print records.")
    gr_wr.add_argument("--mq-host", help="turns on mqtt writer.")
    gr_wr.add_argument(
        "--mq-topic", default="pv/aurora", help="MQTT topic, also needs --mq-host."
    )
    gr_wr.add_argument(
        "--mq-downsample",
        default=1,
        type=int,
        help="Downsample MQTT pub to 1 per downsample reads. One (default) means no downsampling",
    )
    ap.add_argument(
        "--syslog-host",
        help="Set this string to a syslog host to turn on syslog. "
        "Set it to the empty string to disable any setting in the config file.",
    )
    ap.add_argument(
        "--syslog-port",
        default="514/udp",
        help="Set this string to a syslog host to turn on syslog (%(default)s). "
        "Can also specify protocol, eg 514/tcp",
    )
    opt = ap.parse_args()
    print(opt)

    if opt.db_write and not opt.password:
        try:
            pwd = keyring.get_password(opt.user, "postgres")
        except:
            pwd = None
        if not pwd:
            pwd = getpass("enter password: ")
        opt.password = pwd

    log.debug(opt)
    return opt


def get_syslog_handler_args(
    host: Optional[str] = None, port: str = "514"
) -> Optional[Dict[str, Any]]:
    if not host:
        return None
    if host.startswith("/dev"):
        # unix socket
        log.info(f"syslog address: {host}")
        return {"address": host}
    SOCKET_DICT = {"udp": socket.SOCK_DGRAM, "tcp": socket.SOCK_STREAM}
    pp = port.split("/")
    port = pp[0]
    protocol = pp[-1] if len(pp) > 1 else "udp"
    socktype = SOCKET_DICT.get(protocol, socket.SOCK_DGRAM)
    log.info(f"syslog address: ({host}, {port}/{protocol})")
    return {"address": (host, int(port)), "socktype": socktype}


def pv_bulk_cmd(pv_cmd, operations):
    """
    Poll the inverter with a list of operations and push data onto queue.

    :param pv_cmd: function that can issue commands and return
        response. In general it accepts a command and subcommand (which
        may be None).
    :param operations: sequence of strings.  The current implementation
        looks up the relevant command, subcommand, decoder and formatter
        to poll that value.
    :param queue: asyncio queue, push data here.
    """
    utc = datetime.utcnow()
    log.debug(f"pv_build_cmd: polling at (utc) {utc}")
    pv_data = OrderedDict()
    pv_data[apl.TIME_FIELD] = utc.replace(microsecond=0, tzinfo=apl.tz_utc)
    for ssc in operations:
        cmd, sc, (decoder, _) = pv.command.allops[ssc]
        resp = pv_cmd(cmd, sc)
        pv_data[ssc] = round(decoder(resp), 2)
    log.debug("pv_bulk_cmd: returning.")
    return pv_data


class PvReader:
    """Producer for aioconveyor"""

    def __init__(
        self,
        host: str,
        port: int,
        connect_timeout: float,
        read_delay: float,
        inv_addr: int,
        operations: Sequence[str] = operations,
        default_timeout: Optional[float] = 60.0,
    ) -> None:
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.read_delay = read_delay
        self.inv_addr = inv_addr
        self.operations = operations
        self.default_timeout = default_timeout

    def connect(self) -> socket.socket:
        try:
            self.socket = socket.create_connection(
                (self.host, self.port), self.connect_timeout
            )
            if self.default_timeout:
                self.socket.settimeout(self.default_timeout)
        except ConnectionRefusedError:
            log.exception(f"PvReader.connect(): connection refused: {self}")
            raise

        self.pv_cmd = partial(
            pv.execcmd, self.socket, self.inv_addr, readdelay=self.read_delay
        )
        return self.socket

    async def read(self, event: Event) -> Dict:
        try:
            log.info(f"PvReader.read(): event {event}")
            data = pv_bulk_cmd(self.pv_cmd, self.operations)
        except socket.error:
            log.error(
                f"PvReader.read(): socket timeout {self}. This occurs regularly when "
                "the sun goes down, use a supervisor retry-count * --default-timeout to get "
                "you through the night."
            )
            raise
        except Exception:
            log.exception(f"PvReader.read(): error reading from inverter {self}.")
            raise

        return data

    def __str__(self):
        return f"{self.host}:{self.port} inv_add: {self.inv_addr}"


async def mock_pv_bulk_cmd(event: Event) -> Dict:
    return mpv.pv_bulk_cmd(pv_cmd=None, operations=None)


class PostgresConsumer:
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        field_names=mpv.PV_HEAD,
        aurora_table=apl.TABLE_RAW,
    ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.field_names = field_names
        self.aurora_table = aurora_table
        self.conn = None

        self.ins_sql = apl.compose_insert(
            field_names=field_names, table_name=aurora_table
        )
        log.debug(f"PostgresConsumer: insert sql: '{self.ins_sql}'")

    async def connect(self):
        self.conn = await apl.get_connection(
            host=self.host, password=self.password, user=self.user, database=self.database
        )
        await apl.create_tables(self.conn, table_name=self.aurora_table)

    async def write(self, event: Event, payload: Dict):
        if self.conn is None:
            try:
                await self.connect()
            except Exception:
                log.exception(f"PostgresWriter: failed to connect")
                raise

        try:
            await apl.pg_insert_raw(conn=self.conn, sql=self.ins_sql, pv_dict=payload)
            log.debug("PostgresConsumer: data written")
            return 0
        except Exception:
            log.exception(
                f"PostgresWriter: failed to write: data: '{payload}'\nsql: {self.ins_sql}"
            )
            raise
        return 0


async def pretty_printer(event: Event, payload: Dict):
    pprint.pprint(payload)
    return 0


class MqttConsumer:
    def __init__(self, host: str, topic: str, downsample: int = 0) -> None:
        self.host = host
        self.topic = topic
        self.downsample = downsample
        self.downcount = downsample

    async def start_client(self) -> mq.Client:
        self.client = mq.Client(self.host)
        await self.client.connect()
        return self.client

    async def write(self, event: Event, payload: Dict):
        # this will bail if user sets downsample to zero ;)
        mod = event.loop_counter % self.downsample
        if mod:
            return mod
        payload_copy = dict(payload)
        payload_copy[apl.TIME_FIELD] = payload_copy[apl.TIME_FIELD].isoformat()
        msg = json.dumps(payload_copy)
        await self.client.publish(self.topic, msg, qos=1)
        return 0


async def main():
    opt = get_args()
    log.setLevel(opt.log_level)
    # log.debug(opt)
    log_handler_args = get_syslog_handler_args(opt.syslog_host, opt.syslog_port)
    try:
        if log_handler_args:
            log_handler = SysLogHandler(**log_handler_args)
            log_fmt = logging.Formatter(
                f"%(asctime)-15s %(name)s %(levelname)s %(message)s"
            )
            log_handler.setFormatter(log_fmt)
            log.addHandler(log_handler)
            log.info(f"added syslog handler: '{log_handler_args}'")
    except ConnectionRefusedError:
        log.error(f"syslog couldn't open '{log_handler_args}'. Logs will go to console.")
    log.info("aurora starting")

    if opt.pv_mock:
        producer = mock_pv_bulk_cmd
    else:
        pv_reader = PvReader(
            host=opt.pv_host,
            port=opt.pv_port,
            connect_timeout=opt.connect_timeout,
            read_delay=opt.read_delay,
            inv_addr=opt.inv_addr,
            default_timeout=opt.default_timeout,
        )
        pv_reader.connect()
        producer = pv_reader.read

    consumers = []  # writers
    if opt.db_write:
        pg_writer = PostgresConsumer(
            host=opt.host,
            database=opt.db,
            user=opt.user,
            password=opt.password,
            aurora_table=opt.aurora_table,
        )
        consumers.append(pg_writer.write)
    if opt.pp_write:
        consumers.append(pretty_printer)
    if opt.mq_host:
        mq_writer = MqttConsumer(
            host=opt.mq_host, topic=opt.mq_topic, downsample=opt.mq_downsample
        )
        await mq_writer.start_client()
        consumers.append(mq_writer.write)

    conv = AioGenConveyor(
        produce=producer,
        consumers=consumers,
        loop_interval=opt.loop_interval,
    )
    conv.start()
    log.info("main thread started")
    while conv.running:
        log.debug("main: loop")
        await asyncio.sleep(2)
    log.info("main: aioconveyor thread exited, application will terminate")
    log.info("main: aurora exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.error("module: aurora exiting")
