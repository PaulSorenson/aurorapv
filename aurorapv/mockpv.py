#!/usr/bin/env python3

import asyncio
import logging
import sys
from collections import OrderedDict
from datetime import datetime
from io import StringIO
from .aurora_pg_load import tz_utc, tz_local, TIME_FIELD

# extract from raw aurora CSV file
csv_data = f"""{TIME_FIELD},gridPowerAll,powerPeakToday,dailyEnergy,weeklyEnergy,partialEnergy,getEnergy10,frequencyAll,gridVoltageAll,gridVoltageAverage,gridCurrentAll,bulkVoltageDcDc,in1Voltage,in1Current,in2Voltage,in2Current,pin1All,pin2All,iLeakDcDc,iLeakInverter,boosterTemp
1970-01-01T00:00:00+00:00,0.0,0.0,0,11501,36758499,0,49.98800277709961,233.16793823242188,22.541196823120117,0.0,64.66751861572266,71.85869598388672,0.0,70.7794418334961,0.0,0.0,0.0,0.0,0.0,8.999601364135742"""

log = logging.getLogger()

csv = StringIO(csv_data)


def get_lines():
    for row in csv:
        yield row.strip()


lines = get_lines()
# names coming from PV reader
PV_HEAD = next(lines).split(",")
DATA = next(lines).split(",")


def pv_bulk_cmd(pv_cmd, operations):
    now = datetime.now()
    log.info(f"mock_pv_time: {now}")
    val = [now]
    val.extend([round(float(v), 2) for v in DATA[1:]])
    d = OrderedDict(zip(PV_HEAD, val))
    return d
