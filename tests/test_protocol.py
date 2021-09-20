#!/usr/bin/env python3

import pytest
from pyaurora import protocol


def test_tolong():
    assert protocol.tolong(bytearray(4)) == 0


def test_tofloat():
    assert protocol.tofloat(bytearray(4)) == 0.0


def test_pad01():
    assert protocol.pad(bytearray(8), 8) == bytearray(8)


def test_pad02():
    assert protocol.pad(bytearray(4), 8) == bytearray(8)


def test_pad03():
    with pytest.raises(Exception):
        protocol.pad(bytearray(4), 9)


def test_makecmd01():
    assert protocol.makecmd(2, 4) == b"\x02\x04\x00\x00\x00\x00\x00\x00j\xe7"


def test_makecmd01():
    assert protocol.makecmd(2, 4, 7) == b"\x02\x04\x07\x00\x00\x00\x00\x00\xbb\xfb"
