#!/usr/bin/env python3
# This file is part of VoltDB.
# Copyright (C) 2026 Volt Active Data Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

"""
Offline serialization round-trip tests for the legacy Python client's wire
codecs (src/voltdbclient.py).

These lock the current serialization *behavior* -- what a value encodes to and
decodes back into -- and complement the API-shape guard in test_api_snapshot.py.

They run with no cluster and no socket: a FastSerializer constructed with no
host never opens a connection, so we write into its write buffer and feed those
bytes straight back into its read buffer (see _loopback). That makes these true
unit tests -- millisecond-fast, deterministic, and cross-platform -- and lets
them replace the older EchoServer-based round-trip test, which
needed a live Java echo process purely to bounce the bytes back.

Scope note: this covers the scalar/array/table wire codecs and the typed
geography path -- GEOGRAPHY / GEOGRAPHY_POINT and the Geography / XYZPoint value
types, including WKT parsing/formatting and the S2 binary format. The geography
types' byte-level parity with the C++ EE is checked by the live end-to-end test,
not here; these offline tests prove the codecs are self-consistent.
"""

import array
import datetime
import decimal
import gc
import math
import socket
import struct
import unittest
import warnings

from voltdbclient import (FastSerializer, Geography, VoltColumn, VoltProcedure,
                          VoltResponse, VoltTable, XYZPoint, int16toBytes,
                          int32toBytes, int64toBytes, isNaN)

decimal.getcontext().prec = 19  # VoltDB DECIMAL is 38 digits / scale 12; matches the client


def _loopback(fs):
    """Feed everything written to fs.wbuf back into fs.read_buffer, with no
    socket -- the offline stand-in for flush() + bufferForRead(), minus the
    4-byte length framing those add for the network."""
    fs.read_buffer.clear()
    fs.read_buffer.append(fs.wbuf.tobytes())
    fs.wbuf = array.array('B')


class _RoundTripCase(unittest.TestCase):
    """Shared helpers for asserting a value survives a write/read cycle."""

    def assertValueEqual(self, expected, actual):
        """Equality that copes with the two values that don't compare naively:
        NaN (never equal to itself) and varbinary (array.array vs. what the
        reader returns)."""
        if isinstance(expected, float) and isNaN(expected):
            self.assertTrue(isNaN(actual), "expected NaN, got %r" % (actual,))
        elif isinstance(expected, array.array):
            self.assertEqual(bytes(actual), bytes(expected))
        else:
            self.assertEqual(actual, expected)

    def roundtrip_scalar(self, volttype, value):
        fs = FastSerializer()
        fs.writeWireType(volttype, value)
        _loopback(fs)
        self.assertEqual(fs.readByte(), volttype, "type byte did not round-trip")
        return fs.read(volttype)

    def roundtrip_array(self, volttype, values):
        fs = FastSerializer()
        fs.writeWireTypeArray(volttype, values)
        _loopback(fs)
        self.assertEqual(fs.readByte(), volttype, "array type byte did not round-trip")
        return list(fs.readArray(volttype))


# Representative values per wire type, including NULL and boundary cases.
# Timestamps/dates are fixed (not datetime.now()) to keep the suite deterministic.
FS = FastSerializer
SCALAR_VECTORS = [
    # Note: a type's minimum value is VoltDB's NULL sentinel (see
    # TestIntegerNullSentinel), so the storable minimum here is min + 1.
    (FS.VOLTTYPE_TINYINT,  [None, 0, 1, -21, 127, -127]),
    (FS.VOLTTYPE_SMALLINT, [None, 0, 128, -256, 32767, -32767]),
    (FS.VOLTTYPE_INTEGER,  [None, 0, -32768, 2147483647, -2147483647]),
    (FS.VOLTTYPE_BIGINT,   [None, -52423, 2147483647, -9223372036854775807]),
    (FS.VOLTTYPE_FLOAT,    [None, 0.0, float("-inf"), float("inf"), float("nan"),
                            -0.009999999776482582, 3.141592653589793]),
    (FS.VOLTTYPE_STRING,   [None, u"", u"hello world", u"ça", u"\U0001f600 emoji"]),
    (FS.VOLTTYPE_VARBINARY, [None, array.array('B', []), array.array('B', [0, 128, 255])]),
    (FS.VOLTTYPE_BOOLEAN,  [None, True, False]),
    (FS.VOLTTYPE_TIMESTAMP, [None,
                             datetime.datetime(1970, 1, 1, 0, 0, 0),
                             datetime.datetime(2023, 6, 15, 12, 30, 45, 123456),
                             datetime.datetime(1999, 12, 31, 23, 59, 59, 999999)]),
    (FS.VOLTTYPE_DATE,     [None,
                            datetime.date(1970, 1, 1),
                            datetime.date(2023, 6, 15),
                            datetime.date(9999, 12, 31),
                            datetime.date(1583, 1, 1)]),
    (FS.VOLTTYPE_DECIMAL,  [None,
                            decimal.Decimal("0"),
                            decimal.Decimal("-837461"),
                            decimal.Decimal("8571391.193847158139"),
                            decimal.Decimal("-1348392.109386749180")]),
]


class TestScalarRoundTrip(_RoundTripCase):
    """Each wire type encodes and decodes back to the original value."""

    def test_scalars(self):
        for volttype, values in SCALAR_VECTORS:
            for value in values:
                with self.subTest(volttype=volttype, value=value):
                    self.assertValueEqual(value, self.roundtrip_scalar(volttype, value))


class TestArrayRoundTrip(_RoundTripCase):
    """Homogeneous arrays of each element type survive a write/read cycle."""

    def test_arrays(self):
        # Arrays of the fixed-shape scalar types (varbinary/boolean arrays are
        # not part of the array wire format the client emits).
        array_types = [FS.VOLTTYPE_TINYINT, FS.VOLTTYPE_SMALLINT, FS.VOLTTYPE_INTEGER,
                       FS.VOLTTYPE_BIGINT, FS.VOLTTYPE_STRING, FS.VOLTTYPE_TIMESTAMP,
                       FS.VOLTTYPE_DATE, FS.VOLTTYPE_DECIMAL]
        vectors = dict(SCALAR_VECTORS)
        for volttype in array_types:
            values = vectors[volttype]
            with self.subTest(volttype=volttype):
                result = self.roundtrip_array(volttype, values)
                self.assertEqual(len(result), len(values))
                for expected, actual in zip(values, result):
                    self.assertValueEqual(expected, actual)


class TestVoltTableRoundTrip(_RoundTripCase):
    """A full table -- every (non-geo) column type, multiple rows, and NULLs --
    round-trips, asserted by content rather than by VoltTable/VoltColumn __eq__
    (both of which are intentionally lenient; see test_voltcolumn_equality)."""

    COLUMNS = [
        (FS.VOLTTYPE_TINYINT, "c_tinyint"),
        (FS.VOLTTYPE_SMALLINT, "c_smallint"),
        (FS.VOLTTYPE_INTEGER, "c_integer"),
        (FS.VOLTTYPE_BIGINT, "c_bigint"),
        (FS.VOLTTYPE_FLOAT, "c_float"),
        (FS.VOLTTYPE_STRING, "c_string"),
        (FS.VOLTTYPE_VARBINARY, "c_varbinary"),
        (FS.VOLTTYPE_TIMESTAMP, "c_timestamp"),
        (FS.VOLTTYPE_DATE, "c_date"),
        (FS.VOLTTYPE_DECIMAL, "c_decimal"),
        (FS.VOLTTYPE_BOOLEAN, "c_boolean"),
    ]

    ROWS = [
        [1, 128, 100000, 9000000000, 3.14, u"row one", array.array('B', [1, 2, 3]),
         datetime.datetime(2023, 6, 15, 12, 30, 45, 123456), datetime.date(2023, 6, 15),
         decimal.Decimal("123.456"), True],
        # An all-NULL row exercises every type's null path.
        [None, None, None, None, None, None, None, None, None, None, None],
        [-21, -256, -32768, -9223372036854775807, float("nan"), u"ça",
         array.array('B', [255]), datetime.datetime(1970, 1, 1), datetime.date(1583, 1, 1),
         decimal.Decimal("-1348392.109386749180"), False],
    ]

    def test_table(self):
        fs = FastSerializer()
        table = VoltTable(fs)
        for coltype, colname in self.COLUMNS:
            table.columns.append(VoltColumn(type=coltype, name=colname))
        for row in self.ROWS:
            table.tuples.append(list(row))
        table.writeToSerializer()
        _loopback(fs)

        result = VoltTable(fs)
        result.readFromSerializer()

        # Columns: assert type and name field-by-field (not via the lenient __eq__).
        self.assertEqual(len(result.columns), len(self.COLUMNS))
        for got, (coltype, colname) in zip(result.columns, self.COLUMNS):
            self.assertEqual(got.type, coltype)
            self.assertEqual(got.name, colname)

        # Tuples: assert value-by-value (NaN and varbinary handled by the helper).
        self.assertEqual(len(result.tuples), len(self.ROWS))
        for got_row, expected_row in zip(result.tuples, self.ROWS):
            self.assertEqual(len(got_row), len(expected_row))
            for expected, actual in zip(expected_row, got_row):
                self.assertValueEqual(expected, actual)


class TestIntegerNullSentinel(_RoundTripCase):
    """VoltDB represents an integer NULL as the minimum value of the type, so
    writing that minimum reads back as None -- it cannot be stored as data.
    Pinned here to document why the type minimums are absent from the scalar
    value vectors above."""

    def test_min_value_is_null(self):
        sentinels = [
            (FS.VOLTTYPE_TINYINT, -128),
            (FS.VOLTTYPE_SMALLINT, -32768),
            (FS.VOLTTYPE_INTEGER, -2147483648),
            (FS.VOLTTYPE_BIGINT, -9223372036854775808),
        ]
        for volttype, minimum in sentinels:
            with self.subTest(volttype=volttype):
                self.assertIsNone(self.roundtrip_scalar(volttype, minimum))


class TestVoltColumnEquality(unittest.TestCase):
    """Characterize VoltColumn's intentionally lenient equality so a future
    change to it is a conscious decision. Per voltdbclient.py:1784, a column
    with an empty/None name compares equal to any other column regardless of
    type (HSQLDB returns no column name for an empty result)."""

    def test_voltcolumn_equality_is_lenient_on_empty_name(self):
        a = VoltColumn(type=FS.VOLTTYPE_INTEGER, name="x")
        b = VoltColumn(type=FS.VOLTTYPE_STRING, name="y")
        self.assertNotEqual(a, b)   # both named, differing type -> not equal

        b.name = ""                 # empty name triggers the lenient short-circuit
        self.assertEqual(a, b)      # equal despite differing type


# ---------------------------------------------------------------------------
# Geography: the GEOGRAPHY / GEOGRAPHY_POINT codecs and the Geography / XYZPoint
# value types -- WKT parsing/formatting and the S2 binary format.
# ---------------------------------------------------------------------------

# Input WKT (integer coords) and the canonical form the client emits for it
# (formatPoint renders at least one fractional digit). Holes are listed
# clockwise on input per the OGC convention and come back out clockwise.
SHELL_WKT = "POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))"
SHELL_CANONICAL = "POLYGON ((0.0 0.0, 30.0 0.0, 30.0 30.0, 0.0 30.0, 0.0 0.0))"

HOLED_WKT = ("POLYGON ((0 0, 40 0, 40 40, 0 40, 0 0), "
             "(10 10, 10 20, 20 20, 20 10, 10 10))")
HOLED_CANONICAL = ("POLYGON ((0.0 0.0, 40.0 0.0, 40.0 40.0, 0.0 40.0, 0.0 0.0), "
                   "(10.0 10.0, 10.0 20.0, 20.0 20.0, 20.0 10.0, 10.0 10.0))")


class TestGeographyPoint(unittest.TestCase):
    """GEOGRAPHY_POINT wire codec and WKT helpers. Points are (longitude,
    latitude) tuples throughout."""

    POINTS = [(0.0, 0.0), (-73.5, 40.7), (179.999, -89.999), (-180.0, 45.0)]

    def test_binary_round_trip(self):
        for pt in self.POINTS:
            with self.subTest(point=pt):
                fs = FastSerializer()
                fs.writeGeographyPoint(pt)
                _loopback(fs)
                got = fs.readGeographyPoint()
                self.assertAlmostEqual(got[0], pt[0], places=9)
                self.assertAlmostEqual(got[1], pt[1], places=9)

    def test_null_binary_round_trip(self):
        fs = FastSerializer()
        fs.writeGeographyPoint(None)
        _loopback(fs)
        self.assertIsNone(fs.readGeographyPoint())

    def test_write_rejects_non_tuple(self):
        fs = FastSerializer()
        with self.assertRaises(TypeError):
            fs.writeGeographyPoint("not a point")
        with self.assertRaises(TypeError):
            fs.writeGeographyPoint((1.0, 2.0, 3.0))

    def test_wkt_round_trip(self):
        for pt in self.POINTS:
            with self.subTest(point=pt):
                wkt = Geography.pointToWKT(pt)
                self.assertTrue(wkt.startswith("POINT ("))
                back = Geography.pointFromWKT(wkt)
                self.assertAlmostEqual(back[0], pt[0], places=9)
                self.assertAlmostEqual(back[1], pt[1], places=9)

    def test_point_from_wkt_non_match_is_none(self):
        self.assertIsNone(Geography.pointFromWKT("not wkt at all"))

    def test_point_from_wkt_none_raises(self):
        with self.assertRaises(ValueError):
            Geography.pointFromWKT(None)

    def test_format_point_keeps_negative_coords(self):
        # Regression: formatPoint once used `x < EPSILON` instead of
        # `abs(x) < EPSILON`, silently zeroing every negative coordinate.
        self.assertEqual(Geography.formatPoint((-73.5, -40.25)), "-73.5 -40.25")
        # Values within EPSILON of zero are snapped to 0.0.
        self.assertEqual(Geography.formatPoint((1e-13, 5.0)), "0.0 5.0")


class TestXYZPoint(unittest.TestCase):
    """XYZPoint converts (lng, lat) to/from the S2 unit-sphere representation and
    the two directions are clean inverses."""

    def test_lng_lat_round_trip(self):
        for lng, lat in [(0.0, 0.0), (-73.5, 40.7), (120.0, -33.3)]:
            with self.subTest(lng=lng, lat=lat):
                back = XYZPoint.fromGeographyPoint((lng, lat)).toGeogrpahyPoint()
                self.assertAlmostEqual(back[0], lng, places=9)
                self.assertAlmostEqual(back[1], lat, places=9)

    def test_on_unit_sphere(self):
        xyz = XYZPoint.fromGeographyPoint((-73.5, 40.7))
        self.assertAlmostEqual(math.sqrt(xyz.x**2 + xyz.y**2 + xyz.z**2), 1.0, places=12)


class TestGeographyWKT(unittest.TestCase):
    """geographyFromWKT parses polygons; __str__ emits WKT; they are inverses."""

    def test_shell_only(self):
        g = Geography.geographyFromWKT(SHELL_WKT)
        self.assertEqual(str(g), SHELL_CANONICAL)

    def test_polygon_with_hole(self):
        g = Geography.geographyFromWKT(HOLED_WKT)
        self.assertEqual(str(g), HOLED_CANONICAL)

    def test_parse_format_idempotent(self):
        # Parsing the canonical output must reproduce it exactly: the canonical
        # WKT is a fixed point of geographyFromWKT -> __str__.
        for wkt in [SHELL_CANONICAL, HOLED_CANONICAL]:
            with self.subTest(wkt=wkt):
                self.assertEqual(str(Geography.geographyFromWKT(wkt)), wkt)

    def test_whitespace_and_case_insensitive(self):
        compact = "polygon((0 0,30 0,30 30,0 30,0 0))"
        self.assertEqual(str(Geography.geographyFromWKT(compact)), SHELL_CANONICAL)

    def test_negative_and_decimal_coords(self):
        wkt = "POLYGON ((-10.5 -20.25, 10.5 -20.25, 0.0 15.75, -10.5 -20.25))"
        # Round-trips to a fixed point (exact string equality after normalization).
        once = str(Geography.geographyFromWKT(wkt))
        twice = str(Geography.geographyFromWKT(once))
        self.assertEqual(once, twice)

    def test_malformed_raises(self):
        bad_inputs = [
            "",                                  # empty
            "garbage",                           # not WKT
            "POINT (1 2)",                        # wrong geometry type
            "POLYGON (0 0, 1 0, 1 1, 0 0)",       # missing inner parenthesis
            "POLYGON ((0 0, 1 1, 0 0))",          # ring with < 4 points
            "POLYGON ((0 0, 1 0, 1 1, 0 1))",     # ring not closed
            "POLYGON ((0 0, 1 0, 1 1, 0 0)",      # missing closing parenthesis
        ]
        for wkt in bad_inputs:
            with self.subTest(wkt=wkt):
                with self.assertRaises(ValueError):
                    Geography.geographyFromWKT(wkt)

    def test_none_raises(self):
        with self.assertRaises(ValueError):
            Geography.geographyFromWKT(None)


class TestGeographyBinary(unittest.TestCase):
    """GEOGRAPHY (polygon) S2 binary codec: flatten/unflatten round-trips."""

    def _binary_round_trip(self, geo):
        fs = FastSerializer()
        fs.writeGeography(geo)
        _loopback(fs)
        return fs.readGeography()

    def test_shell_round_trip(self):
        g = Geography.geographyFromWKT(SHELL_WKT)
        # The binary path writes and reads the same float64s, so the result is
        # byte-for-byte identical -- assert on the exact WKT rendering.
        self.assertEqual(str(self._binary_round_trip(g)), SHELL_CANONICAL)

    def test_holed_round_trip(self):
        g = Geography.geographyFromWKT(HOLED_WKT)
        self.assertEqual(str(self._binary_round_trip(g)), HOLED_CANONICAL)

    def test_null_round_trip(self):
        self.assertIsNone(self._binary_round_trip(None))

    def test_serialized_size_matches_bytes_written(self):
        # GeographyValue.getLengthInBytes() (Java) is the byte count *excluding*
        # the 4-byte length prefix that flatten() prepends.
        g = Geography.geographyFromWKT(HOLED_WKT)
        fs = FastSerializer()
        fs.writeGeography(g)
        self.assertEqual(len(fs.wbuf), 4 + g.serializedSize())

    def test_triangle_is_155_bytes(self):
        # Pins parity with the Java client's documented minimum polygon size:
        # 155 bytes = the number of bytes needed to store a triangle.
        triangle = Geography.geographyFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 0))")
        self.assertEqual(triangle.serializedSize(), 155)


class TestParameterArrayDetection(unittest.TestCase):
    """VoltProcedure.as_array decides whether a call parameter is serialized as a
    scalar or as an array. GEOGRAPHY_POINT is the one scalar type whose value is
    itself a sequence (an (lng, lat) tuple), so it needs a special case; these
    tests pin that and confirm the other types are unaffected."""

    def setUp(self):
        self.proc = VoltProcedure(None, "unused")  # as_array needs no connection

    def as_array(self, paramtype, value):
        return self.proc.as_array(paramtype, value)

    def test_geography_point_scalar_is_not_array(self):
        # (lng, lat) is a single point, not an array of two numbers.
        self.assertFalse(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT, (10.0, 20.0)))
        self.assertFalse(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT, [10.0, 20.0]))
        self.assertFalse(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT, (-73, 40)))  # ints ok

    def test_geography_point_array_is_array(self):
        self.assertTrue(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT, [(1.0, 2.0), (3.0, 4.0)]))
        self.assertTrue(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT, [(1.0, 2.0)]))  # 1-element array
        self.assertTrue(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT,
                                      [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]))

    def test_geography_point_null_is_scalar(self):
        # A NULL point (None) is scalar; writeGeographyPoint handles the null.
        self.assertFalse(self.as_array(FS.VOLTTYPE_GEOGRAPHY_POINT, None))

    def test_geography_polygon_object_is_scalar(self):
        # A Geography polygon object is not iterable -> scalar (unchanged).
        g = Geography.geographyFromWKT(SHELL_WKT)
        self.assertFalse(self.as_array(FS.VOLTTYPE_GEOGRAPHY, g))

    def test_plain_scalars_unaffected(self):
        self.assertFalse(self.as_array(FS.VOLTTYPE_INTEGER, 42))
        self.assertFalse(self.as_array(FS.VOLTTYPE_FLOAT, 3.14))
        self.assertFalse(self.as_array(FS.VOLTTYPE_BIGINT, None))

    def test_strings_and_varbinary_unaffected(self):
        self.assertFalse(self.as_array(FS.VOLTTYPE_STRING, "hello"))         # string is scalar
        self.assertFalse(self.as_array(FS.VOLTTYPE_VARBINARY, b"\x01\x02"))  # bytes as varbinary scalar
        self.assertTrue(self.as_array(FS.VOLTTYPE_TINYINT, b"\x01\x02"))     # bytes as non-varbinary -> array

    def test_ordinary_lists_are_arrays(self):
        self.assertTrue(self.as_array(FS.VOLTTYPE_INTEGER, [1, 2, 3]))
        self.assertTrue(self.as_array(FS.VOLTTYPE_STRING, ["a", "b"]))


class _CannedSocket:
    """Serves a fixed byte string to recv() and swallows sendall(), so
    FastSerializer.authenticate() can be driven with no real network."""

    def __init__(self, response):
        self._data = bytes(response)
        self._pos = 0

    def sendall(self, _data):
        pass  # the login request the client writes is irrelevant here

    def recv(self, nbytes):
        chunk = self._data[self._pos:self._pos + nbytes]
        self._pos += len(chunk)
        return chunk

    def close(self):
        pass


def _login_response(host_id, connection_id, timestamp, coord, build_string):
    """A length-prefixed successful login response, matching what the server
    writes in ClientInterface (version, status, host id, connection id, cluster
    instance id = timestamp + coord, then a length-prefixed build string)."""
    build = build_string.encode("utf-8")
    body = struct.pack(">bb", 0, 0)                 # version, status (0 = success)
    body += struct.pack(">i", host_id)
    body += struct.pack(">q", connection_id)
    body += struct.pack(">q", timestamp)
    body += struct.pack(">i", coord)
    body += struct.pack(">i", len(build)) + build
    return struct.pack(">i", len(body)) + body


class TestLoginResponseCapture(unittest.TestCase):
    """FastSerializer.authenticate() records the login response's fields instead
    of discarding them. The host id in particular is the node's stable identity
    within the cluster, which the topology-aware client keys its connections on."""

    def _authenticate_against(self, response):
        fs = FastSerializer()  # host=None: no real socket or connection
        fs.socket = _CannedSocket(response)
        fs.authenticate("", "")
        return fs

    def test_fields_captured(self):
        fs = self._authenticate_against(_login_response(
            host_id=3, connection_id=1234567890123,
            timestamp=987654321, coord=167772161, build_string="volt-16.0.0"))
        self.assertEqual(fs.hostId, 3)
        self.assertEqual(fs.connectionId, 1234567890123)
        self.assertEqual(fs.clusterInstanceId, (987654321, 167772161))
        self.assertEqual(fs.buildString, "volt-16.0.0")

    def test_empty_build_string(self):
        fs = self._authenticate_against(_login_response(0, 0, 0, 0, ""))
        self.assertEqual(fs.hostId, 0)
        self.assertEqual(fs.buildString, "")

    def test_defaults_none_without_login(self):
        # A socket-less serializer (used only for offline serialization) never
        # authenticates, so the fields are present but None -- never absent.
        fs = FastSerializer()
        self.assertIsNone(fs.hostId)
        self.assertIsNone(fs.connectionId)
        self.assertIsNone(fs.clusterInstanceId)
        self.assertIsNone(fs.buildString)


class TestConnectFailureCleanup(unittest.TestCase):
    """A FastSerializer whose connect fails must not leave its socket open.
    Nothing can reach the socket once __init__ raises, so an unclosed one is
    an fd held until garbage collection -- CPython reports that as a
    ResourceWarning, which is what this test listens for."""

    def test_failed_connect_closes_socket(self):
        # A port nothing is listening on: bind an ephemeral one, then free it.
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", ResourceWarning)
            with self.assertRaises(OSError):
                FastSerializer("127.0.0.1", port)
            gc.collect()
        leaked = [str(w.message) for w in caught
                  if issubclass(w.category, ResourceWarning)]
        self.assertEqual(leaked, [])


class _ChoppySocket:
    """recv() returns at most ``step`` bytes, to exercise message reassembly."""

    def __init__(self, data, step):
        self._data = bytes(data)
        self._pos = 0
        self._step = step

    def recv(self, nbytes):
        chunk = self._data[self._pos:self._pos + min(nbytes, self._step)]
        self._pos += len(chunk)
        return chunk


def _response_body(status_string=None, app_status_string=None):
    """A response body (no length prefix) with one two-row, three-column table."""
    fs = FastSerializer()
    present = (0x20 if status_string is not None else 0) | \
              (0x80 if app_status_string is not None else 0)
    fs.writeByte(1)                       # version
    fs.writeInt64(424242)                 # client handle
    fs.writeByte(present - 256 if present > 127 else present)
    fs.writeByte(1)                       # status
    if status_string is not None:
        fs.writeString(status_string)
    fs.writeByte(-128)                    # app status: NULL
    if app_status_string is not None:
        fs.writeString(app_status_string)
    fs.writeInt32(17)                     # round-trip time
    fs.writeInt16(1)                      # table count
    table = VoltTable(fs)
    table.columns = [VoltColumn(type=FS.VOLTTYPE_STRING, name="S"),
                     VoltColumn(type=FS.VOLTTYPE_BIGINT, name="B"),
                     VoltColumn(type=FS.VOLTTYPE_FLOAT, name="F")]
    table.tuples = [[u"x\u00e9", 7, 1.5], [None, None, None]]
    table.writeToSerializer()
    return fs.wbuf.tobytes()


def _response_fields(r):
    return (r.version, r.clientHandle, r.status, r.statusString, r.appStatus,
            r.appStatusString, r.roundtripTime,
            [([(c.type, c.name) for c in t.columns], t.tuples) for t in r.tables])


class TestCodecFastPaths(unittest.TestCase):
    """The precompiled-struct readers and writers keep the original bytes and
    values, including wraparound and NULL handling."""

    def test_integer_writers_match_byte_lists(self):
        cases = [(FastSerializer.writeInt16, int16toBytes),
                 (FastSerializer.writeInt32, int32toBytes),
                 (FastSerializer.writeInt64, int64toBytes)]
        values = [0, 1, -1, 127, -128, 32767, -32768, 2 ** 31 - 1, -2 ** 31,
                  2 ** 63 - 1, -2 ** 63, 2 ** 70 + 5, -2 ** 70 - 5, True, False]
        for write, to_bytes in cases:
            for v in values:
                fs = FastSerializer()
                write(fs, v)
                self.assertEqual(fs.wbuf.tobytes(), bytes(to_bytes(v)), (write.__name__, v))

    def test_integer_writers_reject_non_integers(self):
        for write in (FastSerializer.writeInt16, FastSerializer.writeInt32,
                      FastSerializer.writeInt64):
            with self.assertRaises(TypeError):
                write(FastSerializer(), 1.5)

    def test_read_string_null_and_empty(self):
        fs = FastSerializer()
        fs.writeString(None)
        fs.writeString(u"")
        fs.writeString(u"abc")
        _loopback(fs)
        self.assertIsNone(fs.readString())
        self.assertEqual(fs.readString(), u"")
        self.assertEqual(fs.readString(), u"abc")

    def test_little_endian_input_reads(self):
        fs = FastSerializer()
        fs.setInputByteOrder(1)
        fs.read_buffer.set(struct.pack("<hiqd", -2, 3, -4, 2.5))
        self.assertEqual([fs.readInt16(), fs.readInt32(), fs.readInt64(), fs.readFloat64()],
                         [-2, 3, -4, 2.5])

    def test_prepend_length(self):
        fs = FastSerializer()
        fs.writeRawBytes(b"abcde")
        fs.prependLength()
        self.assertEqual(fs.wbuf.tobytes(), b"\x00\x00\x00\x05abcde")

    def test_response_from_body_matches_socket_read(self):
        for strings in [(None, None), (u"ok", None), (None, u"app"), (u"s", u"a")]:
            body = _response_body(*strings)
            frame = struct.pack(">i", len(body)) + body
            fs = FastSerializer()
            fs.socket = _ChoppySocket(frame, 3)
            from_socket = VoltResponse(fs)
            from_body = VoltResponse._from_body(FastSerializer(), body)
            self.assertEqual(_response_fields(from_socket), _response_fields(from_body))
            self.assertEqual(from_body.statusString, strings[0])
            self.assertEqual(from_body.appStatusString, strings[1])
            self.assertIsNone(from_body.appStatus)
            self.assertEqual(from_body.tables[0].tuples,
                             [[u"x\u00e9", 7, 1.5], [None, None, None]])

    def test_buffer_for_read_detects_eof_mid_message(self):
        body = _response_body()
        fs = FastSerializer()
        fs.socket = _ChoppySocket(struct.pack(">i", len(body)) + body[:-3], 5)
        with self.assertRaises(IOError):
            fs.bufferForRead()


if __name__ == "__main__":
    unittest.main()
