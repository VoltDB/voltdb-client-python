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
"""Offline tests for VoltBulkLoader. No cluster: FakeSocket connections are
answered by a scripted server thread, and every batch the loader sends is
decoded with voltdbclient's own VoltTable reader."""

import datetime
import decimal
import struct
import threading
import time
import unittest

from test_voltclient import (build_table_response, make_routed_client,
                             make_table, routing_snapshot)
from test_voltclient import wait_for as voltclient_wait
from voltdbclient import FastSerializer as FS, VoltTable
import voltclient
from voltclient import (MP_INIT_PID, PartitionInfo, VoltBulkLoader,
                        VoltClientError, VoltTypeError)

ADDR0, ADDR1 = ("volt0", 21212), ("volt1", 21212)
HOST_IDS = {ADDR0: 0, ADDR1: 1}
LEADER_ADDR = {0: ADDR0, 1: ADDR1}
PARTITIONS = {0: PartitionInfo(0, 0, (0, 1)),
              1: PartitionInfo(1, 1, (0, 1)),
              MP_INIT_PID: PartitionInfo(MP_INIT_PID, 1, (1,))}

# (table, column, type name, partition column?)
SCHEMA = [
    ("T", "ID", "BIGINT", True), ("T", "NAME", "VARCHAR", False), ("T", "SCORE", "FLOAT", False),
    ("R", "ID", "BIGINT", False), ("R", "NAME", "VARCHAR", False),
    ("ALLTYPES", "K", "VARCHAR", True), ("ALLTYPES", "A", "TINYINT", False),
    ("ALLTYPES", "B", "SMALLINT", False), ("ALLTYPES", "C", "INTEGER", False),
    ("ALLTYPES", "D", "BIGINT", False), ("ALLTYPES", "E", "FLOAT", False),
    ("ALLTYPES", "F", "TIMESTAMP", False), ("ALLTYPES", "G", "DECIMAL", False),
    ("ALLTYPES", "H", "VARBINARY", False),
    ("GEO", "ID", "BIGINT", True), ("GEO", "SHAPE", "GEOGRAPHY", False),
]


class Index:
    """An int-like value (as numpy integers are): has __index__, is not int."""
    def __init__(self, v):
        self.v = v

    def __index__(self):
        return self.v


def read_param(frame, off):
    t = frame[off]
    off += 1
    if t in (FS.VOLTTYPE_STRING, FS.VOLTTYPE_VARBINARY):
        (n,) = struct.unpack_from(">i", frame, off)
        raw = bytes(frame[off + 4:off + 4 + n])
        return (raw.decode() if t == FS.VOLTTYPE_STRING else raw), off + 4 + n
    if t == FS.VOLTTYPE_TINYINT:
        return struct.unpack_from(">b", frame, off)[0], off + 1
    if t == FS.VOLTTYPE_VOLTTABLE:
        (n,) = struct.unpack_from(">i", frame, off)
        fser = FS()
        fser.read_buffer.clear()
        fser.read_buffer.append(bytes(frame[off:off + 4 + n]))
        return VoltTable(fser).readFromSerializer(), off + 4 + n
    raise AssertionError("unexpected parameter type %d" % t)


def parse_invocation(frame):
    """(proc name, handle, [params]) of one version-0 invocation frame."""
    (nlen,) = struct.unpack_from(">i", frame, 1)
    name = bytes(frame[5:5 + nlen]).decode()
    off = 5 + nlen
    (handle,) = struct.unpack_from(">q", frame, off)
    (count,) = struct.unpack_from(">h", frame, off + 8)
    off += 10
    params = []
    for _ in range(count):
        p, off = read_param(frame, off)
        params.append(p)
    return name, handle, params


class FakeServer:
    """Answers what the client writes: @SystemCatalog from SCHEMA (and the
    given primary-key tables), loads with ``on_load(rows) -> status``."""

    def __init__(self, fakes, pk_tables=(), on_load=None):
        self.fakes = fakes
        self.pk_tables = set(pk_tables)
        self.on_load = on_load or (lambda rows: 1)
        self.loads = []  # (addr, proc, params)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=2)

    def load_calls(self):
        with self._lock:
            return list(self.loads)

    def _run(self):
        offsets = {addr: 0 for addr in self.fakes}
        while not self._stop.is_set():
            for addr, fake in self.fakes.items():
                data = bytes(fake.sent)
                off = offsets[addr]
                while off + 4 <= len(data):
                    (n,) = struct.unpack_from(">i", data, off)
                    if off + 4 + n > len(data):
                        break
                    self._answer(addr, fake, *parse_invocation(data[off + 4:off + 4 + n]))
                    off += 4 + n
                offsets[addr] = off
            time.sleep(0.001)

    def _answer(self, addr, fake, name, handle, params):
        if name == "@SystemCatalog" and params[0] == "COLUMNS":
            rows = [[t, c, ty, pos, "PARTITION_COLUMN" if part else None]
                    for pos, (t, c, ty, part) in enumerate(SCHEMA, 1)]
            table = make_table([(FS.VOLTTYPE_STRING, "TABLE_NAME"),
                                (FS.VOLTTYPE_STRING, "COLUMN_NAME"),
                                (FS.VOLTTYPE_STRING, "TYPE_NAME"),
                                (FS.VOLTTYPE_INTEGER, "ORDINAL_POSITION"),
                                (FS.VOLTTYPE_STRING, "REMARKS")], rows)
            fake.feed(build_table_response(handle, [table]))
        elif name == "@SystemCatalog" and params[0] == "PRIMARYKEYS":
            table = make_table([(FS.VOLTTYPE_STRING, "TABLE_NAME"),
                                (FS.VOLTTYPE_STRING, "COLUMN_NAME")],
                               [[t, "ID"] for t in sorted(self.pk_tables)])
            fake.feed(build_table_response(handle, [table]))
        elif name.startswith("@Load"):
            with self._lock:
                self.loads.append((addr, name, params))
            rows = params[-1].tuples
            status = self.on_load(rows)
            if status == 1:
                fake.feed(build_table_response(
                    handle, [make_table([(FS.VOLTTYPE_BIGINT, "")], [[len(rows)]])]))
            else:
                fake.feed(build_table_response(handle, [], status=status))
        else:
            raise AssertionError("unexpected invocation %s" % name)


class LoaderTestBase(unittest.TestCase):

    def setUp(self):
        self.client, self.fakes = make_routed_client(
            routing_snapshot(partitions=PARTITIONS), HOST_IDS)
        self.server = None

    def tearDown(self):
        if self.server is not None:
            self.server.stop()
        self.client.close()

    def serve(self, **kwargs):
        self.server = FakeServer(self.fakes, **kwargs)
        return self.server

    def loader(self, table, **kwargs):
        if self.server is None:
            self.serve()
        return VoltBulkLoader(self.client, table, **kwargs)

    def partition(self, vtype, value):
        return self.client._topology.hashinator.get_partition_for_parameter(vtype, value)


class TestLoaderSetup(LoaderTestBase):

    def test_metadata_from_catalog(self):
        loader = self.loader("t")
        self.assertEqual(loader.table_name, "T")
        self.assertEqual(loader.column_names, ("ID", "NAME", "SCORE"))
        self.assertEqual(loader.column_types,
                         (FS.VOLTTYPE_BIGINT, FS.VOLTTYPE_STRING, FS.VOLTTYPE_FLOAT))
        loader.close()

    def test_metadata_and_counters_are_read_only(self):
        loader = self.loader("t")
        for name in ("table_name", "column_names", "column_types", "completed_row_count",
                     "failed_row_count", "outstanding_row_count"):
            with self.assertRaises(AttributeError, msg=name):
                setattr(loader, name, None)
        loader.close()

    def test_unknown_table(self):
        with self.assertRaises(VoltClientError):
            self.loader("NOPE")

    def test_unsupported_column_type(self):
        with self.assertRaisesRegex(VoltClientError, "SHAPE"):
            self.loader("GEO")

    def test_upsert_requires_primary_key(self):
        with self.assertRaisesRegex(VoltClientError, "primary key"):
            self.loader("T", upsert=True)

    def test_upsert_flag_sent_when_table_has_primary_key(self):
        self.serve(pk_tables={"T"})
        with self.loader("T", upsert=True) as loader:
            loader.insert_row(1, "a", 1.0)
        (_, _, params), = self.server.load_calls()
        self.assertEqual(params[2], 1)


class TestBatching(LoaderTestBase):

    def test_rows_batched_by_partition_and_sent_to_the_leader(self):
        with self.loader("T", batch_size=4) as loader:
            for i in range(40):
                loader.insert_row(i, "n%d" % i, i / 2)
        loads = self.server.load_calls()
        seen = []
        for addr, proc, (pkey, table, upsert, vt) in loads:
            self.assertEqual(proc, "@LoadSinglepartitionTable")
            self.assertEqual(table, "T")
            self.assertEqual(upsert, 0)
            ids = [row[0] for row in vt.tuples]
            self.assertLessEqual(len(ids), 4)
            parts = {self.partition(FS.VOLTTYPE_BIGINT, i) for i in ids}
            self.assertEqual(len(parts), 1, "a batch spans partitions")
            self.assertEqual(addr, LEADER_ADDR[parts.pop()], "batch not sent to its leader")
            self.assertEqual(pkey, struct.pack("<q", ids[0]))  # VoltType.valueToBytes
            seen.extend(ids)
        self.assertEqual(sorted(seen), list(range(40)))
        self.assertEqual((loader.completed_row_count, loader.failed_row_count), (40, 0))

    def test_partial_batches_wait_for_flush(self):
        loader = self.loader("T", batch_size=100)
        for i in range(3):
            loader.insert_row(i, "x", 0.0)
        time.sleep(0.05)
        self.assertEqual(self.server.load_calls(), [])
        self.assertEqual(loader.outstanding_row_count, 3)
        loader.drain()
        self.assertEqual(sum(len(p[-1].tuples) for _, _, p in self.server.load_calls()), 3)
        self.assertEqual(loader.outstanding_row_count, 0)
        loader.close()

    def test_replicated_table_uses_multipartition_load(self):
        with self.loader("R", batch_size=10) as loader:
            for i in range(25):
                loader.insert_row(i, "r")
        loads = self.server.load_calls()
        self.assertTrue(loads)
        for _, proc, params in loads:
            self.assertEqual(proc, "@LoadMultipartitionTable")
            self.assertEqual(params[:2], ["R", 0])
            self.assertEqual(len(params), 3)
        self.assertEqual(sum(len(p[-1].tuples) for _, _, p in loads), 25)

    def test_concurrent_producers_load_every_row_once(self):
        loader = self.loader("T", batch_size=7)

        def produce(base):
            for i in range(base, base + 500):
                loader.insert_row(i, "c", 0.0)
        threads = [threading.Thread(target=produce, args=(k * 500,)) for k in range(4)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        loader.close()
        ids = [row[0] for _, _, p in self.server.load_calls() for row in p[-1].tuples]
        self.assertEqual(sorted(ids), list(range(2000)))
        self.assertEqual(loader.completed_row_count, 2000)


class TestValues(LoaderTestBase):

    def test_every_supported_type_round_trips(self):
        ts = datetime.datetime(2026, 10, 5, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc)
        rows = [
            ("k1", 7, -300, 70000, 2 ** 40, 1.5, ts, decimal.Decimal("-12.345"), b"\x00\xff"),
            ("k2", None, None, None, None, None, None, None, None),
            ("k3", -127, 32767, -(2 ** 31) + 1, -(2 ** 63) + 1, -0.25,
             int(ts.timestamp()) * 1000000, 42, bytearray(b"ab")),
        ]
        with self.loader("ALLTYPES", batch_size=1) as loader:
            for r in rows:
                loader.insert_row(*r)
        got = {row[0]: row for _, _, p in self.server.load_calls() for row in p[-1].tuples}
        k1, k2, k3 = got["k1"], got["k2"], got["k3"]
        self.assertEqual(k1[1:6], [7, -300, 70000, 2 ** 40, 1.5])
        self.assertAlmostEqual(k1[6].timestamp(), ts.timestamp(), delta=1e-6)
        self.assertEqual(k1[7], decimal.Decimal("-12.345"))
        self.assertEqual(bytes(k1[8]), b"\x00\xff")
        self.assertEqual(k2[1:], [None] * 8)
        self.assertEqual(k3[1:6], [-127, 32767, -(2 ** 31) + 1, -(2 ** 63) + 1, -0.25])
        self.assertEqual(k3[7], decimal.Decimal(42))
        self.assertEqual(bytes(k3[8]), b"ab")

    def test_int_like_values_are_accepted(self):
        with self.loader("T") as loader:
            loader.insert_row(Index(5), "x", Index(2))
        (_, _, params), = self.server.load_calls()
        self.assertEqual(params[-1].tuples[0][:1], [5])
        self.assertEqual(params[0], struct.pack("<q", 5))

    def test_decimal_encoding_matches_voltdbclient(self):
        for text in ("0", "1", "-1", "0.000000000001", "-99999999999999999999999999.999999999999",
                     "12345.678", "-0.5", "100"):
            d = decimal.Decimal(text)
            ref = FS()
            ref.writeDecimal(d)
            self.assertEqual(voltclient._decimal_bytes(d), ref.wbuf.tobytes(), text)

    def test_timestamp_conversion(self):
        utc = datetime.datetime(2001, 2, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc)
        self.assertEqual(voltclient._timestamp_micros(utc), 981173106000007)
        local = datetime.datetime(2001, 2, 3, 4, 5, 6, 7)
        self.assertEqual(voltclient._timestamp_micros(local),
                         voltclient._timestamp_micros(local.astimezone()))
        self.assertEqual(voltclient._timestamp_micros(123), 123)


class TestFailures(LoaderTestBase):

    def test_rejected_batch_is_retried_row_by_row(self):
        failed = []
        self.serve(on_load=lambda rows: -2 if any(r[0] == 13 for r in rows) else 1)
        with self.loader("T", batch_size=5, failure_callback=lambda f, o: failed.append((f, o))) as loader:
            for i in range(30):
                loader.insert_row(i, "n", 0.0)
        self.assertEqual([f for f, _ in failed], [(13, "n", 0.0)])
        self.assertEqual(failed[0][1].status, -2)
        self.assertEqual((loader.completed_row_count, loader.failed_row_count), (29, 1))
        single_row_loads = [p for _, _, p in self.server.load_calls() if len(p[-1].tuples) == 1]
        self.assertTrue(single_row_loads, "the rejected batch was not retried row by row")

    def test_client_side_rejects(self):
        failed = []
        bad = [("x", "a", 1.0),          # str for BIGINT
               (True, "a", 1.0),         # bool for BIGINT
               (2 ** 63, "a", 1.0),      # out of BIGINT range
               (-(2 ** 63), "a", 1.0),   # BIGINT NULL sentinel
               (None, "a", 1.0),         # NULL partition key
               (1, "a"),                 # wrong column count
               (1, 5, 1.0),              # int for VARCHAR
               (1, "a", "1.5")]          # str for FLOAT
        with self.loader("T", failure_callback=lambda f, o: failed.append((f, o))) as loader:
            for r in bad:
                loader.insert_row(*r)
        self.assertEqual([f for f, _ in failed], bad)
        self.assertTrue(all(isinstance(o, VoltClientError) for _, o in failed))
        self.assertIn("ID", str(failed[0][1]))
        self.assertEqual(loader.failed_row_count, len(bad))
        self.assertEqual(self.server.load_calls(), [])

    def test_decimal_limits(self):
        failed = []
        with self.loader("ALLTYPES", failure_callback=lambda f, o: failed.append(o)) as loader:
            for d in ("1.0000000000001", "1" * 27, "NaN"):
                loader.insert_row("k", None, None, None, None, None, None,
                                  decimal.Decimal(d), None)
        self.assertEqual(len(failed), 3)
        self.assertTrue(all(isinstance(o, VoltTypeError) for o in failed))

    def test_lost_connections_fail_rows_and_drain_returns(self):
        failed = []
        loader = self.loader("T", batch_size=4, failure_callback=lambda f, o: failed.append(f))
        for fake in self.fakes.values():
            fake.close()
        for i in range(10):
            loader.insert_row(i, "n", 0.0)
        loader.close()
        self.assertEqual(sorted(f[0] for f in failed), list(range(10)))
        self.assertEqual((loader.completed_row_count, loader.failed_row_count), (0, 10))

    def test_closed_loader_rejects_rows(self):
        loader = self.loader("T")
        loader.close()
        with self.assertRaises(VoltClientError):
            loader.insert_row(1, "a", 1.0)

    def test_failures_without_callback_are_logged(self):
        self.serve(on_load=lambda rows: -2 if any(r[0] % 10 == 3 for r in rows) else 1)
        with self.assertLogs("voltclient", level="WARNING") as logs:
            with self.loader("T", batch_size=5) as loader:
                for i in range(30):
                    loader.insert_row(i, "n", 0.0)
        self.assertEqual((loader.completed_row_count, loader.failed_row_count), (27, 3))
        warnings = [r.getMessage() for r in logs.records]
        self.assertEqual(len(warnings), 2, warnings)  # first failure, then the total at close
        self.assertIn("First failure", warnings[0])
        self.assertIn("3 row(s) failed", warnings[1])


class TestConcurrentLifecycle(LoaderTestBase):

    def test_interrupt_leaves_with_block_without_draining(self):
        # Rows still in flight must not make __exit__ wait on a Ctrl-C.
        release = self._held_server()
        start = time.monotonic()
        with self.assertRaises(KeyboardInterrupt):
            with self.loader("T", batch_size=1, failure_callback=lambda f, o: None) as loader:
                loader.insert_row(0, "a", 0.0)
                raise KeyboardInterrupt
        self.assertLess(time.monotonic() - start, 1.0)
        self.assertEqual(loader.outstanding_row_count, 1)
        release.set()
        loader.close()  # stop its retry thread so later tests see no leftovers

    def _held_server(self):
        """A server that holds load responses until the returned event is set."""
        release = threading.Event()
        self.serve(on_load=lambda rows: release.wait(5) and 1)
        return release

    def test_drain_sends_rows_inserted_while_draining(self):
        release = self._held_server()
        loader = self.loader("T", batch_size=100)
        loader.insert_row(0, "a", 0.0)
        drainer = threading.Thread(target=loader.drain, daemon=True)
        drainer.start()
        time.sleep(0.1)                  # drain has sent row 0 and is waiting
        loader.insert_row(1, "b", 0.0)   # a partial batch inserted mid-drain
        release.set()
        drainer.join(timeout=3)
        self.assertFalse(drainer.is_alive(), "drain() never sent the late row")
        self.assertEqual(loader.completed_row_count, 2)
        loader.close()

    def test_close_rejects_rows_once_started(self):
        release = self._held_server()
        loader = self.loader("T", batch_size=100)
        loader.insert_row(0, "a", 0.0)
        closer = threading.Thread(target=loader.close, daemon=True)
        closer.start()
        self.assertTrue(voltclient_wait(lambda: loader._closed))
        with self.assertRaises(VoltClientError):
            loader.insert_row(1, "b", 0.0)
        release.set()
        closer.join(timeout=3)
        self.assertFalse(closer.is_alive())
        self.assertEqual((loader.completed_row_count, loader.failed_row_count,
                          loader.outstanding_row_count), (1, 0, 0))


if __name__ == "__main__":
    unittest.main()
