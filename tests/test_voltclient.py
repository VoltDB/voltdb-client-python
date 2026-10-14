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
Offline unit tests for the topology-aware VoltDB client
(src/voltclient.py).

No cluster and no real socket: a FakeSocket stands in for the connection, so we
can deliver hand-built response frames -- including out of order, never, or a
mid-stream disconnect -- deterministically. That exercises the real reader
thread, demux map, completion seam, sweeper, and teardown paths.

The wire format of the response frames built here mirrors
VoltResponse.deserialize (version, int64 handle, present-fields byte, status,
appStatus, roundtripTime, table count), all big-endian to match the client.
"""

import array
import asyncio
import concurrent.futures
import json
import random
import struct
import threading
import time
import unittest
import zlib

from voltdbclient import FastSerializer, VoltColumn, VoltProcedure, VoltTable
import voltclient
from voltclient import (VoltClient, VoltClientError, VoltConnectionError,
                        VoltNoConnectionsError, VoltTimeoutError, VoltTypeError,
                        ASYNC_PROC_HANDLE, ASYNC_TOPO_HANDLE, AffinityStats,
                        ElasticHashinator, HostInfo, PartitionCallResult,
                        PartitionInfo, ProcedureInfo, TopologySnapshot,
                        MP_INIT_PID, VoltType)

FS = FastSerializer


def build_response(handle, status=0, roundtrip=0):
    """Build a minimal, length-prefixed client response frame (no tables, no
    optional strings, no exception) carrying the given handle."""
    body = bytearray()
    body += struct.pack(">b", 0)           # version
    body += struct.pack(">q", handle)      # clientHandle
    body += struct.pack(">B", 0)           # present-fields bitmap: none set
    body += struct.pack(">b", status)      # status
    body += struct.pack(">b", 0)           # appStatus
    body += struct.pack(">i", roundtrip)   # roundtripTime
    body += struct.pack(">h", 0)           # table count
    return struct.pack(">i", len(body)) + bytes(body)


class FakeSocket:
    """A blocking, in-memory stand-in for a connected socket.

    recv() blocks until bytes are fed or the socket is closed (then returns b''
    to signal EOF, exactly as a real closed socket does). sendall() records the
    invocation bytes the client writes so tests can inspect them.
    """

    def __init__(self):
        self._buf = bytearray()
        self._cond = threading.Condition()
        self._closed = False
        self.sent = bytearray()

    def feed(self, data):
        with self._cond:
            self._buf.extend(data)
            self._cond.notify_all()

    def recv(self, nbytes):
        with self._cond:
            while not self._buf and not self._closed:
                self._cond.wait()
            if not self._buf:  # closed with nothing left
                return b""
            chunk = bytes(self._buf[:nbytes])
            del self._buf[:len(chunk)]
            return chunk

    def sendall(self, data):
        with self._cond:
            if self._closed:
                raise OSError("socket is closed")
            self.sent.extend(data)

    def settimeout(self, _timeout):
        pass

    def gettimeout(self):
        return None

    def setsockopt(self, *_args):
        pass

    def setblocking(self, *_args):
        pass

    def shutdown(self, *_args):
        pass

    def close(self):
        with self._cond:
            self._closed = True
            self._cond.notify_all()


def wait_for(predicate, timeout=2.0):
    """Spin until predicate() is true or the timeout elapses; return its value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.005)
    return predicate()


def make_client_with_fake(**kwargs):
    """A VoltClient with no real hosts plus one injected fake connection, backed
    by a target as a real connection always is (so it is routable). Reconnect is
    stubbed to fail fast, so if the fake drops the client does not attempt a real
    dial. The maintenance loop is parked (huge interval, overridable) so it does
    not inject @Subscribe traffic into call-mechanics assertions; subscription
    behavior has its own tests. Returns (client, connection, fake_socket)."""
    kwargs.setdefault("reconnect_interval", 3600.0)
    client = VoltClient(**kwargs)

    def _no_reconnect(host, port):
        raise OSError("fake-connection test: reconnect disabled")

    client._open_serializer = _no_reconnect
    fake = FakeSocket()
    fser = FastSerializer()  # no host: no socket, no auth
    fser.socket = fake
    target = client._add_target("fake", 21212)
    conn = client._register_connection(fser, "fake", 21212, target)
    return client, conn, fake


def make_reconnecting_client(**kwargs):
    """A VoltClient (no seed hosts) whose _open_serializer hands out
    FakeSocket-backed serializers, so connect and reconnect work with no real
    network. The subscription step is stubbed out of the reconnect loop: the
    fakes never answer, so a pending @Subscribe would otherwise hold up the
    drain-then-close paths these tests assert on.
    Returns (client, fakes) where fakes records each socket opened."""
    client = VoltClient(**kwargs)
    client._ensure_subscription = lambda: None
    fakes = []

    def fake_open(host, port):
        fake = FakeSocket()
        fakes.append(fake)
        fser = FastSerializer()
        fser.socket = fake
        return fser

    client._open_serializer = fake_open
    return client, fakes


def ping_bytes():
    """The wire bytes of an @Ping invocation on the reserved PING_HANDLE."""
    return voltclient._serialize_invocation(
        FastSerializer.BIG_ENDIAN, "@Ping", (), (), voltclient.PING_HANDLE)


class TestVoltType(unittest.TestCase):
    """VoltType must mirror FastSerializer's VOLTTYPE_* constants exactly, so a
    type added to the codec cannot silently go missing from the new client's
    public surface (or drift to a different value)."""

    def test_mirrors_every_codec_constant(self):
        codec = {name[len("VOLTTYPE_"):]: getattr(FS, name)
                 for name in dir(FS) if name.startswith("VOLTTYPE_")}
        exported = {name: value for name, value in vars(VoltType).items()
                    if name.isupper()}
        self.assertEqual(codec, exported)

    def test_geo_value_classes_are_re_exported(self):
        import voltdbclient
        self.assertIs(voltclient.Geography, voltdbclient.Geography)
        self.assertIs(voltclient.XYZPoint, voltdbclient.XYZPoint)
        for name in ("VoltType", "Geography", "XYZPoint"):
            self.assertIn(name, voltclient.__all__)


class TestInvocationSerialization(unittest.TestCase):
    """The off-to-the-side serializer must match the legacy wire format exactly
    (only the handle differs from the legacy hardcoded 1)."""

    def _legacy_invocation_bytes(self, name, types, params):
        # Drive the legacy VoltProcedure.call against a fake socket, feeding it a
        # canned response so it does not block, and capture what it sent.
        fake = FakeSocket()
        fser = FastSerializer()
        fser.socket = fake
        fser.procedure_timeout = None
        fake.feed(build_response(1))
        VoltProcedure(fser, name, types).call(params)
        return bytes(fake.sent)

    def test_matches_legacy_scalars(self):
        name = "Test.insert"
        types = [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_BIGINT]
        params = ["widget", 42]
        expected = self._legacy_invocation_bytes(name, types, params)
        actual = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, name, types, params, handle=1)
        self.assertEqual(actual, expected)

    def test_matches_legacy_array_param(self):
        name = "Test.bulk"
        types = [FastSerializer.VOLTTYPE_INTEGER]
        params = [[1, 2, 3]]
        expected = self._legacy_invocation_bytes(name, types, params)
        actual = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, name, types, params, handle=1)
        self.assertEqual(actual, expected)

    def test_version2_frame_with_partition_destination(self):
        # The version-2 layout, byte by byte, per the Java client's
        # ProcedureInvocation.flattenToBuffer / ProcedureInvocationExtensions:
        # naming a destination switches the version byte to 2 and inserts an
        # extension section between the client handle and the parameters.
        actual = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, "Foo", [FastSerializer.VOLTTYPE_INTEGER],
            [7], handle=99, partition_destination=5)
        expected = (
            b"\x00\x00\x00\x20"                   # frame length (32)
            b"\x02"                               # invocation version 2
            b"\x00\x00\x00\x03Foo"                # procedure name
            b"\x00\x00\x00\x00\x00\x00\x00\x63"   # client handle (99)
            b"\x02"                               # extension count
            b"\x02\x00"                           # ALL_PARTITION, empty payload
            b"\x03\x03\x00\x00\x00\x05"           # PARTITION_DESTINATION,
                                                  #   4-byte payload (log2 form
                                                  #   length 3), partition 5
            b"\x00\x01"                           # parameter count
            b"\x05\x00\x00\x00\x07")              # INTEGER 7
        self.assertEqual(actual, expected)

    def test_destination_changes_only_version_and_extension_section(self):
        # Same invocation with and without a destination: the version-2 frame
        # is the version-0 frame with the version byte flipped and the 9-byte
        # extension section spliced in after the handle -- name, handle, and
        # parameters serialize identically in both layouts.
        name = "Test.insert"
        types = [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_BIGINT]
        params = ["widget", 42]
        v0 = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, name, types, params, handle=7)
        v2 = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, name, types, params, handle=7,
            partition_destination=3)
        ext = b"\x02" + b"\x02\x00" + b"\x03\x03\x00\x00\x00\x03"
        handle_end = 4 + 1 + 4 + len(name) + 8  # length, version, name, handle
        spliced = (struct.pack(">i", len(v0) - 4 + len(ext)) + b"\x02"
                   + v0[5:handle_end] + ext + v0[handle_end:])
        self.assertEqual(v2, spliced)

    def test_handle_is_embedded(self):
        name = "@Ping"
        h1 = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, name, (), (), handle=1)
        h2 = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, name, (), (), handle=2)
        # Same procedure, different handle -> different bytes (in the handle slot).
        self.assertNotEqual(h1, h2)

    def test_param_count_mismatch_raises(self):
        with self.assertRaises(ValueError):
            voltclient._serialize_invocation(
                FastSerializer.BIG_ENDIAN, "P",
                [FastSerializer.VOLTTYPE_BIGINT], [], handle=1)

    def test_reused_scratch_does_not_bleed_between_calls(self):
        # Serializing a different invocation in between must not corrupt the
        # result: the scratch serializer is cleared before each use.
        first = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, "@Ping", (), (), handle=5)
        voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, "Test.insert",
            [FastSerializer.VOLTTYPE_STRING], ["something longer"], handle=6)
        again = voltclient._serialize_invocation(
            FastSerializer.BIG_ENDIAN, "@Ping", (), (), handle=5)
        self.assertEqual(first, again)

    def test_scratch_serializer_is_thread_local(self):
        # Each sending thread gets its own scratch instance (never shared), which
        # is what makes reuse safe without a lock.
        got = {}

        def grab(key):
            got[key] = voltclient._scratch_serializer(FastSerializer.BIG_ENDIAN)

        t1 = threading.Thread(target=grab, args=("t1",))
        t2 = threading.Thread(target=grab, args=("t2",))
        t1.start(); t1.join()
        t2.start(); t2.join()
        main = voltclient._scratch_serializer(FastSerializer.BIG_ENDIAN)
        # Hold references so identities can't be recycled; all three are distinct.
        self.assertIsNot(got["t1"], got["t2"])
        self.assertIsNot(got["t1"], main)
        self.assertIsNot(got["t2"], main)


class TestConcurrencyCore(unittest.TestCase):

    def test_unique_handles(self):
        with VoltClient() as client:
            handles = {client._next_handle() for _ in range(1000)}
            self.assertEqual(len(handles), 1000)
            self.assertEqual(min(handles), 1)
            self.assertEqual(max(handles), 1000)

    def test_async_call_completes(self):
        client, conn, fake = make_client_with_fake()
        try:
            future = client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 1)
            handle = next(iter(conn._pending))
            fake.feed(build_response(handle, roundtrip=7))
            resp = future.result(timeout=2.0)
            self.assertEqual(resp.clientHandle, handle)
            self.assertEqual(resp.roundtripTime, 7)
        finally:
            client.close()

    def test_sync_call_blocks_and_returns(self):
        client, conn, fake = make_client_with_fake()
        try:
            result = {}

            def run():
                result["resp"] = client.call("@Ping")

            caller = threading.Thread(target=run)
            caller.start()
            wait_for(lambda: len(conn._pending) == 1)
            handle = next(iter(conn._pending))
            fake.feed(build_response(handle, roundtrip=99))
            caller.join(timeout=2.0)
            self.assertFalse(caller.is_alive())
            self.assertEqual(result["resp"].roundtripTime, 99)
        finally:
            client.close()

    def test_reader_splits_and_joins_frames(self):
        # Responses arrive coalesced and cut at arbitrary points: each frame is
        # still decoded once, whole, and completes its own call.
        client, conn, fake = make_client_with_fake()
        try:
            futures = [client.call_async("@Ping") for _ in range(3)]
            wait_for(lambda: len(conn._pending) == 3)
            handles = sorted(conn._pending)
            data = b"".join(build_response(h, roundtrip=h) for h in handles)
            for cut in (3, 10, len(data) - 1):
                fake.feed(data[:cut])
                data = data[cut:]
                time.sleep(0.01)
            fake.feed(data)
            results = [f.result(timeout=2.0) for f in futures]
            self.assertEqual(sorted(r.clientHandle for r in results), handles)
            self.assertEqual([r.roundtripTime for r in results],
                             [r.clientHandle for r in results])
        finally:
            client.close()

    def test_concurrent_waits_on_overlapping_futures(self):
        # concurrent.futures.wait() locks each future's condition; threads
        # waiting on overlapping sets must not deadlock each other or the reader.
        client, conn, fake = make_client_with_fake()
        try:
            futures = [client.call_async("@Ping") for _ in range(256)]
            wait_for(lambda: len(conn._pending) == 256)
            stop = threading.Event()

            def waiter(seed):
                rnd = random.Random(seed)
                while not stop.is_set():
                    concurrent.futures.wait(rnd.sample(futures, 8), timeout=0)

            waiters = [threading.Thread(target=waiter, args=(i,), daemon=True)
                       for i in range(4)]
            for t in waiters:
                t.start()
            time.sleep(0.2)
            fake.feed(b"".join(build_response(h) for h in sorted(conn._pending)))
            # A deadlock would also block this wait, so it runs on a thread.
            done = []
            checker = threading.Thread(target=lambda: done.extend(
                concurrent.futures.wait(futures, timeout=2.0)[0]), daemon=True)
            checker.start()
            checker.join(timeout=5.0)
            stop.set()
            for t in waiters:
                t.join(timeout=2.0)
            self.assertEqual(len(done), 256)
            self.assertFalse(any(t.is_alive() for t in waiters))
        finally:
            client.close()

    def test_out_of_order_demux(self):
        client, conn, fake = make_client_with_fake()
        try:
            futures = [client.call_async("@Ping") for _ in range(3)]
            wait_for(lambda: len(conn._pending) == 3)
            # Snapshot handle -> call while all three are outstanding.
            pending = dict(conn._pending)
            handles = sorted(pending)
            self.assertEqual(handles, [1, 2, 3])  # unique, monotonic

            # Respond in reverse of send order; each must still route correctly.
            for handle in reversed(handles):
                fake.feed(build_response(handle, roundtrip=handle))

            for handle, call in pending.items():
                resp = call.future.result(timeout=2.0)
                self.assertEqual(resp.clientHandle, handle)
                self.assertEqual(resp.roundtripTime, handle)
            # Sanity: the returned futures are the same objects, in send order.
            self.assertEqual([f.result().clientHandle for f in futures], [1, 2, 3])
        finally:
            client.close()

    def test_server_push_routed_not_to_a_call(self):
        client, conn, fake = make_client_with_fake()
        try:
            pushes = []
            client._on_push = lambda c, h, r: pushes.append((h, r))
            # A normal call, plus an unsolicited push interleaved before it.
            future = client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 1)
            handle = next(iter(conn._pending))
            fake.feed(build_response(ASYNC_TOPO_HANDLE))
            fake.feed(build_response(handle, roundtrip=1))
            resp = future.result(timeout=2.0)
            self.assertEqual(resp.roundtripTime, 1)
            self.assertTrue(wait_for(lambda: len(pushes) == 1))
            self.assertEqual(pushes[0][0], ASYNC_TOPO_HANDLE)
        finally:
            client.close()

    def test_every_reserved_handle_is_a_push(self):
        # All reserved handles sit above the ceiling, so none is taken for a call.
        reserved = [voltclient.ASYNC_TOPO_HANDLE, voltclient.ASYNC_PROC_HANDLE,
                    voltclient.ASYNC_GRACEFUL_STOP_HANDLE,
                    voltclient.SNAPSHOT_STREAMING_CATALOG_HANDLE,
                    voltclient.SNAPSHOT_STREAMING_TABLE_CHUNK_HANDLE,
                    voltclient.ASYNC_GRACEFUL_STOP_HANDLE_CANCEL]
        for handle in reserved:
            self.assertGreater(handle, voltclient.MAX_CLIENT_HANDLE)
        client, conn, fake = make_client_with_fake()
        try:
            pushes = []
            client._on_push = lambda c, h, r: pushes.append(h)
            fake.feed(build_response(voltclient.ASYNC_GRACEFUL_STOP_HANDLE_CANCEL))
            self.assertTrue(wait_for(lambda: len(pushes) == 1))
            self.assertEqual(pushes[0], voltclient.ASYNC_GRACEFUL_STOP_HANDLE_CANCEL)
        finally:
            client.close()

    def test_asyncio_wrap_future_bridge(self):
        # The documented bridge for non-blocking chained workflows:
        # await asyncio.wrap_future(client.call_async(...)).
        client, conn, fake = make_client_with_fake()
        try:
            future = client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 1)
            handle = next(iter(conn._pending))

            async def collect():
                wrapped = asyncio.wrap_future(future)
                fake.feed(build_response(handle, roundtrip=5))
                return await asyncio.wait_for(wrapped, timeout=2.0)

            resp = asyncio.run(collect())
            self.assertEqual(resp.roundtripTime, 5)
        finally:
            client.close()

    def test_unknown_handle_is_dropped(self):
        client, conn, fake = make_client_with_fake()
        try:
            future = client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 1)
            handle = next(iter(conn._pending))
            # A response for a handle we never issued: must not crash the reader,
            # and must leave the real call outstanding.
            fake.feed(build_response(handle + 12345))
            time.sleep(0.1)
            self.assertFalse(future.done())
            self.assertEqual(len(conn._pending), 1)
            # The reader is still healthy: the real response still completes.
            fake.feed(build_response(handle))
            self.assertIsNotNone(future.result(timeout=2.0))
        finally:
            client.close()


class TestDeadlineSweeper(unittest.TestCase):

    def test_call_times_out_cleanly(self):
        client, conn, fake = make_client_with_fake(sweep_interval=0.02)
        try:
            future = client.call_async("SlowProc", timeout=0.05)
            with self.assertRaises(VoltTimeoutError):
                future.result(timeout=2.0)
            # The timed-out call is removed, not leaked.
            self.assertTrue(wait_for(lambda: len(conn._pending) == 0))
        finally:
            client.close()

    def test_sync_call_times_out_cleanly(self):
        client, conn, fake = make_client_with_fake(sweep_interval=0.02)
        try:
            with self.assertRaises(VoltTimeoutError):
                client.call("SlowProc", timeout=0.05)
        finally:
            client.close()


class TestBoundedOutstanding(unittest.TestCase):
    """The outstanding-request cap: calls past max_outstanding block until a
    completion frees a slot, and every terminal outcome frees exactly one."""

    def test_invalid_max_outstanding_rejected(self):
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                VoltClient(max_outstanding=bad)

    def test_property_tracks_in_flight_calls(self):
        client, conn, fake = make_client_with_fake()
        try:
            self.assertEqual(client.outstanding_requests, 0)
            futures = [client.call_async("@Ping") for _ in range(3)]
            self.assertEqual(client.outstanding_requests, 3)
            wait_for(lambda: len(conn._pending) == 3)
            for handle in list(conn._pending):
                fake.feed(build_response(handle))
            for future in futures:
                future.result(timeout=2.0)
            self.assertTrue(wait_for(
                lambda: client.outstanding_requests == 0))
        finally:
            client.close()

    def test_call_past_cap_blocks_until_a_completion(self):
        client, conn, fake = make_client_with_fake(max_outstanding=2)
        try:
            client.call_async("@Ping")
            client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 2)
            first_handle = min(conn._pending)

            third = {}

            def blocked_caller():
                third["future"] = client.call_async("@Ping")

            caller = threading.Thread(target=blocked_caller)
            caller.start()
            # The third call must be waiting for a slot, not sent.
            time.sleep(0.1)
            self.assertTrue(caller.is_alive())
            self.assertEqual(client.outstanding_requests, 2)
            self.assertEqual(len(conn._pending), 2)

            # One completion frees one slot; the blocked call proceeds.
            fake.feed(build_response(first_handle))
            caller.join(timeout=2.0)
            self.assertFalse(caller.is_alive())
            self.assertEqual(client.outstanding_requests, 2)
            self.assertTrue(wait_for(lambda: len(conn._pending) == 2))
        finally:
            client.close()

    def test_slot_wait_draws_on_the_call_timeout(self):
        # One budget for the whole call: the execution deadline is computed
        # before the slot wait, so time spent blocked for a slot is spent from
        # the call's timeout, not added to it.
        client, conn, fake = make_client_with_fake(max_outstanding=1)
        try:
            client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 1)
            first_handle = next(iter(conn._pending))

            submitted = time.monotonic()

            def blocked_caller():
                client.call_async("@Ping", timeout=10)

            caller = threading.Thread(target=blocked_caller)
            caller.start()
            time.sleep(0.5)  # hold the second call blocked for a while
            fake.feed(build_response(first_handle))
            caller.join(timeout=2.0)
            self.assertFalse(caller.is_alive())

            call = next(iter(conn._pending.values()))
            self.assertLessEqual(
                call.deadline, submitted + 10.25,
                "deadline was re-based after the slot wait: the wait added "
                "to the timeout instead of drawing on it")
        finally:
            client.close()

    def test_blocked_call_times_out(self):
        client, conn, fake = make_client_with_fake(max_outstanding=1)
        try:
            client.call_async("@Ping")
            started = time.monotonic()
            with self.assertRaises(VoltTimeoutError):
                client.call_async("@Ping", timeout=0.05)
            self.assertLess(time.monotonic() - started, 1.0)
            # The failed acquire must not leak a slot.
            self.assertEqual(client.outstanding_requests, 1)
        finally:
            client.close()

    def test_slot_freed_on_deadline_expiry(self):
        client, conn, fake = make_client_with_fake(
            max_outstanding=1, sweep_interval=0.02)
        try:
            future = client.call_async("SlowProc", timeout=0.05)
            with self.assertRaises(VoltTimeoutError):
                future.result(timeout=2.0)
            self.assertTrue(wait_for(
                lambda: client.outstanding_requests == 0))
        finally:
            client.close()

    def test_slot_freed_on_connection_loss(self):
        client, conn, fake = make_client_with_fake()
        try:
            future = client.call_async("@Ping")
            self.assertEqual(client.outstanding_requests, 1)
            fake.close()
            with self.assertRaises(VoltConnectionError):
                future.result(timeout=2.0)
            self.assertTrue(wait_for(
                lambda: client.outstanding_requests == 0))
        finally:
            client.close()

    def test_slot_freed_when_send_fails(self):
        client, conn, fake = make_client_with_fake()
        try:
            # A socket that dies before the send: the call fails -- raised, or
            # through its Future once the writer finds the socket closed -- and
            # its slot is released either way.
            fake.close()
            try:
                future = client.call_async("@Ping")
            except VoltClientError:
                pass
            else:
                with self.assertRaises(VoltClientError):
                    future.result(timeout=2.0)
            self.assertTrue(wait_for(lambda: client.outstanding_requests == 0))
        finally:
            client.close()

    def test_slot_freed_with_no_connections(self):
        with VoltClient() as client:
            with self.assertRaises(VoltNoConnectionsError):
                client.call_async("@Ping")
            self.assertEqual(client.outstanding_requests, 0)

    def test_already_terminal_future_frees_its_slot(self):
        # A call cancelled before its bytes hit the wire returns an
        # already-cancelled Future; the slot must still be released.
        client, conn, fake = make_client_with_fake()
        try:
            def cancelled_send(proc_name, param_types, params, deadline,
                               partition_destination=None):
                future = concurrent.futures.Future()
                future.cancel()
                return future

            conn.send = cancelled_send
            future = client.call_async("@Ping")
            self.assertTrue(future.cancelled())
            self.assertEqual(client.outstanding_requests, 0)
        finally:
            client.close()

    def test_close_wakes_a_blocked_caller(self):
        client, conn, fake = make_client_with_fake(max_outstanding=1)
        client.call_async("@Ping")

        outcome = {}

        def blocked_caller():
            try:
                client.call_async("@Ping", timeout=30)
            except VoltClientError as e:
                outcome["error"] = e

        caller = threading.Thread(target=blocked_caller)
        caller.start()
        time.sleep(0.1)
        self.assertTrue(caller.is_alive())
        client.close()
        caller.join(timeout=2.0)
        self.assertFalse(caller.is_alive())
        self.assertIsInstance(outcome.get("error"), VoltClientError)

    def test_fan_out_composes_with_wait_and_as_completed(self):
        client, conn, fake = make_client_with_fake()
        try:
            futures = [client.call_async("@Ping") for _ in range(5)]
            wait_for(lambda: len(conn._pending) == 5)
            # Complete in reverse of send order; the stdlib helpers must see
            # every completion regardless of arrival order.
            for handle in sorted(conn._pending, reverse=True):
                fake.feed(build_response(handle, roundtrip=handle))

            done, not_done = concurrent.futures.wait(futures, timeout=2.0)
            self.assertEqual(len(done), 5)
            self.assertEqual(not_done, set())

            handles = [f.result().clientHandle
                       for f in concurrent.futures.as_completed(futures,
                                                                timeout=2.0)]
            self.assertEqual(sorted(handles), sorted(f.result().clientHandle
                                                     for f in futures))
        finally:
            client.close()


class TestConnectionLoss(unittest.TestCase):

    def test_disconnect_fails_pending_and_drops_connection(self):
        client, conn, fake = make_client_with_fake()
        try:
            future = client.call_async("@Ping")
            wait_for(lambda: len(conn._pending) == 1)
            # Simulate the peer going away: reader's recv sees EOF.
            fake.close()
            with self.assertRaises(VoltConnectionError):
                future.result(timeout=2.0)
            # The dead connection is removed from the pool.
            self.assertTrue(wait_for(lambda: conn not in client._connections))
        finally:
            client.close()

    def test_send_on_dead_connection_raises(self):
        client, conn, fake = make_client_with_fake()
        try:
            fake.close()
            wait_for(lambda: conn not in client._connections)
            # No connections left in the pool -> a clear error, not a hang.
            with self.assertRaises(VoltConnectionError):
                client.call_async("@Ping")
        finally:
            client.close()


class TestLifecycle(unittest.TestCase):

    def test_context_manager_closes(self):
        with make_client_with_fake()[0] as client:
            self.assertFalse(client._closed)
        self.assertTrue(client._closed)

    def test_close_leaves_no_threads(self):
        client, conn, fake = make_client_with_fake()
        names = {t.name for t in threading.enumerate()}
        self.assertIn("voltclient-sweeper", names)
        self.assertTrue(any(n.startswith("voltclient-reader") for n in names))

        client.close()

        leftover = [t for t in threading.enumerate()
                    if t.name.startswith("voltclient-") and t.is_alive()]
        self.assertEqual(leftover, [], "background threads leaked after close()")

    def test_close_is_idempotent(self):
        client, conn, fake = make_client_with_fake()
        client.close()
        client.close()  # must not raise


class TestReconnect(unittest.TestCase):

    def test_reconnects_after_connection_lost(self):
        client, fakes = make_reconnecting_client(reconnect_interval=0.02)
        try:
            target = client._add_target("h", 21212)
            client._connect_target(target)
            first = target.connection
            self.assertIsNotNone(first)
            self.assertEqual(len(fakes), 1)

            # Peer drops: the reader sees EOF, the target is marked for reconnect,
            # and the reconnect loop dials a fresh connection.
            fakes[0].close()
            self.assertTrue(wait_for(
                lambda: target.connection is not None
                and target.connection is not first))
            self.assertGreaterEqual(len(fakes), 2)
            self.assertIn(target.connection, client._connections)
        finally:
            client.close()

    def test_backoff_grows_and_caps(self):
        with VoltClient(initial_backoff=0.5, max_backoff=2.0,
                        reconnect_jitter=0.0) as client:
            target = voltclient._Target("h", 21212)
            for expected in (0.5, 1.0, 2.0, 2.0):  # doubles, then caps at 2.0
                client._schedule_retry(target, 100.0)
                self.assertEqual(target.backoff, expected)
            # With no jitter, the next attempt is exactly now + backoff.
            self.assertEqual(target.next_attempt, 100.0 + 2.0)

    def test_failed_reconnect_backs_off_without_hammering(self):
        client = VoltClient(reconnect_interval=0.01, initial_backoff=0.05,
                            max_backoff=1.0, reconnect_jitter=0.0)
        try:
            def refuse(host, port):
                raise OSError("connection refused")
            client._open_serializer = refuse
            target = client._add_target("h", 21212)
            with self.assertRaises(OSError):
                client._connect_target(target)
            client._schedule_retry(target, time.monotonic())
            baseline = target.backoff
            # The reconnect loop keeps failing; backoff must grow, not stay flat.
            self.assertTrue(wait_for(lambda: target.backoff > baseline, timeout=2.0))
            self.assertIsNone(target.connection)
        finally:
            client.close()

    def test_update_targets_adds_and_removes(self):
        client, fakes = make_reconnecting_client(reconnect_interval=0.02)
        try:
            client._update_targets([("a", 21212), ("b", 21212)])
            self.assertTrue(wait_for(lambda: len(client._connections) == 2))

            # Dropping a target closes its connection; adding leaves the other.
            client._update_targets([("b", 21212)])
            self.assertTrue(wait_for(
                lambda: {(c.host, c.port) for c in client._connections}
                == {("b", 21212)}))
        finally:
            client.close()


class TestHealthCheck(unittest.TestCase):

    def test_idle_connection_is_pinged(self):
        client, conn, fake = make_client_with_fake(
            sweep_interval=0.02, connection_response_timeout=0.06)
        try:
            # Silent connection: after idle exceeds a third of the timeout, the
            # maintenance thread pings it.
            self.assertTrue(wait_for(lambda: conn._outstanding_ping))
            wait_for(lambda: ping_bytes() in bytes(fake.sent))
            self.assertIn(ping_bytes(), bytes(fake.sent))
            # Answering the ping resets liveness and keeps the connection up.
            fake.feed(build_response(voltclient.PING_HANDLE))
            self.assertTrue(wait_for(lambda: not conn._outstanding_ping))
            self.assertIn(conn, client._connections)
        finally:
            client.close()

    def test_unanswered_ping_declares_connection_dead(self):
        client, conn, fake = make_client_with_fake(
            sweep_interval=0.02, connection_response_timeout=0.05)
        try:
            # Ping is sent but never answered; after the full timeout the
            # connection is declared dead and dropped from the pool.
            self.assertTrue(wait_for(lambda: conn not in client._connections,
                                     timeout=3.0))
        finally:
            client.close()

    def test_busy_connection_is_not_pinged(self):
        client, conn, fake = make_client_with_fake()
        try:
            now = time.monotonic()
            conn._last_response = now  # just heard from the server
            conn.health_check(now, response_timeout=30.0)
            self.assertFalse(conn._outstanding_ping)
            self.assertEqual(len(bytes(fake.sent)), 0)
        finally:
            client.close()

    def test_ping_response_clears_flag_and_is_not_a_push(self):
        client, conn, fake = make_client_with_fake()
        try:
            pushes = []
            client._on_push = lambda c, h, r: pushes.append(h)
            conn._outstanding_ping = True
            fake.feed(build_response(voltclient.PING_HANDLE))
            self.assertTrue(wait_for(lambda: not conn._outstanding_ping))
            self.assertEqual(pushes, [])
        finally:
            client.close()


def make_table(columns, rows):
    """A VoltTable populated directly (no wire round-trip) for parser tests.
    `columns` is a list of (voltdb_type, name); `rows` is a list of value lists."""
    table = VoltTable(None)
    for coltype, name in columns:
        table.columns.append(VoltColumn(type=coltype, name=name))
    for row in rows:
        table.tuples.append(list(row))
    return table


# The three discovery tables, built with the same column names and shapes the
# server returns. Reused across the parsing and refresh tests.
def overview_table(hosts):
    """hosts: list of (host_id, ip, hostname, client_port); emitted as the narrow
    (HOST_ID, KEY, VALUE) rows OVERVIEW actually returns."""
    rows = []
    for host_id, ip, hostname, port in hosts:
        rows.append([host_id, "IPADDRESS", ip])
        rows.append([host_id, "HOSTNAME", hostname])
        rows.append([host_id, "CLIENTPORT", str(port)])
    return make_table([(FS.VOLTTYPE_INTEGER, "HOST_ID"),
                       (FS.VOLTTYPE_STRING, "KEY"),
                       (FS.VOLTTYPE_STRING, "VALUE")], rows)


def raw_config(entries):
    """A hashinator config blob in the raw format @Statistics TOPO returns:
    int32 count then interleaved big-endian (token, partition) int32 pairs.
    `entries`: list of (token, partition), tokens ascending from -2**31."""
    flat = [v for entry in entries for v in entry]
    return struct.pack(">i%di" % len(flat), len(entries), *flat)


def cooked_config(entries):
    """The same ring in the compressed 'cooked' encoding: zlib over int32 count,
    the tokens transposed into four byte planes (most significant first), then
    the partition ids as plain big-endian int32s."""
    count = len(entries)
    body = bytearray(struct.pack(">i", count))
    for shift in (24, 16, 8, 0):
        body += bytes(((token & 0xFFFFFFFF) >> shift) & 0xFF
                      for token, _ in entries)
    body += struct.pack(">%di" % count, *(p for _, p in entries))
    return zlib.compress(bytes(body))


# A tiny but valid ring for tests that just need *a* working hashinator:
# everything below token 0 is partition 0, everything at-or-above is partition 1.
DEFAULT_RING = [(-2 ** 31, 0), (0, 1)]
DEFAULT_HASHCONFIG = raw_config(DEFAULT_RING)


def topo_tables(part_rows, hashconfig=DEFAULT_HASHCONFIG, hashtype="ELASTIC"):
    """part_rows: list of (partition, sites, leader). Returns [topology, config]
    like @Statistics TOPO; pass hashconfig=None to omit the second table."""
    topo = make_table([(FS.VOLTTYPE_INTEGER, "Partition"),
                       (FS.VOLTTYPE_STRING, "Sites"),
                       (FS.VOLTTYPE_STRING, "Leader")], part_rows)
    if hashconfig is None:
        return [topo]
    cfg = make_table([(FS.VOLTTYPE_STRING, "HASHTYPE"),
                      (FS.VOLTTYPE_VARBINARY, "HASHCONFIG")],
                     [[hashtype, array.array('B', hashconfig)]])
    return [topo, cfg]


def procedures_table(procs):
    """procs: list of (name, remarks_json_str). Full 9-column PROCEDURES layout so
    the by-name lookup is exercised against the real column positions."""
    cols = [(FS.VOLTTYPE_STRING, c) for c in
            ("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
             "RESERVED1", "RESERVED2", "RESERVED3", "REMARKS")]
    cols += [(FS.VOLTTYPE_SMALLINT, "PROCEDURE_TYPE"),
             (FS.VOLTTYPE_STRING, "SPECIFIC_NAME")]
    rows = [[None, None, name, None, None, None, remarks, 1, name]
            for name, remarks in procs]
    return make_table(cols, rows)


def sp_json(partition_param=0, param_type=FS.VOLTTYPE_BIGINT, read_only=False):
    return json.dumps({"singlePartition": True, "readOnly": read_only,
                       "partitionParameter": partition_param,
                       "partitionParameterType": param_type})


class TestTopologyParsing(unittest.TestCase):
    """The pure discovery parsers turn each system procedure's result table into
    the stored model. Driven with hand-built tables -- no cluster, no socket."""

    def test_parse_overview_pivots_narrow_table(self):
        hosts = voltclient._parse_overview(overview_table([
            (0, "10.0.0.1", "volt0", 21212),
            (1, "10.0.0.2", "volt1", 21212)]))
        self.assertEqual(hosts[0], HostInfo(0, "10.0.0.1", "volt0", 21212))
        self.assertEqual(hosts[1], HostInfo(1, "10.0.0.2", "volt1", 21212))

    def test_parse_topology_leaders_replicas_and_config(self):
        partitions, hashtype, config = voltclient._parse_topology(topo_tables([
            [0, "0:0,1:0", "0:0"],
            [1, "0:1,1:1", "1:1"],
            [MP_INIT_PID, "0:14,1:14", "0:14"]]))
        self.assertEqual(partitions[0], PartitionInfo(0, 0, (0, 1)))
        self.assertEqual(partitions[1], PartitionInfo(1, 1, (0, 1)))
        # The MP initiator's pseudo-partition is kept alongside real partitions.
        self.assertIn(MP_INIT_PID, partitions)
        self.assertEqual(hashtype, "ELASTIC")
        self.assertEqual(config, DEFAULT_HASHCONFIG)

    def test_parse_topology_skips_incomplete_rows(self):
        # A partition with no leader/sites yet (transient) is skipped, not stored.
        partitions, _, _ = voltclient._parse_topology(topo_tables([
            [0, "0:0", "0:0"],
            [1, "", ""]]))
        self.assertIn(0, partitions)
        self.assertNotIn(1, partitions)

    def test_parse_topology_old_server_single_table(self):
        # An older cluster returns only the topology table (no hashinator config).
        partitions, hashtype, config = voltclient._parse_topology(
            topo_tables([[0, "0:0", "0:0"]], hashconfig=None))
        self.assertIn(0, partitions)
        self.assertIsNone(hashtype)
        self.assertIsNone(config)

    def test_parse_procedures_single_and_multi(self):
        procs = voltclient._parse_procedures(procedures_table([
            ("Votes.insert", sp_json(partition_param=0, param_type=FS.VOLTTYPE_BIGINT)),
            ("SummaryReport", json.dumps({"singlePartition": False, "readOnly": True}))]))
        self.assertEqual(procs["Votes.insert"],
                         ProcedureInfo("Votes.insert", True, False, False,
                                       0, FS.VOLTTYPE_BIGINT))
        self.assertEqual(procs["SummaryReport"],
                         ProcedureInfo("SummaryReport", False, True, False, -1, -1))

    def test_parse_procedures_skips_unparseable(self):
        # One procedure with garbage metadata must not lose the others, and the
        # skip is logged (captured here so it doesn't print, and asserted).
        with self.assertLogs("voltclient", level="WARNING") as logged:
            procs = voltclient._parse_procedures(procedures_table([
                ("Good.proc", sp_json()),
                ("Bad.proc", "this is not json")]))
        self.assertIn("Good.proc", procs)
        self.assertNotIn("Bad.proc", procs)
        self.assertTrue(any("Bad.proc" in line for line in logged.output))


class TestMurmur3(unittest.TestCase):
    """The murmur3 port against tokens computed by the Java implementation
    (org.apache.cassandra_voltpatches.MurmurHash3, run through a throwaway
    harness). Any divergence here means values route to wrong partitions, so
    the vectors cover the 8-byte integer path plus lengths that exercise every
    body/tail combination: 0, <8, 8, 9..15, 16, and >16 bytes."""

    JAVA_LONG_TOKENS = {
        0: 685728695,
        1: 4457399,
        -1: -1595624838,
        42: -1230191719,
        1000000: -1422887996,
        2 ** 63 - 1: 1819732939,
        -(2 ** 63 - 1): 1902085590,
        -2147483648: -1298860540,
    }

    JAVA_UTF8_TOKENS = {
        "": 0,
        "a": -2058005147,
        "ab": -1819602454,
        "abc": -1265221825,
        "hello world": 1396662342,
        "The quick brown fox jumps over the lazy dog": -481575813,
        "exactly15bytes.": 2063330919,
        "exactly16bytes..": -741527103,
        "exactly17bytes...": -1123721009,
        "élève-日本語": -199557357,
    }

    JAVA_BYTES_TOKENS = {
        b"\x00": 1175497701,
        b"\xff": 1205483384,
        b"\xde\xad\xbe\xef": 1510557949,
    }

    def test_integer_tokens_match_java(self):
        for value, token in self.JAVA_LONG_TOKENS.items():
            self.assertEqual(
                voltclient._murmur3_token(struct.pack("<q", value)), token,
                "token mismatch for long %d" % value)

    def test_string_tokens_match_java(self):
        for value, token in self.JAVA_UTF8_TOKENS.items():
            self.assertEqual(
                voltclient._murmur3_token(value.encode("utf-8")), token,
                "token mismatch for string %r" % value)

    def test_binary_tokens_match_java(self):
        for value, token in self.JAVA_BYTES_TOKENS.items():
            self.assertEqual(voltclient._murmur3_token(value), token,
                             "token mismatch for bytes %r" % value)


# A four-partition ring with human-checkable boundaries, used by the hashinator
# tests below: (-2**31..-101) -> 0, (-100..-1) -> 1, (0..999) -> 2, (1000..) -> 3.
RING = [(-2 ** 31, 0), (-100, 1), (0, 2), (1000, 3)]


class TestElasticHashinator(unittest.TestCase):
    """Config decoding (both encodings) and the token-ring lookup."""

    def test_lookup_boundaries(self):
        h = ElasticHashinator.from_bytes(raw_config(RING))
        for token, expected in [(-2 ** 31, 0), (-101, 0), (-100, 1), (-1, 1),
                                (0, 2), (999, 2), (1000, 3), (2 ** 31 - 1, 3)]:
            self.assertEqual(h.partition_for_token(token), expected,
                             "wrong partition for token %d" % token)

    def test_cooked_decodes_to_same_ring_as_raw(self):
        raw = ElasticHashinator.from_bytes(raw_config(RING))
        cooked = ElasticHashinator.from_bytes(cooked_config(RING), cooked=True)
        self.assertEqual(cooked._tokens, raw._tokens)
        self.assertEqual(cooked._partitions, raw._partitions)

    def test_cooked_and_raw_agree_on_a_large_ring(self):
        # A deterministic pseudo-random ring, large enough that the cooked
        # encoding's byte-plane transposition is exercised across planes.
        rnd = random.Random(20260720)
        tokens = sorted(set(rnd.randrange(-2 ** 31 + 1, 2 ** 31)
                            for _ in range(1000)))
        ring = [(-2 ** 31, 0)] + [(t, i % 8) for i, t in enumerate(tokens)]
        raw = ElasticHashinator.from_bytes(raw_config(ring))
        cooked = ElasticHashinator.from_bytes(cooked_config(ring), cooked=True)
        self.assertEqual(cooked._tokens, raw._tokens)
        self.assertEqual(cooked._partitions, raw._partitions)

    def test_bad_configs_raise(self):
        bad = [
            raw_config(RING)[:-1],               # truncated
            struct.pack(">i", 0),                # zero entries
            struct.pack(">i", -1),               # negative count
            raw_config([(-2 ** 31, 0), (5, 1), (4, 2)]),  # unsorted tokens
            raw_config([(0, 0), (5, 1)]),        # ring must start at -2**31
            b"",
        ]
        for config in bad:
            with self.assertRaises(VoltClientError, msg=repr(config)):
                ElasticHashinator.from_bytes(config)
        with self.assertRaises(VoltClientError):
            ElasticHashinator.from_bytes(b"this is not zlib", cooked=True)


class TestGetPartitionForParameter(unittest.TestCase):
    """Value-to-bytes semantics: type coercions, null handling, and the errors.
    These pin the *Java client's* behavior -- including its unintuitive corners
    -- because the cluster routes with the same shared code, and divergence
    means silently wrong partitions."""

    def setUp(self):
        self.h = ElasticHashinator.from_bytes(raw_config(RING))

    def part(self, vtype, value):
        return self.h.get_partition_for_parameter(vtype, value)

    def test_java_anchored_partitions(self):
        # Values whose tokens were computed by the Java implementation, placed
        # on the known RING: e.g. bigint 42 hashes to -1230191719, which is
        # below token -100, hence partition 0.
        self.assertEqual(self.part(FS.VOLTTYPE_BIGINT, 42), 0)
        self.assertEqual(self.part(FS.VOLTTYPE_INTEGER, 1000000), 0)
        self.assertEqual(self.part(FS.VOLTTYPE_STRING, "hello world"), 3)
        self.assertEqual(self.part(FS.VOLTTYPE_VARBINARY, b"\xde\xad\xbe\xef"), 3)

    def test_all_integer_widths_hash_identically(self):
        # Every integer type widens to 8 bytes before hashing, so the declared
        # width must not change the answer.
        expected = self.part(FS.VOLTTYPE_BIGINT, 5)
        for vtype in (FS.VOLTTYPE_TINYINT, FS.VOLTTYPE_SMALLINT,
                      FS.VOLTTYPE_INTEGER):
            self.assertEqual(self.part(vtype, 5), expected)

    def test_string_and_bytes_coerce_toward_integer_types(self):
        self.assertEqual(self.part(FS.VOLTTYPE_BIGINT, "42"),
                         self.part(FS.VOLTTYPE_BIGINT, 42))
        self.assertEqual(self.part(FS.VOLTTYPE_BIGINT, "+42"),
                         self.part(FS.VOLTTYPE_BIGINT, 42))
        self.assertEqual(self.part(FS.VOLTTYPE_INTEGER, struct.pack("<i", 77)),
                         self.part(FS.VOLTTYPE_INTEGER, 77))
        self.assertEqual(self.part(FS.VOLTTYPE_BIGINT, struct.pack("<q", 77)),
                         self.part(FS.VOLTTYPE_BIGINT, 77))

    def test_ints_toward_string_types_raise(self):
        # The Java implementation hashes an int offered for a STRING/VARBINARY
        # parameter as the widened integer -- faithfully predicting a mistyped
        # invocation whose writes then fail mispartitioned on the server all
        # but 1-in-partition-count of the time. A deterministic client error
        # beats predicting an intermittent one, so we raise and the caller
        # owns the coercion. Hashing the value as an integer type reproduces
        # the cluster's routing of the mistyped invocation, when wanted.
        for vtype in (FS.VOLTTYPE_STRING, FS.VOLTTYPE_VARBINARY):
            for value in (5, 0, -2 ** 63):
                with self.assertRaises(VoltTypeError,
                                       msg="%r as type %d" % (value, vtype)):
                    self.part(vtype, value)
        # str and bytes pass through untouched for the string-ish types.
        self.assertEqual(
            voltclient._partition_value_to_bytes(FS.VOLTTYPE_STRING, "abc"),
            b"abc")
        self.assertEqual(
            voltclient._partition_value_to_bytes(FS.VOLTTYPE_VARBINARY,
                                                 bytearray(b"\x01\x02")),
            b"\x01\x02")
        # A str for VARBINARY (or bytes for STRING) hashes as its raw bytes,
        # so the same byte sequence agrees across the two declared types.
        self.assertEqual(self.part(FS.VOLTTYPE_VARBINARY, "abc"),
                         self.part(FS.VOLTTYPE_STRING, b"abc"))

    def test_wire_binary_shapes_hash_like_bytes(self):
        # A VARBINARY column reads back as array('B'), so values a caller just
        # read from a table must hash like the equivalent bytes -- everywhere
        # the Java side accepts a byte[].
        raw = b"\xde\xad\xbe\xef"
        for value in (array.array("B", raw), bytearray(raw), memoryview(raw)):
            self.assertEqual(self.part(FS.VOLTTYPE_VARBINARY, value),
                             self.part(FS.VOLTTYPE_VARBINARY, raw))
        self.assertEqual(
            self.part(FS.VOLTTYPE_INTEGER, array.array("B", struct.pack("<i", 77))),
            self.part(FS.VOLTTYPE_INTEGER, 77))

    def test_nulls_map_to_partition_zero(self):
        null_cases = [
            (FS.VOLTTYPE_BIGINT, None),
            (FS.VOLTTYPE_STRING, None),
            (FS.VOLTTYPE_VARBINARY, None),
            (FS.VOLTTYPE_TINYINT, -2 ** 7),      # each width's null sentinel
            (FS.VOLTTYPE_SMALLINT, -2 ** 15),
            (FS.VOLTTYPE_INTEGER, -2 ** 31),
            (FS.VOLTTYPE_BIGINT, -2 ** 63),
            (FS.VOLTTYPE_BIGINT, str(-2 ** 63)),  # parses to the sentinel
        ]
        for vtype, value in null_cases:
            self.assertEqual(self.part(vtype, value), 0,
                             "expected partition 0 for %r as type %d"
                             % (value, vtype))
        # The sentinel belongs to the declared width: -2**31 is null for
        # INTEGER but a perfectly good BIGINT value.
        self.assertEqual(self.part(FS.VOLTTYPE_BIGINT, -2 ** 31), 0)
        self.assertEqual(
            voltclient._partition_value_to_bytes(FS.VOLTTYPE_BIGINT, -2 ** 31),
            struct.pack("<q", -2 ** 31))

    def test_unsupported_types_and_values_raise(self):
        with self.assertRaises(VoltTypeError):  # FLOAT is not partitionable
            self.part(FS.VOLTTYPE_FLOAT, 1.5)
        with self.assertRaises(VoltTypeError):  # even for a null value
            self.part(FS.VOLTTYPE_FLOAT, None)
        for vtype, value in [
                (FS.VOLTTYPE_BIGINT, 12.5),      # float value
                (FS.VOLTTYPE_BIGINT, True),      # boolean value
                (FS.VOLTTYPE_STRING, 1.5),
                (FS.VOLTTYPE_BIGINT, "12abc"),   # not Long.parseLong-able
                (FS.VOLTTYPE_BIGINT, " 12 "),    # int() would take these two,
                (FS.VOLTTYPE_BIGINT, "1_0"),     # Java would not
                (FS.VOLTTYPE_INTEGER, b"\x01\x02"),  # short of the 4-byte width
        ]:
            with self.assertRaises(VoltTypeError,
                                   msg="%r as type %d" % (value, vtype)):
                self.part(vtype, value)

    def test_out_of_range_integers_raise(self):
        for vtype, value in [(FS.VOLTTYPE_TINYINT, 200),
                             (FS.VOLTTYPE_TINYINT, -129),
                             (FS.VOLTTYPE_SMALLINT, 2 ** 15),
                             (FS.VOLTTYPE_INTEGER, 2 ** 31),
                             (FS.VOLTTYPE_BIGINT, 2 ** 63),
                             (FS.VOLTTYPE_BIGINT, -2 ** 63 - 1)]:
            with self.assertRaises(VoltTypeError,
                                   msg="%r as type %d" % (value, vtype)):
                self.part(vtype, value)


class TestSnapshotHashinator(unittest.TestCase):
    """The snapshot builds a ready hashinator from TOPO's advertisement, and
    degrades to no-hashinator (with a warning, never an exception) when the
    cluster's scheme is unknown or the blob is undecodable."""

    def test_elastic_config_builds(self):
        h = voltclient._build_hashinator("ELASTIC", raw_config(RING))
        self.assertEqual(h.partition_for_token(1000), 3)
        # Tolerate case differences and an absent HASHTYPE column.
        self.assertIsNotNone(voltclient._build_hashinator("elastic",
                                                          raw_config(RING)))
        self.assertIsNotNone(voltclient._build_hashinator(None, raw_config(RING)))

    def test_no_config_is_quietly_none(self):
        self.assertIsNone(voltclient._build_hashinator("ELASTIC", None))

    def test_unknown_hashtype_disables_hashing_with_warning(self):
        with self.assertLogs("voltclient", level="WARNING") as logged:
            h = voltclient._build_hashinator("LEGACY", raw_config(RING))
        self.assertIsNone(h)
        self.assertTrue(any("LEGACY" in line for line in logged.output))

    def test_undecodable_config_disables_hashing_with_warning(self):
        with self.assertLogs("voltclient", level="WARNING"):
            h = voltclient._build_hashinator("ELASTIC", b"\x00\x01garbage")
        self.assertIsNone(h)

    def test_client_method_requires_a_hashinator(self):
        client = VoltClient()  # no seed hosts, empty snapshot
        try:
            with self.assertRaises(VoltClientError):
                client.get_partition_for_parameter(FS.VOLTTYPE_BIGINT, 42)
            client._topology = TopologySnapshot(
                hosts={}, partitions={}, procedures={},
                hashinator_config=raw_config(RING),
                hashinator=ElasticHashinator.from_bytes(raw_config(RING)))
            self.assertEqual(
                client.get_partition_for_parameter(FS.VOLTTYPE_BIGINT, 42), 0)
        finally:
            client.close()


class _FakeResponse:
    """Stand-in for a VoltResponse carrying pre-built tables and a status."""

    def __init__(self, tables, status=voltclient._STATUS_SUCCESS, statusString=""):
        self.tables = tables
        self.status = status
        self.statusString = statusString


class TestRefreshTopology(unittest.TestCase):
    """refresh_topology issues the three discovery calls, builds a snapshot, and
    swaps it in atomically. The transport is stubbed (client.call) so this stays
    offline; the real wire path is covered by the live end-to-end test."""

    def _client(self, responses, record=None):
        client = VoltClient()  # no seed hosts: no real connections

        def fake_call(proc_name, param_types=None, params=None, timeout=None):
            if record is not None:
                record.append(proc_name)
            return responses[proc_name]

        client.call = fake_call
        return client

    def _good_responses(self):
        return {
            "@SystemInformation": _FakeResponse([overview_table([
                (0, "10.0.0.1", "volt0", 21212),
                (1, "10.0.0.2", "volt1", 21212)])]),
            "@Statistics": _FakeResponse(topo_tables([
                [0, "0:0,1:0", "0:0"], [1, "0:1,1:1", "1:1"]])),
            "@SystemCatalog": _FakeResponse([procedures_table([
                ("Votes.insert", sp_json())])]),
        }

    def test_refresh_builds_and_swaps_snapshot(self):
        order = []
        client = self._client(self._good_responses(), record=order)
        try:
            self.assertEqual(client.topology, TopologySnapshot.empty())
            snap = client.refresh_topology()
            # Issued in the documented order.
            self.assertEqual(order, ["@SystemInformation", "@Statistics",
                                     "@SystemCatalog"])
            # The stored snapshot is the one returned, fully populated.
            self.assertIs(client.topology, snap)
            self.assertEqual(snap.hosts[1].ip, "10.0.0.2")
            self.assertEqual(snap.partitions[1].leader_host_id, 1)
            self.assertTrue(snap.procedures["Votes.insert"].single_partition)
            self.assertEqual(snap.hashinator_config, DEFAULT_HASHCONFIG)
            # The config was decoded into a working hashinator on the snapshot.
            self.assertEqual(
                snap.hashinator.partition_for_token(-1), 0)
            self.assertEqual(
                snap.hashinator.partition_for_token(0), 1)
        finally:
            client.close()

    def test_failed_discovery_raises_and_leaves_snapshot(self):
        responses = self._good_responses()
        responses["@Statistics"] = _FakeResponse(
            [], status=-2, statusString="cluster busy")
        client = self._client(responses)
        try:
            with self.assertRaises(VoltClientError):
                client.refresh_topology()
            # The previous (empty) snapshot is untouched by the failed refresh.
            self.assertEqual(client.topology, TopologySnapshot.empty())
        finally:
            client.close()

    def test_concurrent_refresh_is_safe(self):
        # Many threads refreshing at once must never crash or leave a torn map;
        # each builds its own snapshot and the last atomic swap wins.
        client = self._client(self._good_responses())
        try:
            errors = []

            def refresh():
                try:
                    client.refresh_topology()
                except Exception as e:  # noqa: BLE001
                    errors.append(e)

            threads = [threading.Thread(target=refresh) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=2.0)
            self.assertEqual(errors, [])
            # The result is a complete, consistent snapshot (never half-built).
            self.assertEqual(len(client.topology.hosts), 2)
            self.assertEqual(len(client.topology.partitions), 2)
        finally:
            client.close()


def build_table_response(handle, tables, status=1):
    """A full length-prefixed response frame carrying serialized VoltTables --
    what a server push (or any table-bearing response) looks like on the wire.
    Unlike build_response, the default status is SUCCESS, as a push's is."""
    fser = FastSerializer()  # socketless: bytes accumulate in the write buffer
    fser.writeByte(0)              # version
    fser.writeInt64(handle)        # clientHandle
    fser.writeByte(0)              # present-fields bitmap: none set
    fser.writeByte(status)
    fser.writeByte(0)              # appStatus
    fser.writeInt32(0)             # roundtripTime
    fser.writeInt16(len(tables))
    for table in tables:
        table.fser = fser
        table.writeToSerializer()
    fser.prependLength()
    return fser.getRawBytes()


def sent_invocations(fake):
    """Parse (proc_name, handle) from each invocation frame the client wrote to
    a FakeSocket, in order. Works for version-0 and version-2 frames alike (the
    name and handle sit at the same offsets); parameters are not decoded."""
    data = bytes(fake.sent)
    out = []
    off = 0
    while off + 4 <= len(data):
        (frame_len,) = struct.unpack_from(">i", data, off)
        frame = data[off + 4:off + 4 + frame_len]
        off += 4 + frame_len
        (name_len,) = struct.unpack_from(">i", frame, 1)
        name = frame[5:5 + name_len].decode()
        (handle,) = struct.unpack_from(">q", frame, 5 + name_len)
        out.append((name, handle))
    return out


def find_invocation(fake, proc_name):
    """The (proc_name, handle) of the first invocation of proc_name written to
    the fake, or None."""
    for name, handle in sent_invocations(fake):
        if name == proc_name:
            return (name, handle)
    return None


def seeded_snapshot():
    """A fully-populated snapshot for push tests to mutate against."""
    config = DEFAULT_HASHCONFIG
    return TopologySnapshot(
        hosts={0: HostInfo(0, "10.0.0.1", "volt0", 21212),
               1: HostInfo(1, "10.0.0.2", "volt1", 21212)},
        partitions={0: PartitionInfo(0, 0, (0, 1)),
                    1: PartitionInfo(1, 1, (0, 1))},
        procedures={"Votes.insert": ProcedureInfo(
            "Votes.insert", True, False, False, 0, FS.VOLTTYPE_BIGINT)},
        hashinator_config=config,
        hashinator=ElasticHashinator.from_bytes(config))


class TestTopologyPushHandling(unittest.TestCase):
    """Server pushes arrive as unsolicited responses on the reserved handles and
    are folded into the snapshot on the reader thread. Fed through the fake
    socket, so the whole path -- reader, demux bypass, parse, swap -- is real."""

    def test_topo_push_updates_partitions_and_flags_refresh(self):
        client, conn, fake = make_client_with_fake()
        try:
            before = seeded_snapshot()
            client._topology = before
            # Partition 0's leadership migrates to host 1; the ring is unchanged.
            fake.feed(build_table_response(ASYNC_TOPO_HANDLE, topo_tables(
                [[0, "0:0,1:0", "1:0"], [1, "0:1,1:1", "1:1"]])))
            self.assertTrue(wait_for(
                lambda: client.topology.partitions[0].leader_host_id == 1))
            after = client.topology
            # Hosts and procedures ride along untouched; the identical config
            # blob means the decoded hashinator is reused, not rebuilt.
            self.assertIs(after.hosts, before.hosts)
            self.assertIs(after.procedures, before.procedures)
            self.assertIs(after.hashinator, before.hashinator)
            # The push carries no addresses: a full refresh is flagged for the
            # reconnect thread so membership can follow.
            self.assertTrue(client._refresh_needed.is_set())
        finally:
            client.close()

    def test_topo_push_with_new_ring_rebuilds_hashinator(self):
        client, conn, fake = make_client_with_fake()
        try:
            before = seeded_snapshot()
            client._topology = before
            new_ring = raw_config([(-2 ** 31, 1)])  # everything to partition 1
            fake.feed(build_table_response(ASYNC_TOPO_HANDLE, topo_tables(
                [[0, "0:0", "0:0"], [1, "1:1", "1:1"]], hashconfig=new_ring)))
            self.assertTrue(wait_for(
                lambda: client.topology.hashinator_config == new_ring))
            after = client.topology
            self.assertIsNot(after.hashinator, before.hashinator)
            self.assertEqual(after.hashinator.partition_for_token(-1), 1)
        finally:
            client.close()

    def test_unusable_topo_push_is_ignored(self):
        client, conn, fake = make_client_with_fake()
        try:
            before = seeded_snapshot()
            client._topology = before
            # A failed push (no tables, bad status) and a single-table push
            # (no hashinator table) must both leave the snapshot untouched.
            fake.feed(build_response(ASYNC_TOPO_HANDLE, status=-2))
            fake.feed(build_table_response(ASYNC_TOPO_HANDLE, topo_tables(
                [[0, "1:0", "1:0"]], hashconfig=None)))
            # Prove both frames were consumed: a later valid push lands.
            fake.feed(build_table_response(ASYNC_TOPO_HANDLE, topo_tables(
                [[0, "0:0,1:0", "1:0"]])))
            self.assertTrue(wait_for(
                lambda: client.topology.partitions[0].leader_host_id == 1))
            self.assertEqual(len(client.topology.partitions), 1)
        finally:
            client.close()

    def test_proc_push_updates_procedures_only(self):
        client, conn, fake = make_client_with_fake()
        try:
            before = seeded_snapshot()
            client._topology = before
            fake.feed(build_table_response(ASYNC_PROC_HANDLE, [
                procedures_table([("New.proc", sp_json(
                    param_type=FS.VOLTTYPE_STRING))])]))
            self.assertTrue(wait_for(
                lambda: "New.proc" in client.topology.procedures))
            after = client.topology
            self.assertNotIn("Votes.insert", after.procedures)  # replaced, not merged
            self.assertIs(after.partitions, before.partitions)
            self.assertIs(after.hashinator, before.hashinator)
            # A procedure push is complete in itself: no follow-up refresh.
            self.assertFalse(client._refresh_needed.is_set())
        finally:
            client.close()


class TestSubscription(unittest.TestCase):
    """The @Subscribe TOPOLOGY lifecycle: sent on one connection, confirmed
    asynchronously, moved when its connection dies, paced when declined. Driven
    by calling _ensure_subscription directly (the maintenance loop is parked by
    make_client_with_fake), so each step is deterministic."""

    def test_subscribe_sent_claimed_and_confirmed(self):
        client, conn, fake = make_client_with_fake()
        try:
            client._ensure_subscription()
            sub = wait_for(lambda: find_invocation(fake, "@Subscribe"))
            self.assertIsNotNone(sub)
            self.assertIn(b"TOPOLOGY", bytes(fake.sent))
            self.assertIs(client._subscribed_conn, conn)
            self.assertFalse(client._refresh_needed.is_set())
            # Confirmation flags the post-subscribe refresh (fetch strictly
            # after the registration is active, so no update can be lost).
            fake.feed(build_response(sub[1], status=1))
            self.assertTrue(wait_for(lambda: client._refresh_needed.is_set()))
            self.assertIs(client._subscribed_conn, conn)
            # A healthy subscription is left alone by later passes.
            client._ensure_subscription()
            self.assertEqual(
                len([1 for name, _ in sent_invocations(fake)
                     if name == "@Subscribe"]), 1)
        finally:
            client.close()

    def test_declined_subscribe_paces_retry(self):
        client, conn, fake = make_client_with_fake()
        try:
            client._ensure_subscription()
            sub = wait_for(lambda: find_invocation(fake, "@Subscribe"))
            with self.assertLogs("voltclient", level="WARNING") as logged:
                fake.feed(build_response(sub[1], status=-2))  # e.g. paused
                self.assertTrue(wait_for(
                    lambda: client._subscribed_conn is None))
            self.assertTrue(any("declined" in line for line in logged.output))
            self.assertGreater(client._subscribe_next_attempt, time.monotonic())
            # Within the pacing window, no new attempt is made.
            client._ensure_subscription()
            self.assertEqual(
                len([1 for name, _ in sent_invocations(fake)
                     if name == "@Subscribe"]), 1)
            # Once the window passes, it tries again.
            client._subscribe_next_attempt = 0.0
            client._ensure_subscription()
            wait_for(lambda: len([1 for name, _ in sent_invocations(fake)
                                  if name == "@Subscribe"]) == 2)
            self.assertEqual(
                len([1 for name, _ in sent_invocations(fake)
                     if name == "@Subscribe"]), 2)
        finally:
            client.close()

    def test_subscription_moves_when_its_connection_dies(self):
        client, conn1, fake1 = make_client_with_fake()
        try:
            fake2 = FakeSocket()
            fser2 = FastSerializer()
            fser2.socket = fake2
            target2 = client._add_target("fake2", 21212)
            conn2 = client._register_connection(fser2, "fake2", 21212, target2)

            client._subscribed_conn = conn1  # as if subscribed on conn1
            fake1.close()  # the subscribed node dies
            self.assertTrue(wait_for(lambda: client._subscribed_conn is None))
            # The next maintenance pass re-subscribes on the surviving node.
            client._ensure_subscription()
            self.assertIs(client._subscribed_conn, conn2)
            self.assertTrue(wait_for(lambda: find_invocation(fake2, "@Subscribe")))
            self.assertIsNone(find_invocation(fake1, "@Subscribe"))
        finally:
            client.close()

    def test_lost_subscribe_call_releases_claim(self):
        # The connection dies after @Subscribe is sent but before any response:
        # the failed future must release the claim so another pass can retry.
        client, conn, fake = make_client_with_fake()
        try:
            client._ensure_subscription()
            self.assertTrue(wait_for(lambda: find_invocation(fake, "@Subscribe")))
            self.assertIs(client._subscribed_conn, conn)
            fake.close()
            self.assertTrue(wait_for(lambda: client._subscribed_conn is None))
        finally:
            client.close()

    def test_loop_establishes_subscription_and_refreshes(self):
        # End to end on the real maintenance loop: subscribe, confirm, and the
        # flagged refresh fetches all three discovery results into the snapshot.
        client, conn, fake = make_client_with_fake(reconnect_interval=0.02)
        try:
            sub = wait_for(lambda: find_invocation(fake, "@Subscribe"))
            self.assertIsNotNone(sub)
            fake.feed(build_response(sub[1], status=1))

            # The overview advertises the fake's own address, so membership
            # convergence keeps the connection rather than retiring it in
            # favor of a dial-able advertised node.
            for proc, tables in (
                    ("@SystemInformation", [overview_table(
                        [(0, "fake", "volt0", 21212)])]),
                    ("@Statistics", topo_tables([[0, "0:0", "0:0"]])),
                    ("@SystemCatalog", [procedures_table(
                        [("Votes.insert", sp_json())])])):
                inv = wait_for(lambda: find_invocation(fake, proc))
                self.assertIsNotNone(inv, "expected a %s discovery call" % proc)
                fake.feed(build_table_response(inv[1], tables))

            self.assertTrue(wait_for(
                lambda: client.topology.hosts.get(0) is not None))
            self.assertEqual(client.topology.partitions[0].leader_host_id, 0)
            self.assertIn("Votes.insert", client.topology.procedures)
            self.assertIs(client._subscribed_conn, conn)
        finally:
            client.close()


def make_topology_client(host_ids, unroutable=(), **kwargs):
    """A client whose _open_serializer hands out FakeSocket-backed serializers,
    each tagged with the host id mapped to the dialed address
    (`host_ids`: {(host, port): host_id}). Addresses in `unroutable` raise on
    dial, standing in for an advertised interface the client cannot reach.
    The subscription step is stubbed out of the reconnect loop, as in
    make_reconnecting_client. Returns (client, fakes-by-address)."""
    client = VoltClient(**kwargs)
    client._ensure_subscription = lambda: None
    fakes = {}

    def fake_open(host, port):
        if (host, port) in unroutable:
            raise OSError("simulated unroutable address %s:%s" % (host, port))
        fake = FakeSocket()
        fakes[(host, port)] = fake
        fser = FastSerializer()
        fser.socket = fake
        fser.hostId = host_ids.get((host, port))
        return fser

    client._open_serializer = fake_open
    return client, fakes


class TestMembershipConvergence(unittest.TestCase):
    """Discovery drives pool membership authoritatively, reconciled by host id:
    dial every node's advertised address, keep a seed only until its node is
    reachable that way, and never strand the client when an advertised interface
    is unroutable (AC#3). Reconnect interval is large so the loop stays out of the
    way and each test drives convergence deterministically."""

    def _snapshot(self, hosts):
        return TopologySnapshot(hosts={h.host_id: h for h in hosts},
                                partitions={}, procedures={}, hashinator_config=None,
                                hashinator=None)

    def test_no_discovery_leaves_seeds_untouched(self):
        client, _ = make_topology_client({}, reconnect_interval=100)
        try:
            client._seeds = {("volt1", 21212)}
            client._add_target("volt1", 21212)
            client._converge_membership()  # empty snapshot -> no-op
            self.assertEqual(set(client._targets), {("volt1", 21212)})
        finally:
            client.close()

    def test_seed_migrates_to_advertised_then_retires(self):
        client, _ = make_topology_client(
            {("volt1", 21212): 0, ("10.0.0.1", 21212): 0}, reconnect_interval=100)
        try:
            client._seeds = {("volt1", 21212)}
            seed_t = client._add_target("volt1", 21212)
            client._connect_target(seed_t)          # seed connects as host id 0
            seed_conn = seed_t.connection
            client._topology = self._snapshot([HostInfo(0, "10.0.0.1", "volt0", 21212)])

            client._converge_membership()
            # advertised address is dialed; seed kept (advertised conn not up yet)
            self.assertIn(("10.0.0.1", 21212), client._targets)
            self.assertIn(("volt1", 21212), client._targets)

            # advertised connection to the same host id comes up...
            client._connect_target(client._targets[("10.0.0.1", 21212)])
            client._converge_membership()
            # ...so the seed leaves the target set...
            self.assertIn(("10.0.0.1", 21212), client._targets)
            self.assertNotIn(("volt1", 21212), client._targets)
            # ...and its now-undesired connection is closed.
            client._close_undesired()
            self.assertNotIn(seed_conn, client._connections)
        finally:
            client.close()

    def test_unroutable_advertised_keeps_seed(self):
        client, _ = make_topology_client(
            {("volt1", 21212): 0}, unroutable={("10.0.0.1", 21212)},
            reconnect_interval=100)
        try:
            client._seeds = {("volt1", 21212)}
            seed_t = client._add_target("volt1", 21212)
            client._connect_target(seed_t)
            client._topology = self._snapshot([HostInfo(0, "10.0.0.1", "volt0", 21212)])

            client._converge_membership()
            self.assertIn(("10.0.0.1", 21212), client._targets)  # advertised desired
            self.assertIn(("volt1", 21212), client._targets)      # seed kept

            # The advertised interface cannot be reached...
            with self.assertRaises(OSError):
                client._connect_target(client._targets[("10.0.0.1", 21212)])
            # ...so after reconciling again the seed remains the live fallback.
            client._converge_membership()
            self.assertIn(("volt1", 21212), client._targets)
            self.assertIn(seed_t.connection, client._connections)
        finally:
            client.close()

    def test_seed_is_advertised_address_no_duplicate(self):
        # Seeded with the exact advertised address: one target, nothing to retire.
        client, _ = make_topology_client({("10.0.0.1", 21212): 0},
                                         reconnect_interval=100)
        try:
            client._seeds = {("10.0.0.1", 21212)}
            client._connect_target(client._add_target("10.0.0.1", 21212))
            client._topology = self._snapshot([HostInfo(0, "10.0.0.1", "volt0", 21212)])
            client._converge_membership()
            self.assertEqual(set(client._targets), {("10.0.0.1", 21212)})
        finally:
            client.close()

    def test_discovery_expands_to_all_cluster_nodes(self):
        # One seed; discovery expands membership to every advertised node.
        client, _ = make_topology_client({("volt1", 21212): 0}, reconnect_interval=100)
        try:
            client._seeds = {("volt1", 21212)}
            client._connect_target(client._add_target("volt1", 21212))
            client._topology = self._snapshot([
                HostInfo(0, "10.0.0.1", "volt-a", 21212),
                HostInfo(1, "10.0.0.2", "volt-b", 21212),
                HostInfo(2, "10.0.0.3", "volt-c", 21212)])
            client._converge_membership()
            for ip in ("10.0.0.1", "10.0.0.2", "10.0.0.3"):
                self.assertIn((ip, 21212), client._targets)
        finally:
            client.close()

    def test_retired_seed_does_not_resurrect(self):
        # The live-cluster case that first exposed the bug: seeded on "localhost"
        # but the node advertises a different address for the same host id. Once
        # migrated, repeated convergence must NOT re-dial the retired seed.
        client, _ = make_topology_client(
            {("localhost", 21212): 0, ("10.0.0.1", 21212): 0}, reconnect_interval=100)
        try:
            client._seeds = {("localhost", 21212)}
            seed_t = client._add_target("localhost", 21212)
            client._connect_target(seed_t)
            seed_conn = seed_t.connection
            client._topology = self._snapshot([HostInfo(0, "10.0.0.1", "volt0", 21212)])

            client._converge_membership()                 # add advertised, keep seed
            client._connect_target(client._targets[("10.0.0.1", 21212)])
            client._converge_membership()                 # advertised up -> retire
            client._close_undesired()

            self.assertNotIn(("localhost", 21212), client._targets)
            self.assertNotIn(("localhost", 21212), client._seeds)  # retired for good
            self.assertNotIn(seed_conn, client._connections)
            # Further passes must not bring the seed back (no oscillation).
            for _ in range(3):
                client._converge_membership()
                client._close_undesired()
            self.assertEqual(set(client._targets), {("10.0.0.1", 21212)})
        finally:
            client.close()

    def test_undesired_connection_drains_before_close(self):
        # A connection whose node is no longer a target is closed only once it has
        # no in-flight calls, so retirement never fails a call already on the wire.
        client, _ = make_topology_client({("a", 21212): 5}, reconnect_interval=100)
        try:
            conn = client._connect_target(client._add_target("a", 21212))
            client._update_targets([])  # no targets -> conn is undesired
            call = voltclient._Call(999, concurrent.futures.Future(),
                                    time.monotonic() + 100, "P")
            conn._pending[999] = call
            client._close_undesired()
            self.assertIn(conn, client._connections, "closed while still draining")
            conn._pending.clear()       # call completed
            client._close_undesired()
            self.assertNotIn(conn, client._connections, "not closed after draining")
        finally:
            client.close()

    def test_pick_connection_skips_undesired(self):
        # New calls go only to connections whose node is still a target, so a
        # draining connection receives none.
        client, _ = make_topology_client({("a", 21212): 1, ("b", 21212): 2},
                                         reconnect_interval=100)
        try:
            client._connect_target(client._add_target("a", 21212))
            client._connect_target(client._add_target("b", 21212))
            client._update_targets([("b", 21212)])  # 'a' now undesired (draining)
            picked = {(c.host, c.port) for c in
                      (client._pick_connection() for _ in range(10))}
            self.assertEqual(picked, {("b", 21212)})
        finally:
            client.close()


class TestConnectTimeDiscovery(unittest.TestCase):
    """VoltClient discovers topology during construction (AC#1: 'on connect'),
    and a discovery failure there must not break construction."""

    class _FakeSeedClient(VoltClient):
        """A VoltClient whose seed connect uses a FakeSocket, so construction --
        and thus the connect-time discovery -- runs with no real network."""

        def _open_serializer(self, host, port):
            fser = FastSerializer()
            fser.socket = FakeSocket()
            fser.hostId = 0
            return fser

    def test_refresh_called_on_connect(self):
        calls = []

        class _Client(self._FakeSeedClient):
            def refresh_topology(self, timeout=None):
                calls.append(True)
                return TopologySnapshot.empty()

        client = _Client(hosts=["seed"], reconnect_interval=100)
        try:
            self.assertEqual(len(calls), 1)  # discovery attempted once, on connect
        finally:
            client.close()

    def test_discovery_failure_does_not_break_construction(self):
        class _Client(self._FakeSeedClient):
            def refresh_topology(self, timeout=None):
                raise VoltClientError("cluster busy")

        # The failed discovery is logged (captured, so it doesn't print) but
        # construction still succeeds and leaves an empty snapshot in place.
        with self.assertLogs("voltclient", level="WARNING") as logged:
            client = _Client(hosts=["seed"], reconnect_interval=100)
        try:
            self.assertFalse(client._closed)
            self.assertEqual(client.topology, TopologySnapshot.empty())
            self.assertTrue(any("discovery failed" in line for line in logged.output))
        finally:
            client.close()


def routing_snapshot(procedures=None, partitions=None,
                     hashconfig=DEFAULT_HASHCONFIG):
    """A snapshot for routing tests. ``hosts`` stays empty so membership
    convergence is a no-op and the connections the test wires up are left
    alone; routing itself never consults ``hosts``."""
    hashinator = (ElasticHashinator.from_bytes(hashconfig)
                  if hashconfig is not None else None)
    return TopologySnapshot(hosts={}, partitions=partitions or {},
                            procedures=procedures or {},
                            hashinator_config=hashconfig, hashinator=hashinator)


def make_routed_client(snapshot, host_ids):
    """A client connected to every address in ``host_ids`` ({(host, port):
    host_id}, each on a FakeSocket tagged with its host id) with ``snapshot``
    installed. Returns (client, fakes-by-address)."""
    client, fakes = make_topology_client(host_ids, reconnect_interval=100)
    for host, port in host_ids:
        client._connect_target(client._add_target(host, port))
    client._topology = snapshot
    return client, fakes


class TestAffinityRouting(unittest.TestCase):
    """Client affinity picks the connection a call should be sent on: a
    single-partition procedure goes to its partition's leader, a
    multi-partition procedure to the MP initiator's host, and everything the
    client cannot confidently place round-robins -- affinity degrades, it
    never fails a call."""

    ADDR0, ADDR1 = ("volt0", 21212), ("volt1", 21212)
    HOST_IDS = {ADDR0: 0, ADDR1: 1}
    # Two nodes, two partitions, each node leading one; the MP initiator's
    # pseudo-partition lives on host 1.
    PARTITIONS = {0: PartitionInfo(0, 0, (0, 1)),
                  1: PartitionInfo(1, 1, (0, 1)),
                  MP_INIT_PID: PartitionInfo(MP_INIT_PID, 1, (1,))}
    PROCEDURES = {
        "Votes.insert": ProcedureInfo("Votes.insert", True, False, False,
                                      0, FS.VOLTTYPE_BIGINT),
        "ByName": ProcedureInfo("ByName", True, False, False,
                                0, FS.VOLTTYPE_STRING),
        "Summary": ProcedureInfo("Summary", False, True, False, -1, -1),
        "Comp": ProcedureInfo("Comp", False, False, True, -1, -1),
    }

    @classmethod
    def setUpClass(cls):
        # Find one BIGINT value hashing to each partition of DEFAULT_RING, so
        # the tests exercise both leaders without hard-coding hash outputs
        # (hash correctness is pinned by the Java parity tests above).
        hashinator = ElasticHashinator.from_bytes(DEFAULT_HASHCONFIG)
        cls.value_for = {}
        for v in range(64):
            cls.value_for.setdefault(
                hashinator.get_partition_for_parameter(FS.VOLTTYPE_BIGINT, v), v)
            if len(cls.value_for) == 2:
                break
        assert set(cls.value_for) == {0, 1}

    # -- the routing decision (_preferred_host needs only a snapshot and the
    # warned-procedures set, so it is tested without a client)

    def stub_client(self, snapshot):
        return type("Stub", (), {"_topology": snapshot,
                                 "_affinity_warned": set()})()

    def preferred(self, snapshot, proc_name, params):
        return VoltClient._preferred_host(self.stub_client(snapshot),
                                          proc_name, params)

    def full_snapshot(self, **kwargs):
        kwargs.setdefault("procedures", self.PROCEDURES)
        kwargs.setdefault("partitions", self.PARTITIONS)
        return routing_snapshot(**kwargs)

    def test_single_partition_prefers_partition_leader(self):
        snap = self.full_snapshot()
        for partition, value in self.value_for.items():
            self.assertEqual(
                self.preferred(snap, "Votes.insert", [value]),
                self.PARTITIONS[partition].leader_host_id)

    def test_multi_partition_prefers_mpi_host(self):
        self.assertEqual(self.preferred(self.full_snapshot(), "Summary", []), 1)

    def test_multi_partition_needs_no_hashinator(self):
        # MP routing involves no hashing, so it works even when the cluster's
        # hash scheme is unsupported (where SP routing must degrade).
        snap = self.full_snapshot(hashconfig=None)
        self.assertEqual(self.preferred(snap, "Summary", []), 1)
        self.assertIsNone(self.preferred(snap, "Votes.insert", [7]))

    def test_compound_round_robins(self):
        self.assertIsNone(self.preferred(self.full_snapshot(), "Comp", [7]))

    def test_unknown_procedure_round_robins(self):
        # System procedures are never in the catalog, so @AdHoc lands here --
        # deliberately: ad-hoc planning load should spread across nodes.
        snap = self.full_snapshot()
        self.assertIsNone(self.preferred(snap, "@AdHoc", ["select 1"]))
        self.assertIsNone(self.preferred(snap, "NoSuchProc", [7]))

    def test_missing_partition_param_round_robins(self):
        snap = self.full_snapshot()
        self.assertIsNone(self.preferred(snap, "Votes.insert", []))
        self.assertIsNone(self.preferred(snap, "Votes.insert", None))

    def test_unhashable_value_round_robins_and_warns(self):
        # An int where the catalog says STRING cannot hash (VoltTypeError):
        # no affinity opinion, send anyway, let the server route it -- and
        # warn, because a mistyped partition parameter for a STRING column is
        # the intermittent-mispartitioned-write trap.
        with self.assertLogs("voltclient", level="WARNING") as logged:
            self.assertIsNone(self.preferred(self.full_snapshot(), "ByName", [5]))
        self.assertTrue(any("ByName" in line for line in logged.output))

    def test_unhashable_value_warns_once_per_procedure(self):
        stub = self.stub_client(self.full_snapshot())
        with self.assertLogs("voltclient", level="WARNING") as logged:
            for _ in range(3):
                self.assertIsNone(
                    VoltClient._preferred_host(stub, "ByName", [5]))
            # A different procedure gets its own (single) warning.
            self.assertIsNone(
                VoltClient._preferred_host(stub, "Votes.insert", ["not-int"]))
        by_name = [line for line in logged.output if "ByName" in line]
        votes = [line for line in logged.output if "Votes.insert" in line]
        self.assertEqual((len(by_name), len(votes)), (1, 1), logged.output)

    def test_unknown_leader_round_robins(self):
        # Hashing worked but the partition is not in the topology map.
        snap = self.full_snapshot(partitions={})
        self.assertIsNone(self.preferred(snap, "Votes.insert", [7]))

    def test_empty_snapshot_round_robins(self):
        snap = TopologySnapshot.empty()
        self.assertIsNone(self.preferred(snap, "Votes.insert", [7]))

    # -- end to end through call_async: bytes land on the right connection --

    def sent_on(self, fakes):
        return {addr: len(fake.sent) for addr, fake in fakes.items()}

    def test_call_lands_on_leader_connection(self):
        client, fakes = make_routed_client(self.full_snapshot(), self.HOST_IDS)
        try:
            leaders = {0: self.ADDR0, 1: self.ADDR1}
            for partition, value in self.value_for.items():
                before = self.sent_on(fakes)
                client.call_async("Votes.insert", [FS.VOLTTYPE_BIGINT], [value])
                wait_for(lambda: self.sent_on(fakes) != before)
                after = self.sent_on(fakes)
                grew = {a for a in fakes if after[a] > before[a]}
                self.assertEqual(grew, {leaders[partition]})
            stats = client.affinity_stats()
            self.assertEqual(stats[self.ADDR0], AffinityStats(1, 0))
            self.assertEqual(stats[self.ADDR1], AffinityStats(1, 0))
        finally:
            client.close()

    def test_unknown_proc_round_robins_across_connections(self):
        client, fakes = make_routed_client(self.full_snapshot(), self.HOST_IDS)
        try:
            for _ in range(4):
                client.call_async("@AdHoc", [FS.VOLTTYPE_STRING], ["select 1"])
            stats = client.affinity_stats()
            self.assertEqual(stats[self.ADDR0], AffinityStats(0, 2))
            self.assertEqual(stats[self.ADDR1], AffinityStats(0, 2))
        finally:
            client.close()

    def test_disconnected_leader_falls_back_to_round_robin(self):
        # Only host 0 is connected; a call for host 1's partition must still
        # go out -- on host 0, counted as round-robin.
        client, fakes = make_routed_client(self.full_snapshot(),
                                           {self.ADDR0: 0})
        try:
            value = self.value_for[1]  # partition 1, led by (absent) host 1
            client.call_async("Votes.insert", [FS.VOLTTYPE_BIGINT], [value])
            self.assertTrue(wait_for(lambda: len(fakes[self.ADDR0].sent) > 0))
            self.assertEqual(client.affinity_stats()[self.ADDR0],
                             AffinityStats(0, 1))
        finally:
            client.close()

    def test_destination_stamped_call_lands_on_leader(self):
        # A destination-stamped call routes by partition -> leader directly;
        # hashconfig=None proves no hashinator is involved. The frame on the
        # wire is a version-2 invocation.
        client, fakes = make_routed_client(
            self.full_snapshot(hashconfig=None), self.HOST_IDS)
        try:
            leaders = {0: self.ADDR0, 1: self.ADDR1}
            for partition, addr in leaders.items():
                before = self.sent_on(fakes)
                client._call_async("SomeDirected", None, None, None,
                                   partition_destination=partition)
                wait_for(lambda: self.sent_on(fakes) != before)
                after = self.sent_on(fakes)
                grew = {a for a in fakes if after[a] > before[a]}
                self.assertEqual(grew, {addr})
                frame = bytes(fakes[addr].sent[before[addr]:])
                self.assertEqual(frame[4], 2)  # invocation version byte
            stats = client.affinity_stats()
            self.assertEqual(stats[self.ADDR0], AffinityStats(1, 0))
            self.assertEqual(stats[self.ADDR1], AffinityStats(1, 0))
        finally:
            client.close()

    def test_destination_without_leader_connection_round_robins(self):
        # The named partition still executes the call wherever it lands (the
        # server forwards by the envelope's destination), so an absent leader
        # connection degrades to round-robin exactly like hashed affinity.
        client, fakes = make_routed_client(
            self.full_snapshot(hashconfig=None), {self.ADDR0: 0})
        try:
            client._call_async("SomeDirected", None, None, None,
                               partition_destination=1)  # led by absent host 1
            client._call_async("SomeDirected", None, None, None,
                               partition_destination=9)  # not in the topology
            self.assertEqual(client.affinity_stats()[self.ADDR0],
                             AffinityStats(0, 2))
        finally:
            client.close()


def keys_table(pairs):
    """The @GetPartitionKeys table: one (PARTITION_ID, PARTITION_KEY) row per
    partition."""
    return make_table([(FS.VOLTTYPE_INTEGER, "PARTITION_ID"),
                       (FS.VOLTTYPE_INTEGER, "PARTITION_KEY")],
                      [[pid, key] for pid, key in pairs])


class TestCallAllPartitions(unittest.TestCase):
    """call_all_partitions fans one invocation per partition out through the
    ordinary async path and aggregates the outcomes without swallowing any
    partition's failure. The transport is stubbed at the call/_call_async seam
    (as in TestRefreshTopology); the wire path is covered by the live e2e."""

    KEYS = [(0, 0), (1, 26), (2, 9)]

    def _client(self, pairs=None, keys_status=voltclient._STATUS_SUCCESS,
                keys_delay=0.0):
        """A VoltClient (no connections) whose call serves @GetPartitionKeys
        from a canned table and whose _call_async records each dispatch and
        returns a Future the test completes by hand."""
        client = VoltClient()  # no seed hosts: no real connections
        # (proc_name, param_types, params, timeout, partition_destination)
        client.dispatched = []
        client.futures = []
        client.keys_timeout = None
        table = keys_table(self.KEYS if pairs is None else pairs)

        def fake_call(proc_name, param_types=None, params=None, timeout=None):
            self.assertEqual(proc_name, "@GetPartitionKeys")
            self.assertEqual(params, ["INTEGER"])
            client.keys_timeout = timeout
            if keys_delay:
                time.sleep(keys_delay)
            return _FakeResponse([table], status=keys_status,
                                 statusString="canned failure")

        def fake_call_async(proc_name, param_types=None, params=None,
                            timeout=None, partition_destination=None):
            client.dispatched.append((proc_name, param_types, params, timeout,
                                      partition_destination))
            future = concurrent.futures.Future()
            client.futures.append(future)
            return future

        client.call = fake_call
        client._call_async = fake_call_async
        return client

    def test_one_call_per_partition_with_key_prepended(self):
        # The catalog is empty here, so this also proves the fan-out works
        # with no topology discovered (validation and affinity both degrade).
        client = self._client()
        try:
            outer = client.call_all_partitions_async(
                "CountByCohort", [FS.VOLTTYPE_STRING], ["gold"], timeout=10)
            self.assertEqual(len(client.dispatched), len(self.KEYS))
            for (pid, key), (proc, types, params, _, dest) in zip(
                    self.KEYS, client.dispatched):
                self.assertEqual(proc, "CountByCohort")
                self.assertEqual(tuple(types),
                                 (FS.VOLTTYPE_INTEGER, FS.VOLTTYPE_STRING))
                self.assertEqual(tuple(params), (key, "gold"))
                # Every leg names its partition on the invocation envelope.
                self.assertEqual(dest, pid)
            self.assertFalse(outer.done())

            responses = [_FakeResponse([]) for _ in client.futures]
            for future, response in zip(client.futures, responses):
                future.set_result(response)
            results = outer.result(timeout=2)
            self.assertEqual([(r.partition_id, r.partition_key) for r in results],
                             self.KEYS)
            for result, response in zip(results, responses):
                self.assertTrue(result.ok)
                self.assertIs(result.response, response)
                self.assertIsNone(result.error)
        finally:
            client.close()

    def test_results_sorted_by_partition_id(self):
        client = self._client(pairs=[(2, 9), (0, 0), (1, 26)])
        try:
            outer = client.call_all_partitions_async("P")
            for future in client.futures:
                future.set_result(_FakeResponse([]))
            self.assertEqual(
                [(r.partition_id, r.partition_key)
                 for r in outer.result(timeout=2)],
                [(0, 0), (1, 26), (2, 9)])
        finally:
            client.close()

    def test_partial_failure_is_surfaced_not_swallowed(self):
        client = self._client()
        try:
            outer = client.call_all_partitions_async("P")
            good = _FakeResponse([])
            bad = _FakeResponse([], status=-3, statusString="constraint")
            client.futures[0].set_result(good)
            client.futures[1].set_exception(VoltTimeoutError("timed out"))
            client.futures[2].set_result(bad)

            results = outer.result(timeout=2)  # aggregate resolves, not raises
            self.assertTrue(results[0].ok)
            # No server response at all: the exception a plain call would
            # have raised, preserved as-is.
            self.assertFalse(results[1].ok)
            self.assertIsNone(results[1].response)
            self.assertIsInstance(results[1].error, VoltTimeoutError)
            # The server said no: a genuine response with a failure status.
            self.assertFalse(results[2].ok)
            self.assertIs(results[2].response, bad)
            self.assertIsNone(results[2].error)
        finally:
            client.close()

    def test_dispatch_error_is_that_partitions_outcome(self):
        client = self._client()
        inner = client._call_async

        def flaky(proc_name, param_types=None, params=None, timeout=None,
                  partition_destination=None):
            if len(client.dispatched) == 1:  # fail the second dispatch only
                client.dispatched.append((proc_name, param_types, params,
                                          timeout, partition_destination))
                raise VoltNoConnectionsError("no connections")
            return inner(proc_name, param_types, params, timeout,
                         partition_destination)

        client._call_async = flaky
        try:
            outer = client.call_all_partitions_async("P")
            self.assertEqual(len(client.dispatched), 3)  # legs 1+3 dispatched
            for future in client.futures:
                future.set_result(_FakeResponse([]))
            results = outer.result(timeout=2)
            self.assertTrue(results[0].ok)
            self.assertIsInstance(results[1].error, VoltNoConnectionsError)
            self.assertTrue(results[2].ok)
        finally:
            client.close()

    def test_get_partition_keys_failure_raises(self):
        client = self._client(keys_status=-2)
        try:
            with self.assertRaises(VoltClientError):
                client.call_all_partitions_async("P")
            self.assertEqual(client.dispatched, [])
        finally:
            client.close()

    def test_empty_key_set_raises(self):
        client = self._client(pairs=[])
        try:
            with self.assertRaises(VoltClientError):
                client.call_all_partitions_async("P")
            self.assertEqual(client.dispatched, [])
        finally:
            client.close()

    def test_rejects_unsuitable_procedure(self):
        # The catalog knows these procedures cannot run once-per-partition,
        # so the client applies the server's all-partition validation before
        # fetching any keys.
        client = self._client()
        client._topology = routing_snapshot(procedures={
            "MpProc": ProcedureInfo("MpProc", False, False, False, -1, -1),
            "WrongParam": ProcedureInfo("WrongParam", True, False, False,
                                        1, FS.VOLTTYPE_BIGINT),
            "Comp": ProcedureInfo("Comp", False, False, True, -1, -1)})
        try:
            for name in ("MpProc", "WrongParam", "Comp", "@Ping"):
                with self.assertRaises(VoltClientError, msg=name):
                    client.call_all_partitions_async(name)
            self.assertIsNone(client.keys_timeout)  # rejected before any I/O
            self.assertEqual(client.dispatched, [])
        finally:
            client.close()

    def test_directed_procedure_fans_out(self):
        # A directed procedure -- single-partitioned with no partition
        # parameter (the catalog reports -1/-1 for exactly this case) -- is a
        # valid target: each leg names its partition on the invocation
        # envelope, and still prepends the partition key, which the server
        # strips before execution.
        client = self._client()
        client._topology = routing_snapshot(procedures={
            "Directed": ProcedureInfo("Directed", True, False, False, -1, -1)})
        try:
            outer = client.call_all_partitions_async(
                "Directed", [FS.VOLTTYPE_STRING], ["gold"], timeout=10)
            self.assertEqual(
                [(dest, params[0]) for _, _, params, _, dest
                 in client.dispatched],
                self.KEYS)
            for future in client.futures:
                future.set_result(_FakeResponse([]))
            self.assertTrue(all(r.ok for r in outer.result(timeout=2)))
        finally:
            client.close()

    def test_one_timeout_budget_covers_keys_and_legs(self):
        # Time spent fetching the partition keys is drawn from the same
        # deadline the legs get, never added to it.
        client = self._client(keys_delay=0.25)
        try:
            client.call_all_partitions_async("P", timeout=8.0)
            self.assertGreater(client.keys_timeout, 7.9)
            for _, _, _, leg_timeout, _ in client.dispatched:
                self.assertLess(leg_timeout, 7.9)
                self.assertGreater(leg_timeout, 7.0)
        finally:
            client.close()

    def test_sync_wrapper_returns_the_aggregate(self):
        client = self._client()

        def instant(proc_name, param_types=None, params=None, timeout=None,
                    partition_destination=None):
            future = concurrent.futures.Future()
            future.set_result(_FakeResponse([]))
            return future

        client._call_async = instant
        try:
            results = client.call_all_partitions("P", timeout=5)
            self.assertEqual(len(results), len(self.KEYS))
            self.assertTrue(all(isinstance(r, PartitionCallResult) and r.ok
                                for r in results))
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
