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
Topology-aware VoltDB Python client.

``VoltClient`` maintains a connection to one or more cluster nodes and supports
multiple simultaneous in flight procedure calls. It leverages the pre-existing
``voltdbclient`` module (``FastSerializer`` for the socket/auth/SSL/wire codecs,
``VoltResponse`` for response parsing).

Concurrency model:

* Every invocation is stamped with a unique 64-bit *client handle*; the server
  echoes it back untouched, so a ``{handle -> call}`` demux map correlates each
  response with the call that is waiting for it.
* One **reader thread per connection** owns the read path: it reads in large
  chunks, decodes every complete response in the buffer, looks each one up by
  handle, and completes the waiting ``Future``. ``Future`` done-callbacks run on
  this thread, so they should be quick.
* Callers **never touch the read path**. They serialize an invocation off to
  the side and append it to the connection's write queue; one **writer thread
  per connection** sends everything queued with a single ``sendall``. A send
  that fails is reported through the call's ``Future``.
* A single shared **sweeper** thread fails any call whose deadline has passed,
  so a call whose response never arrives fails cleanly rather than hanging.
* Outstanding calls are **bounded** (``max_outstanding``): a caller past the
  cap blocks until a completion frees a slot. The cap limits the client's
  memory and the queue depth it can build on the server; it slows the
  producer but never changes how a call is routed.
* Responses bearing a **reserved handle** are server-pushed messages that answer
  no call; they are routed to a push handler instead of a Future -- this is how
  server-initiated notifications such as topology-change pushes are delivered.
  The client subscribes for those pushes (``@Subscribe TOPOLOGY``) on one pool
  connection and moves the subscription elsewhere if that connection dies, so
  routing metadata tracks the cluster without polling.

``VoltBulkLoader`` loads rows into a table in per-partition batches through
``@LoadSinglepartitionTable`` (one server transaction per batch).

This module targets Python 3.9+ and uses only the standard library.
"""

import array
import datetime
import decimal
import json
import logging
import operator
import queue
import random
import re
import socket
import struct
import threading
import time
import zlib
from bisect import bisect_right
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional

from voltdbclient import FastSerializer, Geography, VoltProcedure, VoltResponse, XYZPoint

__all__ = [
    "VoltClient",
    "VoltBulkLoader",
    "VoltType",
    "Geography",
    "XYZPoint",
    "VoltClientError",
    "VoltConnectionError",
    "VoltNoConnectionsError",
    "VoltTimeoutError",
    "VoltTypeError",
    "ASYNC_PROC_HANDLE",
    "ASYNC_TOPO_HANDLE",
    "AffinityStats",
    "ElasticHashinator",
    "HostInfo",
    "PartitionCallResult",
    "PartitionInfo",
    "ProcedureInfo",
    "TopologySnapshot",
]

log = logging.getLogger("voltclient")

# Reserved client handles, mirroring org.voltdb.common.Constants. The server
# reserves the top of the positive int64 range for pinned/unsolicited traffic;
# normal invocations get handles in [1, MAX_CLIENT_HANDLE]. A response whose
# handle is above that ceiling answers no waiting call and is routed to the push
# handler. Subscribing for topology-change notifications uses ASYNC_TOPO_HANDLE.
#
# The value must not equal FastSerializer.NULL_BIGINT_INDICATOR (Long.MIN_VALUE):
# the legacy VoltResponse reads the handle via readInt64(), which maps that one
# value to None. The reserved handles below sit near Long.MAX_VALUE, so they
# round-trip intact.
_LONG_MAX = 2 ** 63 - 1
PING_HANDLE = _LONG_MAX
ASYNC_TOPO_HANDLE = PING_HANDLE - 1
ASYNC_PROC_HANDLE = PING_HANDLE - 2
ASYNC_GRACEFUL_STOP_HANDLE = PING_HANDLE - 3
SNAPSHOT_STREAMING_CATALOG_HANDLE = PING_HANDLE - 4
SNAPSHOT_STREAMING_TABLE_CHUNK_HANDLE = PING_HANDLE - 5
ASYNC_GRACEFUL_STOP_HANDLE_CANCEL = PING_HANDLE - 7
MAX_CLIENT_HANDLE = PING_HANDLE - 64  # ceiling for normal invocation handles

# The multi-partition initiator's pseudo-partition id (MpInitiator.MP_INIT_PID).
# It shows up in the topology alongside real partitions but is never a hash
# target; it is kept in the map so multi-partition work can be routed to its host.
MP_INIT_PID = 16383

# "No partitioning parameter", matching Java's Procedure.PARAMETER_NONE.
_PARAMETER_NONE = -1

# ClientResponse.SUCCESS: the status a completed procedure reports on success.
_STATUS_SUCCESS = 1

# Default per-call deadline, in seconds, when a call does not specify its own.
_DEFAULT_PROCEDURE_TIMEOUT = 120.0
# How often the maintenance thread scans for overdue calls and idle connections.
_DEFAULT_SWEEP_INTERVAL = 0.5
# How often the reconnect thread reconciles live connections against targets.
_DEFAULT_RECONNECT_INTERVAL = 0.25
# Reconnect backoff bounds (seconds) and the fraction of jitter added to each.
_DEFAULT_INITIAL_BACKOFF = 0.5
_DEFAULT_MAX_BACKOFF = 30.0
_DEFAULT_RECONNECT_JITTER = 0.2
# A connection idle this long (seconds) is pinged; if a ping then goes unanswered
# for the full interval, the connection is declared dead.
_DEFAULT_CONNECTION_RESPONSE_TIMEOUT = 30.0
# Maximum calls in flight at once. A cap on outstanding requests bounds the
# client's memory and, more importantly, the queue depth it can build on the
# server: past the server's saturation point, added concurrency buys no
# throughput and only inflates latency. Callers past the cap block until a
# slot frees.
_DEFAULT_MAX_OUTSTANDING = 3000
# How long to wait before re-attempting @Subscribe after the server *declined*
# it (e.g. paused, in admin mode) -- as opposed to a lost connection, which
# re-subscribes on the next reconnect pass. Matches the Java client's pacing.
_SUBSCRIBE_RETRY_DELAY = 120.0
# How long to wait before re-attempting a push-triggered topology refresh that
# failed, so a struggling cluster is not hammered with discovery calls.
_REFRESH_RETRY_DELAY = 5.0


class VoltType:
    """Parameter type constants for ``call``, ``call_async`` and friends.

    These alias ``FastSerializer.VOLTTYPE_*`` from ``voltdbclient`` (same
    values, same wire encoding) so that an application needs only this module::

        from voltclient import VoltClient, VoltType
        client.call("Vote", [VoltType.BIGINT, VoltType.INTEGER], [5551234567, 2])

    Array parameters use their element type. ``Geography`` and ``XYZPoint``,
    the value classes for the two geospatial types, are re-exported here too.
    """
    NULL = FastSerializer.VOLTTYPE_NULL
    TINYINT = FastSerializer.VOLTTYPE_TINYINT
    SMALLINT = FastSerializer.VOLTTYPE_SMALLINT
    INTEGER = FastSerializer.VOLTTYPE_INTEGER
    BIGINT = FastSerializer.VOLTTYPE_BIGINT
    FLOAT = FastSerializer.VOLTTYPE_FLOAT
    STRING = FastSerializer.VOLTTYPE_STRING
    TIMESTAMP = FastSerializer.VOLTTYPE_TIMESTAMP
    DATE = FastSerializer.VOLTTYPE_DATE
    DECIMAL = FastSerializer.VOLTTYPE_DECIMAL
    MONEY = FastSerializer.VOLTTYPE_MONEY
    VOLTTABLE = FastSerializer.VOLTTYPE_VOLTTABLE
    BOOLEAN = FastSerializer.VOLTTYPE_BOOLEAN
    VARBINARY = FastSerializer.VOLTTYPE_VARBINARY
    GEOGRAPHY_POINT = FastSerializer.VOLTTYPE_GEOGRAPHY_POINT
    GEOGRAPHY = FastSerializer.VOLTTYPE_GEOGRAPHY


class VoltClientError(Exception):
    """Base class for all VoltClient errors."""


class VoltConnectionError(VoltClientError):
    """A connection was lost, or could not be used to complete a call."""


class VoltNoConnectionsError(VoltConnectionError):
    """No live connection to the cluster is available."""


class VoltTimeoutError(VoltClientError):
    """A call's deadline passed before its response arrived."""


class VoltTypeError(VoltClientError):
    """A value cannot be hashed as the declared partitioning type."""


@dataclass
class _Call:
    """Bookkeeping for one in-flight invocation."""
    __slots__ = ("handle", "future", "deadline", "proc_name")
    handle: int
    future: Future
    deadline: float
    proc_name: str


@dataclass
class _Target:
    """A cluster node we want a connection to, plus its reconnect bookkeeping.

    The desired set of targets is the client's declared intent; the reconnect
    thread converges the live connections toward it. ``connection`` is the live
    connection to this node, or None when it needs (re)connecting. ``backoff``
    and ``next_attempt`` drive the bounded exponential retry after a failure.
    """
    host: str
    port: int
    connection: Optional["_Connection"] = None
    backoff: float = 0.0
    next_attempt: float = 0.0


# Building an invocation runs on the calling thread inside send(), one call at a
# time per thread, so each sending thread keeps its own FastSerializer for the
# job and reuses it. That avoids rebuilding the serializer's dispatch tables --
# by far the largest per-call cost -- on every invocation. A reused instance
# needs only its write buffer cleared between calls; the write path touches no
# other serializer state. The scratch count is self-bounding: one per thread
# that ever sends, released when that thread ends.
_scratch = threading.local()


# voltdbclient's array-vs-scalar detection, so the wire format stays in lockstep
# with it (including the GEOGRAPHY_POINT special case).
_as_array = VoltProcedure(None, "", ()).as_array


def _scratch_serializer(input_bom):
    fser = getattr(_scratch, "fser", None)
    if fser is None:
        fser = FastSerializer()  # host=None: no socket, no authentication
        _scratch.fser = fser
    else:
        del fser.wbuf[:]  # discard any bytes left from the previous invocation
    if input_bom != fser.inputBOM:
        # Match the connection's byte order (a no-op in practice: big-endian).
        fser.setInputByteOrder(
            1 if input_bom == FastSerializer.LITTLE_ENDIAN else 0)
    return fser


# Invocation-extension type bytes (ProcedureInvocationExtensions). A version-2
# invocation carries a one-byte extension count after the client handle, then
# that many entries: one type byte, one payload-length byte in log2 form
# (0 -> empty, 3 -> 4 bytes), then the payload.
_EXT_ALL_PARTITION = 2
_EXT_PARTITION_DESTINATION = 3


def _serialize_invocation(input_bom, proc_name, param_types, params, handle,
                          partition_destination=None):
    """Serialize a procedure invocation to wire bytes, off to the side.

    Builds into a per-thread scratch ``FastSerializer`` (no host, so no socket
    and no auth) rather than the live connection's serializer, so the
    connection's shared write buffer is never touched. Produces the same bytes as
    ``voltdbclient.VoltProcedure.call``, with the caller-supplied handle in place
    of the hardcoded ``1``.

    With no ``partition_destination`` the bytes follow the original version-0
    invocation layout, byte-identical to the legacy client's. Naming a
    destination partition emits the version-2 layout instead (the Java
    client's ``ProcedureInvocation.flattenToBuffer``): version byte 2, then an
    extension count and typed extension entries between the client handle and
    the parameters. The destination always travels as a pair of extensions,
    the all-partition flag plus the destination partition id -- that is how a
    directed procedure, which has no partition parameter to steer by, is told
    where to run, and it also switches on the server's own vetting of
    all-partition calls. Only that pair is implemented; the other extensions
    the Java client can emit (batch timeout, request priority, request
    timeout) have no caller here.
    """
    param_types = param_types or ()
    params = params or ()
    if len(params) != len(param_types):
        raise ValueError(
            "procedure '%s' expects %d parameter(s) but %d were provided"
            % (proc_name, len(param_types), len(params)))

    fser = _scratch_serializer(input_bom)

    if partition_destination is None:
        fser.writeByte(0)  # version 0: no extension section
        fser.writeString(proc_name)
        fser.writeInt64(handle)
    else:
        fser.writeByte(2)  # version 2: extensions follow the handle
        fser.writeString(proc_name)
        fser.writeInt64(handle)
        fser.writeByte(2)  # extension count
        fser.writeByte(_EXT_ALL_PARTITION)
        fser.writeByte(0)  # empty payload: the type byte is the whole flag
        fser.writeByte(_EXT_PARTITION_DESTINATION)
        fser.writeByte(3)  # log2-form length: a 4-byte payload
        fser.writeInt32(partition_destination)
    fser.writeInt16(len(param_types))
    for i in range(len(param_types)):
        if _as_array(param_types[i], params[i]):
            fser.writeByte(FastSerializer.ARRAY)
            fser.writeByte(param_types[i])
            fser.writeArray(param_types[i], params[i])
        else:
            fser.writeWireType(param_types[i], params[i])
    fser.prependLength()
    return fser.wbuf.tobytes()


_RECV_CHUNK = 256 * 1024
_S_FRAME_LEN = struct.Struct(">i")


class _Connection:
    """One node connection: a ``FastSerializer`` plus its reader and writer threads.

    Owns the demux map for calls sent on this connection; the writer sends them
    and the reader completes them. Constructed already connected and
    authenticated (the ``FastSerializer`` does the handshake in its
    constructor); call ``start()`` once the connection is registered with its
    client.
    """

    def __init__(self, client, fser, host, port):
        self._client = client
        self._fser = fser
        self.host = host
        self.port = port
        self._pending = {}  # handle -> _Call
        self._pending_lock = threading.Lock()
        self._closed = False
        # Routing counters (mutated under the client's _conn_lock): calls this
        # connection was picked for because client affinity chose its node, vs.
        # calls that landed here by round-robin fallback.
        self.affinity_sends = 0
        self.round_robin_sends = 0
        # Liveness bookkeeping for the health check (read/written without a lock;
        # a bool and a float, so the GIL makes their access atomic enough -- a
        # stale read only costs an extra ping or a one-cycle-late detection).
        self._last_response = time.monotonic()
        self._outstanding_ping = False
        self._reader = threading.Thread(
            target=self._read_loop,
            name="voltclient-reader-%s:%s" % (host, port),
            daemon=True)
        # Senders append invocations; the writer sends all queued in one sendall().
        self._wq = []
        self._wq_cond = threading.Condition()
        self._writer = threading.Thread(
            target=self._write_loop,
            name="voltclient-writer-%s:%s" % (host, port),
            daemon=True)

    def start(self):
        self._reader.start()
        self._writer.start()

    def __str__(self):
        return "%s:%s" % (self.host, self.port)

    # -- send path ---------------------------------------------------------

    def send(self, proc_name, param_types, params, deadline,
             partition_destination=None):
        """Serialize and send an invocation; return a Future for its response."""
        handle = self._client._next_handle()
        payload = _serialize_invocation(
            self._fser.inputBOM, proc_name, param_types, params, handle,
            partition_destination)
        return self._submit(handle, payload, proc_name, deadline)

    def send_raw(self, proc_name, build, deadline):
        """send() with caller-encoded bytes: ``build(handle)`` returns the
        length-prefixed invocation."""
        handle = self._client._next_handle()
        return self._submit(handle, build(handle), proc_name, deadline)

    def _submit(self, handle, payload, proc_name, deadline):
        future = Future()
        call = _Call(handle, future, deadline, proc_name)
        with self._pending_lock:
            if self._closed:
                raise VoltConnectionError("connection to %s is closed" % self)
            self._pending[handle] = call

        # Lock the future to RUNNING so a late user cancel() cannot race the
        # response; if it was already cancelled, drop it before sending.
        if not future.set_running_or_notify_cancel():
            with self._pending_lock:
                self._pending.pop(handle, None)
            return future

        try:
            self._enqueue(payload)
        except VoltConnectionError:
            with self._pending_lock:
                popped = self._pending.pop(handle, None)
            if popped is None:
                # The disconnect path already failed it.
                return future
            raise
        return future

    def _enqueue(self, payload):
        with self._wq_cond:
            if self._closed:
                raise VoltConnectionError("connection to %s is closed" % self)
            self._wq.append(payload)
            if len(self._wq) == 1:
                self._wq_cond.notify()

    def _write_loop(self):
        sock = self._fser.socket
        while True:
            with self._wq_cond:
                while not self._wq and not self._closed:
                    self._wq_cond.wait()
                if self._closed:
                    return
                batch, self._wq = self._wq, []
            data = batch[0] if len(batch) == 1 else b"".join(batch)
            try:
                sock.sendall(data)  # the writer is the socket's only sender
            except (IOError, OSError) as e:
                self._handle_disconnect(e)
                return

    def _wake_writer(self):
        with self._wq_cond:
            self._wq_cond.notify_all()

    # -- read path (reader thread only) ------------------------------------

    def _read_loop(self):
        sock = self._fser.socket
        buf = bytearray()
        while not self._closed:
            try:
                chunk = sock.recv(_RECV_CHUNK)
            except (IOError, OSError) as e:
                if not self._closed:
                    self._handle_disconnect(e)
                return
            if not chunk:
                if not self._closed:
                    self._handle_disconnect(IOError("Connection broken"))
                return
            buf += chunk
            n = len(buf)
            off = 0
            try:
                while n - off >= 4:
                    end = off + 4 + _S_FRAME_LEN.unpack_from(buf, off)[0]
                    if end > n:
                        break
                    response = VoltResponse._from_body(self._fser, bytes(buf[off + 4:end]))
                    off = end
                    # Any frame proves the connection is alive, so a busy
                    # connection is never pinged.
                    self._last_response = time.monotonic()
                    self._dispatch(response)
            except Exception as e:  # malformed frame: treat as a lost connection
                if not self._closed:
                    log.exception("reader on %s failed to parse a response", self)
                    self._handle_disconnect(e)
                return
            if off:
                del buf[:off]

    def _dispatch(self, response):
        handle = response.clientHandle
        if handle == PING_HANDLE:
            # Our own health-check ping came back; nothing else to do.
            self._outstanding_ping = False
            return
        if handle is None or handle > MAX_CLIENT_HANDLE:
            self._client._on_push(self, handle, response)
            return
        with self._pending_lock:
            call = self._pending.pop(handle, None)
        if call is None:
            # A response with no waiting call: already timed out, or a
            # handle we never issued. Nothing to complete.
            log.debug("dropping response on %s for unknown handle %d",
                      self, handle)
            return
        self._client._deliver(call, response=response)

    # -- teardown ----------------------------------------------------------

    def expire_overdue(self, now):
        """Fail and remove any calls whose deadline has passed."""
        expired = []
        with self._pending_lock:
            for handle in list(self._pending):
                if self._pending[handle].deadline <= now:
                    expired.append(self._pending.pop(handle))
        for call in expired:
            self._client._deliver(call, exc=VoltTimeoutError(
                "procedure '%s' timed out" % call.proc_name))

    def health_check(self, now, response_timeout):
        """Ping an idle connection; declare it dead if a ping goes unanswered.

        A connection that has produced no frame for a third of the response
        timeout is pinged. If a ping is already outstanding and still nothing has
        arrived after the full timeout, the connection is considered dead -- this
        catches a half-open socket that neither the reader nor a send would notice
        until the next call. A busy connection is never pinged (see the reader).
        """
        if self._closed:
            return
        idle = now - self._last_response
        if self._outstanding_ping:
            if idle > response_timeout:
                self._handle_disconnect(VoltConnectionError(
                    "no response to health-check ping within %.0fs"
                    % response_timeout))
        elif idle > response_timeout / 3:
            self._send_ping()

    def _send_ping(self):
        """Send an ``@Ping`` on the reserved PING_HANDLE, off the demux path."""
        payload = _serialize_invocation(
            self._fser.inputBOM, "@Ping", (), (), PING_HANDLE)
        self._outstanding_ping = True
        try:
            self._enqueue(payload)
        except VoltConnectionError:
            pass  # already closed

    def _fail_all_pending(self, exc):
        with self._pending_lock:
            calls = list(self._pending.values())
            self._pending.clear()
        for call in calls:
            self._client._deliver(call, exc=exc)

    def _handle_disconnect(self, cause):
        """Unexpected loss: fail outstanding calls and drop from the pool."""
        if self._closed:
            return
        self._closed = True
        self._wake_writer()
        try:
            self._fser.socket.close()
        except Exception:
            pass
        self._fail_all_pending(VoltConnectionError(
            "connection to %s lost: %s" % (self, cause)))
        self._client._on_connection_lost(self)

    def close(self):
        """Orderly shutdown initiated by the client."""
        self._closed = True
        self._wake_writer()
        # Shutting the socket down unblocks the reader thread's recv().
        try:
            self._fser.socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self._fser.socket.close()
        except Exception:
            pass
        self._fail_all_pending(VoltConnectionError("client closed"))
        me = threading.current_thread()
        for thread in (self._writer, self._reader):
            if thread.ident is not None and thread is not me:  # started, not us
                thread.join(timeout=2.0)


# -- elastic hashinator ------------------------------------------------------
#
# VoltDB decides which partition owns a row by hashing the partitioning value
# onto a consistent-hash ring of signed 32-bit tokens: the owning partition is
# the ring entry with the greatest token at-or-below the hash. The server
# builds the ring (ElasticHashinator) and publishes it to clients as the
# HASHCONFIG blob in @Statistics TOPO's second table; ElasticHashinator below
# is a read-only port of the lookup side (java: client HashinatorLite, whose
# hashing code is shared with the server's own transaction routing).
#
# Everything here must match the Java implementation *byte for byte*: a client
# whose hash diverges routes work to the wrong partition. The server re-routes
# a mis-sent single-partition call internally, so for call routing a divergence
# is only a silent latency penalty -- but get_partition_for_parameter would
# simply return wrong answers. Do not "fix" oddities (e.g. an integer supplied
# for a string column hashing as an integer) without confirming the Java client
# and server changed first.

_MASK64 = 0xFFFFFFFFFFFFFFFF
_INT32_MIN = -2 ** 31

# Volt hashes a partitioning value's bytes with MurmurHash3 x64_128 using this
# seed, and keeps the upper 32 bits of the first 64-bit half of the digest as
# the ring position (org.apache.cassandra_voltpatches.MurmurHash3). These are
# fixed cluster-wide constants of the ELASTIC hash scheme; a future scheme
# would announce itself through TOPO's HASHTYPE column, not by tuning these.
_MURMUR_SEED = 0
_MURMUR_C1 = 0x87C37B91114253D5
_MURMUR_C2 = 0x4CF5AD432745937F


def _rotl64(v, n):
    """Rotate a 64-bit value left by n bits (java: Long.rotateLeft): the bits
    shifted off the top re-enter at the bottom. Python ints are unbounded, so
    the result is masked back down to 64 bits."""
    return ((v << n) | (v >> (64 - n))) & _MASK64


def _fmix(k):
    """Murmur3's 64-bit finalization mix ("fmix64"): an avalanche function
    that diffuses every input bit across the whole word, so that clustered
    inputs (sequential ids, common string prefixes) still scatter uniformly
    around the ring. The shift distance and the two multipliers are fixed
    constants of the published murmur3 algorithm, not tunables."""
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & _MASK64
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & _MASK64
    k ^= k >> 33
    return k


def _murmur3_token(data):
    """MurmurHash3 x64_128 of ``data``, reduced the way Volt reduces it: the
    upper 32 bits of the first 64-bit half of the digest, as a signed int32
    ring position.

    This is a deliberate line-for-line transliteration of Volt's Java
    implementation (org.apache.cassandra_voltpatches.MurmurHash3) rather than
    a library import: this module is stdlib-only (and hashlib has no murmur3),
    and the output -- bit for bit, including the nonstandard upper-32-of-h1
    reduction -- decides which partition owns every value, so a library whose
    output ever drifted would silently route to wrong partitions. The
    algorithm is frozen. If it appears to misbehave, diff this function
    against the Java source and run tests/pyclient/test_hashinator_parity.py;
    do not modify it in isolation.

    The shape of the algorithm: the input is consumed as 16-byte blocks, each
    split into two unsigned little-endian 64-bit lanes (k1, k2). Each lane is
    scrambled (multiply, rotate, multiply) and folded into its accumulator
    (h1, h2), and each accumulator is then stirred with the other, so the two
    halves of the digest depend on all of the input. The leftover 1..15 bytes
    get the same scramble-and-fold without the cross-stir, and finalization
    mixes in the total length, sums the accumulators, and avalanches them.

    Java does all of this in signed 64-bit longs that overflow silently;
    Python ints never overflow, so every add and multiply is masked with
    _MASK64 to reproduce the wraparound (xor and rotl stay within 64 bits on
    their own). Working unsigned is safe because two's-complement add,
    multiply, xor, and rotate produce the same bit patterns regardless of
    whether the operands are read as signed; signedness only re-enters at the
    final int32 conversion.
    """
    length = len(data)
    h1 = h2 = _MURMUR_SEED

    # Body: as many whole 16-byte blocks as the data holds, two little-endian
    # 64-bit lanes per block ("<%dQ" = little-endian unsigned 64-bit).
    nblocks = length >> 4
    if nblocks:
        lanes = struct.unpack_from("<%dQ" % (nblocks * 2), data)
        for i in range(0, nblocks * 2, 2):
            k1 = (lanes[i] * _MURMUR_C1) & _MASK64
            k1 = (_rotl64(k1, 31) * _MURMUR_C2) & _MASK64
            h1 ^= k1
            h1 = _rotl64(h1, 27)
            h1 = (h1 + h2) & _MASK64
            h1 = (h1 * 5 + 0x52DCE729) & _MASK64
            k2 = (lanes[i + 1] * _MURMUR_C2) & _MASK64
            k2 = (_rotl64(k2, 33) * _MURMUR_C1) & _MASK64
            h2 ^= k2
            h2 = _rotl64(h2, 31)
            h2 = (h2 + h1) & _MASK64
            h2 = (h2 * 5 + 0x38495AB5) & _MASK64

    # Tail: the trailing 1..15 bytes. Bytes 0..7 belong to lane k1 and bytes
    # 8..14 to lane k2, accumulated little-endian exactly like a body block --
    # int.from_bytes over the slice equals the Java switch's per-byte shifted
    # ORs -- except a lane is mixed in only if it has at least one byte, and
    # k2 before k1, matching the Java switch's fall-through order.
    tail = data[nblocks * 16:]
    if len(tail) > 8:
        k2 = (int.from_bytes(tail[8:], "little") * _MURMUR_C2) & _MASK64
        k2 = (_rotl64(k2, 33) * _MURMUR_C1) & _MASK64
        h2 ^= k2
    if tail:
        k1 = (int.from_bytes(tail[:8], "little") * _MURMUR_C1) & _MASK64
        k1 = (_rotl64(k1, 31) * _MURMUR_C2) & _MASK64
        h1 ^= k1

    # Finalization: fold the length in (so "a" and "a\0" differ), combine the
    # accumulators, avalanche each, and combine once more. Only the first half
    # (h1) is consumed, so Java's trailing "h2 += h1" is omitted -- it cannot
    # affect h1.
    h1 ^= length
    h2 ^= length
    h1 = (h1 + h2) & _MASK64
    h2 = (h2 + h1) & _MASK64
    h1 = (_fmix(h1) + _fmix(h2)) & _MASK64

    # Volt's reduction: keep the top 32 bits of h1, reinterpreted as a signed
    # int32 (Java: "(int)(h1 >>> 32)").
    token = h1 >> 32
    return token - 2 ** 32 if token >= 2 ** 31 else token


# The partitionable integer types: declared type -> serialized byte width.
# The width decides the type's SQL NULL sentinel (its most negative value) and
# the accepted value range; the *hash* is width-independent, because every
# integer is widened to 8 bytes before hashing (VoltType.valueToBytes).
_INT_TYPE_WIDTHS = {
    FastSerializer.VOLTTYPE_TINYINT: 1,
    FastSerializer.VOLTTYPE_SMALLINT: 2,
    FastSerializer.VOLTTYPE_INTEGER: 4,
    FastSerializer.VOLTTYPE_BIGINT: 8,
}

# What Java's Long.parseLong accepts: an optional sign and decimal digits.
# Python's int() is looser (whitespace, underscores), so the string-to-integer
# coercion checks this first to stay behaviorally aligned.
_JAVA_LONG_RE = re.compile(r"[+-]?[0-9]+")


def _as_binary(value):
    """``value`` as bytes if it is any of the binary shapes this library
    traffics in -- bytes, bytearray, memoryview, or the ``array('B')`` that a
    VARBINARY column reads back as -- else None."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, array.array) and value.typecode in ("b", "B"):
        return value.tobytes()
    return None


def _partition_value_to_bytes(vtype, value):
    """The exact bytes Volt hashes for a partitioning value of declared type
    ``vtype``, or None for SQL NULL, which is never hashed (java:
    VoltType.valueToBytes plus the coercions in
    HashinatorLite.getHashedPartitionForParameter).

    * Integer types accept int, decimal str, or little-endian bytes; the value
      is widened to 8 little-endian bytes, so 5 hashes identically whether the
      column is TINYINT or BIGINT. The type's null sentinel (e.g. -2**63 for
      BIGINT) is SQL NULL, and a value outside the type's range raises.
    * STRING str values hash as their UTF-8 bytes; VARBINARY bytes hash as-is.
    * An int supplied for a STRING/VARBINARY parameter raises. The Java
      implementation hashes it as the widened integer -- faithfully
      predicting a mistyped invocation whose writes then fail mispartitioned
      on the server all but 1-in-partition-count of the time -- and a
      deterministic error beats predicting an intermittent one. The caller
      decides whether coercing to str is right (and thereby learns the same
      coercion is needed feeding csvloader, sqlcmd, etc.); hashing the value
      as an integer type reproduces the cluster's routing of the mistyped
      invocation exactly, when that is genuinely what is wanted.
    """
    width = _INT_TYPE_WIDTHS.get(vtype)
    if width is None and vtype not in (FastSerializer.VOLTTYPE_STRING,
                                       FastSerializer.VOLTTYPE_VARBINARY):
        raise VoltTypeError("type %s is not a partitionable type" % (vtype,))
    if value is None:
        return None
    if isinstance(value, bool):
        raise VoltTypeError("cannot hash a boolean as a partitioning value")

    if width is not None:
        binary = _as_binary(value)
        if isinstance(value, str):
            if not _JAVA_LONG_RE.fullmatch(value):
                raise VoltTypeError(
                    "cannot convert string %r to an integer partitioning value"
                    % (value,))
            value = int(value)
        elif binary is not None:
            if len(binary) < width:
                raise VoltTypeError(
                    "%d-byte value is too short for a %d-byte integer type"
                    % (len(binary), width))
            value = int.from_bytes(binary[:width], "little", signed=True)
        if not isinstance(value, int):
            raise VoltTypeError("cannot hash %s as an integer partitioning value"
                                % type(value).__name__)
        low = -(1 << (8 * width - 1))
        if value == low:
            return None  # the type's null sentinel is SQL NULL
        if not low < value <= -low - 1:
            raise VoltTypeError("value %d is out of range for the declared "
                                "%d-byte integer type" % (value, width))
        return struct.pack("<q", value)

    # STRING / VARBINARY
    binary = _as_binary(value)
    if binary is not None:
        return binary
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, int):
        raise VoltTypeError(
            "int value for a string-typed partitioning parameter: pass a str "
            "(or hash it as an integer type to reproduce how the cluster "
            "routes a mistyped invocation)")
    raise VoltTypeError("cannot hash %s as a partitioning value"
                        % type(value).__name__)


class ElasticHashinator:
    """The lookup side of VoltDB's consistent-hash ring: maps a partitioning
    value to the partition id that owns it. Construct with ``from_bytes`` from
    the HASHCONFIG blob in ``@Statistics TOPO``'s second table. Immutable and
    safe to share across threads; a topology refresh builds a new instance."""

    __slots__ = ("_tokens", "_partitions")

    def __init__(self, tokens, partitions):
        """``tokens`` (ascending signed int32) and ``partitions`` are parallel
        sequences. The server always emits a first token of -2**31, so every
        possible hash has an owning entry; anything else is a corrupt config."""
        if len(tokens) != len(partitions) or not tokens:
            raise VoltClientError("hashinator tokens/partitions mismatch")
        if tokens[0] != _INT32_MIN:
            raise VoltClientError("hashinator ring does not start at -2**31")
        if any(a > b for a, b in zip(tokens, tokens[1:])):
            raise VoltClientError("hashinator tokens are not sorted")
        self._tokens = list(tokens)
        self._partitions = list(partitions)

    @classmethod
    def from_bytes(cls, config, cooked=False):
        """Decode a HASHCONFIG blob. The default (``cooked=False``) is the
        format ``@Statistics TOPO`` returns: int32 entry count, then that many
        interleaved (token, partition id) int32 pairs, all big-endian,
        uncompressed. ``cooked=True`` decodes the compact variant the Java
        client also supports: zlib-compressed, count then all tokens then all
        partition ids, with the token array transposed into four byte planes,
        most significant first."""
        try:
            if cooked:
                data = zlib.decompress(config)
                (count,) = struct.unpack_from(">i", data)
                if count <= 0 or len(data) != 4 + count * 8:
                    raise VoltClientError("bad cooked hashinator config size")
                planes = data[4:4 + count * 4]
                tokens = []
                for i in range(count):
                    u = ((planes[i] << 24) | (planes[count + i] << 16) |
                         (planes[2 * count + i] << 8) | planes[3 * count + i])
                    tokens.append(u - 2 ** 32 if u >= 2 ** 31 else u)
                partitions = struct.unpack_from(">%di" % count, data, 4 + count * 4)
            else:
                (count,) = struct.unpack_from(">i", config)
                if count <= 0 or len(config) != 4 + count * 8:
                    raise VoltClientError("bad hashinator config size")
                pairs = struct.unpack_from(">%di" % (count * 2), config, 4)
                tokens = pairs[0::2]
                partitions = pairs[1::2]
        except (struct.error, zlib.error) as e:
            raise VoltClientError("undecodable hashinator config: %s" % e) from None
        return cls(tokens, partitions)

    def partition_for_token(self, token):
        """The partition owning a ring position: the entry with the greatest
        token at-or-below ``token`` (java: HashinatorLite.partitionForToken)."""
        return self._partitions[bisect_right(self._tokens, token) - 1]

    def get_partition_for_parameter(self, vtype, value):
        """The partition id owning ``value`` hashed as declared type ``vtype``
        (a ``VoltType`` code). None -- or the type's SQL NULL
        sentinel -- maps to partition 0 without hashing. Raises VoltTypeError
        for a non-partitionable type or a value that cannot hash as ``vtype``."""
        value_bytes = _partition_value_to_bytes(vtype, value)
        if value_bytes is None:
            return 0
        return self.partition_for_token(_murmur3_token(value_bytes))


# -- topology model ---------------------------------------------------------
#
# The client discovers cluster metadata from three system procedures and stores
# it as an immutable snapshot (see TopologySnapshot), including a ready-to-use
# hashinator. Client-affinity routing (VoltClient._preferred_host) is built on
# top of this.


@dataclass(frozen=True)
class HostInfo:
    """A cluster node's advertised, client-facing identity from
    ``@SystemInformation OVERVIEW``. ``ip`` is the advertised *routable* client
    address -- the address a client should dial -- and ``hostname`` is retained
    for consistent display. ``client_port`` is the node's client port."""
    host_id: int
    ip: Optional[str]
    hostname: Optional[str]
    client_port: Optional[int]


@dataclass(frozen=True)
class PartitionInfo:
    """Ownership of one partition. ``leader_host_id`` is the current master --
    where a single-partition call for this partition should be routed --  and
    ``replica_host_ids`` is every host holding a copy, the leader included."""
    partition: int
    leader_host_id: int
    replica_host_ids: tuple


@dataclass(frozen=True)
class ProcedureInfo:
    """A procedure's partitioning metadata from ``@SystemCatalog PROCEDURES``.
    For a single-partition procedure, ``partition_param`` is the index of the
    parameter that determines the partition and ``partition_param_type`` is that
    parameter's VoltType; both are ``-1`` for a non-single-partition procedure."""
    name: str
    single_partition: bool
    read_only: bool
    compound: bool
    partition_param: int
    partition_param_type: int


@dataclass(frozen=True)
class AffinityStats:
    """Routing counters for one connection (see ``VoltClient.affinity_stats``):
    calls sent to it because client affinity chose its node, and calls that
    landed on it by round-robin."""
    affinity_sends: int
    round_robin_sends: int


@dataclass(frozen=True)
class PartitionCallResult:
    """Outcome of one partition's leg of a ``call_all_partitions`` fan-out.
    Exactly one of ``response`` and ``error`` is set: ``response`` is the
    partition's ``VoltResponse`` (which may still report a non-success status),
    and ``error`` is the exception a plain ``call`` would have raised when
    there is no server response at all (timeout, connection loss).
    ``partition_key`` is the server-provided value that reaches the partition;
    it was passed as the procedure's first parameter."""
    partition_key: int
    partition_id: int
    response: Optional[VoltResponse] = None
    error: Optional[BaseException] = None

    @property
    def ok(self):
        """True when this partition returned a successful response."""
        return self.error is None and self.response.status == _STATUS_SUCCESS


@dataclass(frozen=True)
class TopologySnapshot:
    """An immutable, point-in-time view of the cluster metadata that affinity
    routing needs. ``refresh_topology`` builds a fresh snapshot and swaps it in
    with a single atomic assignment, so a reader always sees one internally
    consistent view -- never a half-updated map. Treat the maps as read-only.

    * ``hosts`` -- ``host_id -> HostInfo``
    * ``partitions`` -- ``partition -> PartitionInfo`` (includes ``MP_INIT_PID``)
    * ``procedures`` -- ``procedure name -> ProcedureInfo``
    * ``hashinator_config`` -- raw config bytes from TOPO's second table;
      ``None`` if the cluster returned none
    * ``hashinator`` -- the config decoded into an ``ElasticHashinator``, or
      ``None`` when there is no config or it is unusable (see
      ``_build_hashinator``); the client then runs without partition hashing
    """
    hosts: dict
    partitions: dict
    procedures: dict
    hashinator_config: Optional[bytes]
    hashinator: Optional[ElasticHashinator]

    @classmethod
    def empty(cls):
        return cls(hosts={}, partitions={}, procedures={}, hashinator_config=None,
                   hashinator=None)


def _column_index(table, name):
    """Index of the column named ``name`` (case-insensitive) in a VoltTable, or
    None if it has no such column."""
    for i, col in enumerate(table.columns):
        if col.name and col.name.upper() == name.upper():
            return i
    return None


def _parse_overview(table):
    """Pivot the narrow ``(HOST_ID, KEY, VALUE)`` OVERVIEW table into
    ``host_id -> HostInfo``. We dial ``IPADDRESS`` -- the advertised routable
    client address (``SystemInformation.populateOverviewTable``) -- and
    deliberately ignore ``CLIENTINTERFACE``, which is the raw bind interface and,
    in an advertised-address deployment, is not where a client should connect."""
    hid_i = _column_index(table, "HOST_ID")
    key_i = _column_index(table, "KEY")
    val_i = _column_index(table, "VALUE")
    if hid_i is None or key_i is None or val_i is None:
        return {}
    props = {}  # host_id -> {KEY: VALUE}
    for row in table.tuples:
        props.setdefault(int(row[hid_i]), {})[row[key_i]] = row[val_i]
    hosts = {}
    for host_id, kv in props.items():
        port = kv.get("CLIENTPORT")
        hosts[host_id] = HostInfo(
            host_id=host_id,
            ip=kv.get("IPADDRESS"),
            hostname=kv.get("HOSTNAME"),
            client_port=int(port) if port else None)
    return hosts


def _parse_topology(tables):
    """From ``@Statistics TOPO``'s two tables, build ``partition -> PartitionInfo``
    and return ``(partitions, hashtype, config)``, where ``hashtype`` and
    ``config`` are the raw hashinator scheme name and config bytes. The first
    table has a row per partition with ``Sites`` (``"host:site,host:site"``)
    and ``Leader`` (``"host:site"``) columns; the second holds a single
    ``(HASHTYPE, HASHCONFIG)`` row. A partition with an empty leader or sites
    list is skipped -- the topology is momentarily incomplete."""
    topo = tables[0]
    part_i = _column_index(topo, "Partition")
    sites_i = _column_index(topo, "Sites")
    leader_i = _column_index(topo, "Leader")
    partitions = {}
    if None not in (part_i, sites_i, leader_i):
        for row in topo.tuples:
            sites, leader = row[sites_i], row[leader_i]
            if not sites or not leader:
                continue
            partition = int(row[part_i])
            replicas = tuple(int(s.strip().split(":")[0]) for s in sites.split(","))
            partitions[partition] = PartitionInfo(
                partition=partition,
                leader_host_id=int(leader.split(":")[0]),
                replica_host_ids=replicas)
    hashtype = None
    hashinator_config = None
    if len(tables) > 1 and tables[1].tuples:
        ht_i = _column_index(tables[1], "HASHTYPE")
        if ht_i is not None:
            hashtype = tables[1].tuples[0][ht_i]
        hc_i = _column_index(tables[1], "HASHCONFIG")
        if hc_i is not None:
            raw = tables[1].tuples[0][hc_i]
            if raw is not None:
                hashinator_config = bytes(raw)  # varbinary reads back as array('B')
    return partitions, hashtype, hashinator_config


def _build_hashinator(hashtype, config):
    """Decode TOPO's hashinator advertisement into an ElasticHashinator, or
    None when there is nothing usable. An unrecognized HASHTYPE means the
    cluster hashes with a scheme this client does not implement; guessing would
    route to wrong partitions, so partition hashing is disabled instead (calls
    still work, routed without affinity) and the situation is logged."""
    if config is None:
        return None
    if hashtype is not None and hashtype.upper() != "ELASTIC":
        log.warning("cluster hashinator type %r is not supported: "
                    "partition hashing disabled", hashtype)
        return None
    try:
        return ElasticHashinator.from_bytes(config)
    except VoltClientError as e:
        log.warning("%s: partition hashing disabled", e)
        return None


def _parse_procedures(table):
    """From ``@SystemCatalog PROCEDURES``, extract each procedure's partitioning:
    whether it is single-partition and, if so, the index and VoltType of its
    partitioning parameter. That metadata rides as JSON in the ``REMARKS`` column.
    A procedure whose metadata will not parse is skipped, so one bad entry cannot
    break discovery of the rest."""
    name_i = _column_index(table, "PROCEDURE_NAME")
    remarks_i = _column_index(table, "REMARKS")
    procedures = {}
    if name_i is None or remarks_i is None:
        return procedures
    for row in table.tuples:
        name = row[name_i]
        remarks = row[remarks_i]
        if not remarks:
            continue
        try:
            info = json.loads(remarks)
            single = bool(info.get("singlePartition", False))
            procedures[name] = ProcedureInfo(
                name=name,
                single_partition=single,
                read_only=bool(info.get("readOnly", False)),
                compound=bool(info.get("compound", False)),
                partition_param=(int(info["partitionParameter"])
                                 if single else _PARAMETER_NONE),
                partition_param_type=(int(info["partitionParameterType"])
                                      if single else _PARAMETER_NONE))
        except (ValueError, TypeError, KeyError):
            log.warning("skipping procedure %r: unparseable partitioning metadata",
                        name)
    return procedures


def _parse_partition_keys(table):
    """``[(partition_id, key), ...]`` sorted by partition id, from the
    ``@GetPartitionKeys`` table (one row per partition, ``PARTITION_ID`` and
    ``PARTITION_KEY`` columns); [] if the table lacks those columns."""
    if table is None:
        return []
    pid_i = _column_index(table, "PARTITION_ID")
    key_i = _column_index(table, "PARTITION_KEY")
    if pid_i is None or key_i is None:
        return []
    return sorted((int(row[pid_i]), int(row[key_i])) for row in table.tuples)


def _build_snapshot(overview_resp, topo_resp, procedures_resp):
    """Assemble a TopologySnapshot from the three discovery responses."""
    partitions, hashtype, hashinator_config = _parse_topology(topo_resp.tables)
    return TopologySnapshot(
        hosts=_parse_overview(overview_resp.tables[0]),
        partitions=partitions,
        procedures=_parse_procedures(procedures_resp.tables[0]),
        hashinator_config=hashinator_config,
        hashinator=_build_hashinator(hashtype, hashinator_config))


class VoltClient:
    """A topology-aware VoltDB client holding a connection per cluster node.

    ``hosts`` bootstraps the pool. Membership starts as the reachable seed hosts;
    when topology discovery is enabled it defines the final membership. Auth/SSL
    parameters pass through to ``FastSerializer`` unchanged.

    The client keeps its view of the cluster current by subscription rather
    than polling: it registers for server push notifications (``@Subscribe
    TOPOLOGY``) on one pool connection, and the cluster pushes refreshed
    topology and procedure-catalog data whenever they change -- so leader
    migrations, elastic rebalances, node failures, and rejoins are reflected
    in routing and pool membership without a client restart. If the
    subscribed node is lost, the subscription moves to another connection
    automatically. ``refresh_topology`` remains public as a manual backstop
    for applications that want their own schedule on top.

    Failure contract: a call in flight when its connection is lost fails with
    ``VoltConnectionError`` and its outcome is *unknown* -- it may or may not
    have executed. The client never resends such a call; only the application
    can know whether a retry is safe (idempotent operations, or a lookup
    before resending). A call merely *routed* on stale topology is harmless
    by contrast: the cluster forwards or transparently restarts it on the
    correct node -- a small latency cost, never a correctness one -- and the
    resulting push re-converges this client's routing.

    Public call surface:

    * ``call(proc_name, param_types, params, timeout=None)`` -- blocks and
      returns the ``VoltResponse``.
    * ``call_async(...) -> concurrent.futures.Future`` -- returns immediately,
      unless ``max_outstanding`` calls are already in flight (see
      ``call_async``).

    Use as a context manager so background threads and sockets are always torn
    down::

        with VoltClient(hosts=["hostA", "hostB"]) as client:
            resp = client.call("@Ping")
    """

    def __init__(self, hosts=None, port=21212, username="", password="",
                 usessl=False, kerberos=False, ssl_config_file=None,
                 default_cacerts=True, connect_timeout=8,
                 procedure_timeout=_DEFAULT_PROCEDURE_TIMEOUT,
                 sweep_interval=_DEFAULT_SWEEP_INTERVAL,
                 reconnect_interval=_DEFAULT_RECONNECT_INTERVAL,
                 initial_backoff=_DEFAULT_INITIAL_BACKOFF,
                 max_backoff=_DEFAULT_MAX_BACKOFF,
                 reconnect_jitter=_DEFAULT_RECONNECT_JITTER,
                 connection_response_timeout=_DEFAULT_CONNECTION_RESPONSE_TIMEOUT,
                 max_outstanding=_DEFAULT_MAX_OUTSTANDING):
        if max_outstanding < 1:
            raise ValueError("max_outstanding must be at least 1")
        self._port = port
        self._username = username
        self._password = password
        self._usessl = usessl
        self._kerberos = kerberos
        self._ssl_config_file = ssl_config_file
        self._default_cacerts = default_cacerts
        self._connect_timeout = connect_timeout
        self._procedure_timeout = procedure_timeout
        self._sweep_interval = sweep_interval
        self._reconnect_interval = reconnect_interval
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._reconnect_jitter = reconnect_jitter
        self._connection_response_timeout = connection_response_timeout

        # _conn_lock guards both the live-connection list and the target set,
        # since the reconnect loop moves state between them.
        self._connections = []
        self._targets = {}  # (host, port) -> _Target  (the desired set)
        self._conn_lock = threading.Lock()
        self._rr = -1  # round-robin cursor

        self._handle = 0
        self._handle_lock = threading.Lock()

        # The outstanding-request bound: _outstanding counts calls in flight
        # (slot acquired, terminal outcome not yet delivered); callers at the
        # cap wait on the condition until a completion frees a slot.
        self._max_outstanding = max_outstanding
        self._outstanding = 0
        self._outstanding_cond = threading.Condition()

        # The current cluster metadata snapshot. Replaced wholesale by
        # refresh_topology; the lock serializes concurrent refreshes so the last
        # to finish wins. Reads take no lock -- the assignment is atomic.
        self._topology = TopologySnapshot.empty()
        self._topology_lock = threading.Lock()

        # Topology-push subscription state (guarded by _conn_lock): the
        # connection carrying our @Subscribe TOPOLOGY registration, and the
        # earliest time another attempt may be made after the server declined
        # one. Maintained by _ensure_subscription on the reconnect thread.
        self._subscribed_conn = None
        self._subscribe_next_attempt = 0.0
        # Raised when the routing metadata should be re-fetched in full: after
        # a subscription is confirmed (so nothing pushed before it registered
        # is missed) and after a topology push (which carries no host
        # addresses, so membership convergence needs a fresh OVERVIEW). The
        # reconnect thread services it; _refresh_not_before paces retries.
        self._refresh_needed = threading.Event()
        self._refresh_not_before = 0.0

        # Procedures already warned about for an unhashable partition parameter
        # (see _preferred_host), so the warning fires once per procedure, not
        # once per call. Unsynchronized: a rare race just repeats the warning.
        self._affinity_warned = set()

        # The bootstrap (seed) addresses. Membership converges to the cluster's
        # advertised addresses (see _converge_membership), but a seed is retained
        # as a fallback until its node is reachable the advertised way -- so an
        # unroutable advertised interface never strands us.
        self._seeds = {(host, port) for host in (hosts or ())}

        self._closed = False
        self._sweeper_stop = threading.Event()
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="voltclient-sweeper", daemon=True)
        self._sweeper.start()

        # Bootstrap the target set from the seeds and connect them synchronously,
        # so construction fails fast if none are reachable. Seeds that fail are
        # kept as targets and retried by the reconnect thread.
        failures = []
        for host in (hosts or ()):
            target = self._add_target(host, port)
            try:
                self._connect_target(target)
            except Exception as e:
                self._schedule_retry(target, time.monotonic())
                failures.append((host, e))
        if hosts and not self._connections:
            self.close()
            raise VoltConnectionError(
                "could not connect to any of the seed hosts: %s"
                % ", ".join("%s (%s)" % (h, e) for h, e in failures))

        # Discover topology on connect, so the client is topology-aware as soon
        # as it is constructed. Best-effort: a discovery failure must not fail
        # construction -- the client still works (round-robin), just without
        # affinity metadata until a later refresh. The reconnect thread then
        # converges the pool onto the discovered advertised addresses.
        if self._connections:
            try:
                self.refresh_topology()
            except Exception as e:
                log.warning("initial topology discovery failed: %s", e)

        self._reconnect_stop = threading.Event()
        self._reconnect = threading.Thread(
            target=self._reconnect_loop, name="voltclient-reconnect", daemon=True)
        self._reconnect.start()

    # -- target set (desired membership) -----------------------------------

    def _add_target(self, host, port):
        key = (host, port)
        with self._conn_lock:
            target = self._targets.get(key)
            if target is None:
                target = _Target(host, port)
                self._targets[key] = target
        return target

    def _update_targets(self, targets):
        """Replace the desired membership with ``targets`` (an iterable of
        ``(host, port)``). New targets are dialed and dropped ones are closed by
        the reconnect thread on its next pass. This is the entry point for
        updating membership from discovered topology."""
        wanted = {(h, p) for (h, p) in targets}
        with self._conn_lock:
            for key in wanted:
                if key not in self._targets:
                    self._targets[key] = _Target(key[0], key[1])
            for key in list(self._targets):
                if key not in wanted:
                    del self._targets[key]

    def _schedule_retry(self, target, now):
        """Grow the target's backoff and set the earliest next attempt time."""
        with self._conn_lock:
            target.backoff = (self._initial_backoff if target.backoff <= 0
                              else min(target.backoff * 2, self._max_backoff))
            jitter = target.backoff * self._reconnect_jitter * random.random()
            target.next_attempt = now + target.backoff + jitter

    # -- connection management ---------------------------------------------

    def _open_serializer(self, host, port):
        """Open and authenticate a serializer to a node. Isolated so tests can
        override it to inject a fake socket."""
        fser = FastSerializer(
            host=host, port=port, usessl=self._usessl,
            username=self._username, password=self._password,
            kerberos=self._kerberos, connect_timeout=self._connect_timeout,
            ssl_config_file=self._ssl_config_file,
            default_cacerts=self._default_cacerts)
        # Let the OS notice a peer that vanishes silently, as a backstop to the
        # application-level health-check ping.
        fser.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # The reader blocks on recv indefinitely; deadlines are enforced by the
        # sweeper, not by a per-socket timeout.
        fser.socket.settimeout(None)
        return fser

    def _connect_target(self, target):
        """Open a connection to a target and attach it (blocking). Raises on
        failure, leaving the target unconnected for the reconnect loop to retry."""
        fser = self._open_serializer(target.host, target.port)
        return self._register_connection(fser, target.host, target.port, target)

    def _register_connection(self, fser, host, port, target=None):
        """Wrap a connected serializer and start its reader. Shared by the real
        connect path and by tests that inject a fake socket."""
        conn = _Connection(self, fser, host, port)
        with self._conn_lock:
            if self._closed:
                # Racing with close(): don't register (and start a reader for) a
                # connection that would outlive the client.
                fser.socket.close()
                raise VoltConnectionError("client is closed")
            self._connections.append(conn)
            if target is not None:
                target.connection = conn
                target.backoff = 0.0
                target.next_attempt = 0.0
            # Start the reader while still holding the lock, so any connection
            # visible in _connections is guaranteed to have a started reader --
            # otherwise close() could race in and try to join an unstarted thread.
            conn.start()
        return conn

    def _on_connection_lost(self, conn):
        with self._conn_lock:
            try:
                self._connections.remove(conn)
            except ValueError:
                pass
            # A lost subscription is re-established on another connection by
            # the next reconnect pass (which also re-fetches the topology, so
            # a change pushed into the void is not lost).
            if self._subscribed_conn is conn:
                self._subscribed_conn = None
            # If this node is still desired, mark it for prompt reconnect. Backoff
            # only grows on a failed *attempt*, so a clean loss retries at once.
            target = self._targets.get((conn.host, conn.port))
            if target is not None and target.connection is conn:
                target.connection = None
                target.backoff = 0.0
                target.next_attempt = 0.0

    def _pick_connection(self, preferred_host_id=None):
        """The connection to send the next call on: the one to
        ``preferred_host_id`` when given and connected, else round-robin.
        A preferred node that is not currently connected falls back to
        round-robin rather than waiting -- affinity is advisory, and any node
        will forward the call to the right place."""
        with self._conn_lock:
            # Route only to connections whose node is still a desired target, so a
            # connection being retired (e.g. a seed superseded by its node's
            # advertised address, or a node leaving the cluster) receives no new
            # calls while it drains.
            live = [c for c in self._connections
                    if (c.host, c.port) in self._targets]
            if not live:
                raise VoltNoConnectionsError(
                    "no live connection to the cluster")
            if preferred_host_id is not None:
                for conn in live:
                    if conn._fser.hostId == preferred_host_id:
                        conn.affinity_sends += 1
                        return conn
            self._rr = (self._rr + 1) % len(live)
            conn = live[self._rr]
            conn.round_robin_sends += 1
            return conn

    # -- handles and completion seam ---------------------------------------

    def _next_handle(self):
        with self._handle_lock:
            self._handle += 1
            return self._handle

    def _deliver(self, call, response=None, exc=None):
        """Single completion seam: every terminal outcome for a call -- success,
        timeout, or connection loss -- flows through here. A callback-registration
        API can be added at this one point without changing callers."""
        future = call.future
        if future.done():
            return
        if exc is not None:
            future.set_exception(exc)
        else:
            future.set_result(response)

    def _acquire_slot(self, proc_name, deadline):
        """Claim an outstanding-request slot, waiting until ``deadline`` for
        one to free. The caller passes the same deadline that will govern the
        call in flight, so time spent waiting here is drawn from the call's
        one timeout budget, never added to it. Progress is guaranteed: every
        outstanding call reaches a terminal outcome by its deadline at the
        latest (the sweeper sees to it), and every terminal outcome frees a
        slot."""
        with self._outstanding_cond:
            while self._outstanding >= self._max_outstanding:
                if self._closed:
                    raise VoltClientError("client is closed")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise VoltTimeoutError(
                        "procedure '%s' timed out waiting for an "
                        "outstanding-request slot (max_outstanding=%d)"
                        % (proc_name, self._max_outstanding))
                self._outstanding_cond.wait(remaining)
            self._outstanding += 1

    def _release_slot(self, _future=None):
        """Free an outstanding-request slot and wake one waiting caller.
        Attached to each call's Future as a done-callback, so every terminal
        outcome -- response, timeout, connection loss, or user cancellation --
        releases exactly once."""
        with self._outstanding_cond:
            self._outstanding -= 1
            self._outstanding_cond.notify()

    def _on_push(self, conn, handle, response):
        """A server-pushed message answering no call, delivered on ``conn``'s
        reader thread. The cluster pushes refreshed ``@Statistics TOPO`` and
        ``@SystemCatalog PROCEDURES`` results to subscribed clients whenever
        they change (see ``_ensure_subscription``); anything else is logged
        and dropped. Reader-thread rules apply: parse and swap only, never a
        blocking call."""
        if handle == ASYNC_TOPO_HANDLE:
            self._apply_topology_push(conn, response)
        elif handle == ASYNC_PROC_HANDLE:
            self._apply_procedures_push(conn, response)
        else:
            log.debug("ignoring server push on %s handle=%d status=%s",
                      conn, handle, response.status)

    def _apply_topology_push(self, conn, response):
        """Fold a pushed topology update into the snapshot: partition leaders
        and the hashinator take effect immediately, so routing follows a
        leader migration or rebalance as soon as the push arrives. The push
        carries no host addresses, so a full refresh is also flagged for the
        reconnect thread -- that is what lets pool membership follow node
        joins and leaves."""
        tables = response.tables or []
        if response.status != _STATUS_SUCCESS or len(tables) < 2:
            log.debug("ignoring unusable topology push on %s "
                      "(status=%s, %d tables)", conn, response.status,
                      len(tables))
            return
        try:
            partitions, hashtype, config = _parse_topology(tables)
        except Exception:
            log.exception("ignoring unparseable topology push on %s", conn)
            return
        with self._topology_lock:
            current = self._topology
            if config is not None and config == current.hashinator_config:
                # The common push (a leader moved) leaves the ring untouched;
                # skip re-decoding the config blob on the reader thread.
                hashinator = current.hashinator
            else:
                hashinator = _build_hashinator(hashtype, config)
            self._topology = TopologySnapshot(
                hosts=current.hosts,
                partitions=partitions,
                procedures=current.procedures,
                hashinator_config=config,
                hashinator=hashinator)
        log.debug("applied topology push from %s (%d partitions)",
                  conn, len(partitions))
        self._refresh_needed.set()

    def _apply_procedures_push(self, conn, response):
        """Fold a pushed procedure-catalog update into the snapshot, keeping
        everything else. Complete in itself -- unlike a topology push, no
        follow-up refresh is needed."""
        tables = response.tables or []
        if response.status != _STATUS_SUCCESS or not tables:
            log.debug("ignoring unusable procedure-catalog push on %s "
                      "(status=%s)", conn, response.status)
            return
        try:
            procedures = _parse_procedures(tables[0])
        except Exception:
            log.exception("ignoring unparseable procedure-catalog push on %s",
                          conn)
            return
        with self._topology_lock:
            current = self._topology
            self._topology = TopologySnapshot(
                hosts=current.hosts,
                partitions=current.partitions,
                procedures=procedures,
                hashinator_config=current.hashinator_config,
                hashinator=current.hashinator)
        log.debug("applied procedure-catalog push from %s (%d procedures)",
                  conn, len(procedures))

    # -- maintenance: deadlines + health check -----------------------------

    def _sweep_loop(self):
        while not self._sweeper_stop.wait(self._sweep_interval):
            now = time.monotonic()
            with self._conn_lock:
                conns = list(self._connections)
            for conn in conns:
                conn.expire_overdue(now)
                conn.health_check(now, self._connection_response_timeout)

    # -- reconnect: converge live connections toward the target set --------

    def _reconnect_loop(self):
        while not self._reconnect_stop.wait(self._reconnect_interval):
            self._refresh_if_needed()
            now = time.monotonic()
            # Reconcile the desired target set with discovered topology first, so
            # this pass dials any newly-advertised node and drops any seed whose
            # node is now reachable the advertised way.
            self._converge_membership()
            with self._conn_lock:
                due = [t for t in self._targets.values()
                       if t.connection is None and now >= t.next_attempt]
            for target in due:
                try:
                    self._connect_target(target)
                    log.info("connected to %s:%s", target.host, target.port)
                except Exception as e:
                    log.debug("connect to %s:%s failed: %s",
                              target.host, target.port, e)
                    self._schedule_retry(target, time.monotonic())
            self._close_undesired()
            self._ensure_subscription()

    def _refresh_if_needed(self):
        """Service a flagged full-discovery refresh (see ``_refresh_needed``).
        A failed refresh keeps the flag raised and backs off, so a struggling
        cluster is retried gently -- and the moment connectivity returns, the
        pending flag delivers the fresh topology the outage made stale."""
        if not self._refresh_needed.is_set():
            return
        if time.monotonic() < self._refresh_not_before:
            return
        self._refresh_needed.clear()
        try:
            self.refresh_topology(timeout=self._connection_response_timeout)
        except Exception as e:
            log.debug("push-triggered topology refresh failed: %s", e)
            self._refresh_needed.set()
            self._refresh_not_before = time.monotonic() + _REFRESH_RETRY_DELAY

    # -- topology-push subscription -----------------------------------------

    def _ensure_subscription(self):
        """Keep exactly one live connection registered for topology pushes.

        Runs every reconnect pass. When the subscribed connection has died or
        been retired, pick another at random and send ``@Subscribe TOPOLOGY``
        on it -- directly, not routed, since the registration is a property of
        that one connection. The send does not block this thread; the
        response is handled as it arrives. On success a full refresh is
        flagged, and because it is fetched only *after* the registration is
        active, a change slipping in between cannot be lost (same ordering
        the Java client uses)."""
        now = time.monotonic()
        with self._conn_lock:
            conn = self._subscribed_conn
            if (conn is not None and not conn._closed
                    and (conn.host, conn.port) in self._targets):
                return  # healthy (or a send is in flight and unanswered)
            self._subscribed_conn = None
            if now < self._subscribe_next_attempt:
                return
            live = [c for c in self._connections
                    if (c.host, c.port) in self._targets and not c._closed]
            if not live:
                return
            conn = random.choice(live)
            # Claim before sending; the response callback clears the claim if
            # the subscribe fails, which lets a later pass try elsewhere.
            self._subscribed_conn = conn
        try:
            future = conn.send(
                "@Subscribe", (FastSerializer.VOLTTYPE_STRING,), ("TOPOLOGY",),
                now + self._connection_response_timeout)
        except VoltClientError:
            with self._conn_lock:
                if self._subscribed_conn is conn:
                    self._subscribed_conn = None
            return
        future.add_done_callback(
            lambda f: self._on_subscribe_response(conn, f))

    def _on_subscribe_response(self, conn, future):
        """Bookkeeping for the ``@Subscribe`` outcome; runs on the reader
        thread (or the sweeper, on timeout), so it must stay non-blocking."""
        try:
            response = future.result()
        except Exception:
            # Lost or unresponsive connection: drop the claim so the next
            # reconnect pass subscribes elsewhere right away.
            with self._conn_lock:
                if self._subscribed_conn is conn:
                    self._subscribed_conn = None
            return
        if response.status != _STATUS_SUCCESS:
            # Declined -- the server is up but not accepting the subscription
            # (e.g. paused). Retrying immediately would be a tight loop of
            # failures; wait out the delay instead.
            with self._conn_lock:
                if self._subscribed_conn is conn:
                    self._subscribed_conn = None
                self._subscribe_next_attempt = (time.monotonic()
                                                + _SUBSCRIBE_RETRY_DELAY)
            log.warning("topology subscription declined by %s: %s",
                        conn, response.statusString)
            return
        log.debug("subscribed for topology updates on %s", conn)
        self._refresh_needed.set()

    def _converge_membership(self):
        """Reconcile the desired target set with discovered topology, keyed by
        host id (a node's stable identity -- captured from the login response --
        which address strings cannot reliably provide across NAT/VPN/multi-homed
        interfaces).

        For every discovered node we want a connection to its advertised address
        (``IPADDRESS``). A seed address is kept only until its node is reachable
        the advertised way, then retired -- so a node first reached via a friendly
        seed hostname migrates to its advertised client interface, while an
        unroutable advertised interface leaves the seed in place as a permanent
        fallback. Idempotent and self-correcting: it recomputes the whole desired
        set from the current snapshot each pass, so a dropped advertised
        connection quietly brings its seed back until the advertised one returns.
        """
        snapshot = self._topology
        if not snapshot.hosts:
            return  # no discovery yet: leave the bootstrap seeds untouched

        # Advertised (dial-able) address of each discovered node.
        advertised = {}  # (host, port) -> host_id
        for host_id, info in snapshot.hosts.items():
            if info.ip:
                advertised[(info.ip, info.client_port or self._port)] = host_id
        if not advertised:
            return  # nothing dial-able discovered; don't tear down what works

        with self._conn_lock:
            conns = list(self._connections)

        # Host ids we already reach via an advertised address.
        covered = {c._fser.hostId for c in conns
                   if (c.host, c.port) in advertised and c._fser.hostId is not None}

        desired = set(advertised)
        for key in list(self._seeds):
            if key in advertised:
                continue  # the seed *is* an advertised address; already desired
            seed_conn = next((c for c in conns if (c.host, c.port) == key), None)
            seed_host_id = seed_conn._fser.hostId if seed_conn is not None else None
            if seed_host_id is not None and seed_host_id in covered:
                # This node is reachable via its advertised address: the seed has
                # done its bootstrapping job, so retire it *permanently*. Dropping
                # it from the seed set (rather than merely excluding it this pass)
                # is what prevents thrashing -- otherwise, once its connection was
                # closed the seed would look uncovered again and be re-dialed. An
                # unroutable advertised interface never reaches this branch, so
                # that seed stays for good as the fallback (AC#3).
                self._seeds.discard(key)
                continue
            desired.add(key)  # keep the seed: not yet covered (or unroutable)

        self._update_targets(desired)

    def _close_undesired(self):
        """Close connections whose node is no longer a target -- but only once
        they have drained. An undesired connection already receives no new calls
        (see _pick_connection); here it is closed only when it has no in-flight
        calls, so retiring a seed (or a node leaving the cluster) does not fail a
        call already on the wire. A connection still carrying calls is left for a
        later pass; the sweeper's per-call deadline bounds how long a stuck call
        can defer the close.

        (A call picked just before retirement and still mid-send races this check
        and may fail cleanly with VoltConnectionError -- the same outcome as any
        transient connection loss, which callers already tolerate.)"""
        with self._conn_lock:
            drained = [c for c in self._connections
                       if (c.host, c.port) not in self._targets and not c._pending]
            for conn in drained:
                self._connections.remove(conn)
        for conn in drained:
            conn.close()

    # -- client-affinity routing --------------------------------------------

    def _preferred_host(self, proc_name, params):
        """Host id of the node client affinity would ideally send this call to,
        or None to round-robin.

        A single-partition procedure prefers its partition's leader; a
        multi-partition procedure prefers the multi-partition initiator's host
        (partition ``MP_INIT_PID``) -- either way saving the forwarding hop
        taken when the call lands on some other node. Anything the client
        cannot confidently place returns None: an unknown procedure (system
        procedures such as @AdHoc are never in the catalog -- and @AdHoc
        *should* spread across nodes, since each node parses its own ad-hoc
        SQL), a compound procedure (its sub-calls fan out anyway), missing
        topology, or a partition parameter value that will not hash. Affinity
        degrades to round-robin; it never fails a call."""
        snapshot = self._topology
        proc = snapshot.procedures.get(proc_name)
        if proc is None or proc.compound:
            return None
        if proc.single_partition:
            hashinator = snapshot.hashinator
            if (hashinator is None or params is None
                    or not 0 <= proc.partition_param < len(params)):
                return None
            try:
                partition = hashinator.get_partition_for_parameter(
                    proc.partition_param_type, params[proc.partition_param])
            except VoltTypeError as e:
                # No affinity opinion; send anyway and let the server route it.
                # But say so (once per procedure): a value that won't hash as
                # the catalog's declared type is usually a mistyped parameter,
                # and if that column is a STRING the server will hash the raw
                # numeric bytes while the stored row hashes as text -- writes
                # then fail mispartitioned, but only intermittently. This log
                # is the deterministic signal for that trap.
                if proc_name not in self._affinity_warned:
                    self._affinity_warned.add(proc_name)
                    log.warning("%s: partition parameter %d: %s; routing "
                                "without affinity", proc_name,
                                proc.partition_param, e)
                return None
        else:
            partition = MP_INIT_PID
        info = snapshot.partitions.get(partition)
        return info.leader_host_id if info is not None else None

    def affinity_stats(self):
        """Per-connection routing counters, as ``{(host, port): AffinityStats}``.
        Counters accumulate over a connection's lifetime and reset when it is
        re-established."""
        with self._conn_lock:
            return {(c.host, c.port): AffinityStats(c.affinity_sends,
                                                    c.round_robin_sends)
                    for c in self._connections}

    @property
    def outstanding_requests(self):
        """Number of calls currently in flight: sent (or mid-send) and not yet
        at a terminal outcome. Bounded by ``max_outstanding``."""
        with self._outstanding_cond:
            return self._outstanding

    # -- public API --------------------------------------------------------

    def call_async(self, proc_name, param_types=None, params=None,
                   timeout=None):
        """Send a procedure call; return a Future for its ``VoltResponse``.
        When the procedure's partitioning and the topology are known, the call
        is sent on the connection to the node that will execute it (client
        affinity); otherwise any connection, round-robin.

        At most ``max_outstanding`` calls may be in flight at once. A call past
        the cap blocks until a completion frees a slot, raising
        ``VoltTimeoutError`` if none does in time. The timeout is one budget
        for the whole call: time blocked waiting for a slot and time in flight
        draw on the same deadline, so a call never takes longer than its
        timeout. The cap changes only *when* a call is sent, never *where*: a
        saturated client slows the submitting thread rather than rerouting
        calls away from their partition leader.

        The returned Future composes with ``concurrent.futures.wait`` and
        ``as_completed``. Callbacks attached with ``add_done_callback`` run on
        an internal I/O thread and must be cheap and non-blocking; never issue
        a call or otherwise block from one -- the thread running it is the
        thread that delivers every response (and so frees every slot), so
        blocking it can deadlock the client. For chained or heavy
        continuations, hand off to a worker pool, or bridge to asyncio with
        ``await asyncio.wrap_future(client.call_async(...))``."""
        return self._call_async(proc_name, param_types, params, timeout)

    def _call_async(self, proc_name, param_types, params, timeout,
                    partition_destination=None):
        """``call_async`` plus one internal knob: a destination partition to
        stamp on the invocation (the version-2 extension pair, see
        ``_serialize_invocation``). A destination-stamped call routes straight
        to that partition's leader -- no hashing -- and falls back to
        round-robin like any other call when the leader has no live
        connection. Kept off the public method: the all-partition fan-out is
        its only caller, and a public per-call partition-addressing API is
        deliberately not offered."""
        if self._closed:
            raise VoltClientError("client is closed")
        limit = timeout if timeout is not None else self._procedure_timeout
        deadline = time.monotonic() + limit
        self._acquire_slot(proc_name, deadline)
        try:
            if partition_destination is not None:
                info = self._topology.partitions.get(partition_destination)
                preferred = info.leader_host_id if info is not None else None
            else:
                preferred = self._preferred_host(proc_name, params)
            conn = self._pick_connection(preferred)
            future = conn.send(proc_name, param_types, params, deadline,
                               partition_destination)
        except BaseException:
            self._release_slot()
            raise
        future.add_done_callback(self._release_slot)
        return future

    def _call_raw(self, proc_name, build, preferred_host, timeout=None):
        """Slotted async call whose invocation bytes come from ``build(handle)``,
        sent to ``preferred_host``'s connection when connected."""
        if self._closed:
            raise VoltClientError("client is closed")
        limit = timeout if timeout is not None else self._procedure_timeout
        deadline = time.monotonic() + limit
        self._acquire_slot(proc_name, deadline)
        try:
            conn = self._pick_connection(preferred_host)
            future = conn.send_raw(proc_name, build, deadline)
        except BaseException:
            self._release_slot()
            raise
        future.add_done_callback(self._release_slot)
        return future

    def call(self, proc_name, param_types=None, params=None, timeout=None):
        """Send a procedure call and block until its ``VoltResponse`` (or an
        error). The deadline is enforced by the sweeper, so this never hangs
        indefinitely."""
        return self.call_async(proc_name, param_types, params, timeout).result()

    def call_all_partitions_async(self, proc_name, param_types=None,
                                  params=None, timeout=None):
        """Call a directed or partitioned procedure once on every partition;
        return a Future for the list of per-partition outcomes.

        The target must be a *directed* procedure or one partitioned on its
        *first* parameter, and the caller does not supply that parameter: the
        client fetches one representative partition key per partition
        (``@GetPartitionKeys``, fresh on every call, so the fan-out tracks
        elastic resizes) and sends one invocation per key with the key
        prepended -- ``param_types``/``params`` carry only the remaining
        parameters. Every leg names its destination partition on the
        invocation envelope (see ``_serialize_invocation``), so the server
        executes it on exactly that partition -- the prepended key is a
        compatibility passenger, not what steers the call, and for a directed
        procedure (which declares no partition parameter) the server strips
        it before execution. Legs route to their partition's leader when the
        topology is known and draw on the outstanding-request cap; this
        method blocks for the key fetch's round trip and the per-leg submits
        before returning.

        The Future resolves to one ``PartitionCallResult`` per partition,
        ordered by partition id, once every leg reaches a terminal outcome. A
        leg's failure lands in its result's ``response`` or ``error`` -- never
        thrown away, and never failing the Future, so one dead partition
        cannot discard the others' responses. Only a failure of the fan-out
        *as a whole* raises (before a Future is returned): a closed client, a
        procedure the catalog says is unsuited to all-partition execution, or
        a failed ``@GetPartitionKeys``. The timeout is a single budget for the
        whole operation: the key fetch and every leg share one deadline.

        Callbacks attached to the returned Future run on an internal I/O
        thread; the ``call_async`` contract (cheap, non-blocking, never issue
        a call) applies. Each leg executes on the partition it names, so a
        briefly stale key cannot redirect it during an elastic resize; the
        result list reflects the partitions as enumerated at dispatch, which
        can lag a concurrent resize."""
        if self._closed:
            raise VoltClientError("client is closed")
        limit = timeout if timeout is not None else self._procedure_timeout
        deadline = time.monotonic() + limit

        # The named destinations re-enable the server's own vetting of
        # all-partition calls (InvocationDispatcher), so this check is only a
        # fast-fail courtesy when the catalog knows the procedure. An unknown
        # procedure is dispatched as-is; the server still has the final word.
        proc = self._topology.procedures.get(proc_name)
        if proc_name.startswith("@") or (proc is not None and (
                not proc.single_partition or proc.compound
                or proc.partition_param not in (0, _PARAMETER_NONE))):
            raise VoltClientError(
                "%s is not valid for all-partition execution: the procedure "
                "must be a directed procedure or partitioned on its first "
                "parameter, and must not be a system procedure" % proc_name)

        resp = self._discovery_call(
            "@GetPartitionKeys", [FastSerializer.VOLTTYPE_STRING],
            ["INTEGER"], deadline - time.monotonic())
        pairs = _parse_partition_keys(resp.tables[0] if resp.tables else None)
        if not pairs:
            raise VoltClientError(
                "@GetPartitionKeys returned no usable partition keys")

        full_types = (FastSerializer.VOLTTYPE_INTEGER,) + tuple(param_types or ())
        tail_params = tuple(params or ())
        outer = Future()
        results = [None] * len(pairs)
        lock = threading.Lock()
        remaining = [len(pairs)]

        def record(index, result):
            with lock:
                results[index] = result
                remaining[0] -= 1
                last = remaining[0] == 0
            # done() here means the caller cancelled the aggregate; the legs
            # were already in flight and completed on their own.
            if last and not outer.done():
                outer.set_result(results)

        def on_done(index, partition, key, future):
            try:
                record(index, PartitionCallResult(key, partition,
                                                  response=future.result()))
            except BaseException as e:  # what call() would have raised
                record(index, PartitionCallResult(key, partition, error=e))

        for index, (partition, key) in enumerate(pairs):
            try:
                future = self._call_async(proc_name, full_types,
                                          (key,) + tail_params,
                                          deadline - time.monotonic(),
                                          partition_destination=partition)
            except BaseException as e:
                # A failed dispatch is that partition's outcome, not the
                # fan-out's: the other legs proceed.
                record(index, PartitionCallResult(key, partition, error=e))
                continue
            future.add_done_callback(
                lambda f, i=index, p=partition, k=key: on_done(i, p, k, f))
        return outer

    def call_all_partitions(self, proc_name, param_types=None, params=None,
                            timeout=None):
        """Call a directed or partitioned procedure once on every partition
        and block for the aggregate: one ``PartitionCallResult`` per
        partition, ordered by partition id. An individual partition's failure
        never raises -- check each result's ``ok`` (or ``response``/``error``).
        See ``call_all_partitions_async`` for the full contract."""
        return self.call_all_partitions_async(
            proc_name, param_types, params, timeout).result()

    def get_partition_for_parameter(self, vtype, value):
        """The partition id that owns ``value`` when hashed as declared type
        ``vtype``, per the current topology snapshot.

        ``vtype`` is a ``VoltType`` code and must be the
        *declared* type of the partitioning column or parameter -- for a
        procedure, ``ProcedureInfo.partition_param_type`` from
        ``client.topology.procedures``. Passing the declared type matters:
        the integer ``5`` and the string ``"5"`` hash to different partitions.
        The partitionable types are TINYINT, SMALLINT, INTEGER, BIGINT, STRING,
        and VARBINARY; ``None`` (SQL NULL) always maps to partition 0.

        Raises ``VoltTypeError`` for a non-partitionable ``vtype`` or a value
        that cannot hash as it, and ``VoltClientError`` when no hashinator is
        available (topology not yet discovered, or the cluster advertises a
        hash scheme this client does not implement)."""
        hashinator = self._topology.hashinator
        if hashinator is None:
            raise VoltClientError("no hashinator available: topology not "
                                  "discovered, or unsupported cluster hash type")
        return hashinator.get_partition_for_parameter(vtype, value)

    # -- topology discovery ------------------------------------------------

    @property
    def topology(self):
        """The most recent ``TopologySnapshot`` (empty until the first refresh).
        The refresh swap is a single atomic assignment, so this needs no lock."""
        return self._topology

    def refresh_topology(self, timeout=None):
        """Discover cluster metadata and atomically replace the stored snapshot:
        ``@SystemInformation OVERVIEW`` (host addresses), ``@Statistics TOPO``
        (partition leaders + the hashinator config), and ``@SystemCatalog
        PROCEDURES`` (per-procedure partitioning). Safe to call concurrently with
        in-flight calls and with itself -- each invocation builds its own snapshot
        and the last to complete wins. Returns the new ``TopologySnapshot``.

        Runs automatically: at construction, whenever the topology-push
        subscription is (re)established, and on the heels of a topology push.
        Applications normally never need to call it; it stays public as a
        manual backstop for those that want a refresh on their own schedule.

        Raises ``VoltClientError`` if any discovery call fails, leaving the
        previous snapshot in place."""
        overview = self._discovery_call(
            "@SystemInformation", [FastSerializer.VOLTTYPE_STRING],
            ["OVERVIEW"], timeout)
        topo = self._discovery_call(
            "@Statistics",
            [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_INTEGER],
            ["TOPO", 0], timeout)
        procedures = self._discovery_call(
            "@SystemCatalog", [FastSerializer.VOLTTYPE_STRING],
            ["PROCEDURES"], timeout)
        snapshot = _build_snapshot(overview, topo, procedures)
        with self._topology_lock:
            self._topology = snapshot
        return snapshot

    def _discovery_call(self, proc_name, param_types, params, timeout):
        """Issue one discovery procedure and insist it succeeded; discovery can't
        build a usable map from a failed call, so a failure aborts the refresh."""
        resp = self.call(proc_name, param_types, params, timeout)
        if resp.status != _STATUS_SUCCESS:
            raise VoltClientError(
                "topology discovery via %s %s failed: %s"
                % (proc_name, params[0], resp.statusString))
        return resp

    def close(self):
        """Stop background threads and tear down all connections."""
        if self._closed:
            return
        self._closed = True
        # Wake any caller blocked waiting for an outstanding-request slot; it
        # re-checks _closed and fails fast instead of waiting out its timeout.
        with self._outstanding_cond:
            self._outstanding_cond.notify_all()
        self._sweeper_stop.set()
        # _reconnect is created after the initial connect; guard in case a
        # constructor failure calls close() before it exists.
        reconnect = getattr(self, "_reconnect_stop", None)
        if reconnect is not None:
            self._reconnect_stop.set()
        with self._conn_lock:
            conns = list(self._connections)
            self._connections.clear()
            self._targets.clear()
        for conn in conns:
            conn.close()
        self._sweeper.join(timeout=2.0)
        if getattr(self, "_reconnect", None) is not None:
            self._reconnect.join(timeout=2.0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# -- bulk loader --------------------------------------------------------------
#
# Python counterpart of the Java client's VoltBulkLoader: rows are buffered per
# partition and each batch is one @LoadSinglepartitionTable transaction.

_LOADABLE_TYPES = {"TINYINT": FastSerializer.VOLTTYPE_TINYINT,
                   "SMALLINT": FastSerializer.VOLTTYPE_SMALLINT,
                   "INTEGER": FastSerializer.VOLTTYPE_INTEGER,
                   "BIGINT": FastSerializer.VOLTTYPE_BIGINT,
                   "FLOAT": FastSerializer.VOLTTYPE_FLOAT,
                   "VARCHAR": FastSerializer.VOLTTYPE_STRING,
                   "TIMESTAMP": FastSerializer.VOLTTYPE_TIMESTAMP,
                   "DECIMAL": FastSerializer.VOLTTYPE_DECIMAL,
                   "VARBINARY": FastSerializer.VOLTTYPE_VARBINARY}
_T_TINYINT = FastSerializer.VOLTTYPE_TINYINT
_T_SMALLINT = FastSerializer.VOLTTYPE_SMALLINT
_T_INTEGER = FastSerializer.VOLTTYPE_INTEGER
_T_BIGINT = FastSerializer.VOLTTYPE_BIGINT
_T_FLOAT = FastSerializer.VOLTTYPE_FLOAT
_T_STRING = FastSerializer.VOLTTYPE_STRING
_T_TIMESTAMP = FastSerializer.VOLTTYPE_TIMESTAMP
_T_DECIMAL = FastSerializer.VOLTTYPE_DECIMAL
_T_VARBINARY = FastSerializer.VOLTTYPE_VARBINARY
_NULL_I8 = FastSerializer.NULL_TINYINT_INDICATOR
_NULL_I16 = FastSerializer.NULL_SMALLINT_INDICATOR
_NULL_I32 = FastSerializer.NULL_INTEGER_INDICATOR
_NULL_I64 = FastSerializer.NULL_BIGINT_INDICATOR
_NULL_F64 = FastSerializer.NULL_FLOAT_INDICATOR
_INT_COLUMN = {_T_TINYINT: (struct.Struct(">b"), _NULL_I8),
               _T_SMALLINT: (struct.Struct(">h"), _NULL_I16),
               _T_INTEGER: (struct.Struct(">i"), _NULL_I32),
               _T_BIGINT: (struct.Struct(">q"), _NULL_I64),
               _T_TIMESTAMP: (struct.Struct(">q"), _NULL_I64)}
_S_I32 = struct.Struct(">i")
_S_U32 = struct.Struct(">I")
_S_U64 = struct.Struct(">Q")
_M64 = 0xFFFFFFFFFFFFFFFF
_S_ROW_D = struct.Struct(">d")
_NULL_LEN = b"\xff\xff\xff\xff"                    # NULL string / varbinary
_NULL_DECIMAL = (-(1 << 127)).to_bytes(16, "big", signed=True)
_DECIMAL_SCALE = 12
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_ONE_US = datetime.timedelta(microseconds=1)
_DRAIN_REFLUSH_SECONDS = 0.05


def _timestamp_micros(v):
    """Microseconds since the epoch; a naive datetime is local time, matching
    what the decoder returns for TIMESTAMP columns."""
    if isinstance(v, datetime.datetime):
        if v.tzinfo is None:
            v = v.astimezone()
        return (v - _EPOCH) // _ONE_US
    if isinstance(v, bool):
        raise VoltTypeError("TIMESTAMP needs a datetime or int microseconds")
    return operator.index(v)


def _decimal_bytes(v):
    """DECIMAL wire form: the scale-12 unscaled value, 16 bytes two's complement."""
    if isinstance(v, bool) or not isinstance(v, (decimal.Decimal, int)):
        raise VoltTypeError("DECIMAL needs decimal.Decimal or int, got %s"
                            % type(v).__name__)
    sign, digits, exp = decimal.Decimal(v).as_tuple()
    if not isinstance(exp, int):
        raise VoltTypeError("DECIMAL cannot be NaN or infinite")
    if -exp > _DECIMAL_SCALE:
        raise VoltTypeError("DECIMAL scale %d exceeds %d" % (-exp, _DECIMAL_SCALE))
    if len(digits) + exp > 26:
        raise VoltTypeError("DECIMAL has more than 26 digits before the point")
    unscaled = int("".join(map(str, digits)) or "0") * 10 ** (_DECIMAL_SCALE + exp)
    return (-unscaled if sign else unscaled).to_bytes(16, "big", signed=True)


def _make_row_encoder(types, names):
    """Encoder for one VoltTable row. Strict: a value that does not fit its
    column raises VoltTypeError naming the column, rather than being coerced."""
    def enc(fields):
        parts = []
        i = 0
        try:
            for i in range(len(types)):
                t, v = types[i], fields[i]
                if t == _T_STRING:
                    if v is None:
                        parts.append(_NULL_LEN)
                    elif isinstance(v, str):
                        e = v.encode("utf-8")
                        parts.append(_S_I32.pack(len(e)))
                        parts.append(e)
                    else:
                        raise VoltTypeError("needs str, got %s" % type(v).__name__)
                elif t == _T_FLOAT:
                    if v is None:
                        parts.append(_S_ROW_D.pack(_NULL_F64))
                    elif isinstance(v, (str, bytes, bool)):
                        raise VoltTypeError("needs a number, got %s" % type(v).__name__)
                    else:
                        parts.append(_S_ROW_D.pack(float(v)))
                elif t in _INT_COLUMN:
                    st, null = _INT_COLUMN[t]
                    if v is None:
                        parts.append(st.pack(null))
                        continue
                    if isinstance(v, bool):
                        raise VoltTypeError("needs an integer, got bool")
                    n = _timestamp_micros(v) if t == _T_TIMESTAMP else operator.index(v)
                    if n == null:
                        raise VoltTypeError("%d is the column type's NULL value" % n)
                    parts.append(st.pack(n))
                elif t == _T_DECIMAL:
                    parts.append(_NULL_DECIMAL if v is None else _decimal_bytes(v))
                else:  # VARBINARY
                    if v is None:
                        parts.append(_NULL_LEN)
                    else:
                        b = _as_binary(v)
                        if b is None:
                            raise VoltTypeError("needs bytes, got %s" % type(v).__name__)
                        parts.append(_S_I32.pack(len(b)))
                        parts.append(b)
        except (VoltTypeError, TypeError, ValueError, OverflowError, struct.error) as e:
            raise VoltTypeError("column %s: %s" % (names[i], e)) from None
        data = b"".join(parts)
        return _S_I32.pack(len(data)) + data
    return enc


class VoltBulkLoader:
    """Loads rows into one table in per-partition batches, each sent as a
    single ``@LoadSinglepartitionTable`` call (``@LoadMultipartitionTable`` for
    a replicated table) to the partition's leader.

    ``insert_row(*fields)`` takes values in column order; a row that cannot be
    encoded fails immediately through ``failure_callback(fields, exception)``.
    A batch the server rejects, or whose outcome is unknown (connection lost or
    timed out), is retried row by row, and each row that still fails is
    reported as ``failure_callback(fields, response_or_exception)``. After an
    unknown outcome, insert mode may report a row that did load as a
    duplicate-key failure, or insert it twice without a primary key; use
    ``upsert=True`` if that matters. The callback may run on a client network
    thread: keep it quick, and do not call the loader or the client from it.
    Without one, the first failure is logged as a warning and the rest are only
    counted.
    ``flush()`` sends partial batches; ``drain()`` also waits for every row;
    ``close()`` stops accepting rows, then drains (it is also a context
    manager). ``insert_row`` may be called from several threads.
    ``upsert=True`` requires a primary key."""

    def __init__(self, client, table_name, batch_size=1000, upsert=False,
                 failure_callback=None, max_batches_in_flight=32, timeout=None):
        self._client = client
        self._batch_size = max(1, int(batch_size))
        self._failure_callback = failure_callback
        self._timeout = timeout
        want = table_name.upper()
        t = client.call("@SystemCatalog", (_T_STRING,), ("COLUMNS",)).tables[0]
        ix = {c.name: i for i, c in enumerate(t.columns)}
        cols = sorted((r for r in t.tuples if r[ix["TABLE_NAME"]] == want),
                      key=lambda r: r[ix["ORDINAL_POSITION"]])
        if not cols:
            raise VoltClientError("table %s not found" % table_name)
        self._table_name = want
        self._column_names = tuple(r[ix["COLUMN_NAME"]] for r in cols)
        types = []
        for r in cols:
            vt = _LOADABLE_TYPES.get(r[ix["TYPE_NAME"]])
            if vt is None:
                raise VoltClientError("column %s: type %s is not supported by the "
                                      "bulk loader" % (r[ix["COLUMN_NAME"]], r[ix["TYPE_NAME"]]))
            types.append(vt)
        self._column_types = tuple(types)
        pcols = [i for i, r in enumerate(cols) if r[ix["REMARKS"]] == "PARTITION_COLUMN"]
        self._pidx = pcols[0] if pcols else None
        if self._pidx is not None and client._topology.hashinator is None:
            raise VoltClientError("cannot partition rows: no hashinator in the topology")
        if upsert:
            pk = client.call("@SystemCatalog", (_T_STRING,), ("PRIMARYKEYS",)).tables[0]
            pix = {c.name: i for i, c in enumerate(pk.columns)}
            if not any(r[pix["TABLE_NAME"]] == want for r in pk.tuples):
                raise VoltClientError("upsert needs a primary key; table %s has none" % want)
        self._encode = _make_row_encoder(self._column_types, self._column_names)

        header = bytearray(b"\x00")                        # table status
        header += len(types).to_bytes(2, "big")
        header += bytes(t & 0xFF for t in types)
        for name in self._column_names:
            e = name.encode("utf-8")
            header += _S_I32.pack(len(e)) + e
        self._table_header = _S_I32.pack(len(header)) + bytes(header)
        self._proc = ("@LoadMultipartitionTable" if self._pidx is None
                      else "@LoadSinglepartitionTable")
        pn = self._proc.encode("utf-8")
        self._inv_head = b"\x00" + _S_U32.pack(len(pn)) + pn
        tn = want.encode("utf-8")
        self._name_param = b"\x09" + _S_I32.pack(len(tn)) + tn
        self._upsert_param = b"\x03" + (b"\x01" if upsert else b"\x00")

        self._lock = threading.Lock()      # guards _buffers
        self._buffers = {}                 # partition id -> ([row bytes], [fields])
        self._inflight = threading.BoundedSemaphore(max(1, int(max_batches_in_flight)))
        self._cond = threading.Condition()  # guards the counters
        self._outstanding = 0
        self._completed_row_count = 0
        self._failed_row_count = 0
        self._closed = False               # set under _lock: insert_row checks it there
        self._warned = False               # first failure logged (no failure_callback)
        self._retries = queue.SimpleQueue()
        self._retry_thread = threading.Thread(target=self._retry_loop,
                                              name="voltclient-loader-retry", daemon=True)
        self._retry_thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not issubclass(exc_type, Exception):
            return False  # Ctrl-C or SystemExit: exit now, abandon rows in flight
        self.close()
        return False

    @property
    def table_name(self):
        """The table's name as the catalog spells it (upper case)."""
        return self._table_name

    @property
    def column_names(self):
        """Column names in table order; ``insert_row`` takes values in this order."""
        return self._column_names

    @property
    def column_types(self):
        """Column ``VoltType`` codes in table order."""
        return self._column_types

    @property
    def completed_row_count(self):
        """Rows loaded so far."""
        return self._completed_row_count

    @property
    def failed_row_count(self):
        """Rows rejected so far, by the client or the server."""
        return self._failed_row_count

    @property
    def outstanding_row_count(self):
        """Rows accepted but not yet loaded or failed."""
        return self._outstanding

    # -- producer side ------------------------------------------------------

    def insert_row(self, *fields):
        if self._closed:
            raise VoltClientError("bulk loader is closed")
        if len(fields) != len(self._column_types):
            self._fail_now(fields, VoltClientError(
                "row has %d values, table %s has %d columns"
                % (len(fields), self._table_name, len(self._column_types))))
            return
        try:
            rb = self._encode(fields)
            pid = None if self._pidx is None else self._partition_of(fields[self._pidx])
        except VoltClientError as e:
            self._fail_now(fields, e)
            return
        with self._lock:
            # Checked under the lock close() sets it with, so a row is either
            # buffered before close() drains or rejected.
            if self._closed:
                raise VoltClientError("bulk loader is closed")
            with self._cond:
                self._outstanding += 1
            buf = self._buffers.get(pid)
            if buf is None:
                buf = self._buffers[pid] = ([], [])
            buf[0].append(rb)
            buf[1].append(fields)
            full = len(buf[0]) >= self._batch_size
            if full:
                del self._buffers[pid]
        if full:
            self._send(pid, buf[0], buf[1])

    def insert_rows(self, rows):
        for r in rows:
            self.insert_row(*r)

    def flush(self):
        with self._lock:
            bufs, self._buffers = self._buffers, {}
        for pid, (rbs, fields) in bufs.items():
            self._send(pid, rbs, fields)

    def drain(self):
        """Send partial batches and wait until no row is outstanding. Rows other
        threads insert meanwhile are sent too, so with producers still running
        this returns once they pause and everything sent has finished."""
        while True:
            self.flush()
            with self._cond:
                if not self._outstanding:
                    return
                # Re-flush periodically: a concurrent insert may have left a
                # partial batch that nothing else would send.
                self._cond.wait(_DRAIN_REFLUSH_SECONDS)

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True  # insert_row rejects rows from here on
        self.drain()
        if self._failed_row_count and self._failure_callback is None:
            log.warning("bulk loader for %s: %d row(s) failed", self._table_name,
                        self._failed_row_count)
        self._retries.put(None)
        self._retry_thread.join(timeout=5)

    def _partition_of(self, value):
        if value is None:
            raise VoltTypeError("partition column %s is NULL"
                                % self._column_names[self._pidx])
        vtype = self._column_types[self._pidx]
        if vtype in _INT_COLUMN:
            value = operator.index(value)  # e.g. numpy integers
        return self._client._topology.hashinator.get_partition_for_parameter(vtype, value)

    # -- sending ------------------------------------------------------------

    def _invocation(self, pid, rbs, fields):
        vt = self._table_header + _S_I32.pack(len(rbs)) + b"".join(rbs)
        vt = _S_I32.pack(len(vt)) + vt                     # table length prefix
        if pid is None:
            params = [b"\x00\x03", self._name_param, self._upsert_param, b"\x15", vt]
        else:
            vtype = self._column_types[self._pidx]
            pv = fields[0][self._pidx]
            pbytes = _partition_value_to_bytes(
                vtype, operator.index(pv) if vtype in _INT_COLUMN else pv)
            params = [b"\x00\x04", b"\x19", _S_I32.pack(len(pbytes)), pbytes,
                      self._name_param, self._upsert_param, b"\x15", vt]
        head, tail = self._inv_head, b"".join(params)

        def build(handle):
            body = head + _S_U64.pack(handle & _M64) + tail
            return _S_U32.pack(len(body)) + body
        return build

    def _send(self, pid, rbs, fields, retry=False):
        self._inflight.acquire()
        try:
            info = self._client._topology.partitions.get(MP_INIT_PID if pid is None else pid)
            host = info.leader_host_id if info is not None else None
            fut = self._client._call_raw(self._proc, self._invocation(pid, rbs, fields),
                                         host, self._timeout)
        except Exception as e:
            self._inflight.release()
            self._batch_failed(pid, rbs, fields, e, retry)
            return
        fut.add_done_callback(lambda f: self._batch_done(f, pid, rbs, fields, retry))

    def _batch_done(self, f, pid, rbs, fields, retry):
        self._inflight.release()
        try:
            outcome = f.result()
            ok = outcome.status == 1
        except Exception as e:
            ok, outcome = False, e
        if ok:
            self._finish(len(rbs), 0)
        else:
            self._batch_failed(pid, rbs, fields, outcome, retry)

    def _batch_failed(self, pid, rbs, fields, outcome, retry):
        if retry or len(rbs) == 1:
            for fl in fields:
                self._report(fl, outcome)
            self._finish(0, len(rbs))
        else:
            # Never resubmit from a reader thread: it would wait for in-flight
            # slots that only it can free.
            self._retries.put((pid, rbs, fields))

    def _retry_loop(self):
        while True:
            item = self._retries.get()
            if item is None:
                return
            pid, rbs, fields = item
            for rb, fl in zip(rbs, fields):
                self._send(pid, [rb], [fl], retry=True)

    def _finish(self, ok, failed):
        with self._cond:
            self._completed_row_count += ok
            self._failed_row_count += failed
            self._outstanding -= ok + failed
            if not self._outstanding:
                self._cond.notify_all()

    def _report(self, fields, outcome):
        if self._failure_callback is None:
            if not self._warned:
                self._warned = True
                reason = getattr(outcome, "statusString", None) or outcome
                log.warning("bulk loader for %s: a row failed and no failure_callback "
                            "is set, so further failures are only counted "
                            "(failed_row_count). First failure: %s",
                            self._table_name, reason)
            return
        try:
            self._failure_callback(fields, outcome)
        except Exception:
            log.exception("bulk loader failure_callback raised")

    def _fail_now(self, fields, exc):
        with self._cond:
            self._failed_row_count += 1
        self._report(fields, exc)
