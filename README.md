# VoltDB Python Client Libraries

This repository contains two Python client libraries for VoltDB:

| Module            | Main classes                      | Role                                 |
|-------------------|-----------------------------------|--------------------------------------|
| `voltdbclient.py` | `FastSerializer`, `VoltProcedure` | The original client and wire codec   |
| `voltclient.py`   | `VoltClient`, `VoltBulkLoader`    | Topology-aware client built on it    |

`voltdbclient` is the original client: a single connection to a single server, called synchronously, plus the wire-protocol serialization for all of VoltDB's data types. `voltclient` builds on it — `VoltClient` holds a connection to every cluster node, routes each call to the node that will execute it, supports asynchronous calls, and recovers automatically from cluster changes. New applications should generally start with `VoltClient`; existing `FastSerializer` code needs **no changes** and keeps working as-is.

Because `VoltClient` uses `voltdbclient`'s serialization underneath, the two modules share the same type constants and return the same `VoltResponse` / `VoltTable` objects — they interoperate naturally, and you can migrate one call site at a time.

Both modules are pure Python with no third-party dependencies. `voltclient` and the PyPI package require Python 3.9 or later; `voltdbclient.py` on its own still accepts Python 3.6.

## Installation

Both modules are published to PyPI as one package:

```
pip install voltdbclient
```

Reading Java keystores for TLS and Kerberos authentication need optional third-party packages: `pip install "voltdbclient[extras]"`.

The VoltDB distribution also ships both modules in `lib/python`, where VoltDB's own command-line tools load them from. To use that copy, put the directory on your `PYTHONPATH` (or append it to `sys.path`). `voltclient` imports `voltdbclient`, so if you copy the files rather than the directory, keep the two together.

## Quick start

Connect, call a procedure, and read the result:

```python
from voltclient import VoltClient, VoltType

with VoltClient(hosts=["voltsvr1", "voltsvr2"]) as client:
    response = client.call("Vote",
                           [VoltType.BIGINT, VoltType.INTEGER],
                           [5551234567, 2])
    if response.status == 1:  # ClientResponse.SUCCESS
        for row in response.tables[0].tuples:
            print(row)
```

Give `hosts` any subset of the cluster: the client discovers the full membership at connect time and opens one connection per node. Use the client as a context manager (or call `close()`) so its background threads and sockets are always torn down.

## Calls

- `call(proc_name, param_types, params, timeout=None)` — send and block for the `VoltResponse`.
- `call_async(...)` — send and return a `concurrent.futures.Future` immediately. Compose with the standard library:

```python
import concurrent.futures

futures = [client.call_async("STORE.upsert",
                             [VoltType.STRING, VoltType.VARBINARY],
                             [key, value])
           for key, value in batch]
for f in concurrent.futures.as_completed(futures):
    if f.result().status != 1:
        ...
```

`asyncio` applications can bridge a call with `await asyncio.wrap_future(client.call_async(...))`.

One caution: callbacks registered with `Future.add_done_callback` run on the client's network reader threads. Keep them short and never block in them (no `call` or `call_async`, no lock waits) — parse, record, hand off.

- `call_all_partitions(proc_name, param_types=None, params=None, timeout=None)` — run a procedure once on every partition and return one `PartitionCallResult` per partition (sorted by partition id), each carrying either the partition's `response` or the `error` that ended its leg; `ok` summarizes. Works with procedures partitioned on their first parameter and with directed procedures. `call_all_partitions_async` is the Future form. One timeout budget covers the whole fan-out.
- `get_partition_for_parameter(vtype, value)` — which partition a value hashes to, using the same elastic hashinator the cluster uses. Useful for batching work by partition before sending it.

## Bulk loading

`VoltBulkLoader` loads rows into one table the way the Java client's bulk loader does: rows are grouped by partition and each batch is sent as a single `@LoadSinglepartitionTable` transaction to the partition's leader (`@LoadMultipartitionTable` for a replicated table). One transaction per batch instead of per row cuts both client and server work per row.

```python
from voltclient import VoltBulkLoader

def failed(row, outcome):          # a VoltResponse or an exception
    print("not loaded:", row, outcome)

with VoltBulkLoader(client, "store", batch_size=1000, upsert=True,
                    failure_callback=failed) as loader:
    for key, value in rows:
        loader.insert_row(key, value)   # values in column order
print(loader.completed_row_count, loader.failed_row_count)
```

- The table's columns, types and partition column are read from the catalog when the loader is created. `upsert=True` needs a primary key on the table.
- `insert_row` checks each value against its column (no silent coercion or truncation: a `str` for an `INTEGER` column, an out-of-range number, or a NULL partition key fails that row through `failure_callback` with a `VoltTypeError`). TIMESTAMP accepts `datetime` (naive means local time) or integer microseconds since the epoch; DECIMAL accepts `decimal.Decimal` or `int`; VARBINARY accepts any bytes-like value.
- If the server rejects a batch (a constraint violation, a value too long for its column), none of its rows are inserted, so they are retried one at a time and only the rows that actually fail are reported. A batch whose outcome is unknown (connection lost or timed out) is retried the same way. In insert mode, a row that did load may then be reported as a duplicate-key failure, or inserted twice on a table without a primary key. Use `upsert=True` if that matters.
- `failure_callback` runs on a client network thread. Keep it quick, and don't call the loader (`insert_row`, `flush`, `drain`, `close`) or the client from it. To re-insert a corrected row, hand it to another thread.
- `flush()` sends partially filled batches, `drain()` also waits until every row is loaded or failed (including rows other threads insert meanwhile), and `close()` (or leaving the `with` block) stops accepting rows, then drains. `insert_row` may be called from several threads. Without a `failure_callback`, the first failure is logged as a warning and `close()` logs the total.
- A `batch_size` of 500–1,000 is a good default: much larger batches leave too few in flight per partition to keep the cluster busy. `max_batches_in_flight` (default 32) bounds memory and server queue depth.
- Supported column types: TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, VARCHAR, TIMESTAMP, DECIMAL, VARBINARY.

## Responses and types

Both clients return the same objects, defined in `voltdbclient`:

- `VoltResponse` — `status` (integer; 1 is success — see the VoltDB documentation for the full list), `statusString` (human-readable meaning), `roundtripTime` (milliseconds), `tables` (list of `VoltTable`, or `None`), and `exception` (a `VoltException`, or `None`).
- `VoltTable` — `columns` (list of `VoltColumn`, each with `name` and `type`) and `tuples` (list of rows; each row is a list of values already converted to Python types).
- `VoltException` — `type` (one of the `VOLTEXCEPTION_*` constants: `NONE`, `EEEXCEPTION`, `SQLEXCEPTION`, `CONSTRAINTFAILURE`, `GENERIC`) and `message`.

Parameter types are declared with the `VoltType` constants (`VoltType.BIGINT`, `VoltType.STRING`, ...; these alias the original module's `FastSerializer.VOLTTYPE_*` values), and values map between VoltDB and Python as follows:

| VoltDB type | Python type         |
|-------------|---------------------|
| NULL        | `None`              |
| TINYINT     | `int`               |
| SMALLINT    | `int`               |
| INTEGER     | `int`               |
| BIGINT      | `int`               |
| FLOAT       | `float`             |
| DECIMAL     | `decimal.Decimal`   |
| STRING      | `str`               |
| TIMESTAMP   | `datetime.datetime` |
| VARBINARY   | `bytes` (byte array on read) |

Python does not distinguish integer widths, but a value serialized as a given type must fit it — an out-of-range value raises an error. Array parameters are passed as Python lists, declared with their element type. The `Geography` and `XYZPoint` value classes are importable from `voltclient` as well.

## Topology awareness

The client keeps its view of the cluster current by **subscription, not polling**: it registers for server push notifications on one of its connections, and the cluster pushes refreshed topology and procedure-catalog data whenever they change. In practice this means:

- **Client affinity.** Single-partition procedure calls are sent directly to the node leading their partition, and multi-partition calls to the multi-partition initiator — skipping a server-side forwarding hop. Calls the client cannot place (unknown procedures, `@AdHoc`, unhashable values) simply round-robin; routing degrades, it never fails a call. `affinity_stats()` reports affinity-routed vs round-robin sends per connection.
- **Self-healing membership.** Node failures, rejoins, and elastic cluster expansions flow into the connection pool and the routing tables without an application restart. A lost connection's in-flight calls fail (see the failure contract below); the pool redials and re-converges in the background with capped exponential backoff.
- **No stale-routing hazard.** A call routed on a topology that just changed is forwarded or transparently restarted by the cluster on the correct node — a small latency cost, never a correctness one.

`client.topology` exposes the current snapshot (hosts, partition leaders, procedure partitioning) and `refresh_topology()` remains available as a manual backstop for applications that want their own schedule on top.

## Bounding load: `max_outstanding`

`call_async` admits at most `max_outstanding` calls in flight (default 3000); a caller past the cap blocks until a completion frees a slot, and the wait draws on that call's own timeout. This bound is the client's load governor, and tuning it is the single most useful performance knob:

- Throughput rises with the cap only until the cluster (or the client process) saturates — the "knee" of the curve. Past the knee, extra outstanding calls add **only queue latency**: throughput stays flat while tail latencies grow by orders of magnitude.
- For latency-sensitive applications, set `max_outstanding` near the knee measured on your hardware (often tens, not thousands).
- The 3000 default suits bulk loading, where total throughput matters and per-call latency does not.

## Failure contract

All errors derive from `VoltClientError`:

- `VoltConnectionError` — the connection carrying a call was lost before its response arrived. The call's outcome is **unknown**: it may or may not have executed. The client never resends such a call, because only the application can know whether a retry is safe — retry freely if the operation is idempotent, otherwise look up the outcome first. Sends are written by a background thread, so a call made just as its connection dies may be reported through its `Future` rather than raised by `call_async`; handle both.
- `VoltNoConnectionsError` — no live connections to send on.
- `VoltTimeoutError` — no response within the call's timeout (default 120s), or the call could not obtain an outstanding-request slot in time.
- `VoltTypeError` — a parameter could not be serialized as the declared type (or, for `VoltBulkLoader`, a row value does not fit its column).

Server-side failures (constraint violations, procedure exceptions) are not Python exceptions: they arrive as a normal `VoltResponse` with a non-success `status` and `statusString`.

## Performance characteristics

What to expect, and why (measured with the benchmark harness noted below):

- A synchronous single-connection client's throughput ceiling is one call per round-trip: `FastSerializer` used directly cannot exceed `1/RTT` calls per second. `VoltClient`'s pipelining removes that wall: its ceiling is the Python interpreter itself — one CPU core's worth of serialization and dispatch, independent of network latency. Writes and reads are batched on the socket, so one process sustains on the order of 50,000 single-row calls per second against a local server (measured with small rows; your numbers depend on row size and hardware).
- Latency at moderate concurrency is competitive with any client: per-call time is dominated by the network and the server, and affinity routing removes a network hop from every single-partition call.
- A single Python process will not saturate a VoltDB cluster row by row — that is a property of CPython, not of this client. For loading tables, `VoltBulkLoader` runs one server transaction per batch instead of per row, which is several times faster; beyond that, run several processes.

The benchmark harness lives in the VoltDB source tree at `tests/pyclient/bench.py`. It drives the well-known `voltkv` workload — same schema, keys, and default procedures as the voltkv example app — comparing direct `FastSerializer` usage against `VoltClient` across a sweep of `max_outstanding` values and printing throughput with latency percentiles for each point, then compares loading the key pool row by row with `VoltBulkLoader` at several batch sizes.

## Migrating from `voltdbclient`

Existing `FastSerializer` code needs no changes. When you do want a call site on the new client, the mapping is mechanical — the types and response objects are the same:

```python
# before
fs = FastSerializer("voltsvr1", 21212)
proc = VoltProcedure(fs, "Vote", [FastSerializer.VOLTTYPE_BIGINT,
                                  FastSerializer.VOLTTYPE_INTEGER])
response = proc.call([5551234567, 2])

# after
client = VoltClient(hosts=["voltsvr1"])
response = client.call("Vote", [VoltType.BIGINT, VoltType.INTEGER],
                       [5551234567, 2])
```

Differences to be aware of:

- One `VoltClient` replaces a `FastSerializer` *per node* plus your own pooling and failover code.
- Procedures are not pre-declared; pass the parameter types with each call.
- Authentication, TLS, and Kerberos options pass through the `VoltClient` constructor to `FastSerializer` unchanged.

## Using `voltdbclient` directly

The original API remains fully supported and is the right tool for small scripts talking to one server. Create a `FastSerializer` connection, declare each stored procedure as a `VoltProcedure` with its parameter types, and call it:

```python
from voltdbclient import *

client = FastSerializer("voltsvr1", 21212)  # optional: username, password, usessl, ...
proc = VoltProcedure(client, "@SystemInformation",
                     [FastSerializer.VOLTTYPE_STRING])
response = proc.call(["OVERVIEW"])
client.close()
```

Details worth knowing:

- `VoltProcedure.call(params, response=True, timeout=None)` — with `response=False` the call is sent without waiting for the result (returns `None`); `timeout` (seconds) raises `socket.timeout` if the server does not respond in time.
- The `FastSerializer` constructor also accepts `dump_file_path`, which writes all bytes sent and received to a file — occasionally useful for debugging at the wire level.
- **Thread safety differs between the modules.** Nothing in `voltdbclient` is thread-safe: give each thread its own `FastSerializer`. A `VoltClient`, by contrast, is designed to be shared — many threads may call one instance concurrently.

## Logging

The client logs through the standard `logging` module under the logger name `voltclient` (connection lifecycle at INFO/DEBUG, degraded-routing and subscription warnings at WARNING). It adds no handlers of its own.

Full API details are in the module docstrings: `pydoc voltclient`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build the package and run the tests.
