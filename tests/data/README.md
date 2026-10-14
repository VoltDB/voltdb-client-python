# Hashinator parity fixtures

`test_hashinator_parity.py` checks the Python hashinator in `voltclient` against these fixtures, which come from the server's Java implementation:

| File | Contents |
|---|---|
| `hashinator_ring_raw.bin` | An 8-partition, 16384-token ring (the `ELASTIC_TOTAL_TOKENS` default) in the raw config encoding, the format `@Statistics TOPO` actually returns |
| `hashinator_ring_cooked.bin` | The same ring in the compressed "cooked" encoding |
| `hashinator_vectors.jsonl` | About 725 vectors: declared type, value, and the partition the Java `HashinatorLite` maps it to on that ring |

They are checked in so the test runs offline, with no JVM and no cluster.

The generator, `HashinatorParityGen.java`, runs the server's own hashinator classes, so it lives with the VoltDB server sources (`tests/pyclient/harness/`) rather than here. It is deterministic: a fixed seed, and `ElasticHashinator`'s token placement has no randomness. Regenerating therefore produces identical files unless the hashing algorithm itself changed, which is exactly what the parity test exists to catch. If it did change, regenerate the fixtures there and copy them into this directory together with the client change that follows the new algorithm.
