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
Java-parity test for the Python elastic hashinator (src/voltclient.py).

The fixtures under data/ were produced by the Java implementation - the same
code the cluster routes with - by a generator in the server sources (see
data/README.md): a ring at
production scale (16384 tokens, 8 partitions) in both config encodings, and a
corpus of (declared type, value, expected partition) vectors spanning every
partitionable type, the string/bytes coercions, null sentinels, and murmur3
length boundaries.

The Python hashinator must agree on every vector. A single mismatch is a
release blocker, not a flake: it means a value routes to a different partition
than the cluster itself would choose. Runs offline - no cluster, no JVM.
"""

import base64
import json
import os
import unittest

from voltclient import ElasticHashinator

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def load_fixture(name):
    with open(os.path.join(DATA_DIR, name), "rb") as f:
        return f.read()


class TestJavaParity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hashinator = ElasticHashinator.from_bytes(
            load_fixture("hashinator_ring_raw.bin"))

    def test_ring_is_production_scale(self):
        self.assertEqual(len(self.hashinator._tokens), 16384)
        self.assertEqual(set(self.hashinator._partitions), set(range(8)))

    def test_cooked_fixture_decodes_to_the_same_ring(self):
        cooked = ElasticHashinator.from_bytes(
            load_fixture("hashinator_ring_cooked.bin"), cooked=True)
        self.assertEqual(cooked._tokens, self.hashinator._tokens)
        self.assertEqual(cooked._partitions, self.hashinator._partitions)

    def test_every_vector_matches_java(self):
        decode = {
            "int": lambda v: v,
            "str": lambda v: v,
            "b64": lambda v: base64.b64decode(v),
            "null": lambda v: None,
        }
        mismatches = []
        count = 0
        for line in load_fixture("hashinator_vectors.jsonl").splitlines():
            vector = json.loads(line)
            value = decode[vector["kind"]](vector["value"])
            partition = self.hashinator.get_partition_for_parameter(
                vector["type"], value)
            if partition != vector["partition"]:
                mismatches.append("type %d %s %r: python %d != java %d"
                                  % (vector["type"], vector["kind"],
                                     vector["value"], partition,
                                     vector["partition"]))
            count += 1
        self.assertEqual(mismatches, [])
        # Guard against a silently empty or truncated fixture file.
        self.assertGreater(count, 700)


if __name__ == "__main__":
    unittest.main()
