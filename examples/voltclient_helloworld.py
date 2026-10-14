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
Hello World with the topology-aware VoltClient.

The same work as helloworld.py, on VoltClient: one client for the whole
cluster, inserts sent asynchronously, and each call routed to the node that
leads its partition. Uses the Hello World schema from the VoltDB kit
(doc/tutorials/helloworld): start the database and load helloworld.sql, then

    ./voltclient_helloworld.py [host ...]

Any one host of the cluster is enough; the client finds the others.
"""

import concurrent.futures
import sys

from voltclient import VoltClient, VoltType

SUCCESS = 1  # VoltResponse.status of a call that succeeded

GREETINGS = [("English", "Hello", "World"),
             ("French", "Bonjour", "Monde"),
             ("Spanish", "Hola", "Mundo"),
             ("Danish", "Hej", "Verden"),
             ("Italian", "Ciao", "Mondo")]

hosts = sys.argv[1:] or ["localhost"]

with VoltClient(hosts=hosts) as client:
    # call_async returns a Future at once, so all the inserts are in flight
    # together. Insert takes (dialect, hello, world).
    futures = {client.call_async("Insert",
                                 [VoltType.STRING] * 3,
                                 [dialect, hello, world]): dialect
               for dialect, hello, world in GREETINGS}
    for future in concurrent.futures.as_completed(futures):
        response = future.result()
        if response.status != SUCCESS:
            # For example a duplicate key, when the example runs twice.
            print("Insert %s: %s" % (futures[future], response.statusString))

    # call blocks until the response arrives.
    response = client.call("Select", [VoltType.STRING], ["Spanish"])
    if response.status != SUCCESS:
        sys.exit("Select failed: %s" % response.statusString)
    for hello, world in response.tables[0].tuples:
        print("%s, %s!" % (hello, world))
