The Hello World and Voter examples included here are intended to
be used with example projects shipped with the main distribution.
You will need to have a server running with the appropriate catalog
or ddl loaded for these examples to work.

Install the client first, with `pip install voltdbclient` (or
`pip install -e .` from the root of this repository).

The examples require that the command 'python3' points to Python
3.9 or later. We use 'python3' rather than the bare 'python'
command to ensure we use the appropriate version.

# Hello World with VoltClient

voltclient_helloworld.py is Hello World on the topology-aware VoltClient:
the inserts go out asynchronously, and each call goes to the node that
leads its partition. Start the database and load the schema as below for
helloworld.py, then run it, naming any node of the cluster (default:
localhost):

    ./voltclient_helloworld.py [host ...]

# Hello World

First, follow the instructions in the VoltDB kit's doc/tutorials/helloworld
folder to start the database and load the schema.

Then run the following command to start the python helloworld.py client.  This requires
no arguments and connects to localhost.

    ./helloworld.py

# Voter

First, follow the instructions in the VoltDB kit's examples/voter
folder to start the database and load the schema.

Then run the ./voter.py command with arguments to start the python
voter.py client.

The voter.py client has seven arguments:
    [number of contestants]
    [votes per phone number]
    [transactions per second]
    [client feedback interval (seconds)]
    [test duration (seconds)]
    [lag record delay (seconds)]
    [server list (comma separated)]

A reasonable default invocation that connects to localhost is:

    ./voter.py 6 2 100000 5 120 3 localhost
