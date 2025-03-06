Description

The VoltDB Python client library is a native Python implementation of the VoltDB
wire protocol. It provides the functionality of connecting to a VoltDB server,
authenticating the client, and making procedure calls.  The client supports
Python 3.6 or later.

-------

PLEASE NOTE

The VoltDB Python client library has for some time now been included in
the distribution kit for VoltDB, which is available to all VoltDB customers.
The present repository, VoltDB/voltdb-client-python, is updated from time
to time to match. If you have access to the VoltDB distribution kit, we
recommend that you use the VoltDB client library from the kit.

-------

The core of the Python client library is the FastSerializer class. It manages
the connection to the server and serializes/deserializes the VoltDB primitive
types. There are higher level classes which wraps around the FastSerializer
class to handle compound objects, namely procedure, response, table, and
exception. VoltProcedure, VoltResponse, VoltTable, and VoltException classes
handles them, respectively. Note that none of the classes in the Python client
library is thread safe.

Each VoltDB primitive type is mapped to a Python primitive type. The following
table shows the mapping.

   +------------------+
   |VoltDB   |Python  |
   |---------|--------|
   |NULL     |None    |
   |TINYINT  |integer |
   |SMALLINT |integer |
   |INTEGER  |integer |
   |BIGINT   |integer |
   |FLOAT    |float   |
   |STRING   |string  |
   |DECIMAL  |decimal |
   +------------------+

Although Python does not distinguish between the different integer types, the
size of the integer being serialized needs to fit the corresponding type.

The common work flow is to create a FastSerializer object and connect to the
server. Then create a VoltProcedure object for each stored procedure you want to
call, passing the FastSerializer object to it. For each call, you will get a
VoltResponse object, which may or may not contain a list of VoltTable
objects. In case of failure, a VoltException object may be included.


API

FastSerializer.VOLTTYPE_NULL
FastSerializer.VOLTTYPE_TINYINT
FastSerializer.VOLTTYPE_SMALLINT
FastSerializer.VOLTTYPE_INTEGER
FastSerializer.VOLTTYPE_BIGINT
FastSerializer.VOLTTYPE_FLOAT
FastSerializer.VOLTTYPE_STRING
FastSerializer.VOLTTYPE_TIMESTAMP
FastSerializer.VOLTTYPE_DECIMAL
FastSerializer.VOLTTYPE_DECIMAL_STRING
FastSerializer.VOLTTYPE_VOLTTABLE
    VoltDB types.

FastSerializer(host, port, username, password, dump_file)
    Create a connection to host (string) on port (integer). If username (string)
    and password (string) is given, authenticate the client using them. If
    dump_file (string) is given, all the data received from and sent to the
    server will be written into the file pointed to by dump_file.

FastSerializer.close()
    Closes the connection. No further use of the object is valid.

VoltProcedure(fser, name, paramtypes)
    Create a procedure object which can be used to call the stored procedure
    name (string) with parameters of types paramtypes (list). The parameter
    types are defined in the FastSerializer class as VoltDB types. For parameter
    of type array, you specify the type in the same way as primitive types. fser
    is the FastSerializer object with a valid connection to the server.

VoltProcedure.call(params, response, timeout)
    Make a stored procedure invocation with the parameters params (list). The
    parameters has to match the types defined in the VoltProcedure
    constructor. If a parameter is an array, pass it in as a list. If response
    (bool) is given and False, the invocation will return None. If timeout
    (float) is given, the invocation will wait for timeout seconds at most if
    the server does not respond. A socket.timeout exception will be raised if
    timeout seconds has elapsed. A successful invocation will return a
    VoltReponse object.

VoltResponse.status
    The status code (integer) for a stored procedure invocation. For a list of
    status code, please refer to the VoltDB documentation.

VoltResponse.statusString
    A human-friendly string of the meaning of the status code.

VoltResponse.roundtripTime
    The round-trip time (integer) of the invocation in milliseconds.

VoltResponse.exception
    The VoltException object in case of failure, may be None.

VoltResponse.tables
    A list of VoltTable objects as the result of the invocation. May be None.

VoltException.type
    The type of the VoltDB exception. Can be the following values,
    VOLTEXCEPTION_NONE, VOLTEXCEPTION_EEEXCEPTION, VOLTEXCEPTION_SQLEXCEPTION,
    VOLTEXCEPTION_CONSTRAINTFAILURE, VOLTEXCEPTION_GENERIC.

VoltException.message
    A string explaining the exception.

VoltTable.columns
    A list of VoltColumn objects, representing the columns in the table.

VoltTable.tuples
    A list of rows in the table. A row a list of values deserialized in Python
    types.

VoltColumn.type
    The type of the column. A list of types is defined in the FastSerializer
    class.

VoltColumn.name
    The name of the column as a string.


Example

The following example shows how to make a connection to a VoltDB server instance
running on port 21212 on localhost, and make a single call to the stored
procedure named "Select", which takes a single parameter of type string. Assume
the server is not secure, so that it does not require authentication.

    >>> client = FastSerializer("localhost", 21212)
    >>> proc = VoltProcedure(client, "Select", [FastSerializer.VOLTTYPE_STRING])
    >>> response = proc.call(["English"])
    >>> client.close()
    >>> print response

The output will be a human-readable form of the VoltResponse object.
