#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2011 VoltDB Inc.
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

import array
import socket
import struct
import datetime as _datetime
import time
import decimal
from threading import Lock
try:
    from hashlib import sha1 as sha
except ImportError:
    from sha import sha
from cStringIO import StringIO

decimal.getcontext().prec = 38

def isNaN(d):
    """Since Python has the weird behavior that a float('nan') is not equal to
    itself, we have to test it by ourselves.
    """

    if d == None:
        return False

    # work-around for Python 2.4
    s = array.array("d", [d])
    return (s.tostring() == "\x00\x00\x00\x00\x00\x00\xf8\x7f" or
            s.tostring() == "\x00\x00\x00\x00\x00\x00\xf8\xff" or
            s.tostring() == "\x00\x00\x00\x00\x00\x00\xf0\x7f")

def if_else(cond, a, b):
    """Work around Python 2.4
    """

    if cond: return a
    else: return b

import asyncore
from collections import deque
class VoltDBClient(asyncore.dispatcher):
    def __init__(self, host, port, username, password):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))

        self.username = username
        self.password = password

        self.wbuf = deque()
        self.rbuf = StringIO()
        self.lbuf = StringIO()
        self.len = 0

        self.auth = False
        self.auth_sent = False
        self.fser = FastSerializer()
        # Callbacks
        self.cb = {}
        self.handle = 0
        self.hlock = Lock()

        # Number of messages sent
        self.sent = 0

    def close(self):
        asyncore.dispatcher.close(self)

    def _authenticate(self):
        # Requires sending a length preceded username and password even if
        # authentication is turned off.

        #protocol version
        buf = StringIO()
        # preceded len, fill with 0 for now
        self.fser.writeInt32(buf, 0)
        self.fser.writeByte(buf, 0)

        # service requested
        self.fser.writeString(buf, 'database')

        if self.username:
            # utf8 encode supplied username
            self.fser.writeString(buf, self.username)
        else:
            # no username, just output length of 0
            self.fser.writeString(buf, '')

        # password supplied, sha-1 hash it
        m = sha()
        m.update(self.password)
        pwHash = m.digest()
        buf.write(pwHash)

        pos = buf.tell()
        buf.seek(0)
        self.fser.writeInt32(buf, pos - 4)
        buf.seek(pos)
        self._write(buf.getvalue())

    def _handle_auth(self, buf):
        # A length, version number, and status code is returned
        pos = 0
        (version, pos) = self.fser.readByte(buf, pos)
        (status, pos) = self.fser.readByte(buf, pos)

        if status != 0:
            raise SystemExit("Authentication failed.")

        pos = self.fser.readInt32(buf, pos)[1]
        pos = self.fser.readInt64(buf, pos)[1]
        pos = self.fser.readInt64(buf, pos)[1]
        pos = self.fser.readInt32(buf, pos)[1]
        (l, pos) = self.fser.readInt32(buf, pos)
        for x in xrange(l):
            pos = self.fser.readByte(buf, pos)[1]

        self.auth = True

    def handle_connect(self):
        self._authenticate()

    def handle_close(self):
        self.close()

    def handle_read(self):
        if self.len == 0:
            assert self.lbuf.tell() == 0
            if self._read_fixed_size(self.lbuf, 4):
                self.len = self.fser.readInt32(self.lbuf.getvalue(), 0)[0]

        if self.len == 0:
            return None

        assert self.rbuf.tell() == 0, 'Read buffer is not empty'
        if self._read_fixed_size(self.rbuf, self.len):
            # clear message len
            self.len = 0
            self.lbuf = StringIO()

            if not self.auth:
                self._handle_auth(self.rbuf.getvalue())
            else:
                # Call response handler
                self._handle_response(self.rbuf.getvalue())
            # Clear read buffer
            self.rbuf = StringIO()
        return None

    def _handle_response(self, buf):
        try:
            res = VoltResponse()
            pos = res.deserialize(self.fser, buf, 0)
            assert pos == len(buf)
        except IOError, err:
            res = VoltResponse()
            res.statusString = str(err)

        # dispatch
        self.cb[res.clientHandle](res)
        del self.cb[res.clientHandle]
        return None

    def _read_fixed_size(self, buf, size):
        """Read as many bytes as available into the buffer until the buffer is
        filled with size bytes.

        Return True if buf has size bytes, False otherwise
        """

        try:
            bytes = self.recv(size - buf.tell())
        except socket.error, e:
            self.close()
            return False

        if len(bytes) == 0:
            # closed on the other end
            self.close()
            raise Exception('Closed on the other end')

        buf.write(bytes)
        if buf.tell() == size:
            return True
        else:
            return False

    # def writable(self):
    #     return (len(self.wbuf) > 0)

    def handle_write(self):
        if len(self.wbuf) == 0:
            return
        if self.auth_sent and not self.auth:
            return
        self.auth_sent = True
        data = buffer(self.wbuf[0][0], self.wbuf[0][1],
                      len(self.wbuf[0][0]) - self.wbuf[0][1])
        sent = self.send(data)
        self.wbuf[0][1] += sent
        if self.wbuf[0][1] == len(self.wbuf[0][0]):
            self.wbuf.popleft()
        self.sent += 1
        return None

    def call(self, cb, name, types = [], params = None):
        """Call procedure
        """

        self.hlock.acquire()
        queued = len(self.cb)
        self.hlock.release()

        # See if we have back pressure
        if queued == 3000:
            return False

        self.hlock.acquire()
        handle = self.handle
        self.handle += 1
        self.cb[handle] = cb
        self.hlock.release()

        proc = VoltProcedure(name, types)
        buf = StringIO()
        self.fser.writeInt32(buf, 0)
        proc.serialize(self.fser, buf, handle, params)

        pos = buf.tell()
        buf.seek(0)
        self.fser.writeInt32(buf, pos - 4)

        self._write(buf.getvalue())

        return True

    def _write(self, bytes):
        """Write bytes to the write buffer
        """

        if len(bytes) == 0:
            return None

        return self.wbuf.append([bytes, 0])

class FastSerializer:
    "Primitive type de/serialization in VoltDB formats"

    LITTLE_ENDIAN = '<'
    BIG_ENDIAN = '>'

    ARRAY = -99

    # VoltType enumerations
    VOLTTYPE_NULL = 1
    VOLTTYPE_TINYINT = 3  # int8
    VOLTTYPE_SMALLINT = 4 # int16
    VOLTTYPE_INTEGER = 5  # int32
    VOLTTYPE_BIGINT = 6   # int64
    VOLTTYPE_FLOAT = 8    # float64
    VOLTTYPE_STRING = 9
    VOLTTYPE_TIMESTAMP = 11 # 8 byte long
    VOLTTYPE_DECIMAL = 22  # 16 byte long
    VOLTTYPE_DECIMAL_STRING = 23  # 9 byte long
    VOLTTYPE_MONEY = 20     # 8 byte long
    VOLTTYPE_VOLTTABLE = 21
    VOLTTYPE_VARBINARY = 25

    # SQL NULL indicator for object type serializations (string, decimal)
    NULL_STRING_INDICATOR = -1
    NULL_DECIMAL_INDICATOR = -170141183460469231731687303715884105728
    NULL_TINYINT_INDICATOR = -128
    NULL_SMALLINT_INDICATOR = -32768
    NULL_INTEGER_INDICATOR = -2147483648
    NULL_BIGINT_INDICATOR = -9223372036854775808
    NULL_FLOAT_INDICATOR = -1.7E308

    # default decimal scale
    DEFAULT_DECIMAL_SCALE = 12

    # procedure call result codes
    PROC_OK = 0

    # there are assumptions here about datatype sizes which are
    # machine dependent. the program exits with an error message
    # if these assumptions are not true. it is further assumed
    # that host order is little endian. See isNaN().

    def __init__(self):
        # input can be big or little endian
        self.inputBOM = self.BIG_ENDIAN  # byte order if input stream
        self.localBOM = self.LITTLE_ENDIAN  # byte order of host

        # Type to reader/writer mappings
        self.READER = {self.VOLTTYPE_NULL: self.readNull,
                       self.VOLTTYPE_TINYINT: self.readByte,
                       self.VOLTTYPE_SMALLINT: self.readInt16,
                       self.VOLTTYPE_INTEGER: self.readInt32,
                       self.VOLTTYPE_BIGINT: self.readInt64,
                       self.VOLTTYPE_FLOAT: self.readFloat64,
                       self.VOLTTYPE_STRING: self.readString,
                       self.VOLTTYPE_VARBINARY: self.readVarbinary,
                       self.VOLTTYPE_TIMESTAMP: self.readDate,
                       self.VOLTTYPE_DECIMAL: self.readDecimal,
                       self.VOLTTYPE_DECIMAL_STRING: self.readDecimalString}
        self.WRITER = {self.VOLTTYPE_NULL: self.writeNull,
                       self.VOLTTYPE_TINYINT: self.writeByte,
                       self.VOLTTYPE_SMALLINT: self.writeInt16,
                       self.VOLTTYPE_INTEGER: self.writeInt32,
                       self.VOLTTYPE_BIGINT: self.writeInt64,
                       self.VOLTTYPE_FLOAT: self.writeFloat64,
                       self.VOLTTYPE_STRING: self.writeString,
                       self.VOLTTYPE_VARBINARY: self.writeVarbinary,
                       self.VOLTTYPE_TIMESTAMP: self.writeDate,
                       self.VOLTTYPE_DECIMAL: self.writeDecimal,
                       self.VOLTTYPE_DECIMAL_STRING: self.writeDecimalString}
        self.ARRAY_READER = {self.VOLTTYPE_TINYINT: self.readByteArray,
                             self.VOLTTYPE_SMALLINT: self.readInt16Array,
                             self.VOLTTYPE_INTEGER: self.readInt32Array,
                             self.VOLTTYPE_BIGINT: self.readInt64Array,
                             self.VOLTTYPE_FLOAT: self.readFloat64Array,
                             self.VOLTTYPE_STRING: self.readStringArray,
                             self.VOLTTYPE_TIMESTAMP: self.readDateArray,
                             self.VOLTTYPE_DECIMAL: self.readDecimalArray,
                             self.VOLTTYPE_DECIMAL_STRING: self.readDecimalStringArray}

        self.__compileStructs()

        # Check if the value of a given type is NULL
        self.NULL_DECIMAL_INDICATOR = \
            self.__intToBytes(self.__class__.NULL_DECIMAL_INDICATOR, 0)
        self.NullCheck = {self.VOLTTYPE_NULL:
                              lambda x: None,
                          self.VOLTTYPE_TINYINT:
                              lambda x:
                              if_else(x == self.__class__.NULL_TINYINT_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_SMALLINT:
                              lambda x:
                              if_else(x == self.__class__.NULL_SMALLINT_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_INTEGER:
                              lambda x:
                              if_else(x == self.__class__.NULL_INTEGER_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_BIGINT:
                              lambda x:
                              if_else(x == self.__class__.NULL_BIGINT_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_FLOAT:
                              lambda x:
                              if_else(abs(x - self.__class__.NULL_FLOAT_INDICATOR) < 1e307,
                                      None, x),
                          self.VOLTTYPE_STRING:
                              lambda x:
                              if_else(x == self.__class__.NULL_STRING_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_VARBINARY:
                              lambda x:
                              if_else(x == self.__class__.NULL_STRING_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_DECIMAL:
                              lambda x:
                              if_else(x == self.NULL_DECIMAL_INDICATOR,
                                      None, x)}

    def __compileStructs(self):
        # Compiled structs for each type
        self.byteType = lambda length : '%c%db' % (self.inputBOM, length)
        self.ubyteType = lambda length : '%c%dB' % (self.inputBOM, length)
        self.int16Type = lambda length : '%c%dh' % (self.inputBOM, length)
        self.int32Type = lambda length : '%c%di' % (self.inputBOM, length)
        self.int64Type = lambda length : '%c%dq' % (self.inputBOM, length)
        self.uint64Type = lambda length : '%c%dQ' % (self.inputBOM, length)
        self.float64Type = lambda length : '%c%dd' % (self.inputBOM, length)
        self.stringType = lambda length : '%c%ds' % (self.inputBOM, length)
        self.varbinaryType = lambda length : '%c%ds' % (self.inputBOM, length)

    def setInputByteOrder(self, bom):
        # assuming bom is high bit set?
        if bom == 1:
            self.inputBOM = self.LITTLE_ENDIAN
        else:
            self.inputBOM = self.BIG_ENDIAN

        # recompile the structs
        self.__compileStructs()

    def read(self, buf, pos, type):
        if type not in self.READER:
            print "ERROR: can't read wire type(", type, ") yet."
            exit(-2)

        return self.READER[type](buf, pos)

    def write(self, buf, type, value):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        return self.WRITER[type](buf, value)

    def readWireType(self, buf, pos):
        (type, pos) = self.readByte(buf, pos)
        return self.read(buf, pos, type)

    def writeWireType(self, buf, type, value):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        self.writeByte(buf, type)
        return self.write(buf, type, value)

    def readArray(self, buf, pos, type):
        if type not in self.ARRAY_READER:
            print "ERROR: can't read wire type(", type, ") yet."
            exit(-2)

        return self.ARRAY_READER[type](buf, pos)

    def readNull(self, buf, pos):
        return (None, pos)

    def writeNull(self, buf, value):
        return

    def writeArray(self, buf, type, array):
        if (not array) or (len(array) == 0) or (not type):
            return

        if type not in self.ARRAY_READER:
            print "ERROR: Unsupported date type (", type, ")."
            exit(-2)

        # serialize arrays of bytes as larger values to support
        # strings and varbinary input
        if type != FastSerializer.VOLTTYPE_TINYINT:
            self.writeInt16(buf, len(array))
        else:
            self.writeInt32(buf, len(array))

        for i in array:
            self.WRITER[type](buf, i)

    def writeWireTypeArray(self, buf, type, array):
        if type not in self.ARRAY_READER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        self.writeByte(buf, type)
        return self.writeArray(buf, type, array)

    # byte
    def readByteArrayContent(self, buf, pos, cnt):
        offset = pos + cnt
        val = struct.unpack(self.byteType(cnt), buf[pos:offset])
        return (val, offset)

    def readByteArray(self, buf, pos):
        (length, pos) = self.readInt32(buf, pos)
        (val, pos) = self.readByteArrayContent(buf, pos, length)
        val = map(self.NullCheck[self.VOLTTYPE_TINYINT], val)
        return (val, pos)

    def readByte(self, buf, pos):
        (val, pos) = self.readByteArrayContent(buf, pos, 1)
        return (self.NullCheck[self.VOLTTYPE_TINYINT](val[0]), pos)

    def writeByte(self, buf, value):
        if value == None:
            val = self.__class__.NULL_TINYINT_INDICATOR
        else:
            val = value
        return buf.write(struct.pack(self.byteType(1), val))

    # int16
    def readInt16ArrayContent(self, buf, pos, cnt):
        offset = pos + cnt * 2
        val = struct.unpack(self.int16Type(cnt), buf[pos:offset])
        return (val, offset)

    def readInt16Array(self, buf, pos):
        (length, pos) = self.readInt16(buf, pos)
        (val, pos) = self.readInt16ArrayContent(buf, pos, length)
        val = map(self.NullCheck[self.VOLTTYPE_SMALLINT], val)
        return (val, pos)

    def readInt16(self, buf, pos):
        (val, pos) = self.readInt16ArrayContent(buf, pos, 1)
        return (self.NullCheck[self.VOLTTYPE_SMALLINT](val[0]), pos)

    def writeInt16(self, buf, value):
        if value == None:
            val = self.__class__.NULL_SMALLINT_INDICATOR
        else:
            val = value
        return buf.write(struct.pack(self.int16Type(1), val))

    # int32
    def readInt32ArrayContent(self, buf, pos, cnt):
        offset = pos + cnt * 4
        val = struct.unpack(self.int32Type(cnt), buf[pos:offset])
        return (val, offset)

    def readInt32Array(self, buf, pos):
        (length, pos) = self.readInt16(buf, pos)
        (val, pos) = self.readInt32ArrayContent(buf, pos, length)
        val = map(self.NullCheck[self.VOLTTYPE_INTEGER], val)
        return (val, pos)

    def readInt32(self, buf, pos):
        (val, pos) = self.readInt32ArrayContent(buf, pos, 1)
        return (self.NullCheck[self.VOLTTYPE_INTEGER](val[0]), pos)

    def writeInt32(self, buf, value):
        if value == None:
            val = self.__class__.NULL_INTEGER_INDICATOR
        else:
            val = value
        return buf.write(struct.pack(self.int32Type(1), val))

    # int64
    def readInt64ArrayContent(self, buf, pos, cnt):
        offset = pos + cnt * 8
        val = struct.unpack(self.int64Type(cnt), buf[pos:offset])
        return (val, offset)

    def readInt64Array(self, buf, pos):
        (length, pos) = self.readInt16(buf, pos)
        (val, pos) = self.readInt64ArrayContent(buf, pos, length)
        val = map(self.NullCheck[self.VOLTTYPE_BIGINT], val)
        return (val, pos)

    def readInt64(self, buf, pos):
        (val, pos) = self.readInt64ArrayContent(buf, pos, 1)
        return (self.NullCheck[self.VOLTTYPE_BIGINT](val[0]), pos)

    def writeInt64(self, buf, value):
        if value == None:
            val = self.__class__.NULL_BIGINT_INDICATOR
        else:
            val = value
        return buf.write(struct.pack(self.int64Type(1), val))

    # float64
    def readFloat64ArrayContent(self, buf, pos, cnt):
        offset = pos + cnt * 8
        val = struct.unpack(self.float64Type(cnt), buf[pos:offset])
        return (val, offset)

    def readFloat64Array(self, buf, pos):
        (length, pos) = self.readInt16(buf, pos)
        (val, pos) = self.readFloat64ArrayContent(buf, pos, length)
        val = map(self.NullCheck[self.VOLTTYPE_FLOAT], val)
        return (val, pos)

    def readFloat64(self, buf, pos):
        (val, pos) = self.readFloat64ArrayContent(buf, pos, 1)
        return (self.NullCheck[self.VOLTTYPE_FLOAT](val), pos)

    def writeFloat64(self, buf, value):
        if value == None:
            val = self.__class__.NULL_FLOAT_INDICATOR
        else:
            val = value
        # work-around for python 2.4
        tmp = array.array("d", [val])
        if self.inputBOM != self.localBOM:
            tmp.byteswap()
        return buf.write(tmp.tostring())

    # string
    def readStringContent(self, buf, pos, cnt):
        if cnt == 0:
            return ('', pos)

        offset = pos + cnt
        val = struct.unpack(self.stringType(cnt), buf[pos:offset])
        return (val[0].decode("utf-8"), offset)

    def readString(self, buf, pos):
        # length preceeded (4 byte value) string
        (length, pos) = self.readInt32(buf, pos)
        if self.NullCheck[self.VOLTTYPE_STRING](length) == None:
            return (None, pos)
        return self.readStringContent(buf, pos, length)

    def readStringArray(self, buf, pos):
        retval = []
        (cnt, pos) = self.readInt16(buf, pos)

        for i in xrange(cnt):
            (s, pos) = self.readString(buf, pos)
            retval.append(s)

        return (tuple(retval), pos)

    def writeString(self, buf, value):
        if value is None:
            self.writeInt32(buf, self.NULL_STRING_INDICATOR)
            return None

        encoded_value = value.encode("utf-8")
        self.writeInt32(buf, len(encoded_value))
        return buf.write(encoded_value)

    # varbinary
    def readVarbinaryContent(self, buf, pos, cnt):
        if cnt == 0:
            return (array.array('c', []), pos)

        offset = pos + cnt
        val = struct.unpack(self.varbinaryType(cnt), buf[pos:offset])
        return (array.array('c', val[0]), offset)

    def readVarbinary(self, buf, pos):
        # length preceeded (4 byte value) string
        (length, pos) = self.readInt32(buf, pos)
        if self.NullCheck[self.VOLTTYPE_VARBINARY](length) == None:
            return (None, pos)
        return self.readVarbinaryContent(buf, pos, length)

    def writeVarbinary(self, buf, value):
        if value is None:
            self.writeInt32(buf, self.NULL_STRING_INDICATOR)
            return None

        self.writeInt32(buf, len(value))
        return buf.write(value)

    # date
    # The timestamp we receive from the server is a 64-bit integer representing
    # microseconds since the epoch. It will be converted to a datetime object in
    # the local timezone.
    def readDate(self, buf, pos):
        (raw, pos) = self.readInt64(buf, pos)
        if raw == None:
            return (None, pos)
        # microseconds before or after Jan 1, 1970 UTC
        return (_datetime.datetime.fromtimestamp(raw/1000000.0), pos)

    def readDateArray(self, buf, pos):
        retval = []
        (raw, pos) = self.readInt64Array(buf, pos)

        for i in raw:
            val = None
            if i != None:
                val = _datetime.datetime.fromtimestamp(i/1000000.0)
            retval.append(val)

        return (tuple(retval), pos)

    def writeDate(self, buf, value):
        if value is None:
            val = self.__class__.NULL_BIGINT_INDICATOR
        else:
            seconds = int(value.strftime("%s"))
            val = seconds * 1000000 + value.microsecond
        return buf.write(struct.pack(self.int64Type(1), val))

    def readDecimal(self, buf, pos):
        offset = pos + 16
        if self.NullCheck[self.VOLTTYPE_DECIMAL](buf[pos:offset]) == None:
            return (None, offset)
        val = list(struct.unpack(self.ubyteType(16), buf[pos:offset]))
        mostSignificantBit = 1 << 7
        isNegative = (val[0] & mostSignificantBit) != 0
        unscaledValue = -(val[0] & mostSignificantBit) << 120
        # Clear the highest bit
        # Unleash the powers of the butterfly
        val[0] &= ~mostSignificantBit
        # Get the 2's complement
        for x in xrange(16):
            unscaledValue += val[x] << ((15 - x) * 8)
        unscaledValue = map(lambda x: int(x), str(abs(unscaledValue)))
        return (decimal.Decimal((isNegative, tuple(unscaledValue),
                                 -self.__class__.DEFAULT_DECIMAL_SCALE)),
                offset)

    def readDecimalArray(self, buf, pos):
        retval = []
        (cnt, pos) = self.readInt16(buf, pos)
        for i in xrange(cnt):
            (val, pos) = self.readDecimal(buf, pos)
            retval.append(val)
        return (tuple(retval), pos)

    def readDecimalString(self, buf, pos):
        (encoded_string, pos) = self.readString(buf, pos)
        if encoded_string == None:
            return (None, pos)
        val = decimal.Decimal(encoded_string)
        (sign, digits, exponent) = val.as_tuple()
        if -exponent > self.__class__.DEFAULT_DECIMAL_SCALE:
            raise ValueError("Scale of this decimal is %d and the max is 12"
                             % (-exponent))
        if len(digits) > 38:
            raise ValueError("Precision of this decimal is %d and the max is 38"
                             % (len(digits)))
        return (val, pos)

    def readDecimalStringArray(self, buf, pos):
        retval = []
        (cnt, pos) = self.readInt16(buf, pos)
        for i in xrange(cnt):
            (val, pos) = self.readDecimalString(buf, pos)
            retval.append(val)
        return (tuple(retval), pos)

    def __intToBytes(self, value, sign):
        value_bytes = ""
        if sign == 1:
            value = ~value + 1      # 2's complement
        # Turn into byte array
        while value != 0 and value != -1:
            byte = value & 0xff
            # flip the high order bits to 1 only if the number is negative and
            # this is the highest order byte
            if value >> 8 == 0 and sign == 1:
                mask = 1 << 7
                while mask > 0 and (byte & mask) == 0:
                    byte |= mask
                    mask >> 1
            value_bytes = struct.pack(self.ubyteType(1), byte) + value_bytes
            value = value >> 8
        if len(value_bytes) > 16:
            raise ValueError("Precision of this decimal is >38 digits");
        if sign == 1:
            ret = struct.pack(self.ubyteType(1), 0xff)
        else:
            ret = struct.pack(self.ubyteType(1), 0)
        # Pad it
        ret *= 16 - len(value_bytes)
        ret += value_bytes
        return ret

    def writeDecimal(self, buf, pos, num):
        if num is None:
            return buf.write(self.NULL_DECIMAL_INDICATOR)
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of the type decimal.Decimal")
        (sign, digits, exponent) = num.as_tuple()
        precision = len(digits)
        scale = -exponent
        if (scale > self.__class__.DEFAULT_DECIMAL_SCALE):
            raise ValueError("Scale of this decimal is %d and the max is 12"
                             % (scale))
        rest = precision - scale
        if rest > 26:
            raise ValueError("Precision to the left of the decimal point is %d"
                             " and the max is 26" % (rest))
        scale_factor = self.__class__.DEFAULT_DECIMAL_SCALE - scale
        unscaled_int = int(decimal.Decimal((0, digits, scale_factor)))
        data = self.__intToBytes(unscaled_int, sign)
        return buf.write(data)

    def writeDecimalString(self, buf, num):
        if num is None:
            return self.writeString(buf, None)
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of type decimal.Decimal")
        return self.writeString(buf, num.to_eng_string())

    # cash!
    def readMoney(self, buf, pos):
        # money-unit * 10,000
        return self.readInt64(buf, pos)

class VoltColumn:
    "definition of one VoltDB table column"
    def __init__(self, type = None, name = None):
        self.type = type
        self.name = name

    def __str__(self):
        # If the name is empty, use the default "modified tuples". Has to do
        # this because HSQLDB doesn't return a column name if the table is
        # empty.
        return "(%s: %d)" % (self.name and self.name or "modified tuples",
                             self.type)

    def __eq__(self, other):
        # For now, if we've been through the query on a column with no name,
        # just assume that there's no way the types are matching up cleanly
        # and there ain't no one for to give us no pain
        if (not self.name or not other.name):
            return True
        return (self.type == other.type and self.name == other.name)

    def deserialize(self, fser, buf, pos):
        (self.type, pos) = fser.readByte(buf, pos)
        (self.name, pos) = fser.readString(buf, pos)
        return pos

    # def writeType(self, fser):
    #     fser.writeByte(self.type)

    # def writeName(self, fser):
    #     fser.writeString(self.name)

class VoltTable:
    "definition and content of one VoltDB table"
    def __init__(self):
        self.columns = []  # column defintions
        self.tuples = []

    def __str__(self):
        result = ""

        result += "column count: %d\n" % (len(self.columns))
        result += "row count: %d\n" % (len(self.tuples))
        result += "cols: "
        result += ", ".join(map(lambda x: str(x), self.columns))
        result += "\n"
        result += "rows -\n"
        result += "\n".join(map(lambda x:
                                    str(map(lambda y: if_else(y == None, "NULL", y),
                                            x)), self.tuples))

        return result

    def __getstate__(self):
        return (self.columns, self.tuples)

    def __setstate__(self, state):
        self.columns, self.tuples = state

    def __eq__(self, other):
        if len(self.tuples) > 0:
            return (self.columns == other.columns) and \
                (self.tuples == other.tuples)
        return (self.tuples == other.tuples)

    # The VoltTable is always serialized in big-endian order.
    #
    # How to read a table off the wire.
    # 1. Read the length of the whole table
    # 2. Read the columns
    #    a. read the column header size
    #    a. read the column count
    #    b. read column definitions.
    # 3. Read the tuples count.
    #    a. read the row count
    #    b. read tuples recording string lengths
    def deserialize(self, fser, buf, pos):
        # 1.
        (tablesize, pos) = fser.readInt32(buf, pos)
        # 2.
        (headersize, pos) = fser.readInt32(buf, pos)
        (statuscode, pos) = fser.readByte(buf, pos)
        (columncount, pos) = fser.readInt16(buf, pos)
        for i in xrange(columncount):
            column = VoltColumn()
            self.columns.append(column)
        for c in self.columns:
            pos = c.deserialize(fser, buf, pos)

        # 3.
        (rowcount, pos) = fser.readInt32(buf, pos)
        for i in xrange(rowcount):
            (rowsize, pos) = fser.readInt32(buf, pos)
            # list comprehension: build list by calling read for each column in
            # row/tuple
            row = []
            for j in xrange(columncount):
                (col, pos) = fser.read(buf, pos, self.columns[j].type)
                row.append(col)
            self.tuples.append(row)

        return pos

    # def writeToSerializer(self):
    #     table_fser = FastSerializer()

    #     # We have to pack the header into a buffer first so that we can
    #     # calculate the size
    #     header_fser = FastSerializer()

    #     header_fser.writeByte(0)
    #     header_fser.writeInt16(len(self.columns))
    #     map(lambda x: x.writeType(header_fser), self.columns)
    #     map(lambda x: x.writeName(header_fser), self.columns)

    #     table_fser.writeInt32(header_fser.size() - 4)
    #     table_fser.writeRawBytes(header_fser.getRawBytes())

    #     table_fser.writeInt32(len(self.tuples))
    #     for i in self.tuples:
    #         row_fser = FastSerializer()

    #         map(lambda x: row_fser.write(self.columns[x].type, i[x]),
    #             xrange(len(i)))

    #         table_fser.writeInt32(row_fser.size())
    #         table_fser.writeRawBytes(row_fser.getRawBytes())

    #     fser.writeRawBytes(table_fser.getRawBytes())


class VoltException:
    # Volt SerializableException enumerations
    VOLTEXCEPTION_NONE = 0
    VOLTEXCEPTION_EEEXCEPTION = 1
    VOLTEXCEPTION_SQLEXCEPTION = 2
    VOLTEXCEPTION_CONSTRAINTFAILURE = 3
    VOLTEXCEPTION_GENERIC = 4

    def __init__(self):
        self.type = self.VOLTEXCEPTION_NONE
        self.typestr = "None"
        self.message = ""

    def deserialize(self, fser, buf, pos):
        (self.length, pos) = fser.readInt32(buf, pos)
        if self.length == 0:
            self.type = self.VOLTEXCEPTION_NONE
            return pos
        (self.type, pos) = fser.readByte(buf, pos)
        # quick and dirty exception skipping
        if self.type == self.VOLTEXCEPTION_NONE:
            return pos

        self.message = []
        (self.message_len, pos) = fser.readInt32(buf, pos)
        for i in xrange(0, self.message_len):
            (b, pos) = fser.readByte(buf, pos)
            self.message.append(chr(b))
        self.message = ''.join(self.message)

        if self.type == self.VOLTEXCEPTION_GENERIC:
            self.typestr = "Generic"
        elif self.type == self.VOLTEXCEPTION_EEEXCEPTION:
            self.typestr = "EE Exception"
            # serialized size from EEException.java is 4 bytes
            (self.error_code, pos) = fser.readInt32(buf, pos)
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION or \
                self.type == self.VOLTEXCEPTION_CONSTRAINTFAILURE:
            self.sql_state_bytes = []
            for i in xrange(0, 5):
                (b, pos) = fser.readByte(buf, pos)
                self.sql_state_bytes.append(chr(b))
            self.sql_state_bytes = ''.join(self.sql_state_bytes)

            if self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
                self.typestr = "SQL Exception"
            else:
                self.typestr = "Constraint Failure"
                (self.constraint_type, pos) = fser.readInt32(buf, pos)
                (self.table_name, pos) = fser.readString(buf, pos)
                (self.buffer_size, pos) = fser.readInt32(buf, pos)
                self.buffer = []
                for i in xrange(0, self.buffer_size):
                    (b, pos) = fser.readByte(buf, pos)
                    self.buffer.append(b)
        else:
            for i in xrange(0, self.length - 3 - 2 - self.message_len):
                pos = fser.readByte(buf, pos)[1]
            print "Python client deserialized unknown VoltException."

        return pos

    def __str__(self):
        msgstr = "VoltException: type: %s\n" % self.typestr
        if self.type == self.VOLTEXCEPTION_EEEXCEPTION:
            msgstr += "  Error code: %d\n" % self.error_code
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
            msgstr += "  SQL code: "
            msgstr += self.sql_state_bytes
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
            msgstr += "  Constraint violation type: %d\n" + self.constraint_type
            msgstr += "  on table: %s\n" + self.table_name
        return msgstr

class VoltResponse:
    "VoltDB called procedure response (ClientResponse.java)"
    def __init__(self):
        self.version = -1
        self.clientHandle = -1
        self.status = -1
        self.statusString = ""
        self.appStatus = -1
        self.appStatusString = ""
        self.roundtripTime = -1
        self.exception = None
        self.tables = None

    def deserialize(self, fser, buf, pos):
        # serialization order: response-length, status, roundtripTime, exception,
        # tables[], info, id.
        (self.version, pos) = fser.readByte(buf, pos)
        (self.clientHandle, pos) = fser.readInt64(buf, pos)
        (presentFields, pos) = fser.readByte(buf, pos);
        (self.status, pos) = fser.readByte(buf, pos)
        if presentFields & (1 << 5) != 0:
            (self.statusString, pos) = fser.readString(buf, pos)
        else:
            self.statusString = None
        (self.appStatus, pos) = fser.readByte(buf, pos)
        if presentFields & (1 << 7) != 0:
            (self.appStatusString, pos) = fser.readString(buf, pos)
        else:
            self.appStatusString = None
        (self.roundtripTime, pos) = fser.readInt32(buf, pos)
        if presentFields & (1 << 6) != 0:
            self.exception = VoltException()
            pos = self.exception.deserialize(fser, buf, pos)
        else:
            self.exception = None

        # tables[]
        (tablecount, pos) = fser.readInt16(buf, pos)
        self.tables = []
        for i in xrange(tablecount):
            table = VoltTable()
            pos = table.deserialize(fser, buf, pos)
            self.tables.append(table)

        return pos

    def __str__(self):
        tablestr = "\n\n".join([str(i) for i in self.tables])
        if self.exception is None:
            return "Status: %d\nInformation: %s\n%s" % (self.status,
                                                        self.statusString,
                                                        tablestr)
        else:
            msgstr = "Status: %d\nInformation: %s\n%s\n" % (self.status,
                                                            self.statusString,
                                                            tablestr)
            msgstr += "Exception: %s" % (self.exception)
            return msgstr

class VoltProcedure:
    "VoltDB called procedure interface"
    def __init__(self, name, paramtypes = []):
        self.name = name             # procedure class name
        self.paramtypes = paramtypes # list of fser.WIRE_* values

    def serialize(self, fser, buf, handle, params = None):
        fser.writeByte(buf, 0)  # version number
        fser.writeString(buf, self.name)
        fser.writeInt64(buf, handle)            # client handle
        fser.writeInt16(buf, len(self.paramtypes))
        for i in xrange(len(self.paramtypes)):
            if hasattr(params[i], '__iter__'):
                fser.writeByte(buf, FastSerializer.ARRAY)
                fser.writeByte(buf, self.paramtypes[i])
                fser.writeArray(buf, self.paramtypes[i], params[i])
            else:
                fser.writeWireType(buf, self.paramtypes[i], params[i])
        return buf
