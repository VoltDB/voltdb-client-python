#!/usr/bin/env python3
# This file is part of VoltDB.
# Copyright (C) 2008-2026 Volt Active Data Inc.
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

import sys
if sys.hexversion < 0x03060000:
    # For now, we allow a minimum of 3.6 so that the current API can
    # also be used on older systems. New features may not all be
    # available when running on versions of Python older than 3.9.
    # Volt applications using this API may enforce stricter limits.
    raise Exception("Python version 3.6 or greater is required (3.9+ is preferred).")

import array
import atexit
import socket
import base64, textwrap
import struct
import datetime
import decimal
import hashlib
import re
import math
import operator
import os
import stat
import time

try:
    import ssl
    ssl_available = True
except ImportError as e:
    ssl_available = False
    ssl_exception = e

try:
    import jks
    pyjks_available = True
except ImportError as e:
    pyjks_available = False
    pyjks_exception = e

try:
    from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption
    pkcs12_available = True
except ImportError as e:
    pkcs12_available = False
    pkcs12_exception = e

try:
    import gssapi
    kerberos_available = True
except ImportError as e:
    kerberos_available = False
    kerberos_exception = e

logger = None

def use_logging():
    import logging
    global logger
    logger = logging.getLogger()

def error(text):
    if logger:
        logger.error(text)
    else:
        print(text)

decimal.getcontext().prec = 38

def int16toBytes(val):
    return [val >>  8 & 0xff,
            val >>  0 & 0xff]

def int32toBytes(val):
    return [val >> 24 & 0xff,
            val >> 16 & 0xff,
            val >>  8 & 0xff,
            val >>  0 & 0xff]

def int64toBytes(val):
    return [val >> 56 & 0xff,
            val >> 48 & 0xff,
            val >> 40 & 0xff,
            val >> 32 & 0xff,
            val >> 24 & 0xff,
            val >> 16 & 0xff,
            val >>  8 & 0xff,
            val >>  0 & 0xff]

_S_U16 = struct.Struct('>H')
_S_U32 = struct.Struct('>I')
_S_U64 = struct.Struct('>Q')

def isNaN(d):
    # Per IEEE 754, 'NaN == NaN' must be false,
    # so we cannot check for simple equality
    if d == None:
        return False
    else: # routine misnamed, returns true for 'Inf' too
        return math.isnan(d) or math.isinf(d)

class ReadBuffer(object):
    """
    Read buffer management class.
    """
    def __init__(self):
        self.clear()

    def clear(self):
        self._buf = bytes()
        self._off = 0

    def buffer_length(self):
        return len(self._buf)

    def remaining(self):
        return (len(self._buf) - self._off)

    def get_buffer(self):
        return self._buf

    def append(self, content):
        self._buf += content

    def shift(self, size):
        self._off += size

    def read(self, size):
        return self._buf[self._off:self._off+size]

    def unpack(self, format, size):
        try:
            values = struct.unpack_from(format, self._buf, self._off)
        except struct.error as e:
            error('Exception unpacking %d bytes using format "%s": %s' % (size, format, str(e)))
            raise e
        self.shift(size)
        return values

    def unpack_struct(self, st):
        """unpack() with a precompiled struct.Struct."""
        try:
            values = st.unpack_from(self._buf, self._off)
        except struct.error as e:
            error('Exception unpacking %d bytes using format "%s": %s' % (st.size, st.format, str(e)))
            raise e
        self._off += st.size
        return values

    def set(self, content):
        """Replace the buffer with one complete message body (bytes)."""
        self._buf = content
        self._off = 0

unique_tag = '%x' % int(time.time() * 1000000)
scratch_dir = None
temporary_files = []

def remove_temporary_files():
    global temporary_files
    for tf in temporary_files:
        try:
            os.unlink(tf)
        except:
            pass
    temporary_files = []

atexit.register(remove_temporary_files)

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
    VOLTTYPE_DATE = 12 # 4 byte long
    VOLTTYPE_DECIMAL = 22  # fixed precision decimal
    VOLTTYPE_MONEY = 20     # 8 byte long
    VOLTTYPE_VOLTTABLE = 21
    VOLTTYPE_BOOLEAN = 23
    VOLTTYPE_VARBINARY = 25
    VOLTTYPE_GEOGRAPHY_POINT = 26
    VOLTTYPE_GEOGRAPHY = 27

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

    # protocol constants
    AUTH_HANDSHAKE_VERSION = 2
    AUTH_SERVICE_NAME = 4
    AUTH_HANDSHAKE = 5

    # procedure call result codes
    PROC_OK = 0

    # there are assumptions here about datatype sizes which are
    # machine dependent. the program exits with an error message
    # if these assumptions are not true. it is further assumed
    # that host order is little endian. See isNaN().

    # default ssl configuration
    if (ssl_available):
        DEFAULT_SSL_CONFIG = {
        'keyfile': None,
        'keypass': None,
        'certfile': None,
        'cert_reqs': ssl.CERT_NONE,
        'ca_certs': None,
        'do_handshake_on_connect': True
        }
    else:
        DEFAULT_SSL_CONFIG = {}

    def __init__(self, host = None,
                 port = 21212,
                 usessl = False,
                 username = "",
                 password = "",
                 kerberos = False,
                 dump_file_path = None,
                 connect_timeout = 8,
                 procedure_timeout = None,
                 default_timeout = None,
                 ssl_config_file = None,
                 default_cacerts = True):
        """
        :param host: host string for connection or None
        :param port: port for connection or None
        :param usessl: switch for use ssl or not
        :param username: authentication user name for connection or None
        :param password: authentication password for connection or None
        :param kerberos: use Kerberos authentication
        :param dump_file_path: path to optional dump file or None
        :param connect_timeout: timeout (secs) or None for authentication (default=8)
        :param procedure_timeout: timeout (secs) or None for procedure calls (default=None)
        :param default_timeout: default timeout (secs) or None for all other operations (default=None)
        :param ssl_config_file: config file that defines java keystore and truststore files
        :param default_cacerts: if true, use installation default cacerts when truststore unspecified
        """
        # connect a socket to host, port and get a file object
        self.wbuf = array.array('B')
        self.host = host
        self.port = port
        self.usessl = usessl
        if kerberos is None:
            self.usekerberos = False
        else:
            self.usekerberos = kerberos
        self.kerberosprincipal = None
        self.ssl_config = self.DEFAULT_SSL_CONFIG
        self.ssl_config_file = ssl_config_file
        self.default_cacerts = default_cacerts and usessl
        if not dump_file_path is None:
            self.dump_file = open(dump_file_path, "wb")
        else:
            self.dump_file = None
        self.default_timeout = default_timeout
        self.procedure_timeout = procedure_timeout

        # Populated from the server's login response by authenticate(); None on a
        # socket-less instance (host=None) that is only used for serialization.
        self.hostId = None
        self.connectionId = None
        self.clusterInstanceId = None
        self.buildString = None

        self.socket = None
        if self.host != None and self.port != None:
            ai = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG)[0]
            # ai = (family, socktype, proto, canonname, sockaddr)
            ss = socket.socket(ai[0], ai[1], ai[2])
            try:
                if self.usessl:
                    if ssl_available:
                        self.socket = self.__wrap_socket(ss)
                    else:
                        error("ERROR: To use SSL functionality please install the Python ssl module.")
                        raise ssl_exception
                else:
                    self.socket = ss
                self.socket.setblocking(1)
                self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                try:
                    self.socket.connect(ai[4])
                except Exception:
                    error("ERROR: Failed to connect to %s port %s" % (ai[4][0], ai[4][1]))
                    raise
            except Exception:
                # Once __init__ raises, nothing can reach this socket to close it,
                # so close it here rather than leave it to the garbage collector.
                (self.socket or ss).close()
                self.socket = None
                raise
            #if self.usessl:
            #    print('Cipher suite: ' + str(self.socket.cipher()))

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
                       self.VOLTTYPE_TIMESTAMP: self.readTimestamp,
                       self.VOLTTYPE_DATE: self.readDate,
                       self.VOLTTYPE_BOOLEAN: self.readBoolean,
                       self.VOLTTYPE_DECIMAL: self.readDecimal,
                       self.VOLTTYPE_GEOGRAPHY_POINT: self.readGeographyPoint,
                       self.VOLTTYPE_GEOGRAPHY: self.readGeography}
        self.WRITER = {self.VOLTTYPE_NULL: self.writeNull,
                       self.VOLTTYPE_TINYINT: self.writeByte,
                       self.VOLTTYPE_SMALLINT: self.writeInt16,
                       self.VOLTTYPE_INTEGER: self.writeInt32,
                       self.VOLTTYPE_BIGINT: self.writeInt64,
                       self.VOLTTYPE_FLOAT: self.writeFloat64,
                       self.VOLTTYPE_STRING: self.writeString,
                       self.VOLTTYPE_VARBINARY: self.writeVarbinary,
                       self.VOLTTYPE_TIMESTAMP: self.writeTimestamp,
                       self.VOLTTYPE_DATE: self.writeDate,
                       self.VOLTTYPE_BOOLEAN: self.writeBoolean,
                       self.VOLTTYPE_DECIMAL: self.writeDecimal,
                       self.VOLTTYPE_GEOGRAPHY_POINT: self.writeGeographyPoint,
                       self.VOLTTYPE_GEOGRAPHY: self.writeGeography}
        self.ARRAY_READER = {self.VOLTTYPE_TINYINT: self.readByteArray,
                             self.VOLTTYPE_SMALLINT: self.readInt16Array,
                             self.VOLTTYPE_INTEGER: self.readInt32Array,
                             self.VOLTTYPE_BIGINT: self.readInt64Array,
                             self.VOLTTYPE_FLOAT: self.readFloat64Array,
                             self.VOLTTYPE_STRING: self.readStringArray,
                             self.VOLTTYPE_TIMESTAMP: self.readTimestampArray,
                             self.VOLTTYPE_DATE: self.readDateArray,
                             self.VOLTTYPE_BOOLEAN: self.readBooleanArray,
                             self.VOLTTYPE_DECIMAL: self.readDecimalArray,
                             self.VOLTTYPE_GEOGRAPHY_POINT: self.readGeographyPointArray,
                             self.VOLTTYPE_GEOGRAPHY: self.readGeographyArray}

        self.__compileStructs()

        # Check if the value of a given type is NULL
        self.NULL_DECIMAL_INDICATOR = \
            self.__intToBytes(self.__class__.NULL_DECIMAL_INDICATOR, 0)
        self.NullCheck = {self.VOLTTYPE_NULL:
                              lambda x: None,
                          self.VOLTTYPE_TINYINT:
                              lambda x: None if x == self.__class__.NULL_TINYINT_INDICATOR else x,
                          self.VOLTTYPE_SMALLINT:
                              lambda x: None if x == self.__class__.NULL_SMALLINT_INDICATOR else x,
                          self.VOLTTYPE_INTEGER:
                              lambda x: None if x == self.__class__.NULL_INTEGER_INDICATOR else x,
                          self.VOLTTYPE_BIGINT:
                              lambda x: None if x == self.__class__.NULL_BIGINT_INDICATOR else x,
                          self.VOLTTYPE_FLOAT:
                              lambda x: None if abs(x - self.__class__.NULL_FLOAT_INDICATOR) < 1e307 else x,
                          self.VOLTTYPE_STRING:
                              lambda x: None if x == self.__class__.NULL_STRING_INDICATOR else x,
                          self.VOLTTYPE_VARBINARY:
                              lambda x: None if x == self.__class__.NULL_STRING_INDICATOR else x,
                          self.VOLTTYPE_DECIMAL:
                              lambda x: None if x == self.NULL_DECIMAL_INDICATOR else x}

        self.read_buffer = ReadBuffer()

        if self.usekerberos:
            if not kerberos_available:
                raise RuntimeError("Requested Kerberos authentication but unable to import the GSSAPI package.")
            if not self.has_ticket():
                raise RuntimeError("Requested Kerberos authentication but no valid ticket found. Authenticate with Kerberos first.")
            assert not self.socket is None
            self.socket.settimeout(connect_timeout)
            self.authenticate(str(self.kerberosprincipal), "")
        elif not username is None and not password is None and not host is None:
            assert not self.socket is None
            self.socket.settimeout(connect_timeout)
            self.authenticate(username, password)

        if self.socket:
            self.socket.settimeout(self.default_timeout)

    # Front end to SSL socket support.
    #
    # The SSL config file can be one of:
    # - a properties file (see below)
    # - a truststore to verify server identity, in Java keystore format
    # - a certificate chain to verify server identity, in PEM format
    #
    # "Java keystore" is either the traditional JKS keystore as created
    # by the keytool program, or else a PKCS12 file, which is now the
    # preferred output from keytool. Both are binary encodings.
    #
    # We initially look at the first few bytes of the file to decide
    # what sort of file we are dealing with.
    #
    # A properties file contains a sequence of key=value lines which
    # are used to provide arguments to the SSL context methods:
    #   Key                Value                    Used in call to
    #   ------------------ ------------------------ ------------------
    #   keystore           path to keystore         load_cert_chain
    #   keystorepassword   password for keystore    --
    #   keystorecert       cert for keystore        load_cert_chain
    #   truststore         path to truststore       load_verify_locations
    #   truststorepassword password for truststore  --
    #   cacerts            path to PEM cert chain   load_verify_locations
    #   ssl_version        ignored                  --
    #
    # Thus keystore identifies the client (needed if mutual authentication
    # is required by the server side), whereas truststore and cacerts
    # identify the server. If truststore and cacerts are both specified,
    # cacerts takes precedence.
    #
    # Keystore can only be set via the properties file, and it can
    # be a Java keystore with a single private key entry, or a PEM
    # file containing a PKCS#8 private key and a chain of X.509
    # certificates. Truststore can be set directly as the SSL
    # config file, or via the properties file. Either way it can
    # be a Java keystore or a PEM file, containing a chain of
    # X.509 certificates.
    #
    # For PEM only, the keystore can be built from separate files,
    # one (property 'keystore') having the PKCS#8 private key, the
    # other (property 'keystorecert') having the X.509 cert chain.
    # This is to accomodate certain certificate issuers.
    #
    # The cacerts property should be regarded as deprecated, since
    # it is not supported by Java programs, thus preventing the use
    # of a common properties file.
    #
    # If the ssl_config_file name is absent or empty, then:
    # - if default_cacerts is true, certficate checks will use the
    #   installation default cacerts
    # - if default_cacerts is false, no certificate checks will be
    #   done; we will blindly accept the server's cert

    def __wrap_socket(self, ss):
        parsed_config = {}
        if self.ssl_config_file:
            parsed_config = self.__process_ssl_config_file()

        # Process keystore/truststore files; non-PEM files need conversion to PEM

        keystore_type = keycert_type = truststore_type = None
        if 'keystore' in parsed_config and parsed_config['keystore']:
            keystore_type = self.__classify(parsed_config['keystore'])
        if 'keystorecert' in parsed_config and parsed_config['keystorecert']:
            keycert_type = self.__classify(parsed_config['keystorecert'])
        if 'truststore' in parsed_config and parsed_config['truststore']:
            truststore_type = self.__classify(parsed_config['truststore'])

        store_type = keystore_type or truststore_type
        if store_type:
            if keystore_type and truststore_type and keystore_type != truststore_type:
                raise RuntimeError("keystore file and truststore file must have same format")
            elif keycert_type and keycert_type != 'pem' and keystore_type != 'pem':
                raise RuntimeError("keystore cert file only valid if PEM and with PEM keystore")
            elif store_type == 'pem':
                self.__set_up_pem_files(parsed_config)
            elif store_type == 'jks':
                self.__convert_jks_files(parsed_config)
            elif store_type == 'bin': # assumed to be pkcs12
                self.__convert_pkcs12_files(parsed_config)
            elif store_type == 'prop':
                raise RuntimeError ("keystore or truststore path from property file is a property file")
            else:
                raise RuntimeError("internal error, unknown store type '%s'" % store_type)

        # Additional cacerts (may supersede truststore)

        if 'cacerts' in parsed_config and parsed_config['cacerts']:
            cacerts_type = self.__classify(parsed_config['cacerts'])
            if cacerts_type != 'pem':
                raise RuntimeError("cacerts file %s is not PEM format" % parsed_config['cacerts'])
            self.ssl_config['ca_certs'] = parsed_config['cacerts']
            self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

        if self.default_cacerts:
            self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

        # convert ssl_config to python SSLContext and wrap socket

        context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
        context.verify_mode = self.ssl_config['cert_reqs']

        if self.ssl_config['certfile']:
            context.load_cert_chain(certfile=self.ssl_config['certfile'],
                                    keyfile=self.ssl_config['keyfile'],
                                    password=self.ssl_config['keypass'])

        if self.ssl_config['cert_reqs'] != ssl.CERT_NONE:
            if self.ssl_config['ca_certs']:
                context.load_verify_locations(cafile=self.ssl_config['ca_certs'])
            else:
                context.load_default_certs()

        if sys.hexversion >= 0x03070000:
            protocols = os.getenv('TLS_ENABLED_PROTOCOLS')
            if protocols:
                min, max = self.__select_protocols(protocols)
                if min and max:
                    context.minimum_version = min
                    context.maximum_version = max
                else:
                    print("TLS_ENABLED_PROTOCOLS ignored: no supported versions found")

            ciphers = os.getenv('TLS_PREFERRED_CIPHERS')
            if ciphers:
                try:
                    context.set_ciphers(self.__select_ciphers(context, ciphers))
                except ssl.SSLError as ex:
                    print("TLS_PREFERRED_CIPHERS ignored: %s" % ex)

        return context.wrap_socket(ss)

    def __process_ssl_config_file(self):
        resolved_file = os.path.expandvars(os.path.expanduser(self.ssl_config_file))
        file_type = self.__classify(resolved_file)
        if file_type == 'prop':
            properties = resolve_paths(read_properties_file(resolved_file, True))
        elif file_type == 'pem':
            properties = { 'cacerts': resolved_file }
        elif file_type == 'jks' or file_type == 'bin':
            properties = { 'truststore': resolved_file }
        else:
            raise RuntimeError('Unexpected file classification: %s' % file_type)
        return properties

    def __classify(self, file):
        sample_size = 64
        with open(file, mode='rb') as f:
            buff = f.read(sample_size)
        if len(buff) == sample_size:
            if buff[:4] == bytes.fromhex('feedfeed'): # jks if starts with the right magic
                return 'jks'
            for b in buff: # binary if contains C0/C1 control except CR, LF, tab
                if (b & 0x7f) < 0x20 and b != 0x0d and b != 0x0a and b != 0x09:
                    return 'bin'
        if buff.decode().startswith('-----BEGIN'):
            return 'pem'
        return 'prop' # compatible default

    def __unique_name(self, path, cksum):
        if not scratch_dir:
            self.__set_scratch_dir()
        base = os.path.basename(path) or 'noname'
        return scratch_dir + '/' + base + '-' + cksum + '-' + unique_tag

    def __set_scratch_dir(self):
        global scratch_dir
        try:
            dir = os.path.expanduser('~/.voltssl')
            if dir[0] != '~': # expanded ok
                if not os.path.exists(dir):
                     os.mkdir(dir, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)
                scratch_dir = dir
        except: # mkdir failed
            pass
        if not scratch_dir:
            scratch_dir = os.getenv('TMPDIR', '/tmp')

    def __set_up_pem_files(self, pem_config):
        use_ks = 'keystore' in pem_config and pem_config['keystore']
        use_kc = 'keystorecert' in pem_config and pem_config['keystorecert']
        use_ts = 'truststore' in pem_config and pem_config['truststore']
        if use_ks:
            if self.__has_cert(pem_config['keystore']) or not use_ts:
                self.ssl_config['keyfile'] = None
                self.ssl_config['certfile'] = pem_config['keystore']
            elif use_kc: # key and cert in separate files
                self.ssl_config['keyfile'] = pem_config['keystore']
                self.ssl_config['certfile'] = pem_config['keystorecert']
            else: # fall back on certs from truststore (ok for self-signed cert)
                self.ssl_config['keyfile'] = pem_config['keystore']
                self.ssl_config['certfile'] = pem_config['truststore']
            self.ssl_config['keypass'] = pem_config.get('keystorepassword')
        if use_ts:
            self.ssl_config['ca_certs'] = pem_config['truststore']
            self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

    def __has_cert(self, keyfile):
        with open(keyfile, 'r') as f:
            data = f.read()
        return 'CERTIFICATE' in data

    def __convert_jks_files(self, jks_config):
        if not pyjks_available:
            if os.getenv('VOLTDB_CONTAINER'):
                print("Java KeyStore support is unavailable in this container.\n" +
                      "Consider using PEM files instead, for VoltDB V14 LTS and later.\n" +
                      "You can use --ssl=nocheck to skip verification of the server certificate.\n");
            else:
                error("To use Java KeyStore please install the 'pyjks' module.\n" +
                      "It may be more convenient to use a PEM file instead.\n");
            raise pyjks_exception

        def load_keystore(filename, password):
            with open(filename, 'rb') as f:
               data = f.read()
            ks = jks.KeyStore.loads(data, password)
            cksum = hashlib.md5(data).hexdigest()
            return ks, cksum

        def write_pem(der_bytes, type, f):
            f.write("-----BEGIN %s-----\n" % type)
            f.write("\r\n".join(textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64)))
            f.write("\n-----END %s-----\n" % type)

        # extract key and certs from jks keystore with cacheing
        use_key_cert = False
        if 'keystore' in jks_config and jks_config['keystore']:
            kpass = jks_config.get('keystorepassword')
            ks, cksum = load_keystore(jks_config['keystore'], kpass)
            kfname = self.__unique_name(jks_config['keystore'], cksum)
            keyfilename = kfname + '.key.pem'
            certfilename = kfname + '.cert.pem'

            if os.path.exists(keyfilename) and os.path.exists(certfilename):
                use_key_cert = os.path.getsize(certfilename) > 0
            else:
                keyfile = self.__create_temp(keyfilename)
                certfile = self.__create_temp(certfilename)
                for alias, pk in list(ks.private_keys.items()):
                    # print("Private key: %s" % pk.alias)
                    if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
                        write_pem(pk.pkey, "RSA PRIVATE KEY", keyfile)
                    else:
                        write_pem(pk.pkey_pkcs8, "PRIVATE KEY", keyfile)
                    for c in pk.cert_chain:
                        write_pem(c[1], "CERTIFICATE", certfile)
                    use_key_cert = True
                keyfile.close()
                certfile.close()

            if use_key_cert:
                self.ssl_config['keyfile'] = keyfilename
                self.ssl_config['certfile'] = certfilename

        # extract ca certs from jks truststore with cacheing
        use_ca_cert = False
        if 'truststore' in jks_config and jks_config['truststore']:
            tpass = jks_config.get('truststorepassword')
            ts, cksum = load_keystore(jks_config['truststore'], tpass)
            tfname = self.__unique_name(jks_config['truststore'], cksum)
            cafilename = tfname + '.ca.cert.pem'

            if os.path.exists(cafilename):
                use_ca_cert = os.path.getsize(cafilename) > 0
            else:
                cafile = self.__create_temp(cafilename)
                for alias, c in list(ts.certs.items()):
                    # print("Certificate: %s" % c.alias)
                    write_pem(c.cert, "CERTIFICATE", cafile)
                    use_ca_cert = True
                cafile.close()

            if use_ca_cert:
                self.ssl_config['ca_certs'] = cafilename
                self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

    def __convert_pkcs12_files(self, p12_config):
        if not pkcs12_available:
            if os.getenv('VOLTDB_CONTAINER'):
                print("PKCS12 certificate support is unavailable in this container.\n" +
                      "Consider using PEM files instead, for VoltDB V14 LTS and later.\n" +
                      "You can use --ssl=nocheck to skip verification of the server certificate.\n");
            else:
                error("To use PKCS12 certificate support please install the 'cryptography' module.\n" +
                      "It may be more convenient to use a PEM file instead.\n");
            raise pkcs12_exception

        def checksum(fname):
            with open(fname, 'rb') as f:
               data = f.read()
            return hashlib.md5(data).hexdigest()

        def write_private_key(fname, key):
            with self.__create_temp(fname) as f:
                f.write(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('ascii'))

        def write_cert_chain(fname, cert, more):
            with self.__create_temp(fname) as f:
                if cert:
                    f.write(cert.public_bytes(Encoding.PEM).decode('ascii'))
                if more:
                    for cert in more:
                        f.write(cert.public_bytes(Encoding.PEM).decode('ascii'))

        # pkcs12 keystore file to PEM files: private key and certificate
        use_key_cert = False
        if 'keystore' in p12_config and p12_config['keystore']:
            kpass = p12_config.get('keystorepassword')
            if kpass: kpass = kpass.encode('utf-8')
            kfname = self.__unique_name(p12_config['keystore'], checksum(p12_config['keystore']))
            keyfilename = kfname + '.key.pem'
            certfilename = kfname + '.cert.pem'

            if os.path.exists(keyfilename) and os.path.exists(certfilename):
                use_key_cert = os.path.getsize(certfilename) > 0
            else:
                with open(p12_config['keystore'], 'rb') as f:
                    keystore = pkcs12.load_key_and_certificates(f.read(), kpass)
                if not keystore[0]:
                    raise RuntimeError('No private key entry in keystore')
                if not keystore[1]:
                    raise RuntimeError('No certificate entry in keystore')
                write_private_key(keyfilename, keystore[0])
                write_cert_chain(certfilename, keystore[1], keystore[2])
                use_key_cert = True

            if use_key_cert:
                self.ssl_config['keyfile'] = keyfilename
                self.ssl_config['certfile'] = certfilename

        # pkcs12 truststore to single PEM file with cert chain
        use_ca_cert = False
        if 'truststore' in p12_config and p12_config['truststore']:
            tpass = p12_config.get('truststorepassword')
            if tpass: tpass = tpass.encode('utf-8')
            tfname = self.__unique_name(p12_config['truststore'], checksum(p12_config['truststore']))
            cafilename = tfname + '.ca.cert.pem'

            if os.path.exists(cafilename):
                use_ca_cert = os.path.getsize(cafilename) > 0
            else:
                with open(p12_config['truststore'], 'rb') as f:
                    truststore = pkcs12.load_key_and_certificates(f.read(), tpass)
                if not (truststore[1] or truststore[2]):
                    raise RuntimeError('No certificates in truststore')
                write_cert_chain(cafilename, truststore[1], truststore[2])
                use_ca_cert = True

            if use_ca_cert:
                self.ssl_config['ca_certs'] = cafilename
                self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

    __protodict = { }
    if ssl_available and sys.hexversion >= 0x03070000: # for ssl.TLSVersion
        __protodict = { 'TLSv1.2': ssl.TLSVersion.TLSv1_2,
                        'TLSv1.3': ssl.TLSVersion.TLSv1_3 }

    def __select_protocols(self, protstr):
        proto = [ self.__protodict[p] for p in protstr.split(',')
                                      if p in self.__protodict ]
        return (min(proto), max(proto)) if proto else (None, None)

    def __select_ciphers(self, context, namestr):
        supported = list(c['name'] for c in context.get_ciphers())
        selected = []
        for name in namestr.split(','):
            if name in supported:
                selected.append(name)
            else:
                name2 = self.__java_to_openssl_cipher(name)
                if name2 in supported:
                    selected.append(name2)
        return ':'.join(selected)

    __tlspattern = re.compile(r'^TLS_(.*)_WITH_(.*)_(.*)$')
    __aespattern = re.compile(r'^(AES)-(\d*)(-.*)$')

    def __java_to_openssl_cipher(self, name):
        m = self.__tlspattern.match(name)
        if not m:
            return name
        handshake = m.group(1).replace('_','-')
        cipher = m.group(2).replace('_','-')
        hmac = m.group(3)
        m = self.__aespattern.match(cipher)
        if m:
            cipher = m.group(1) + m.group(2) + m.group(3)
        return handshake + '-' + cipher + '-' + hmac

    def __create_temp(self, filename):
        f = open(filename, 'w')
        os.chmod(filename, stat.S_IRUSR|stat.S_IWUSR)
        temporary_files.append(filename)
        return f

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
        self._s_int8 = struct.Struct(self.inputBOM + 'b')
        self._s_int16 = struct.Struct(self.inputBOM + 'h')
        self._s_int32 = struct.Struct(self.inputBOM + 'i')
        self._s_int64 = struct.Struct(self.inputBOM + 'q')
        self._s_float64 = struct.Struct(self.inputBOM + 'd')

    def close(self):
        if self.dump_file != None:
            self.dump_file.close()
        self.socket.close()

    def authenticate(self, username, password):
        # Requires sending a length preceded username and password even if
        # authentication is turned off.

        #protocol version
        self.writeByte(1)
        #sha256
        self.writeByte(1)

        # service requested
        if (self.usekerberos):
            self.writeString("kerberos")
        else:
            self.writeString("database")

        if username:
            # utf8 encode supplied username or kerberos principal name
            self.writeString(username)
        else:
            # no username, just output length of 0
            self.writeString("")

        # password supplied, sha-256 hash it
        m = hashlib.sha256()
        encoded_password = password.encode("utf-8")
        m.update(encoded_password)
        pwHash = bytearray(m.digest())
        self.wbuf.extend(pwHash)

        self.prependLength()
        self.flush()

        ioerror_message = "ERROR: Connection failed. Please check that the host, port, and ssl settings are correct."
        sslerror_message = "ERROR: Connection failed due to a problem with TLS/SSL, possibly a configuration msismatch."

        # A length, version number, and status code is returned
        # If there was a problem with ssl handshaking it will probably show up
        # on this first read
        try:
            self.bufferForRead()
        except ssl.SSLError as e:
            error(sslerror_message)
            raise e
        except IOError as e:
            error(ioerror_message)
            raise e
        except socket.timeout:
            raise RuntimeError("Authentication timed out after %d seconds."
                                % self.socket.gettimeout())
        version = self.readByte()
        status = self.readByte()

        if (version == self.AUTH_HANDSHAKE_VERSION):
            #service name supplied by VoltDB Server
            service_string = self.readString().encode('ascii','ignore')
            try:
                service_name = gssapi.Name(service_string, name_type=gssapi.NameType.kerberos_principal)
                ctx = gssapi.SecurityContext(name=service_name, mech=gssapi.MechType.kerberos)
                in_token = None
                out_token = ctx.step(in_token)
                while not ctx.complete:
                    self.writeByte(self.AUTH_HANDSHAKE_VERSION)
                    self.writeByte(self.AUTH_HANDSHAKE)
                    self.wbuf.extend(out_token)
                    self.prependLength()
                    self.flush()

                    try:
                        self.bufferForRead()
                    except IOError as e:
                        error(ioerror_message)
                        raise e
                    except socket.timeout:
                        raise RuntimeError("Authentication timed out after %d seconds."
                                            % self.socket.gettimeout())
                    version = self.readByte()
                    status = self.readByte()
                    if version != self.AUTH_HANDSHAKE_VERSION or status != self.AUTH_HANDSHAKE:
                        raise RuntimeError("Authentication failed.")

                    in_token = self.readVarbinaryContent(self.read_buffer.remaining()).tobytes()
                    out_token = ctx.step(in_token)

                try:
                    self.bufferForRead()
                except IOError as e:
                    error(ioerror_message)
                    raise e
                except socket.timeout:
                    raise RuntimeError("Authentication timed out after %d seconds."
                                        % self.socket.gettimeout())
                version = self.readByte()
                status = self.readByte()

            except Exception as e:
                raise RuntimeError("Authentication failed.")


        if status != 0:
            reason = "Authentication failed."
            # Must match assignments in Constants.java
            status_text = ("Server has too many connections.",
                           "Connection timed out during authentication.",
                           "Wire protocol format violation error.",
                           "Failed to authenticate to rejoining node.",
                           "Export not enabled for server.",
                           "Server requires use of TLS/SSL.",
                           "Client certificate required for mutual authentication.",
                           "Client certificate revoked.")
            if status > 0 and status <= len(status_text):
                reason = status_text[status-1]
            raise RuntimeError(reason)

        # Capture the login response rather than discarding it. The server sends,
        # in order: this node's host id, the connection id assigned to us, the
        # cluster instance id (a timestamp paired with a coordinator address), and
        # the server's build string. The host id is the node's stable identity
        # within the cluster; the topology-aware client keys connections on it.
        self.hostId = self.readInt32()
        self.connectionId = self.readInt64()
        self.clusterInstanceId = (self.readInt64(), self.readInt32())
        self.buildString = self.readStringContent(self.readInt32())

    def has_ticket(self):
        '''
        Checks to see if the user has a valid ticket.
        '''
        default_cred = None
        retval = False
        try:
            default_cred = gssapi.creds.Credentials(usage='initiate')
            if default_cred.lifetime > 0:
                self.kerberosprincipal = str(default_cred.name)
                retval = True
            else:
                error("ERROR: Kerberos principal found but login expired.")
        except gssapi.raw.misc.GSSError as e:
            error("ERROR: unable to find default principal from Kerberos cache.")
        return retval

    def setInputByteOrder(self, bom):
        # assuming bom is high bit set?
        if bom == 1:
            self.inputBOM = self.LITTLE_ENDIAN
        else:
            self.inputBOM = self.BIG_ENDIAN

        # recompile the structs
        self.__compileStructs()

    def prependLength(self):
        # write 32 bit array length at offset 0, NOT including the
        # size of this length preceding value. This value is written
        # in the network order.
        ttllen = self.wbuf.buffer_info()[1] * self.wbuf.itemsize
        self.wbuf[0:0] = array.array('B', int32toBytes(ttllen))

    def size(self):
        """Returns the size of the write buffer.
        """

        return (self.wbuf.buffer_info()[1] * self.wbuf.itemsize)

    def flush(self):
        if self.socket is None:
            error("ERROR: not connected to server.")
            raise IOError("No Connection")

        if self.dump_file != None:
            self.dump_file.write(self.wbuf)
            self.dump_file.write(b"\n")
        self.socket.sendall(self.wbuf.tobytes())
        self.wbuf = array.array('B')

    def bufferForRead(self):
        if self.socket is None:
            error("ERROR: not connected to server.")
            raise IOError("No Connection")

        # fully buffer a new length preceded message from socket
        # read the length. the read until the buffer is completed.
        responseprefix = bytes()
        while (len(responseprefix) < 4):
            responseprefix += self.socket.recv(4 - len(responseprefix))
            if responseprefix == b'':
                raise IOError("Connection broken")
        if self.dump_file != None:
            self.dump_file.write(responseprefix)
        responseLength = struct.unpack(self.int32Type(1), responseprefix)[0]
        pieces = []
        remaining = responseLength
        while remaining > 0:
            message = self.socket.recv(remaining)
            if not message:
                raise IOError("Connection broken")
            pieces.append(message)
            remaining -= len(message)
        self.read_buffer.set(b"".join(pieces))
        if not self.dump_file is None:
            self.dump_file.write(self.read_buffer.get_buffer())
            self.dump_file.write(b"\n")

    def read(self, type):
        reader = self.READER.get(type)
        if reader is None:
            error("ERROR: can't read wire type(%d) yet." % (type))
            raise IOError("ERROR: can't read wire type(%d) yet." % (type))

        return reader()

    def write(self, type, value):
        writer = self.WRITER.get(type)
        if writer is None:
            error("ERROR: can't write wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

        return writer(value)

    def readWireType(self):
        type = self.readByte()
        return self.read(type)

    def writeWireType(self, type, value):
        writer = self.WRITER.get(type)
        if writer is None:
            error("ERROR: can't write wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

        self.writeByte(type)
        return writer(value)

    def getRawBytes(self):
        return self.wbuf

    def writeRawBytes(self, value):
        """Appends the given raw bytes to the end of the write buffer.
        """

        self.wbuf.extend(value)

    def __str__(self):
        return repr(self.wbuf)

    def readArray(self, type):
        if type not in self.ARRAY_READER:
            error("ERROR: can't read wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

        return self.ARRAY_READER[type]()

    def readNull(self):
        return None

    def writeNull(self, value):
        return

    def writeArray(self, type, array):
        if (not array) or (len(array) == 0) or (not type):
            return

        if type not in self.ARRAY_READER:
            error("ERROR: Unsupported data type (%d)." % (type))
            raise IOError("ERROR: Unsupported data type (%d)." % (type))

        # serialize arrays of bytes as larger values to support
        # strings and varbinary input
        if type != FastSerializer.VOLTTYPE_TINYINT:
            self.writeInt16(len(array))
        else:
            self.writeInt32(len(array))

        for i in array:
            self.WRITER[type](i)

    def writeWireTypeArray(self, type, array):
        if type not in self.ARRAY_READER:
            error("ERROR: can't write wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

        self.writeByte(type)
        self.writeArray(type, array)

    # byte
    def readByteArrayContent(self, cnt):
        offset = cnt * struct.calcsize('b')
        return self.read_buffer.unpack(self.byteType(cnt), offset)

    def readByteArray(self):
        length = self.readInt32()
        val = self.readByteArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_TINYINT], val))
        return val

    def readByte(self):
        val = self.read_buffer.unpack_struct(self._s_int8)[0]
        return None if val == self.NULL_TINYINT_INDICATOR else val

    def readBoolean(self):
        val = self.readByteArrayContent(1)[0]
        return self.getBoolean(val)

    def readBooleanArray(self):
        length = self.readInt32()
        raw = self.readByteArrayContent(length)
        retval = []
        for i in raw:
            retval.append(self.getBoolean(i))

        return tuple(retval)

    def getBoolean(self, value):
        if value == 1:
            return True
        elif value == 0:
            return False

        return None

    def getBoolen(self, value):
        """Deprecated misspelled alias for getBoolean(); retained for backward compatibility."""
        return self.getBoolean(value)

    def writeBoolean(self, value):
        if value == None:
            val = 127
        elif value:
            val = 1
        else:
            val = 0

        self.writeByte(val)

    def readByteRaw(self):
        val = self.read_buffer.unpack_struct(self._s_int8)[0]
        if val > 127:
            return val - 256
        else:
            return val

    def writeByte(self, value):
        if value == None:
            value = self.__class__.NULL_TINYINT_INDICATOR
        if value < 0:
            value += 256
        self.wbuf.append(value)

    # int16
    def readInt16ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('h')
        return self.read_buffer.unpack(self.int16Type(cnt), offset)

    def readInt16Array(self):
        length = self.readInt16()
        val = self.readInt16ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_SMALLINT], val))
        return val

    def readInt16(self):
        val = self.read_buffer.unpack_struct(self._s_int16)[0]
        return None if val == self.NULL_SMALLINT_INDICATOR else val

    def writeInt16(self, value):
        if value is None:
            value = self.NULL_SMALLINT_INDICATOR
        # Masking keeps int16toBytes' wraparound for out-of-range values.
        self.wbuf.frombytes(_S_U16.pack(operator.index(value) & 0xFFFF))

    # int32
    def readInt32ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('i')
        return self.read_buffer.unpack(self.int32Type(cnt), offset)

    def readInt32Array(self):
        length = self.readInt16()
        val = self.readInt32ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_INTEGER], val))
        return val

    def readInt32(self):
        val = self.read_buffer.unpack_struct(self._s_int32)[0]
        return None if val == self.NULL_INTEGER_INDICATOR else val

    def writeInt32(self, value):
        if value is None:
            value = self.NULL_INTEGER_INDICATOR
        self.wbuf.frombytes(_S_U32.pack(operator.index(value) & 0xFFFFFFFF))

    # int64
    def readInt64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('q')
        return self.read_buffer.unpack(self.int64Type(cnt), offset)

    def readInt64Array(self):
        length = self.readInt16()
        val = self.readInt64ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_BIGINT], val))
        return val

    def readInt64(self):
        val = self.read_buffer.unpack_struct(self._s_int64)[0]
        return None if val == self.NULL_BIGINT_INDICATOR else val

    def writeInt64(self, value):
        if value is None:
            value = self.NULL_BIGINT_INDICATOR
        self.wbuf.frombytes(_S_U64.pack(operator.index(value) & 0xFFFFFFFFFFFFFFFF))

    # float64
    def readFloat64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('d')
        return self.read_buffer.unpack(self.float64Type(cnt), offset)

    def readFloat64Array(self):
        length = self.readInt16()
        val = self.readFloat64ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_FLOAT], val))
        return val

    def readFloat64(self):
        val = self.read_buffer.unpack_struct(self._s_float64)[0]
        return None if abs(val - self.NULL_FLOAT_INDICATOR) < 1e307 else val

    def writeFloat64(self, value):
        if value == None:
            val = self.__class__.NULL_FLOAT_INDICATOR
        else:
            val = float(value)
        self.wbuf.frombytes(self._s_float64.pack(val))

    # string
    def readStringContent(self, cnt):
        if cnt == 0:
            return ""

        offset = cnt * struct.calcsize('c')
        val = self.read_buffer.unpack(self.stringType(cnt), offset)
        return val[0].decode("utf-8")

    def readString(self):
        # length preceeded (4 byte value) string
        length = self.readInt32()
        if length == self.NULL_STRING_INDICATOR:
            return None
        if length == 0:
            return ""
        rb = self.read_buffer
        start = rb._off
        if start + length > len(rb._buf):
            return self.readStringContent(length)  # reports the short buffer
        rb._off = start + length
        return rb._buf[start:start + length].decode("utf-8")

    def readStringArray(self):
        retval = []
        cnt = self.readInt16()

        for i in range(cnt):
            retval.append(self.readString())

        return tuple(retval)

    def writeString(self, value):
        if value is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
            return

        encoded_value = value.encode("utf-8")
        self.wbuf.frombytes(_S_U32.pack(len(encoded_value)))
        self.wbuf.frombytes(encoded_value)

    # varbinary
    def readVarbinaryContent(self, cnt):
        if cnt == 0:
            return array.array('B', [])

        offset = cnt * struct.calcsize('c')
        val = self.read_buffer.unpack(self.varbinaryType(cnt), offset)

        return array.array('B', val[0])

    def readVarbinary(self):
        # length preceeded (4 byte value) string
        length = self.readInt32()
        if self.NullCheck[self.VOLTTYPE_VARBINARY](length) == None:
            return None
        return self.readVarbinaryContent(length)

    def writeVarbinary(self, value):
        if value is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
            return

        self.writeInt32(len(value))
        self.wbuf.extend(value)

    # timestamp
    # The timestamp we receive from the server is a 64-bit integer representing
    # microseconds since the epoch. It will be converted to a datetime object in
    # the local timezone.
    def readTimestamp(self):
        raw = self.readInt64()
        if raw == None:
            return None
        # microseconds before or after Jan 1, 1970 UTC
        return datetime.datetime.fromtimestamp(raw/1000000.0)

    def readTimestampArray(self):
        retval = []
        raw = self.readInt64Array()

        for i in raw:
            val = None
            if i != None:
                val = datetime.datetime.fromtimestamp(i/1000000.0)
            retval.append(val)

        return tuple(retval)

    def writeTimestamp(self, value):
        if value is None:
            val = self.__class__.NULL_BIGINT_INDICATOR
        else:
            seconds = int(value.strftime("%s"))
            val = seconds * 1000000 + value.microsecond
        self.wbuf.extend(int64toBytes(val))

    # date
    # The timestamp we receive from the server is an encoded 32-bit integer that represent the date
    # Encoding - 2 bytes year, 1 byte month, 1 byte day
    # It will be converted to a datetime.date object
    def readDate(self):
        raw = self.readInt32()
        if raw == None:
            return None
        return self._decode_date(raw)

    def readDateArray(self):
        retval = []
        raw = self.readInt32Array()
        for i in raw:
            val = None
            if i != None:
                val = self._decode_date(i)
            retval.append(val)

        return tuple(retval)

    def writeDate(self, value):
        if value is None:
            val = self.__class__.NULL_INTEGER_INDICATOR
        else:
            val = self._encode_date(value.year, value.month, value.day)
        self.wbuf.extend(int32toBytes(val))

    @staticmethod
    def _decode_date(encoded: int) -> datetime.date:
        year = (encoded >> 16) & 0xFFFF
        month = (encoded >> 8) & 0xFF
        day = encoded & 0xFF
        return datetime.date(year, month, day)

    @staticmethod
    def _encode_date(year: int, month: int, day: int) -> int:
        return (year << 16) | (month << 8) | day

    def readDecimal(self):
        offset = 16 * struct.calcsize('b')
        if self.NullCheck[self.VOLTTYPE_DECIMAL](self.read_buffer.read(offset)) == None:
            self.read_buffer.shift(offset)
            return None
        val = list(self.read_buffer.unpack(self.ubyteType(16), offset))
        mostSignificantBit = 1 << 7
        isNegative = (val[0] & mostSignificantBit) != 0
        unscaledValue = -(val[0] & mostSignificantBit) << 120
        # Clear the highest bit
        # Unleash the powers of the butterfly
        val[0] &= ~mostSignificantBit
        # Get the 2's complement
        for x in range(16):
            unscaledValue += val[x] << ((15 - x) * 8)
        unscaledValue = [int(x) for x in str(abs(unscaledValue))]
        return decimal.Decimal((isNegative, tuple(unscaledValue),
                                -self.__class__.DEFAULT_DECIMAL_SCALE))

    def readDecimalArray(self):
        retval = []
        cnt = self.readInt16()
        for i in range(cnt):
            retval.append(self.readDecimal())
        return tuple(retval)

    def __intToBytes(self, value, sign):
        value_bytes = bytes()
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

    def writeDecimal(self, num):
        if num is None:
            self.wbuf.extend(self.NULL_DECIMAL_INDICATOR)
            return
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
        self.wbuf.extend(data)

    def writeDecimalString(self, num):
        if num is None:
            self.writeString(None)
            return
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of type decimal.Decimal")
        self.writeString(num.to_eng_string())

    # cash!
    def readMoney(self):
        # money-unit * 10,000
        return self.readInt64()

    def readGeographyPoint(self):
        # returns a tuple of a pair of doubles representing (longitude, latitude)
        lng = self.readFloat64()
        lat = self.readFloat64()
        if (lat == Geography.NULL_COORD) and (lng == Geography.NULL_COORD):
            return None
        return (lng, lat)

    def readGeographyPointArray(self):
        retval = []
        cnt = self.readInt16()
        for i in range(cnt):
            retval.append(self.readGeographyPoint())
        return tuple(retval)

    def writeGeographyPoint(self, point):
        if point is None:
            self.writeFloat64(Geography.NULL_COORD)
            self.writeFloat64(Geography.NULL_COORD)
            return
        if not isinstance(point, tuple):
            raise TypeError("point must be a 2-tuple of floats")
        if len(point) != 2:
            raise TypeError("point must be a 2-tuple of floats")
        self.writeFloat64(point[0])
        self.writeFloat64(point[1])

    def readGeography(self):
        return Geography.unflatten(self)

    def readGeographyArray(self):
        retval = []
        cnt = self.readInt16()
        for i in range(cnt):
            retval.append(Geography.unflatten(self))
        return tuple(retval)

    def writeGeography(self, geo):
        if geo is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
        else:
            geo.flatten(self)

class XYZPoint(object):
    """
    Google's S2 geometry library uses (x, y, z) representation of polygon vertices,
    but the interface we expose to users is (longitude, latitude).  This class is
    the internal representation for vertices.
    """
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    @staticmethod
    def fromGeographyPoint(p):
        # p is a (longitude, latitude) pair, matching the rest of this module
        lngRadians = p[0] * (math.pi / 180) # AKA theta
        latRadians = p[1] * (math.pi / 180) # AKA phi

        cosPhi = math.cos(latRadians)
        x = math.cos(lngRadians) * cosPhi
        y = math.sin(lngRadians) * cosPhi
        z = math.sin(latRadians)

        return XYZPoint(x, y, z)

    def toGeographyPoint(self):
        latRadians = math.atan2(self.z, math.sqrt(self.x * self.x + self.y * self.y))
        lngRadians = math.atan2(self.y, self.x)

        latDegrees = latRadians * (180 / math.pi)
        lngDegrees = lngRadians * (180 / math.pi)
        return (lngDegrees, latDegrees)

    def toGeogrpahyPoint(self):
        """Deprecated misspelled alias for toGeographyPoint(); retained for backward compatibility."""
        return self.toGeographyPoint()

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        """Overrides the default implementation (unnecessary in Python 3)"""
        return not self.__eq__(other)

    def __str__(self):
        p = self.toGeographyPoint()
        return "(%s,%s)" % (p[0], p[1])

class Geography(object):
    """
    S2-esque geography element representing a polygon for now
    """

    EPSILON = 1.0e-12
    NULL_COORD = 360.0

    def __init__(self, loops=[]):
        self.loops = loops

    # Serialization format for polygons.
    #
    # This is the format used by S2 in the EE.  Most of the
    # metadata (especially lat/lng rect bounding boxes) are
    # ignored here in Java.
    #
    # 1 byte       encoding version
    # 1 byte       boolean owns_loops
    # 1 byte       boolean has_holes
    # 4 bytes      number of loops
    #   And then for each loop:
    #     1 byte       encoding version
    #     4 bytes      number of vertices
    #     ((number of vertices) * sizeof(double) * 3) bytes    vertices as XYZPoints
    #     1 byte       boolean origin_inside
    #     4 bytes      depth (nesting level of loop)
    #     33 bytes     bounding box
    # 33 bytes     bounding box
    #
    # We use S2 in the EE for all geometric computation, so polygons sent to
    # the EE will be missing bounding box and other info.  We indicate this
    # by passing INCOMPLETE_ENCODING_FROM_JAVA in the version field.  This
    # tells the EE to compute bounding boxes and other metadata before storing
    # the polygon to memory.

    # for encoding byte + lat min, lat max, lng min, lng max as doubles
    BOUND_LENGTH_IN_BYTES = 33
    POLYGON_OVERHEAD_IN_BYTES = 7 + BOUND_LENGTH_IN_BYTES
    # 1 byte     for encoding version
    # 4 bytes    for number of vertices
    # number of vertices * 8 * 3  bytes  for vertices as XYZPoints
    # 1 byte     for origin_inside_
    # 4 bytes    for depth_
    # length of bound
    LOOP_OVERHEAD_IN_BYTES = 10 + BOUND_LENGTH_IN_BYTES
    VERTEX_SIZE_IN_BYTES = 24

    def serializedSize(self):
        length = Geography.POLYGON_OVERHEAD_IN_BYTES
        for loop in self.loops:
            length += Geography.loopSerializedSize(loop)
        return length

    @staticmethod
    def loopSerializedSize(loop):
        return Geography.LOOP_OVERHEAD_IN_BYTES + (len(loop) * Geography.VERTEX_SIZE_IN_BYTES)

    @staticmethod
    def unflatten(fs):
        length = fs.readInt32() # size
        if (length == fs.NULL_STRING_INDICATOR):
            return None

        version = fs.readByteRaw() # encoding version
        fs.readByteRaw() # owns loops
        fs.readByteRaw() # has holes
        numLoops = fs.readInt32()
        loops = []
        indexOfOuterRing = 0
        for i in range(numLoops):
            depth, loop = Geography.__unflattenLoop(fs)
            if depth == 0:
                indexOfOuterRing = i
            loops.append(loop)

        Geography.__unflattenBound(fs)

        return Geography(loops);

    @staticmethod
    def __unflattenLoop(fs):
        # 1 byte     for encoding version
        # 4 bytes    for number of vertices
        # number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        # 1 byte     for origin_inside_
        # 4 bytes    for depth_
        # length of bound

        loop = []
        fs.readByteRaw() # encoding version
        numVertices = fs.readInt32()
        for i in range(numVertices):
            x = fs.readFloat64()
            y = fs.readFloat64()
            z = fs.readFloat64()
            loop.append(XYZPoint(x, y, z))

        fs.readByteRaw() # origin_inside_
        depth = fs.readInt32() # depth
        Geography.__unflattenBound(fs);
        return (depth, loop)

    @staticmethod
    def __unflattenBound(fs):
        fs.readByteRaw() # for encoding version
        fs.readFloat64()
        fs.readFloat64()
        fs.readFloat64()
        fs.readFloat64()

    def flatten(self, fs):
        fs.writeInt32(self.serializedSize()) # prepend length

        fs.writeByte(0); # encoding version
        fs.writeByte(1); # owns_loops

        if len(self.loops) > 1: # has_holes
            fs.writeByte(1)
        else:
            fs.writeByte(0)
        fs.writeInt32(len(self.loops))
        depth = 0
        for loop in self.loops:
            Geography.__flattenLoop(loop, depth, fs);
            depth = 1;
        Geography.__flattenEmptyBound(fs);

    @staticmethod
    def __flattenLoop(loop, depth, fs):
        # 1 byte     for encoding version
        # 4 bytes    for number of vertices
        # number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        # 1 byte     for origin_inside_
        # 4 bytes    for depth_
        # length of bound
        fs.writeByte(0);
        fs.writeInt32(len(loop))
        for xyzp in loop:
            fs.writeFloat64(xyzp.x)
            fs.writeFloat64(xyzp.y)
            fs.writeFloat64(xyzp.z)

        fs.writeByte(0);  # origin_inside
        fs.writeInt32(depth); # depth
        Geography.__flattenEmptyBound(fs);

    @staticmethod
    def __flattenEmptyBound(fs):
        fs.writeByte(0); # for encoding version
        fs.writeFloat64(Geography.NULL_COORD)
        fs.writeFloat64(Geography.NULL_COORD)
        fs.writeFloat64(Geography.NULL_COORD)
        fs.writeFloat64(Geography.NULL_COORD)

    @staticmethod
    def formatPoint(point):
        # auto convert XYZ points
        if isinstance(point, XYZPoint):
            point = point.toGeographyPoint()

        # Explicitly test for differences less than 1.0e-12 and
        # force them to be zero.  Otherwise you may find a case
        # where two points differ in the less significant bits, but
        # they format as the same number.
        lng = point[0]
        if abs(lng) < Geography.EPSILON:
            lng = 0.0
        lat = point[1]
        if abs(lat) < Geography.EPSILON:
            lat = 0.0

        # Format with up to 12 digits after the decimal point, matching the Java
        # client's DecimalFormat("##0.0###########"): trailing zeros are trimmed
        # but at least one fractional digit is kept.
        def fmt(v):
            s = "%.12f" % v
            s = s.rstrip("0")
            if s.endswith("."):
                s += "0"
            return s
        return fmt(lng) + " " + fmt(lat)

    @staticmethod
    def pointToWKT(point):
        # auto convert XYZ points
        if isinstance(point, XYZPoint):
            point = point.toGeographyPoint()

        # This is not GEOGRAPHY_POINT.  This is wkt syntax.
        return "POINT (" + Geography.formatPoint(point) + ")"


    wktPointMatcher = re.compile(r"^\s*point\s*\(\s*(-?\d+[\.\d*]*)\s+(-?\d+[\.\d*]*)\s*\)", flags=re.IGNORECASE)
    @staticmethod
    def pointFromWKT(wkt):
        if wkt is None:
            raise ValueError("None passed to pointFromWKT")
        match = Geography.wktPointMatcher.search(wkt)
        if match is None:
            return None
        lngStr = match.group(1)
        latStr = match.group(2)
        if latStr is None or lngStr is None:
            return None
        lng = float(lngStr)
        lat = float(latStr)
        return (lng, lat)

    # Tokenizer for WKT polygons: parentheses, commas, signed decimal numbers
    # (optionally in scientific notation), bare words like POLYGON, whitespace
    # (skipped), and a catch-all for anything unrecognized.
    wktPolygonTokenizer = re.compile(
        r"(?P<lparen>\()|(?P<rparen>\))|(?P<comma>,)"
        r"|(?P<number>[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)"
        r"|(?P<word>[A-Za-z]+)|(?P<ws>\s+)|(?P<bad>.)")

    @staticmethod
    def geographyFromWKT(wkt):
        """Parse an OGC well-known-text POLYGON into a Geography.

        Only polygons are supported (matching the Java client). The first ring is
        the outer shell, listed counter-clockwise; each subsequent ring is a hole,
        listed clockwise. Internally every loop is stored counter-clockwise (the
        S2 convention) with the repeated closing vertex removed -- the exact
        inverse of what __str__() emits. Raises ValueError on malformed input.
        """
        if wkt is None:
            raise ValueError("None passed to geographyFromWKT")

        msg = "Improperly formatted WKT for polygon: "

        tokens = []
        for m in Geography.wktPolygonTokenizer.finditer(wkt):
            kind = m.lastgroup
            if kind == "ws":
                continue
            if kind == "bad":
                raise ValueError(msg + "unrecognized character %r" % m.group())
            tokens.append((kind, m.group()))

        pos = [0]
        def nextToken():
            if pos[0] < len(tokens):
                tok = tokens[pos[0]]
                pos[0] += 1
                return tok
            return ("eof", None)
        def pushBack():
            pos[0] -= 1

        kind, text = nextToken()
        if kind != "word" or text.lower() != "polygon":
            raise ValueError(msg + "expected WKT to start with POLYGON")
        kind, text = nextToken()
        if kind != "lparen":
            raise ValueError(msg + "expected left parenthesis after POLYGON")

        loops = []
        currentLoop = None
        isShell = True
        polygonOpen = True
        while polygonOpen:
            kind, text = nextToken()
            if kind == "lparen":
                if currentLoop is not None:
                    raise ValueError(msg + "missing closing parenthesis")
                currentLoop = []
            elif kind == "number":
                if currentLoop is None:
                    raise ValueError(msg + "missing opening parenthesis")
                lng = float(text)
                kind, text = nextToken()
                if kind != "number":
                    raise ValueError(msg + "missing latitude in longitude latitude pair")
                lat = float(text)
                currentLoop.append((lng, lat))
                kind, text = nextToken()
                if kind != "comma":
                    if kind != "rparen":
                        raise ValueError(msg + "missing comma between longitude latitude pairs")
                    pushBack()  # hand the ')' to the rparen branch to close the ring
            elif kind == "rparen":
                Geography.__diagnoseLoop(currentLoop, msg)
                currentLoop.pop()  # drop the repeated closing vertex
                if not isShell:
                    # WKT holes are clockwise; S2 wants counter-clockwise. Reverse
                    # every vertex except the first, which is held invariant.
                    currentLoop[1:] = currentLoop[1:][::-1]
                isShell = False
                loops.append([XYZPoint.fromGeographyPoint(p) for p in currentLoop])
                currentLoop = None
                kind, text = nextToken()
                if kind == "rparen":
                    polygonOpen = False
                elif kind != "comma":
                    raise ValueError(msg + "unrecognized token in WKT")
            elif kind == "eof":
                raise ValueError(msg + "premature end of input")
            else:
                raise ValueError(msg + "unrecognized token in WKT")

        kind, text = nextToken()
        if kind != "eof":
            raise ValueError(msg + "unrecognized input after WKT")

        return Geography(loops)

    @staticmethod
    def __diagnoseLoop(loop, msgPrefix):
        # 4 points = 3 distinct vertices + the repeated closing vertex.
        if loop is None or len(loop) < 4:
            raise ValueError(msgPrefix + "a polygon ring must contain at least 4 points "
                             "(including the repeated closing vertex)")
        if loop[0] != loop[-1]:
            raise ValueError(msgPrefix + "closing points of ring are not equal: "
                             "%r != %r" % (loop[0], loop[-1]))

    def __str__(self):
        # return representation in Well Known Text (WKT)
        wkt = "POLYGON ("

        isFirstLoop = True
        for loop in self.loops:
            if not isFirstLoop:
                wkt += ", "
            wkt += "("

            # iterate backwards
            startIdx = len(loop) - 1
            endIdx = 0
            increment = -1
            # reverse direction for first loop
            if isFirstLoop:
                startIdx = 1
                endIdx = len(loop)
                increment = 1

            wkt += Geography.formatPoint(loop[0]) + ", "
            for idx in range(startIdx, endIdx, increment):
                xyzp = loop[idx]
                wkt += Geography.formatPoint(xyzp) + ", "

            # Repeat the start vertex to close the loop as WKT requires.
            wkt += Geography.formatPoint(loop[0]) + ")"
            isFirstLoop = False

        wkt += ")"
        return wkt

    def __repr__(self):
        return self.__str__()

class VoltColumn:
    "definition of one VoltDB table column"
    def __init__(self, fser = None, type = None, name = None):
        if fser != None:
            self.type = fser.readByte()
            self.name = None
        elif type != None and name != None:
            self.type = type
            self.name = name

    def __str__(self):
        # If the name is empty, use the default "modified tuples". Has to do
        # this because HSQLDB doesn't return a column name if the table is
        # empty.
        return "(%s: %d)" % (self.name or "modified tuples" ,
                             self.type)

    def __eq__(self, other):
        # For now, if we've been through the query on a column with no name,
        # just assume that there's no way the types are matching up cleanly
        # and there ain't no one for to give us no pain
        if (not self.name or not other.name):
            return True
        return (self.type == other.type and self.name == other.name)

    def readName(self, fser):
        self.name = fser.readString()

    def writeType(self, fser):
        fser.writeByte(self.type)

    def writeName(self, fser):
        fser.writeString(self.name)

class VoltTable:
    "definition and content of one VoltDB table"
    def __init__(self, fser):
        self.fser = fser
        self.columns = []  # column definitions
        self.tuples = []

    def __str__(self):
        result = ""

        result += "column count: %d\n" % (len(self.columns))
        result += "row count: %d\n" % (len(self.tuples))
        result += "cols: "
        result += ", ".join([str(x) for x in self.columns])
        result += "\n"
        result += "rows -\n"
        result += "\n".join([str(["NULL" if y is None else y for y in x]) for x in self.tuples])

        return result

    def __getstate__(self):
        return (self.columns, self.tuples)

    def __setstate__(self, state):
        self.fser = None
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
    def readFromSerializer(self):
        # 1.
        tablesize = self.fser.readInt32()
        limit_position = self.fser.read_buffer._off + tablesize
        # 2.
        headersize = self.fser.readInt32()
        statuscode = self.fser.readByte()
        columncount = self.fser.readInt16()
        for i in range(columncount):
            column = VoltColumn(fser = self.fser)
            self.columns.append(column)
        list([x.readName(self.fser) for x in self.columns])

        # 3.
        rowcount = self.fser.readInt32()
        readers = [self.fser.READER.get(c.type) for c in self.columns]
        if None in readers:
            readers = [lambda t=c.type: self.fser.read(t) for c in self.columns]
        rb = self.fser.read_buffer
        tuples = self.tuples
        for i in range(rowcount):
            rb._off += 4  # row size
            tuples.append([read() for read in readers])

        # advance offset to end of table-size on read_buffer
        if self.fser.read_buffer._off != limit_position:
            self.fser.read_buffer._off = limit_position

        return self

    def writeToSerializer(self):
        table_fser = FastSerializer()

        # We have to pack the header into a buffer first so that we can
        # calculate the size
        header_fser = FastSerializer()

        header_fser.writeByte(0)
        header_fser.writeInt16(len(self.columns))
        list([x.writeType(header_fser) for x in self.columns])
        list([x.writeName(header_fser) for x in self.columns])

        table_fser.writeInt32(header_fser.size() - 4)
        table_fser.writeRawBytes(header_fser.getRawBytes())

        table_fser.writeInt32(len(self.tuples))
        for i in self.tuples:
            row_fser = FastSerializer()

            list([row_fser.write(self.columns[x].type, i[x]) for x in range(len(i))])

            table_fser.writeInt32(row_fser.size())
            table_fser.writeRawBytes(row_fser.getRawBytes())

        table_fser.prependLength()
        self.fser.writeRawBytes(table_fser.getRawBytes())


class VoltException:
    # Volt SerializableException enumerations
    VOLTEXCEPTION_NONE = 0
    VOLTEXCEPTION_EEEXCEPTION = 1
    VOLTEXCEPTION_SQLEXCEPTION = 2
    VOLTEXCEPTION_CONSTRAINTFAILURE = 3
    VOLTEXCEPTION_GENERIC = 4

    def __init__(self, fser):
        self.type = self.VOLTEXCEPTION_NONE
        self.typestr = "None"
        self.message = ""

        if fser != None:
            self.deserialize(fser)

    def deserialize(self, fser):
        self.length = fser.readInt32()
        if self.length == 0:
            self.type = self.VOLTEXCEPTION_NONE
            return
        self.type = fser.readByte()
        # quick and dirty exception skipping
        if self.type == self.VOLTEXCEPTION_NONE:
            return

        self.message = []
        self.message_len = fser.readInt32()
        for i in range(0, self.message_len):
            self.message.append(chr(fser.readByte()))
        self.message = ''.join(self.message)

        if self.type == self.VOLTEXCEPTION_GENERIC:
            self.typestr = "Generic"
        elif self.type == self.VOLTEXCEPTION_EEEXCEPTION:
            self.typestr = "EE Exception"
            # serialized size from EEException.java is 4 bytes
            self.error_code = fser.readInt32()
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION or \
                self.type == self.VOLTEXCEPTION_CONSTRAINTFAILURE:
            self.sql_state_bytes = []
            for i in range(0, 5):
                self.sql_state_bytes.append(chr(fser.readByte()))
            self.sql_state_bytes = ''.join(self.sql_state_bytes)

            if self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
                self.typestr = "SQL Exception"
            else:
                self.typestr = "Constraint Failure"
                self.constraint_type = fser.readInt32()
                self.table_name = fser.readString()
                self.buffer_size = fser.readInt32()
                self.buffer = []
                for i in range(0, self.buffer_size):
                    self.buffer.append(fser.readByte())
        else:
            for i in range(0, self.length - 3 - 2 - self.message_len):
                fser.readByte()
            error("Python client deserialized unknown VoltException.")

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
    def __init__(self, fser):
        self.fser = fser
        self.version = -1
        self.clientHandle = -1
        self.status = -1
        self.statusString = ""
        self.appStatus = -1
        self.appStatusString = ""
        self.roundtripTime = -1
        self.exception = None
        self.tables = None

        if fser != None:
            self.deserialize(fser)

    @classmethod
    def _from_body(cls, fser, body):
        """Decode a response already received: ``body`` is one complete message
        without its length prefix."""
        response = cls(None)
        response.fser = fser
        fser.read_buffer.set(body)
        response._read(fser)
        return response

    def deserialize(self, fser):
        fser.bufferForRead()
        self._read(fser)

    def _read(self, fser):
        # serialization order: response-length, status, roundtripTime, exception,
        # tables[], info, id.
        self.version = fser.readByte()
        self.clientHandle = fser.readInt64()
        presentFields = fser.readByteRaw();
        self.status = fser.readByte()
        if presentFields & (1 << 5) != 0:
            self.statusString = fser.readString()
        else:
            self.statusString = None
        self.appStatus = fser.readByte()
        if presentFields & (1 << 7) != 0:
            self.appStatusString = fser.readString()
        else:
            self.appStatusString = None
        self.roundtripTime = fser.readInt32()
        if presentFields & (1 << 6) != 0:
            self.exception = VoltException(fser)
        else:
            self.exception = None

        # tables[]
        tablecount = fser.readInt16()
        self.tables = []
        for i in range(tablecount):
            table = VoltTable(fser)
            self.tables.append(table.readFromSerializer())

    def __str__(self):
        tablestr=""
        if self.tables != None:
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

# Never serialized as arrays (iter() raises TypeError or, for str, is excluded).
_SCALAR_PARAM_TYPES = (int, float, str, decimal.Decimal, datetime.datetime,
                       datetime.date, type(None))

class VoltProcedure:
    "VoltDB called procedure interface"
    def __init__(self, fser, name, paramtypes = []):
        self.fser = fser             # FastSerializer object
        self.name = name             # procedure class name
        self.paramtypes = paramtypes # list of fser.WIRE_* values

    def call(self, params = None, response = True, timeout = None):
        self.fser.writeByte(0)  # version number
        self.fser.writeString(self.name)
        self.fser.writeInt64(1)            # client handle
        self.fser.writeInt16(len(self.paramtypes))
        for i in range(len(self.paramtypes)):
            if self.as_array(self.paramtypes[i], params[i]):
                self.fser.writeByte(FastSerializer.ARRAY)
                self.fser.writeByte(self.paramtypes[i])
                self.fser.writeArray(self.paramtypes[i], params[i])
            else:
                self.fser.writeWireType(self.paramtypes[i], params[i])
        self.fser.prependLength() # prepend the total length of the invocation
        self.fser.flush()

        # The timeout in effect for the procedure call is the timeout argument
        # if not None or self.procedure_timeout. Exceeding that time will raise
        # a timeout exception. Restores the original timeout value when done.
        # This default argument usage does not allow overriding with None.
        if timeout is None:
            timeout = self.fser.procedure_timeout
        original_timeout = self.fser.socket.gettimeout()
        self.fser.socket.settimeout(timeout)
        try:
            try:
                res = VoltResponse(self.fser)
            except socket.timeout:
                res = VoltResponse(None)
                res.statusString = "timeout: procedure call took longer than %d seconds" % timeout
            except IOError as err:
                res = VoltResponse(None)
                res.statusString = str(err)
        finally:
            self.fser.socket.settimeout(original_timeout)
        return response and res or None

    def as_array(self, paramtype, param):
        if isinstance(param, _SCALAR_PARAM_TYPES):
            return False
        try:
            iter(param) # throws TypeError if not a python array type
            if isinstance(param, str):
                return False
            if isinstance(param, bytes) or isinstance(param, bytearray):
                return paramtype != FastSerializer.VOLTTYPE_VARBINARY # as non-array if we want varbinary
            if paramtype == FastSerializer.VOLTTYPE_GEOGRAPHY_POINT:
                # GEOGRAPHY_POINT is the one scalar type whose value is itself a
                # sequence: a single point is (lng, lat) -- two numbers -- while an
                # array of points is a sequence of such tuples. iter() alone can't
                # distinguish them, so treat a length-2 sequence of numbers as the
                # scalar point and anything else as an array of points.
                return not (len(param) == 2
                            and all(isinstance(c, (int, float)) for c in param))
            return True
        except TypeError:
            return False

# Reads a properties file that is broadly compatible
# with the forms supported by Java, in particular for
# allowable separators between key and value. Note,
# the key can optionally be converted to lower case
# for compatibility with the previous implementation.

def read_properties_file(filename, lowerkeys=False):
    separator = re.compile(r'\s*[=:]\s*|\s+')
    properties = {}
    count = 0
    with open(filename, mode='rt', encoding='utf-8') as f:
        for line in f:
            count = count + 1
            line = line.strip()
            if line and line[0] != '#' and line[0] != '!':
                m = separator.search(line)
                if m and m.start() > 0:
                    key = line[:m.start()]
                    val = line[m.end():]
                    properties[key.lower() if lowerkeys else key] = val
                else:
                    raise ValueError('Malformed property at line %s in %s: %s' % (count, filename, line))
    return properties

# Expand leading ~ in known pathnames from properties

def resolve_paths(properties):
    for key in ('keystore', 'keystorecert', 'truststore', 'cacerts'):
        if key in properties and properties[key].startswith('~'):
            properties[key] = os.path.expanduser(properties[key])
    return properties
