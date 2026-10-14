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
Backward-compatibility guard for the public API of the legacy Python client
(src/voltdbclient.py).

This introspects the public surface -- the documented classes, their
constructors, and their public methods -- and asserts it matches a recorded
snapshot (EXPECTED_API below). Any drift -- a renamed or removed method, an
added method, or a changed parameter name, kind, or default -- fails the test
with a readable diff, forcing a conscious, reviewable update to the snapshot.

This is a *shape* guard only: it verifies signatures, not behavior. Behavior is
covered by the offline serialization unit tests and the live end-to-end test in
this directory.

To regenerate EXPECTED_API after an intentional public-API change, run:

    python3 test_api_snapshot.py --dump

and paste the printed assignment over the EXPECTED_API assignment below. The
diff in code review is then exactly the API change being made.
"""

import inspect
import sys
import unittest

import voltdbclient

# The public classes whose shape customer applications and internal tooling
# (voltcli, voltadmin, checkstats, ...) depend on.
PUBLIC_CLASSES = ["FastSerializer", "VoltProcedure", "VoltResponse", "VoltTable",
                  "VoltColumn", "VoltException", "Geography", "XYZPoint"]

# Sentinel recorded for a parameter that has no default value.
NO_DEFAULT = "<required>"


def _params(func):
    """Return [(name, kind, default_repr), ...] for a callable's parameters,
    excluding an implicit ``self``. Passing a class introspects its constructor.

    We record ``repr(default)`` and the parameter *kind* rather than the raw
    ``str(inspect.signature(...))`` on purpose: the tuple form is stable across
    Python versions (signature string formatting is not), and the kind catches
    a parameter being made positional-only or keyword-only -- both breaking
    changes -- not just a renamed or re-defaulted one.
    """
    params = []
    for name, p in inspect.signature(func).parameters.items():
        if name == "self":
            continue
        default = NO_DEFAULT if p.default is inspect.Parameter.empty else repr(p.default)
        params.append((name, p.kind.name, default))
    return params


def _is_method(obj):
    # Plain methods and staticmethods surface as functions; classmethods as
    # bound methods. Capture both so we don't miss e.g. Geography.unflatten.
    return inspect.isfunction(obj) or inspect.ismethod(obj)


def snapshot(module):
    """Introspect the public API into {class: {member: [(name, kind, default)]}}.

    Members are the constructor (recorded as ``__init__``) plus every public
    (non-underscore) method.
    """
    api = {}
    for cname in PUBLIC_CLASSES:
        cls = getattr(module, cname)
        members = {"__init__": _params(cls)}
        for mname, m in inspect.getmembers(cls, _is_method):
            if not mname.startswith("_"):
                members[mname] = _params(m)
        api[cname] = members
    return api


EXPECTED_API = {'FastSerializer': {'__init__': [('host', 'POSITIONAL_OR_KEYWORD', 'None'),
                                                ('port', 'POSITIONAL_OR_KEYWORD', '21212'),
                                                ('usessl', 'POSITIONAL_OR_KEYWORD', 'False'),
                                                ('username', 'POSITIONAL_OR_KEYWORD', "''"),
                                                ('password', 'POSITIONAL_OR_KEYWORD', "''"),
                                                ('kerberos', 'POSITIONAL_OR_KEYWORD', 'False'),
                                                ('dump_file_path', 'POSITIONAL_OR_KEYWORD', 'None'),
                                                ('connect_timeout', 'POSITIONAL_OR_KEYWORD', '8'),
                                                ('procedure_timeout', 'POSITIONAL_OR_KEYWORD', 'None'),
                                                ('default_timeout', 'POSITIONAL_OR_KEYWORD', 'None'),
                                                ('ssl_config_file', 'POSITIONAL_OR_KEYWORD', 'None'),
                                                ('default_cacerts', 'POSITIONAL_OR_KEYWORD', 'True')],
                                   'authenticate': [('username', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                                    ('password', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'bufferForRead': [],
                                   'close': [],
                                   'flush': [],
                                   'getBoolean': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'getBoolen': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'getRawBytes': [],
                                   'has_ticket': [],
                                   'prependLength': [],
                                   'read': [('type', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readArray': [('type', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readBoolean': [],
                                   'readBooleanArray': [],
                                   'readByte': [],
                                   'readByteArray': [],
                                   'readByteArrayContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readByteRaw': [],
                                   'readDate': [],
                                   'readDateArray': [],
                                   'readDecimal': [],
                                   'readDecimalArray': [],
                                   'readFloat64': [],
                                   'readFloat64Array': [],
                                   'readFloat64ArrayContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readGeography': [],
                                   'readGeographyArray': [],
                                   'readGeographyPoint': [],
                                   'readGeographyPointArray': [],
                                   'readInt16': [],
                                   'readInt16Array': [],
                                   'readInt16ArrayContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readInt32': [],
                                   'readInt32Array': [],
                                   'readInt32ArrayContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readInt64': [],
                                   'readInt64Array': [],
                                   'readInt64ArrayContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readMoney': [],
                                   'readNull': [],
                                   'readString': [],
                                   'readStringArray': [],
                                   'readStringContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readTimestamp': [],
                                   'readTimestampArray': [],
                                   'readVarbinary': [],
                                   'readVarbinaryContent': [('cnt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'readWireType': [],
                                   'setInputByteOrder': [('bom', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'size': [],
                                   'write': [('type', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                             ('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeArray': [('type', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                                  ('array', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeBoolean': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeByte': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeDate': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeDecimal': [('num', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeDecimalString': [('num', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeFloat64': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeGeography': [('geo', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeGeographyPoint': [('point', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeInt16': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeInt32': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeInt64': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeNull': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeRawBytes': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeString': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeTimestamp': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeVarbinary': [('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeWireType': [('type', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                                     ('value', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                   'writeWireTypeArray': [('type', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                                          ('array', 'POSITIONAL_OR_KEYWORD', '<required>')]},
                'Geography': {'__init__': [('loops', 'POSITIONAL_OR_KEYWORD', '[]')],
                              'flatten': [('fs', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'formatPoint': [('point', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'geographyFromWKT': [('wkt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'loopSerializedSize': [('loop', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'pointFromWKT': [('wkt', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'pointToWKT': [('point', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'serializedSize': [],
                              'unflatten': [('fs', 'POSITIONAL_OR_KEYWORD', '<required>')]},
                'VoltColumn': {'__init__': [('fser', 'POSITIONAL_OR_KEYWORD', 'None'),
                                            ('type', 'POSITIONAL_OR_KEYWORD', 'None'),
                                            ('name', 'POSITIONAL_OR_KEYWORD', 'None')],
                               'readName': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')],
                               'writeName': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')],
                               'writeType': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')]},
                'VoltException': {'__init__': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                  'deserialize': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')]},
                'VoltProcedure': {'__init__': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                               ('name', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                               ('paramtypes', 'POSITIONAL_OR_KEYWORD', '[]')],
                                  'as_array': [('paramtype', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                               ('param', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                  'call': [('params', 'POSITIONAL_OR_KEYWORD', 'None'),
                                           ('response', 'POSITIONAL_OR_KEYWORD', 'True'),
                                           ('timeout', 'POSITIONAL_OR_KEYWORD', 'None')]},
                'VoltResponse': {'__init__': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')],
                                 'deserialize': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')]},
                'VoltTable': {'__init__': [('fser', 'POSITIONAL_OR_KEYWORD', '<required>')],
                              'readFromSerializer': [],
                              'writeToSerializer': []},
                'XYZPoint': {'__init__': [('x', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                          ('y', 'POSITIONAL_OR_KEYWORD', '<required>'),
                                          ('z', 'POSITIONAL_OR_KEYWORD', '<required>')],
                             'fromGeographyPoint': [('p', 'POSITIONAL_OR_KEYWORD', '<required>')],
                             'toGeographyPoint': [],
                             'toGeogrpahyPoint': []}}


class TestPublicApiSnapshot(unittest.TestCase):
    maxDiff = None  # show the full diff when the surface drifts

    def test_public_api_unchanged(self):
        self.assertEqual(
            snapshot(voltdbclient), EXPECTED_API,
            "The public API of voltdbclient.py changed. If this change is "
            "intentional, regenerate the snapshot:\n"
            "    python3 test_api_snapshot.py --dump\n"
            "and paste the result over EXPECTED_API in this file.")


if __name__ == "__main__":
    if "--dump" in sys.argv:
        import pprint
        prefix = "EXPECTED_API = "
        body = pprint.pformat(snapshot(voltdbclient), sort_dicts=True, width=100)
        # Pad continuation lines by the prefix width so pprint's align-to-bracket
        # layout still lines up once the "EXPECTED_API = " prefix is prepended.
        print(prefix + body.replace("\n", "\n" + " " * len(prefix)))
    else:
        unittest.main()
