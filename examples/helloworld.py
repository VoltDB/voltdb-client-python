#!/usr/bin/env python3

# This file is part of VoltDB.
# Copyright (C) 2008-2021 VoltDB Inc.
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

from voltdbclient import *

client = FastSerializer("localhost", 21212)
proc = VoltProcedure( client, "Insert", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING] )

proc.call([ "English", "Hello", "World" ])
proc.call([ "French" , "Bonjour", "Monde" ])
proc.call([ "Spanish", "Hola", "Mundo" ])
proc.call([ "Danish", "Hej", "Verden" ])
proc.call([ "Italian", "Ciao", "Mondo" ])

proc = VoltProcedure( client, "Select", [FastSerializer.VOLTTYPE_STRING])
response = proc.call( ["Spanish"] )

for x in response.tables:
    print(x)
