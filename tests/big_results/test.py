#!/usr/bin/env python2.6

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.getcwd())))
from voltdbclient import *

# Globals
# Set profile to True to enable profiling.
class G:
    profile = False
    sqlcmd_results_file = 'results-sqlcmd.txt'
    results = None
    sqlcmd_tuples = []

class Timer(object):
    def __init__(self, tag):
        self.tag = tag
    def __enter__(self):
        self.t = time.time()
    def __exit__(self, *args, **kwargs):
        print '%s time: %.2f seconds' % (self.tag, time.time() - self.t)

def run_sqlcmd():
    os.system('sqlcmd --query="exec BigResults" > %s' % G.sqlcmd_results_file)
    with open(G.sqlcmd_results_file) as f:
        for line in f:
            if line.startswith('ZZZ'):
                fields = line.split()
                G.sqlcmd_tuples.append([fields[0], int(fields[1]), int(fields[2])])

def run_pythonclient():
    G.results = VoltProcedure(FastSerializer('localhost'), "BigResults", []).call([])

with Timer('sqlcmd'):
    run_sqlcmd()

with Timer('pythonclient'):
    if G.profile:
        import cProfile as profile
        import pstats
        profile.run('run_pythonclient()', 'stats')
        p = pstats.Stats('stats')
        p.strip_dirs().sort_stats('time').print_stats(10)
    else:
        run_pythonclient()
    nerrors = 0
    ituple = 0
    for tuple in G.results.tables[0].tuples:
        if tuple != G.sqlcmd_tuples[ituple]:
            if nerrors < 10:
                print 'Mismatch on tuple %d: %s != %s' % (ituple+1, tuple, G.sqlcmd_tuples[ituple])
            nerrors += 1
        ituple += 1
    print 'tuple errors: %d' % nerrors
