#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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
import time
import random
import threading
from voltdbclient import *

# main method of voter client
def main():
    # prints required command line arguments if these were not passed in correctly
    if len(sys.argv) < 8:
        print "ClientVoter [number of contestants] [votes per phone number] [transactions per second] [client feedback interval (seconds)] [test duration (seconds)] [lag record delay (seconds)] [server list (comma separated)]"
        exit(1)

    # checks for validity of 1st command line argument
    # NOTE: 0th command line argument is the file name of this python program
    global max_contestant
    max_contestant = int(sys.argv[1])
    if max_contestant < 1 or max_contestant > 12:
        print "Number of contestants must be between 1 and 12"
        exit(1)

    # sets up global variables, including:
    global results_lock             # a lock for manipulating result data
    global params_lock              # a lock for manipulating the parameter list
    global requestPermit            # control rate at which requests are generated
    global availableThreads         # Allow a thread to indicate it is available to service a request
    global shouldContinue
    global min_execution_secs       # minimum number of seconds used to execute stored procedure
    global max_execution_secs       # maximum number of seconds used to execute stored procedure
    global tot_execution_secs       # total number of seconds used to execute stored procedures
    global tot_executions           # total number of executed stored procedures
    global tot_executions_latency   # total number of executed stored procedures as a measure for latency
    global check_latency            # boolean value: if false, latency is not yet being measures; if true, latency is being measured
    global latency_counter          # array used to show how many stored procedures fell into various time ranges for latency (e.g.: 200 stored procedures had latency in range between 25 and 50 milliseconds
    global vote_result_counter      # array used to show how many votes got (0) Accepted, (1) Rejected due to invalid contestant, (2) Rejected due to voter being over the limit (of phone calls)
    global invocation_params
    results_lock = threading.Lock()
    params_lock = threading.Lock()
    requestPermit = threading.Semaphore(0)
    availableThreads = threading.Semaphore(0)
    invocation_params = []
    shouldContinue = True
    min_execution_secs = 999999
    max_execution_secs = -1
    tot_execution_secs = 0
    tot_executions = 0
    tot_executions_latency = 0
    check_latency = False
    latency_counter = [0, 0, 0, 0, 0, 0, 0, 0, 0]
    vote_result_counter = [0, 0, 0]

    # assigns values to other variables using command line arguments and creativity
    max_votes_per_phone_number = long(sys.argv[2])
    transactions_per_sec = long(sys.argv[3])
    transactions_per_milli = transactions_per_sec / float(1000) # uses millis, not secs
    client_feedback_interval_secs = long(sys.argv[4])
    test_duration_secs = long(sys.argv[5])
    lag_latency_secs = long(sys.argv[6])
    server_list = sys.argv[7]
    this_outstanding = 0
    last_outstanding = 0
    contestant_names = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine Nygren,Tania Mattioli"

    print "Allowing %d votes per phone number" % max_votes_per_phone_number
    print "Submitting %d SP calls/sec" % transactions_per_sec
    print "Feedback interval = %d second(s)" % client_feedback_interval_secs
    print "Running for %d second(s)" % test_duration_secs
    print "Latency not recorded for %d second(s)" % lag_latency_secs

    phone_number = None
    contestant_number = None
    transactions_this_sec = 0
    last_milli = time.time() * 1000 # uses millis, not secs
    this_milli = time.time() * 1000 # uses millis, not secs

    # parses the list of servers specified at command line and creates corresponding URL for each, adding these to the dictionary
    volt_servers = server_list.rsplit(",")

    # invokes the stored procedure 'Initialize' to set up database with contestant names/numbers
    # uses quick parse hack to process the response of the invocation
    # contestant names/numbers entered into database if this is the first client to connect; otherwise, existing configuration info retrieved
    client = FastSerializer(volt_servers[0])
    initprocedure = VoltProcedure( client, "Initialize", [ FastSerializer.VOLTTYPE_INTEGER, FastSerializer.VOLTTYPE_STRING ])

    response = initprocedure.call( [max_contestant, contestant_names ] )

    # sets up start and end times of the voting process (and of latency measurements) based on command line-specified duration and delay values
    start_time = time.time()
    end_time = start_time + test_duration_secs
    current_time = start_time
    last_feedback_time = start_time
    num_sp_calls = 0
    start_recording_latency = start_time + lag_latency_secs

    thread_list = []

    for x in range(5):
        thread = doQueries(volt_servers[x % len(volt_servers)])
        thread.setDaemon(True)
        thread.start()

    # main while loop of voter client, used to invoke stored procedure 'Vote' repeatedly
    while end_time > current_time:
        availableThreads.acquire()
        num_sp_calls = num_sp_calls + 1

        # generates random 10-digit 'phone number' and not entirely random contestant number
        # the contestant number (as generated below) is most likely to be 2
        # NOTE: every 100 votes, the contestant number is made to be potentially invalid
        phone_number = random.randint(1000000000, 9999999999)
        contestant_number = (int(random.random() * max_contestant) * int(random.random() * max_contestant)) % max_contestant + 1
        if num_sp_calls % 100 == 0:
            contestant_number = (int(random.random() * max_contestant) + 1) * 2

        params_lock.acquire()
        invocation_params.append([ phone_number, contestant_number ])
        params_lock.release()
        requestPermit.release()

        # if more votes per second are happening than the command line argument allows: waits until enough time has passed to resume voting
        # this block uses millis, not secs
        transactions_this_sec = transactions_this_sec + 1
        if transactions_this_sec >= transactions_per_milli:
            this_milli = time.time() * 1000
            while this_milli <= last_milli:
                this_milli = time.time() * 1000
                time.sleep(0) #yield to other threads
            last_milli = this_milli
            transactions_this_sec = 0

        current_time = time.time()

        if not check_latency and current_time >= start_recording_latency:
            check_latency = True

        # if enough time has passed since last status report: reports current voting status (prints some data to console)
        if current_time >= (last_feedback_time + client_feedback_interval_secs):
            elapsed_time_secs_2 = time.time() - start_time
            last_feedback_time = current_time
            run_time_secs = end_time - start_time
            if tot_executions_latency == 0:
                tot_executions_latency = 1

            percent_complete = (float(elapsed_time_secs_2) / float(run_time_secs)) * 100
            if percent_complete > 100:
                percent_complete = 100

            # lock necessary, because global variables manipulated in this block may also be used by other threads (those responsible for invoking stored procedure 'Vote')
            # execution times are printed in millis, not secs
            results_lock.acquire()
            this_outstanding = num_sp_calls - tot_executions
            avg_latency = float(tot_execution_secs) * 1000 / float(tot_executions_latency)
            print "%f%% Complete | SP Calls: %d at %f SP/sec | outstanding = %d (%d) | min = %d | max = %d | avg = %f" % (percent_complete, num_sp_calls, (float(num_sp_calls) / float(elapsed_time_secs_2)), this_outstanding, (this_outstanding - last_outstanding), (min_execution_secs * 1000), (max_execution_secs * 1000), avg_latency)
            last_outstanding = this_outstanding
            results_lock.release()
    shouldContinue = False
    # joins outstanding threads (those responsible for invoking stored procedure 'Vote')
    for thread in thread_list:
        if thread.isAlive():
            thread.join()

    elapsed_time_secs = time.time() - start_time

    # prints statistics about the numbers of accepted/rejected votes
    print
    print "****************************************************************************"
    print "Voting Results"
    print "****************************************************************************"
    print " - Accepted votes = %d" % vote_result_counter[0]
    print " - Rejected votes (invalid contestant) = %d" % vote_result_counter[1]
    print " - Rejected votes (voter over limit) = %d" % vote_result_counter[2]
    print

    winner_name = "<<UNKNOWN>>"
    winner_votes = -1

    # invokes the stored procedure 'Results' to retrieve all stored tuples in database
    # uses quick parse hack to process the response of the invocation
    # analyzes the processed data to determine number of votes per contestant, winner, and number of votes for winner
    resultsprocedure = VoltProcedure( client, "Results", [])
    response = resultsprocedure.call([])
    table = response.tables[0]
    if len(table.tuples) == 0:
        print " - No results to report."
    else:
        for row in table.tuples:
            result_name = row[0]
            result_votes = row[2]
            print " - Contestant %s received %d vote(s)" % (result_name, result_votes)

            if result_votes > winner_votes:
                winner_votes = result_votes
                winner_name = result_name

    # prints winner data
    # prints statistics about average latency and distribution of stored procedures across ranges in latency
    print
    print " - Contestant %s was the winner with %d vote(s)" % (winner_name, winner_votes)
    print
    print "****************************************************************************"
    print "System Statistics"
    print "****************************************************************************"
    print " - Ran for %f second(s)" % elapsed_time_secs
    print " - Performed %d Stored Procedure call(s)" % num_sp_calls
    print " - At %f call(s) per second" % (num_sp_calls / elapsed_time_secs)
    print " - Average Latency = %f ms" % (float(tot_execution_secs) * 1000 / float(tot_executions_latency))
    print " - Latency   0ms -  25ms = %d" % latency_counter[0]
    print " - Latency  25ms -  50ms = %d" % latency_counter[1]
    print " - Latency  50ms -  75ms = %d" % latency_counter[2]
    print " - Latency  75ms - 100ms = %d" % latency_counter[3]
    print " - Latency 100ms - 125ms = %d" % latency_counter[4]
    print " - Latency 125ms - 150ms = %d" % latency_counter[5]
    print " - Latency 150ms - 175ms = %d" % latency_counter[6]
    print " - Latency 175ms - 200ms = %d" % latency_counter[7]
    print " - Latency 200ms+        = %d" % latency_counter[8]

# class, whose objects run in separate threads
# responsible for invoking stored procedure 'Vote' and processing results (updating statistics)
class doQueries(threading.Thread):
    # accepts url used to connect to server as parameter
    # accepts data used to invoke stored procedure 'Vote' as parameter
    def __init__ (self, server):
        threading.Thread.__init__(self)
        self.client = FastSerializer( server )
        self.proc = VoltProcedure( self.client, "Vote", [ FastSerializer.VOLTTYPE_BIGINT, FastSerializer.VOLTTYPE_TINYINT, FastSerializer.VOLTTYPE_BIGINT ])

    # the method that gets called when this new thread is started
    def run(self):
        global vote_result_counter
        global tot_executions
        global tot_executions_latency
        global tot_execution_secs
        global check_latency
        global latency_counter
        global min_execution_secs
        global max_execution_secs
        global requestPermit
        global shouldContinue
        global max_contestant
        global results_lock
        global params_lock
        global availableThreads
        global invocation_params
        num_sp_calls = 0
        while shouldContinue:
            #coordinate so that threads can always block on either semaphores or a request, indicate availability and then wait for a permit
            #to generate a request
            availableThreads.release();
            requestPermit.acquire()
            num_sp_calls = num_sp_calls + 1

            params_lock.acquire();
            params = invocation_params.pop()
            params_lock.release()

            # invokes the stored procedure 'Vote' to cast a vote for some contestant by voter with some phone number
            # times the invocation
            # uses quick parse hack to process the response of the invocation
            # analyzes the processed data to determine if invocation was successful (if not, exits)
            time_before = time.time()
            response = self.proc.call( [params[0], params[1], max_contestant] )
            time_after = time.time()
            if response.status != 1:
                print "Failed to execute!!!"
                exit(-1)
            else:
                # updates statistics about execution count, execution times, vote successes/failures, and latency
                # lock necessary, because any other thread may need to access the same global variables, which could result in inconsistencies without proper thread safety
                results_lock.acquire()
                tot_executions = tot_executions + 1
                table = response.tables[0]
                vote_result = table.tuples[0][0]
                vote_result_counter[vote_result] = vote_result_counter[vote_result] + 1
                if check_latency:
                    execution_time = time_after - time_before
                    tot_executions_latency = tot_executions_latency + 1
                    tot_execution_secs = tot_execution_secs + execution_time

                    if execution_time < min_execution_secs:
                        min_execution_secs = execution_time
                    if execution_time > max_execution_secs:
                        max_execution_secs = execution_time

                    latency_bucket = int(execution_time * 40)
                    if latency_bucket > 8:
                        latency_bucket = 8
                    latency_counter[latency_bucket] = latency_counter[latency_bucket] + 1
                results_lock.release()

# used to call main method of voter client
if __name__ == "__main__":
    main()
