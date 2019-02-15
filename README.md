# Insight-DE-2019A-Project


Revieweraser
=============

[![BSD3 license](https://img.shields.io/badge/license-BSD3-blue.svg)](https://github.com/ima-hima/Insight-DE-2019A-Project
/blob/master/LICENSE)


Python code to parse EDGAR output and create new log files containing summary.

**Project status:** 1.0 release

| Directory           | Description of Contents
|:------------------- |:---------------------------------------- |
| `src`               | main code base                           |
| `run.sh`            | shell script to run source               |
| `insight_testsuite` | test suites and test data                |
| `input`             | where EDGAR files will reside            |
| `output`            | log files will be written                |


####Approximate pseudocode

    ip_lookup -> dictionary of last query time -> set(ip)
    log_table -> dictionary of ip -> [original_query_time, num_documents]
    prev_time -> previous value of query time in log file; not value of last query

    interval  <- interval from file
    prev_time <- minimum time available in datetime

    for each line in log, check current_time:
        if current_time's changed:
            for log_item in ip_lookup for each where query_time in
                [prev_time + 1 - interval, current_time - interval]:
                    for log_item in log_table, sorted by original_query_time:
                        print log_item
                    delete log_table[log_item]
                delete ip_lookup[current_time - interval]
            change prev_time to current_time
        if line[ip] exists in log_table:    ### this means we're still inside interval
            update log_table[ip][num_documents] += 1
            delete ip_lookup[current_time - interval + 1][ip]
            add    ip_lookup[current_time][ip]
        else:    ### line[ip] doesn't exist
            add ip_lookup[current_time]            = ip
            add log_table[ip][original_query_time] = current_time
            add log_table[ip][num_documents]       = 1

    for log_item in log_table, sorted by original_query_time:
        print log_item, final_time = current_time

#### Test cases in my-tests

1. time has expired but ip doesn't appear again
1. change in time > 1
1. queries are still active at end of log
1. other inactivity periods


#### TODOs

1. Could be more modular
1. The data structure is a little complicated. Making it an object would be nice.
1. Not currently using Pythonic EAFP coding style vs. LBYL.
1. Dictionary comprehension and row printing could be a little prettier.

