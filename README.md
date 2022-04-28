# cbperf
Utility for loading data into Couchbase and running performance tests.

Runs on any POSIX style client such as macOS and Linux.

## Prerequisites
- Python 3
- Homebrew (for macOS)

## Quick Start
Setup Python environment:
````
$ cd cbperf
$ ./setup.sh
````
Load 1,000 records of data using the default schema:
````
$ ./cb_perf load --host couchbase.example.com --count 1000
````
Load data and run the standard set of tests using the user profile demo schema:
````
$ ./cb_perf run --host couchbase.example.com --count 1000 --schema profile_demo
````
List information about a Couchbase cluster:
````
$ ./cb_perf list --host couchbase.example.com -u developer -p password
````
List detailed information about a Couchbase cluster including health information:
````
$ ./cb_perf list --host couchbase.example.com --ping -u developer -p password
````
List available schemas:
````
$ ./cb_perf schema --list
````
