# CBPerf 2.0
Utility for loading data into Couchbase and running performance tests. Includes the ability to load randomized data based on a template.

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
$ ./cb_perf load --host couchbase.example.com --count 1000 --schema default
````
Load data from a test file:
````
$ cat data/data_file.txt | bin/cb_perf load --host couchbase.example.com -b bucket
````
Export data from a bucket to CSV (default output file location is $HOME)
````
$ bin/cb_perf export --host couchbase.example.com -b sample_app
````
Get a document from a bucket using the key:
````
$ bin/cb_perf get --host couchbase.example.com -b employees -k employees:1
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
$ ./cb_perf schema
````
## Randomizer tokens
Note: Except for the US States the random data generated may not be valid. For example the first four digits of the random credit card may not represent a valid financial institution. The intent is to simulate real data. Any similarities to real data is purely coincidental.  

| Token            | Description                                                   |
|------------------|---------------------------------------------------------------|
| date_time        | Data/time string in form %Y-%m-%d %H:%M:%S                    |
| rand_credit_card | Random credit card format number                              |
| rand_ssn         | Random US Social Security format number                       |
| rand_four        | Random four digits                                            |
| rand_account     | Random 10 digit number                                        |
| rand_id          | Random 16 digit number                                        |
| rand_zip_code    | Random US Zip Code format number                              |
| rand_dollar      | Random dollar amount                                          |
| rand_hash        | Random 16 character alphanumeric string                       |
| rand_address     | Random street address                                         |
| rand_city        | Random city name                                              |
| rand_state       | Random US State name                                          |
| rand_first       | Random first name                                             |
| rand_last        | Random last name                                              |
| rand_nickname    | Random string with a concatenated first initial and last name |
| rand_email       | Random email address                                          |
| rand_username    | Random username created from a name and numbers               |
| rand_phone       | Random US style phone number                                  |
| rand_bool        | Random boolean value                                          |
| rand_year        | Random year from 1920 to present                              |
| rand_month       | Random month number                                           |
| rand_day         | Random day number                                             |
| rand_date_1      | Near term random date with slash notation                     |
| rand_date_2      | Near term random date with dash notation                      |
| rand_date_3      | Near term random date with spaces                             |
| rand_dob_1       | Date of Birth with slash notation                             |
| rand_dob_2       | Date of Birth with dash notation                              |
| rand_dob_3       | Date of Birth with spaces                                     |
| rand_image       | Random 128x128 pixel JPEG image                               |
## Options
Usage: cb_perf command options

| Command | Description               |
|---------|---------------------------|
| load    | Load data                 |
| get     | Get data                  |
| list    | List cluster information  |
| export  | Export data               |
| clean   | Remove buckets            |
| schema  | Schema management options |

| Option                                 | Description                                                |
|----------------------------------------|------------------------------------------------------------|
| -u USER, --user USER                   | User Name                                                  |
| -p PASSWORD, --password PASSWORD       | User Password                                              |
| -h HOST, --host HOST                   | Cluster Node or Domain Name                                |
| -b BUCKET, --bucket BUCKET             | Bucket name                                                |
| -s SCOPE, --scope SCOPE                | Scope name                                                 |
| -c COLLECTION, --collection COLLECTION | Collection name                                            |
| -k KEY, --key KEY                      | Key name or pattern                                        |
| -d DATA, --data DATA                   | Data to import                                             |
| --tls                                  | Enable SSL (default)                                       |
| -e, --external                         | Use external network for clusters with an external network |
| --schema SCHEMA                        | Schema name                                                |
| --count COUNT                          | Record Count                                               |
| --file FILE                            | File mode schema JSON file                                 |
| --id ID                                | ID field (for file mode)                                   |
| --directory DIRECTORY                  | Directory for export operations                            |
