# cbperf
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
## Randomizer tokens
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
| rand_nickname    | Random string with a concatinated first initial and last name |
| rand_email       | Random email address                                          |
| rand_username    | Random username created from a name and numbers               |
| rand_phone       | Random US style phone number                                  |
| rand_bool        | Random boolean value                                          |
| rand_year        | Random year from 1920 to present                              |
| rand_month       | Random month number                                           |
| rand_day         | Random day number                                             |
| rand_date_1      | Near term random date with slash notation                     |
| rand_date_2      | Near term random date with dash notation                      |
| rand_date_3      | Near term random date with spaces|
|rand_dob_1|Date of Birth with slash notation|
|rand_dob_2|Date of Birth with dash notation|
|rand_dob_3|Date of Birth with spaces|
|rand_image|Random 128x128 pixel JPEG image|
## Options
| Option                           | Description                                                |
|----------------------------------|------------------------------------------------------------|
| -u USER, --user USER             | User Name                                                  |
| -p PASSWORD, --password PASSWORD | User Password                                              |
| -h HOST, --host HOST             | Cluster Node or Domain Name                                |
| -b BUCKET, --bucket BUCKET       | Bucket name (for file mode)                                |
| --tls                            | Enable SSL (default)                                       |
| -e, --external                   | Use external network for clusters with an external network |
| --schema SCHEMA                  | Schema name                                                |
| --count COUNT                    | Record Count                                               |
|--threads THREADS| Manually specify run threads/processes                     |
|--memquota MEMQUOTA| Manaully specift bucket memory quota                       |
|--file FILE| File mode schema JSON file                                 |
|--inventory INVENTORY| Location of alternate custom inventory JSON file           |
|--id ID| ID field (for file mode)                                   |
|--ramp| Run Ramp Style Test                                        |
|--sync| Use Multithreaded Synchronous Mode (instead of async)      |
|--noinit| Skip init phase                                            |
|--skipbucket| Use preexisting bucket                                     |
|--skiprules| Do not run rules if defined                                |
