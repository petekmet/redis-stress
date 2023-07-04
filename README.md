# Redis Stress Test

RediStressTest.java - contains test mothods to create/drop Redis Search indexes and to launch production of reference data and time-series record for bonds, futures, options.

Actual subject of the test is to execute search query and fetch large result set as fast as possible.

## Prerequisities
JDK 17
Maven 3.6

## How to proceed

- test methods expect redis available locally on port 6379
- flush redis database using CLI, use command FLUSHDB. This will wipe everything including indexes.
- create indexes

~~~
mvn test -Dtest="RediStressTest#testCreateOptionIndex"
~~~

## Generate data

Using command below generate random data with respective time-series according desired model. It will generate 200 000 + 20 800 000 keys with respective hashmaps.

~~~
mvn test -Dtest="RediStressTest#testGenerateOption"
~~~

## Read options data

Subject of the stress test. Measured is time to read-out all entity instances (200 000 + 200 000) according the query in testLoadOptions method:

~~~
mvn test -Dtest="RediStressTest#testLoadOptions"
~~~
