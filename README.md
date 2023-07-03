# Redis Stress Test

RediStressTest.java - contains test mothods to create/drop Redis Search indexes and to launch production of reference data and time-series record for bonds, futures, options.

## How to proceed

- test methods expect redis available locally on port 6379
- flush redis database using CLI, use command FLUSHDB. This will wipe everything including indexes.
- create indexes

~~~
mvn test -Dtest="RediStressTest#testCreateIndexAll"
~~~

- generate reference data with respective time-series

~~~
mvn test -Dtest="RediStressTest#testGenerateOption"
~~~

- to read options data

~~~
mvn test -Dtest="RediStressTest#testLoadOptions"
~~~

There are multiple variants of test methods for data loading (per-partes, paralell)