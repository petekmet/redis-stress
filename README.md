# Redis Stress Test

RediStressTest.java - contains test mothods to create/drop Redis Search indexes and to launch production of reference data and time-series record for bonds, futures, options.

Example:
~~~
mvn test -Dtest="RediStressTest#testGenerateOption
~~~