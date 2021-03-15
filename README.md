# CPEN 431 MP Test Client

We recommend that you first build a fatJar (for simplicity) using `gradle shadowJar`. This will create a large JAR file with all the dependencies. You can then use this JAR file on a GCP instance to run tests. The JAR file will be named based on the root director. For instance, if you clone the test client repo into the directory `CPEN431_MP_Tests` then the JAR file will be named `CPEN431_MP_Tests-1.0-SNAPSHOT-all.jar` and will be in the `build/libs` directory.

To run the tests, you can copy the JAR file wherever you want and use the following command for running Milestone 1 tests:

`java -cp  CPEN431_MP_Tests-1.0-SNAPSHOT-all.jar cpen431.mp.Tests.TestDriver 1 servers.txt 123456` where the `1` is for Milestone 1 and `servers.txt` contains a list of some DHT servers and `123456` is a student id (not critical for Milestone 1 or Milestone 2).

If you want to run some tests for Milestone 2, replace the argument `1` with the argument `2`.

