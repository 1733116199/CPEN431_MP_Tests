# CPEN 431 MP Test Client

**This is the test client runs some (not all) tests for Milestone 3**.

We recommend that you first build a fatJar (for simplicity) using `gradle shadowJar`. This will create a large JAR file with all the dependencies. You can then use this JAR file on a GCP instance to run tests. The JAR file will be named based on the root director. For instance, if you clone the test client repo into the directory `CPEN431_MP_Tests` then the JAR file will be named `CPEN431_MP_Tests-1.0-SNAPSHOT-all.jar` and will be in the `build/libs` directory.

To run the tests, you can copy the JAR file wherever you want and use the following command for running Milestone 3 tests:

```
java -jar CPEN431_MP_Tests-1.0-SNAPSHOT-all.jar servers.txt
```

The results of running the tests will appear in a file named `milestone3.log`.

You should run a deployment of 35+ server nodes for these tests.

