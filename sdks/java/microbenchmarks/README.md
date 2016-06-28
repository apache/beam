# Microbenchmarks for parts of the Beam SDK

To run benchmarks:

 1. Run `mvn install` in the top directory to install the SDK.

 2. Build the benchmark package:

        cd microbenchmarks
        mvn package

 3. run benchmark harness:

        java -jar target/microbenchmarks.jar

 4. (alternate to step 3)
    to run just a subset of benchmarks, pass a regular expression that
    matches the benchmarks you want to run (this can match against the class
    name, or the method name).  E.g., to run any benchmarks with
    "DoFnReflector" in the name:

        java -jar target/microbenchmarks.jar ".*DoFnReflector.*"

