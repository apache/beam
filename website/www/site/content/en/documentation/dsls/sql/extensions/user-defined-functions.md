---
type: languages
title: "Beam SQL extensions: User-defined functions"
aliases: /documentation/dsls/sql/user-defined-functions/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Beam SQL extensions: User-defined functions

If Beam SQL does not have a built-in function to meet your needs, you can create your own **user-defined functions (UDFs)** and **user-defined aggregate functions (UDAFs)** in Java and invoke them in your SQL queries.

There are two primary ways to make your functions available to the SQL engine:

1. **Programmatically**: Registering functions directly in your pipeline code using `SqlTransform.registerUdf()` or `SqlTransform.registerUdaf()`.
2. **Dynamically via JARs**: Packaging functions into a JAR and loading them in the SQL Shell with the `CREATE FUNCTION` DDL statement. This is the recommended approach for interactive environments.


## Create and specify a User Defined Function (UDF)

A UDF can be the following:
- Any Java method that takes zero or more scalar fields and
  returns one scalar value.
- A `SerializableFunction`.

Below is an example of UDF and how to use it in DSL:

{{< highlight java >}}
/**
 * A example UDF for test.
 */
public static class CubicInteger implements BeamSqlUdf {
  public static Integer eval(Integer input){
    return input * input * input;
  }
}

/**
 * Another example UDF with {@link SerializableFunction}.
 */
public static class CubicIntegerFn implements SerializableFunction<Integer, Integer> {
  @Override
  public Integer apply(Integer input) {
    return input * input * input;
  }
}

// Define a SQL query which calls the above UDFs
String sql =
    "SELECT f_int, cubic1(f_int), cubic2(f_int)"
      + "FROM PCOLLECTION "
      + "WHERE f_int = 2";

// Create and apply the PTransform representing the query.
// Register the UDFs used in the query by calling '.registerUdf()' with
// either a class which implements BeamSqlUdf or with
// an instance of the SerializableFunction;
PCollection<Row> result =
    input.apply(
        "udfExample",
        SqlTransform
            .query(sql)
            .registerUdf("cubic1", CubicInteger.class)
            .registerUdf("cubic2", new CubicIntegerFn())
{{< /highlight >}}

## Create and specify a User Defined Aggregate Function (UDAF)

Beam SQL can accept a `CombineFn` as UDAF. Registration is similar to the UDF
example above:

{{< highlight java >}}
/**
 * UDAF(CombineFn) for test, which returns the sum of square.
 */
public static class SquareSum extends CombineFn<Integer, Integer, Integer> {
  @Override
  public Integer createAccumulator() {
    return 0;
  }

  @Override
  public Integer addInput(Integer accumulator, Integer input) {
    return accumulator + input * input;
  }

  @Override
  public Integer mergeAccumulators(Iterable<Integer> accumulators) {
    int v = 0;
    Iterator<Integer> ite = accumulators.iterator();
    while (ite.hasNext()) {
      v += ite.next();
    }
    return v;
  }

  @Override
  public Integer extractOutput(Integer accumulator) {
    return accumulator;
  }
}

// Define a SQL query which calls the above UDAF
String sql =
    "SELECT f_int1, squaresum(f_int2) "
      + "FROM PCOLLECTION "
      + "GROUP BY f_int1";

// Create and apply the PTransform representing the query.
// Register the UDAFs used in the query by calling '.registerUdaf()' by
// providing it an instance of the CombineFn
PCollection<Row> result =
    input.apply(
        "udafExample",
        SqlTransform
            .query(sql)
            .registerUdaf("squaresum", new SquareSum()));
{{< /highlight >}}

---
## Creating a UDF JAR for Dynamic Loading
To load functions dynamically, you package them into a JAR file. Beam uses Java's Service Provider Interface (SPI) to discover the functions within the JAR.

### 1. Implement a UdfProvider

The core of this mechanism is the `UdfProvider` interface. You create a public class that implements this interface, which then exposes your UDFs and UDAFs to the Beam SQL engine.

`UdfProvider` has two methods you need to implement:
* `userDefinedScalarFunctions()`: Returns a map of scalar functions, where the key is the SQL function name and the value is an instance of the function class.
* `userDefinedAggregateFunctions()`: Returns a map of aggregate functions.

{{< highlight java >}}

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.udf.AggregateFn;
import org.apache.beam.sdk.extensions.sql.udf.ScalarFn;
import org.apache.beam.sdk.extensions.sql.udf.UdfProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
* A UDF provider that makes functions available to the Beam SQL engine.
* The @AutoService annotation registers this class as a service provider.
*/
@AutoService(UdfProvider.class)
public class UdfTestProvider implements UdfProvider {

    @Override
    public Map < String, ScalarFn > userDefinedScalarFunctions() {
        // Maps the SQL function name (e.g., "increment") to the class instance.
        return ImmutableMap.of(
            "increment", new IncrementFn()
        );
    }

    @Override
    public Map < String, AggregateFn << ? , ? , ? >> userDefinedAggregateFunctions() {
        // Maps the SQL aggregate function name (e.g., "my_sum") to the class instance.
        return ImmutableMap.of("my_sum", new MySum());
    }

    /**
     * A simple UDF that increments a long by 1.
     * The actual function logic is in a method annotated with @ApplyMethod.
     */
    public static class IncrementFn extends ScalarFn {
        @ApplyMethod
        public Long increment(Long i) {
            return i + 1;
        }
    }

    /**
     * A simple UDAF that sums long values.
     */
    public static class MySum implements AggregateFn < Long, Long, Long > {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long addInput(Long accumulator, Long input) {
            return accumulator + input;
        }

        @Override
        public Long mergeAccumulators(Long accumulator, Iterable < Long > accumulators) {
            for (Long x: accumulators) {
                accumulator += x;
            }
            return accumulator;
        }

        @Override
        public Long extractOutput(Long accumulator) {
            return accumulator;
        }
    }
}
{{< /highlight >}}

The `@AutoService(UdfProvider.class)` annotation is from Google's AutoService library. It automatically generates the required `META-INF/services/org.apache.beam.sdk.extensions.sql.udf.UdfProvider` file in your JAR, which allows the SPI mechanism to find your provider class.

### 2. Configure the Maven `pom.xml`

Your project's `pom.xml` must include dependencies for Beam SQL and the AutoService library.

```
<?xml version="1.0"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>my-beam-udfs</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-sdks-java-extensions-sql</artifactId>
      <version>2.55.1</version> <!-- Use a recent Beam version -->
    </dependency>
    <dependency>
      <groupId>com.google.auto.service</groupId>
      <artifactId>auto-service-annotations</artifactId>
      <version>1.1.1</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.8.1</version>
        <configuration>
          <source>1.8</source>
          <target>1.8</target>
          <annotationProcessorPaths>
            <path>
              <groupId>com.google.auto.service</groupId>
              <artifactId>auto-service</artifactId>
              <version>1.1.1</version>
            </path>
          </annotationProcessorPaths>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

### 3. Build the JAR
Navigate to your project's root directory and run the Maven `package` command:

```bash
mvn package
```

This creates the JAR file (e.g., `target/my-beam-udfs-1.0.0.jar`), which is now ready to be used in the SQL Shell.

---
## Using Functions in the SQL Shell with `CREATE FUNCTION`

Once your JAR is built, you can load it using the `CREATE FUNCTION` statement. When the JAR is loaded, Beam SQL uses the `UdfProvider` you created to find and register the functions.

The path provided to `USING JAR` can be a local file path or a path on any distributed filesystem supported by your pipeline's `FileSystems` configuration (e.g., Google Cloud Storage, HDFS).

### Loading a UDF

```
CREATE FUNCTION increment USING JAR 'gs://my-bucket/udfs/my-beam-udfs-1.0.0.jar';

-- Use the function in a query

SELECT increment(0);

-- Returns: 1
```

### Loading a UDAF

```
CREATE AGGREGATE FUNCTION my_sum USING JAR 'gs://my-bucket/udfs/my-beam-udfs-1.0.0.jar';

-- Use the aggregate function in a query

SELECT my_sum(f_long) FROM PCOLLECTION;
```