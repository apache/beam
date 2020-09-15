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

If Beam SQL does not have a scalar function or aggregate function to meet your
needs, they can be authored in Java and invoked in your SQL query. These
are commonly called UDF (for scalar functions) and UDAFs (for aggregate functions).

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
      + "GROUP BY f_int2";

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

