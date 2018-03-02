---
layout: section
title: "Beam DSLs: SQL"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/
---

# Beam SQL

This page describes the implementation of Beam SQL, and how to simplify a Beam pipeline with DSL APIs.

## 1. Overview {#overview}

SQL is a well-adopted standard to process data with concise syntax. With DSL APIs (currently available only in Java), now `PCollections` can be queried with standard SQL statements, like a regular table. The DSL APIs leverage [Apache Calcite](http://calcite.apache.org/) to parse and optimize SQL queries, then translate into a composite Beam `PTransform`. In this way, both SQL and normal Beam `PTransforms` can be mixed in the same pipeline.

There are two main pieces to the SQL DSL API:

* [BeamSql]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/BeamSql.html): the interface for creating `PTransforms` from SQL queries;
* [Row]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/values/Row.html) contains named columns with corresponding data types. Beam SQL queries can be made only against collections of type `PCollection<Row>`;

We'll look at each of these below.

## 2. Usage of DSL APIs {#usage}

### Row

Before applying a SQL query to a `PCollection`, the data in the collection must be in `Row` format. A `Row` represents a single, immutable record in a Beam SQL `PCollection`. The names and types of the fields/columns in the row are defined by its associated [RowType]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/values/RowType.html).
For SQL queries, you should use the [RowSqlType.builder()]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/RowSqlType.html) to create `RowTypes`, it allows creating schemas with all supported SQL types (see [Data Types](#data-types) for more details on supported primitive data types).


A `PCollection<Row>` can be obtained multiple ways, for example:

  * **From in-memory data** (typically for unit testing).

    **Note:** you have to explicitly specify the `Row` coder. In this example we're doing it by calling `Create.of(..).withCoder()`:

    ```java
    // Define the record type (i.e., schema).
    RowType appType = 
        RowSqlType
          .builder()
          .withIntegerField("appId")
          .withVarcharField("description")
          .withTimestampField("rowtime")
          .build();

    // Create a concrete row with that type.
    Row row = 
        Row
          .withRowType(appType)
          .addValues(1, "Some cool app", new Date())
          .build();

    // Create a source PCollection containing only that row
    PCollection<Row> testApps = 
        PBegin
          .in(p)
          .apply(Create
                    .of(row)
                    .withCoder(appType.getRowCoder()));
    ```
  * **From a `PCollection<T>` of records of some other type**  (i.e.  `T` is not already a `Row`), by applying a `ParDo` that converts input records to `Row` format.

    **Note:** you have to manually set the coder of the result by calling `setCoder(appType.getRowCoder())`:
    ```java
    // An example POJO class.
    class AppPojo {
      Integer appId;
      String description;
      Date timestamp;
    }

    // Acquire a collection of POJOs somehow.
    PCollection<AppPojo> pojos = ...

    // Convert them to Rows with the same schema as defined above via a DoFn.
    PCollection<Row> apps = pojos
      .apply(
          ParDo.of(new DoFn<AppPojo, Row>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              // Get the current POJO instance
              AppPojo pojo = c.element();

              // Create a Row with the appType schema 
              // and values from the current POJO
              Row appRow = 
                    Row
                      .withRowType(appType)
                      .addValues(
                        pojo.appId, 
                        pojo.description, 
                        pojo.timestamp)
                      .build();

              // Output the Row representing the current POJO
              c.output(appRow);
            }
          }))
      .setCoder(appType.getRowCoder());
    ```

  * **As an output of another `BeamSql` query**. Details in the next section.

Once you have a `PCollection<Row>` in hand, you may use the `BeamSql` APIs to apply SQL queries to it.

### BeamSql

`BeamSql.query(queryString)` method is the only API to create a `PTransform` from a string representation of the SQL query. You can apply this `PTransform` to either a single `PCollection` or a `PCollectionTuple` which holds multiple `PCollections`:

  * when applying to a single `PCollection` it can be referenced via the table name `PCOLLECTION` in the query:
    ```java
    PCollection<Row> filteredNames = testApps.apply(
        BeamSql.query(
          "SELECT appId, description, rowtime "
            + "FROM PCOLLECTION "
            + "WHERE id=1"));
    ```
  * when applying to a `PCollectionTuple`, the tuple tag for each `PCollection` in the tuple defines the table name that may be used to query it. Note that table names are bound to the specific `PCollectionTuple`, and thus are only valid in the context of queries applied to it.  

    For example, you can join two `PCollections`:  
    ```java
    // Create the schema for reviews
    RowType reviewType = 
        RowSqlType.
          .withIntegerField("appId")
          .withIntegerField("reviewerId")
          .withFloatField("rating")
          .withTimestampField("rowtime")
          .build();
    
    // Obtain the reviews records with this schema
    PCollection<Row> reviewsRows = ...

    // Create a PCollectionTuple containing both PCollections.
    // TupleTags IDs will be used as table names in the SQL query
    PCollectionTuple namesAndFoods = PCollectionTuple.of(
        new TupleTag<>("Apps"), appsRows), // appsRows from the previous example
        new TupleTag<>("Reviews"), reviewsRows));

    // Compute the total number of reviews 
    // and average rating per app 
    // by joining two PCollections
    PCollection<Row> output = namesAndFoods.apply(
        BeamSql.query(
            "SELECT Names.appId, COUNT(Reviews.rating), AVG(Reviews.rating)"
                + "FROM Apps INNER JOIN Reviews ON Apps.appId == Reviews.appId"));
    ```

[BeamSqlExample](https://github.com/apache/beam/blob/master/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/example/BeamSqlExample.java) in the code repository shows basic usage of both APIs.

## 3. Functionality in Beam SQL {#functionality}
Just as the unified model for both bounded and unbounded data in Beam, SQL DSL provides the same functionalities for bounded and unbounded `PCollection` as well. Here's the supported SQL grammar supported in [BNF](http://en.wikipedia.org/wiki/Backus%E2%80%93Naur_Form)-like form. An `UnsupportedOperationException` is thrown for unsupported features.

```
query:
	{
          select
      |   query UNION [ ALL ] query
      |   query MINUS [ ALL ] query
      |   query INTERSECT [ ALL ] query
	}
    [ ORDER BY orderItem [, orderItem ]* LIMIT count [OFFSET offset] ]

orderItem:
      expression [ ASC | DESC ]

select:
      SELECT
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ ( LEFT | RIGHT ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]

booleanExpression:
    expression [ IS NULL | IS NOT NULL ]
  | expression [ > | >= | = | < | <= | <> ] expression
  | booleanExpression [ AND | OR ] booleanExpression
  | NOT booleanExpression
  | '(' booleanExpression ')'

joinCondition:
      ON booleanExpression

tableReference:
      tableName [ [ AS ] alias ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' expression [, expression ]* ')'
  |   HOP '(' expression [, expression ]* ')'
  |   TUMBLE '(' expression [, expression ]* ')'
  |   SESSION '(' expression [, expression ]* ')'

```

### 3.1. Supported Features {#features}

#### 3.1.1 Aggregations {#features-aggregations}

Major standard aggregation functions are supported:
 * `COUNT`
 * `MAX`
 * `MIN`
 * `SUM`
 * `AVG`
 * `VAR_POP`
 * `VAR_SAMP`
 * `COVAR_POP`
 * `COVAR_SAMP`

**Note:** `DISTINCT` aggregation is not supported yet.

#### 3.1.2 Windowing {#features-windowing}

Beam SQL supports windowing functions specified in `GROUP BY` clause. `TIMESTAMP` field is required in this case. It is used as event timestamp for rows. 

Supported windowing functions:
* `TUMBLE`, or fixed windows. Example of how define a fixed window with duration of 1 hour:
``` 
    SELECT f_int, COUNT(*) 
    FROM PCOLLECTION 
    GROUP BY 
      f_int,
      TUMBLE(f_timestamp, INTERVAL '1' HOUR)
```
* `HOP`, or sliding windows. Example of how to define a sliding windows for every 30 minutes with 1 hour duration:
```
    SELECT f_int, COUNT(*)
    FROM PCOLLECTION 
    GROUP BY 
      f_int, 
      HOP(f_timestamp, INTERVAL '30' MINUTE, INTERVAL '1' HOUR)
```
* `SESSION`, session windows. Example of how to define a session window with 5 minutes gap duration:
```
    SELECT f_int, COUNT(*) 
    FROM PCOLLECTION 
    GROUP BY 
      f_int, 
      SESSION(f_timestamp, INTERVAL '5' MINUTE)
```

**Note:** when no windowing function is specified in the query, then windowing strategy of the input `PCollections` is unchanged by the SQL query. If windowing function is specified in the query, then the windowing function of the `PCollection` is updated accordingly, but trigger stays unchanged.


#### 3.1.3 Joins {#features-joins}

#### 3.1.3.1 Overview

Supported `JOIN` types in Beam SQL:
* `INNER`, `LEFT OUTER`, `RIGHT OUTER`;
* Only equijoins (where join condition is an equality check) are supported.

Unsupported `JOIN` types in Beam SQL:
* `CROSS JOIN` is not supported (full cartesian product with no `ON` clause);
* `FULL OUTER JOIN` is not supported (combination of `LEFT OUTER` and `RIGHT OUTER` joins);

The scenarios of join can be categorized into 3 cases:

1. Bounded input `JOIN` bounded input;
2. Unbounded input `JOIN` unbounded input;
3. Unbounded input `JOIN` bounded input;

Each of these scenarios is described below:

#### 3.1.3.1 Bounded JOIN Bounded {#join-bounded-bounded}

Standard join implementation is used. All elements from one input are matched with all elements from another input. Due to the fact that both inputs are bounded, no windowing or triggering is involved.

#### 3.1.3.2 Unbounded JOIN Unbounded {#join-unbounded-unbounded}

Standard join implementation is used. All elements from one input are matched with all elements from another input.

**Windowing and Triggering**

Following properties must be satisfied when joining unbounded inputs:
 * inputs must have compatible windows, otherwise `IllegalArgumentException` will be thrown;
 * triggers on each input should only fire once per window. Currently this means that the only supported trigger in this case is `DefaultTrigger` with zero allowed lateness. Using any other trigger will result in `UnsupportedOperationException` thrown;

This means that inputs are joined per-window. That is, when the trigger fires (only once), then join is performed on all elements in the current window in both inputs. This allows to reason about what kind of output is going to be produced.

**Note:** similarly to `GroupByKeys` `JOIN` will update triggers using `Trigger.continuationTrigger()`. Other aspects of the inputs' windowing strategies remain unchanged.

#### 3.1.3.3 Unbounded JOIN Bounded {#join-unbounded-bounded}

For this type of `JOIN` bounded input is treated as a side-input by the implementation.

This means that 

* window/trigger is inherented from upstreams, which should be consistent;


#### 3.1.4 User Defined Function (UDF) and User Defined Aggregate Function (UDAF) {#features-udfs-udafs}

If the required function is not available, developers can register their own UDF(for scalar function) and UDAF(for aggregation function).

##### **3.1.4.1 Create and specify User Defined Function (UDF)**

A UDF can be 1) any Java method that takes zero or more scalar fields and return one scalar value, or 2) a `SerializableFunction`. Below is an example of UDF and how to use it in DSL:

```java
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
PCollection<BeamSqlRow> result =
    input.apply(
        "udfExample",
        BeamSql
            .query(sql)
            .registerUdf("cubic1", CubicInteger.class)
            .registerUdf("cubic2", new CubicIntegerFn())
```

##### **3.1.4.2 Create and specify User Defined Aggregate Function (UDAF)**

Beam SQL can accept a `CombineFn` as UDAF. Registration is similar to the UDF example above:

```java
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
PCollection<BeamSqlRow> result =
    input.apply(
        "udafExample",
        BeamSql
            .query(sql)
            .registerUdaf("squaresum", new SquareSum()));
```

### 3.2. Data Types {#data-types}
Each type in Beam SQL maps to a Java class to holds the value in `Row`. The following table lists the relation between SQL types and Java classes, which are supported in current repository:

| SQL Type | Java class |
| ---- | ---- |
| Types.INTEGER | java.lang.Integer |
| Types.SMALLINT | java.lang.Short |
| Types.TINYINT | java.lang.Byte |
| Types.BIGINT | java.lang.Long |
| Types.FLOAT | java.lang.Float |
| Types.DOUBLE | java.lang.Double |
| Types.DECIMAL | java.math.BigDecimal |
| Types.VARCHAR | java.lang.String |
| Types.TIMESTAMP | java.util.Date |
{:.table}

### 3.3. Built-in SQL functions {#built-in-functions}

Beam SQL has implemented lots of build-in functions defined in [Apache Calcite](http://calcite.apache.org). The available functions are listed as below:

**Comparison functions and operators**

| Operator syntax | Description |
| ---- | ---- |
| value1 = value2 | Equals |
| value1 <> value2 | Not equal |
| value1 > value2 | Greater than |
| value1 >= value2 | Greater than or equal |
| value1 < value2 | Less than |
| value1 <= value2 | Less than or equal |
| value IS NULL | Whether value is null |
| value IS NOT NULL | Whether value is not null |
{:.table}

**Logical functions and operators**

| Operator syntax | Description |
| ---- | ---- |
| boolean1 OR boolean2 | Whether boolean1 is TRUE or boolean2 is TRUE |
| boolean1 AND boolean2 | Whether boolean1 and boolean2 are both TRUE |
| NOT boolean | Whether boolean is not TRUE; returns UNKNOWN if boolean is UNKNOWN |
{:.table}

**Arithmetic functions and operators**

| Operator syntax | Description|
| ---- | ---- |
| numeric1 + numeric2 | Returns numeric1 plus numeric2|
| numeric1 - numeric2 | Returns numeric1 minus numeric2|
| numeric1 * numeric2 | Returns numeric1 multiplied by numeric2|
| numeric1 / numeric2 | Returns numeric1 divided by numeric2|
| MOD(numeric, numeric) | Returns the remainder (modulus) of numeric1 divided by numeric2. The result is negative only if numeric1 is negative|
{:.table}

**Math functions**

| Operator syntax | Description |
| ---- | ---- |
| ABS(numeric) | Returns the absolute value of numeric |
| SQRT(numeric) | Returns the square root of numeric |
| LN(numeric) | Returns the natural logarithm (base e) of numeric |
| LOG10(numeric) | Returns the base 10 logarithm of numeric |
| EXP(numeric) | Returns e raised to the power of numeric |
| ACOS(numeric) | Returns the arc cosine of numeric |
| ASIN(numeric) | Returns the arc sine of numeric |
| ATAN(numeric) | Returns the arc tangent of numeric |
| COT(numeric) | Returns the cotangent of numeric |
| DEGREES(numeric) | Converts numeric from radians to degrees |
| RADIANS(numeric) | Converts numeric from degrees to radians |
| SIGN(numeric) | Returns the signum of numeric |
| SIN(numeric) | Returns the sine of numeric |
| TAN(numeric) | Returns the tangent of numeric |
| ROUND(numeric1, numeric2) | Rounds numeric1 to numeric2 places right to the decimal point |
{:.table}

**Date functions**

| Operator syntax | Description |
| ---- | ---- |
| LOCALTIME | Returns the current date and time in the session time zone in a value of datatype TIME |
| LOCALTIME(precision) | Returns the current date and time in the session time zone in a value of datatype TIME, with precision digits of precision |
| LOCALTIMESTAMP | Returns the current date and time in the session time zone in a value of datatype TIMESTAMP |
| LOCALTIMESTAMP(precision) | Returns the current date and time in the session time zone in a value of datatype TIMESTAMP, with precision digits of precision |
| CURRENT_TIME | Returns the current time in the session time zone, in a value of datatype TIMESTAMP WITH TIME ZONE |
| CURRENT_DATE | Returns the current date in the session time zone, in a value of datatype DATE |
| CURRENT_TIMESTAMP | Returns the current date and time in the session time zone, in a value of datatype TIMESTAMP WITH TIME ZONE |
| EXTRACT(timeUnit FROM datetime) | Extracts and returns the value of a specified datetime field from a datetime value expression |
| FLOOR(datetime TO timeUnit) | Rounds datetime down to timeUnit |
| CEIL(datetime TO timeUnit) | Rounds datetime up to timeUnit |
| YEAR(date) | Equivalent to EXTRACT(YEAR FROM date). Returns an integer. |
| QUARTER(date) | Equivalent to EXTRACT(QUARTER FROM date). Returns an integer between 1 and 4. |
| MONTH(date) | Equivalent to EXTRACT(MONTH FROM date). Returns an integer between 1 and 12. |
| WEEK(date) | Equivalent to EXTRACT(WEEK FROM date). Returns an integer between 1 and 53. |
| DAYOFYEAR(date) | Equivalent to EXTRACT(DOY FROM date). Returns an integer between 1 and 366. |
| DAYOFMONTH(date) | Equivalent to EXTRACT(DAY FROM date). Returns an integer between 1 and 31. |
| DAYOFWEEK(date) | Equivalent to EXTRACT(DOW FROM date). Returns an integer between 1 and 7. |
| HOUR(date) | Equivalent to EXTRACT(HOUR FROM date). Returns an integer between 0 and 23. |
| MINUTE(date) | Equivalent to EXTRACT(MINUTE FROM date). Returns an integer between 0 and 59. |
| SECOND(date) | Equivalent to EXTRACT(SECOND FROM date). Returns an integer between 0 and 59. |
{:.table}

**String functions**

| Operator syntax | Description |
| ---- | ---- |
| string \|\| string | Concatenates two character strings |
| CHAR_LENGTH(string) | Returns the number of characters in a character string |
| CHARACTER_LENGTH(string) | As CHAR_LENGTH(string) |
| UPPER(string) | Returns a character string converted to upper case |
| LOWER(string) | Returns a character string converted to lower case |
| POSITION(string1 IN string2) | Returns the position of the first occurrence of string1 in string2 |
| POSITION(string1 IN string2 FROM integer) | Returns the position of the first occurrence of string1 in string2 starting at a given point (not standard SQL) |
| TRIM( { BOTH \| LEADING \| TRAILING } string1 FROM string2) | Removes the longest string containing only the characters in string1 from the start/end/both ends of string1 |
| OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ]) | Replaces a substring of string1 with string2 |
| SUBSTRING(string FROM integer) | Returns a substring of a character string starting at a given point |
| SUBSTRING(string FROM integer FOR integer) | Returns a substring of a character string starting at a given point with a given length |
| INITCAP(string) | Returns string with the first letter of each word converter to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters. |
{:.table}

**Conditional functions**

| Operator syntax | Description |
| ---- | ---- |
| CASE value <br>WHEN value1 [, value11 ]* THEN result1 <br>[ WHEN valueN [, valueN1 ]* THEN resultN ]* <br>[ ELSE resultZ ] <br>END | Simple case |
| CASE <br>WHEN condition1 THEN result1 <br>[ WHEN conditionN THEN resultN ]* <br>[ ELSE resultZ ] <br>END | Searched case |
| NULLIF(value, value) | Returns NULL if the values are the same. For example, NULLIF(5, 5) returns NULL; NULLIF(5, 0) returns 5. |
| COALESCE(value, value [, value ]*) | Provides a value if the first value is null. For example, COALESCE(NULL, 5) returns 5. |
{:.table}

**Type conversion functions**

**Aggregate functions**

| Operator syntax | Description |
| ---- | ---- |
| COUNT(*) | Returns the number of input rows |
| AVG(numeric) | Returns the average (arithmetic mean) of numeric across all input values |
| SUM(numeric) | Returns the sum of numeric across all input values |
| MAX(value) | Returns the maximum value of value across all input values |
| MIN(value) | Returns the minimum value of value across all input values |
{:.table}

## 4. Internals of Beam SQL {#internals-of-sql}
Figure 1 describes the back-end steps from a SQL statement to a Beam `PTransform`.

![Workflow of Beam SQL DSL]({{ "/images/beam_sql_dsl_workflow.png" | prepend: site.baseurl }} "workflow of Beam SQL DSL")

**Figure 1** workflow of Beam SQL DSL

Given a `PCollection` and the query as input, first of all the input `PCollection` is registered as a table in the schema repository. Then it's processed as:

1. SQL query is parsed according to grammar to generate a SQL Abstract Syntax Tree;
2. Validate against table schema, and output a logical plan represented with relational algebras;
3. Relational rules are applied to convert it to a physical plan, expressed with Beam components. An optimizer is optional to update the plan;
4. Finally, the Beam physical plan is compiled as a composite `PTransform`;

Here is an example to show a query that filters and projects from an input `PCollection`:

```
SELECT USER_ID, USER_NAME FROM PCOLLECTION WHERE USER_ID = 1
```

The logical plan is shown as:

```
LogicalProject(USER_ID=[$0], USER_NAME=[$1])
  LogicalFilter(condition=[=($0, 1)])
    LogicalTableScan(table=[[PCOLLECTION]])
```

And compiled as a composite `PTransform`

```
pCollection.apply(BeamSqlFilter...)
           .apply(BeamSqlProject...)
```
