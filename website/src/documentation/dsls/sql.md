---
layout: section
title: "Beam DSLs: SQL"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/
---

# Beam SQL

* TOC
{:toc}

This page describes the implementation of Beam SQL, and how to simplify a Beam pipeline with DSL APIs.

> Note: Beam SQL hasn't been merged to master branch yet (being developed with branch [DSL_SQL](https://github.com/apache/beam/tree/DSL_SQL)), but is coming soon.

## 1. Overview {#overview}

SQL is a well-adopted standard to process data with concise syntax. With DSL APIs (currently available only in Java), now `PCollection`s can be queried with standard SQL statements, like a regular table. The DSL APIs leverage [Apache Calcite](http://calcite.apache.org/) to parse and optimize SQL queries, then translate into a composite Beam `PTransform`. In this way, both SQL and normal Beam `PTransform`s can be mixed in the same pipeline.

There are two main pieces to the SQL DSL API:

* [BeamRecord]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/values/BeamRecord.html): a new data type used to define composite records (i.e., rows) that consist of multiple, named columns of primitive data types. All SQL DSL queries must be made against collections of type `PCollection<BeamRecord>`. Note that `BeamRecord` itself is not SQL-specific, however, and may also be used in pipelines that do not utilize SQL.
* [BeamSql]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/BeamSql.html): the interface for creating `PTransforms` from SQL queries.

We'll look at each of these below.

## 2. Usage of DSL APIs {#usage}

### BeamRecord

Before applying a SQL query to a `PCollection`, the data in the collection must be in `BeamRecord` format. A `BeamRecord` represents a single, immutable row in a Beam SQL `PCollection`. The names and types of the fields/columns in the record are defined by its associated [BeamRecordType]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/values/BeamRecordType.html); for SQL queries, you should use the [BeamRecordSqlType]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/BeamRecordSqlType.html) subclass (see [Data Types](#data-types) for more details on supported primitive data types).


A `PCollection<BeamRecord>` can be created explicitly or implicitly:

Explicitly:
  * **From in-memory data** (typically for unit testing). In this case, the record type and coder must be specified explicitly:
    ```
    // Define the record type (i.e., schema).
    List<String> fieldNames = Arrays.asList("appId", "description", "rowtime");
    List<Integer> fieldTypes = Arrays.asList(Types.INTEGER, Types.VARCHAR, Types.TIMESTAMP);
    BeamRecordSqlType appType = BeamRecordSqlType.create(fieldNames, fieldTypes);

    // Create a concrete row with that type.
    BeamRecord row = new BeamRecord(nameType, 1, "Some cool app", new Date());

    //create a source PCollection containing only that row.
    PCollection<BeamRecord> testApps = PBegin
        .in(p)
        .apply(Create.of(row)
                     .withCoder(nameType.getRecordCoder()));
    ```
  * **From a `PCollection<T>`** where `T` is not already a `BeamRecord`, by applying a `PTransform` that converts input records to `BeamRecord` format:
    ```
    // An example POJO class.
    class AppPojo {
      ...
      public final Integer appId;
      public final String description;
      public final Date timestamp;
    }

    // Acquire a collection of Pojos somehow.
    PCollection<AppPojo> pojos = ...

    // Convert them to BeamRecords with the same schema as defined above via a DoFn.
    PCollection<BeamRecord> apps = pojos.apply(
        ParDo.of(new DoFn<AppPojo, BeamRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(new BeamRecord(appType, pojo.appId, pojo.description, pojo.timestamp));
          }
        }));
    ```


Implicitly:
* **As the result of a `BeamSql` `PTransform`** applied to a `PCollection<BeamRecord>` (details in the next section).

Once you have a `PCollection<BeamRecord>` in hand, you may use the `BeamSql` APIs to apply SQL queries to it.

### BeamSql

`BeamSql` provides two methods for generating a `PTransform` from a SQL query, both of which are equivalent except for the number of inputs they support:

* `BeamSql.query()`, which may be applied to a single `PCollection`. The input collection must be referenced via the table name `PCOLLECTION` in the query:
  ```
  PCollection<BeamRecord> filteredNames = testApps.apply(
      BeamSql.query("SELECT appId, description, rowtime FROM PCOLLECTION WHERE id=1"));
  ```
* `BeamSql.queryMulti()`, which may be applied to a `PCollectionTuple` containing one or more tagged `PCollection<BeamRecord>`s. The tuple tag for each `PCollection` in the tuple defines the table name that may used to query it. Note that table names are bound to the specific `PCollectionTuple`, and thus are only valid in the context of queries applied to it.
  ```
  // Create a reviews PCollection to join to our apps PCollection.
  BeamRecordSqlType reviewType = BeamRecordSqlType.create(
    Arrays.asList("appId", "reviewerId", "rating", "rowtime"),
    Arrays.asList(Types.INTEGER, Types.INTEGER, Types.FLOAT, Types.TIMESTAMP));
  PCollection<BeamRecord> reviews = ... [records w/ reviewType schema] ...

  // Compute the # of reviews and average rating per app via a JOIN.
  PCollectionTuple namesAndFoods = PCollectionTuple.of(
      new TupleTag<BeamRecord>("Apps"), apps),
      new TupleTag<BeamRecord>("Reviews"), reviews));
  PCollection<BeamRecord> output = namesAndFoods.apply(
      BeamSql.queryMulti("SELECT Names.appId, COUNT(Reviews.rating), AVG(Reviews.rating)
                          FROM Apps INNER JOIN Reviews ON Apps.appId == Reviews.appId"));
  ```

Both methods wrap the back-end details of parsing/validation/assembling, and deliver a Beam SDK style API that can express simple TABLE_FILTER queries up to complex queries containing JOIN/GROUP_BY etc.

[BeamSqlExample](https://github.com/apache/beam/blob/DSL_SQL/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/example/BeamSqlExample.java) in the code repository shows basic usage of both APIs.

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

**1. aggregations;**

Beam SQL supports aggregation functions with group_by in global_window, fixed_window, sliding_window and session_window. A field with type `TIMESTAMP` is required to specify fixed_window/sliding_window/session_window. The field is used as event timestamp for rows. See below for several examples:

```
//fixed window, one hour in duration
SELECT f_int, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int, TUMBLE(f_timestamp, INTERVAL '1' HOUR)

//sliding window, one hour in duration and 30 minutes period
SELECT f_int, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int, HOP(f_timestamp, INTERVAL '1' HOUR, INTERVAL '30' MINUTE)

//session window, with 5 minutes gap duration
SELECT f_int, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int, SESSION(f_timestamp, INTERVAL '5' MINUTE)
```

Note:

1. distinct aggregation is not supported yet.
2. the default trigger is `Repeatedly.forever(AfterWatermark.pastEndOfWindow())`;
3. when `time` field in `HOP(dateTime, slide, size [, time ])`/`TUMBLE(dateTime, interval [, time ])`/`SESSION(dateTime, interval [, time ])` is specified, a lateFiring trigger is added as

```
Repeatedly.forever(AfterWatermark.pastEndOfWindow().withLateFirings(AfterProcessingTime
        .pastFirstElementInPane().plusDelayOf(Duration.millis(delayTime.getTimeInMillis()))));
```		

**2. Join (inner, left_outer, right_outer);**

The scenarios of join can be categorized into 3 cases:

1. BoundedTable JOIN BoundedTable
2. UnboundedTable JOIN UnboundedTable
3. BoundedTable JOIN UnboundedTable

For case 1 and case 2, a standard join is utilized as long as the windowFn of the both sides match. For case 3, sideInput is utilized to implement the join. So far there are some constraints:

* Only equal-join is supported, CROSS JOIN is not supported;
* FULL OUTER JOIN is not supported;
* If it's a LEFT OUTER JOIN, the unbounded table should on the left side; If it's a RIGHT OUTER JOIN, the unbounded table should on the right side;
* window/trigger is inherented from upstreams, which should be consistent;

**3. User Defined Function (UDF) and User Defined Aggregate Function (UDAF);**

If the required function is not available, developers can register their own UDF(for scalar function) and UDAF(for aggregation function).

**create and specify User Defined Function (UDF)**

A UDF can be 1) any Java method that takes zero or more scalar fields and return one scalar value, or 2) a `SerializableFunction`. Below is an example of UDF and how to use it in DSL:

```
/**
 * A example UDF for test.
 */
public static class CubicInteger implements BeamSqlUdf{
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

// register and call in SQL
String sql = "SELECT f_int, cubic1(f_int) as cubicvalue1, cubic2(f_int) as cubicvalue2 FROM PCOLLECTION WHERE f_int = 2";
PCollection<BeamSqlRow> result =
    input.apply("udfExample",
        BeamSql.simpleQuery(sql).withUdf("cubic1", CubicInteger.class)
		                        .withUdf("cubic2", new CubicIntegerFn()));
```

**create and specify User Defined Aggregate Function (UDAF)**

Beam SQL can accept a `CombineFn` as UDAF. Here's an example of UDAF:

```
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

//register and call in SQL
String sql = "SELECT f_int1, squaresum(f_int2) AS `squaresum` FROM PCOLLECTION GROUP BY f_int2";
PCollection<BeamSqlRow> result =
    input.apply("udafExample",
        BeamSql.simpleQuery(sql).withUdaf("squaresum", new SquareSum()));
```

### 3.2. Data Types {#data-types}
Each type in Beam SQL maps to a Java class to holds the value in `BeamRecord`. The following table lists the relation between SQL types and Java classes, which are supported in current repository:

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
