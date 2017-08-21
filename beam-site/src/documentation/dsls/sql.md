---
layout: default
title: "DSLs: SQL"
permalink: /documentation/dsls/sql/
---

* [1. Overview](#overview)
* [2. The Internal of Beam SQL](#internal-of-sql)
* [3. Usage of DSL APIs](#usage)
* [4. Functionality in Beam SQL](#functionality)
  * [4.1. Supported Features](#features)
  * [4.2. Data Types](#data-type)
  * [4.3. built-in SQL functions](#built-in-functions)

This page describes the implementation of Beam SQL, and how to simplify a Beam pipeline with DSL APIs.

# <a name="overview"></a>1. Overview
SQL is a well-adopted standard to process data with concise syntax. With DSL APIs, now `PCollection`s can be queried with standard SQL statements, like a regular table. The DSL APIs leverages [Apache Calcite](http://calcite.apache.org/) to parse and optimize SQL queries, then translate into a composite Beam `PTransform`. In this way, both SQL and normal Beam `PTransform`s can be mixed in the same pipeline.

# <a name="internal-of-sql"></a>2. The Internal of Beam SQL
Figure 1 describes the back-end steps from a SQL statement to a Beam `PTransform`.

![Workflow of Beam SQL DSL]({{ "/images/beam_sql_dsl_workflow.png" | prepend: site.baseurl }} "workflow of Beam SQL DSL")

**Figure 1** workflow of Beam SQL DSL

Given a PCollection and the query as input, first of all the input PCollection is registered as a table in the schema repository. Then it's processed as:

1. SQL query is parsed according to grammar to generate a SQL Abstract Syntax Tree;
2. Validate against table schema, and output a logical plan represented with relational algebras;
3. Relational rules are applied to convert it to a physical plan, expressed with Beam components. An optimizer is optional to update the plan;
4. Finally, the Beam physical plan is compiled as a composite `PTransform`;

Here is an example to show a query that filters and projects from a input PCollection:

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

# <a name="usage"></a>3. Usage of DSL APIs 
The DSL interface (`BeamSql.query()` and `BeamSql.simpleQuery()`), is the only endpoint exposed to developers. It wraps the back-end details of parsing/validation/assembling, to deliver a Beam SDK style API that can take either simple TABLE_FILTER queries or complex queries containing JOIN/GROUP_BY etc. 

*Note*, the two APIs are equivalent in functionality, `BeamSql.query()` applies on a `PCollectionTuple` with one or many input `PCollection`s, and `BeamSql.simpleQuery()` is a simplified API which applies on single PCollections.

[BeamSqlExample](https://github.com/apache/beam/blob/DSL_SQL/dsls/sql/src/main/java/org/apache/beam/dsls/sql/example/BeamSqlExample.java) in code repository shows the usage of both APIs:

```
//Step 1. create a source PCollection with Create.of();
PCollection<BeamSqlRow> inputTable = PBegin.in(p).apply(Create.of(row)
    .withCoder(new BeamSqlRowCoder(type)));

//Step 2. (Case 1) run a simple SQL query over input PCollection with BeamSql.simpleQuery;
PCollection<BeamSqlRow> outputStream = inputTable.apply(
    BeamSql.simpleQuery("select c2, c3 from PCOLLECTION where c1=1"));


//Step 2. (Case 2) run the query with BeamSql.query
PCollection<BeamSqlRow> outputStream2 =
    PCollectionTuple.of(new TupleTag<BeamSqlRow>("TABLE_B"), inputTable)
        .apply(BeamSql.query("select c2, c3 from TABLE_B where c1=1"));
```

In Step 1, a `PCollection<BeamSqlRow>` is prepared as the source dataset. The work to generate a queriable `PCollection<BeamSqlRow>` is beyond the scope of Beam SQL DSL. 

Step 2(Case 1) shows the way to run a query with `BeamSql.simpleQuery()`, be aware that the input PCollection is named with a fixed table name __PCOLLECTION__. Step 2(Case 2) is an example to run a query with `BeamSql.query()`. A Table name used in the query is specified when adding PCollection to `PCollectionTuple`. As each call of either `BeamSql.query()` or `BeamSql.simpleQuery()` has its own schema repository, developers need to include all PCollections that would be used in your query.

# <a name="functionality"></a>4. Functionality in Beam SQL
Just as the unified model for both bounded and unbounded data in Beam, SQL DSL provides the same functionalities for bounded and unbounded PCollection as well. 

Note that, SQL support is not fully completed. Queries that include unsupported features would cause a UnsupportedOperationException.

## <a name="features"></a>4.1. Supported Features
The following features are supported in current repository (this chapter will be updated continually).

**1. filter clauses;**

**2. data field projections;**

**3. aggregations;**

Beam SQL supports aggregation functions COUNT/SUM/MAX/MIN/AVG with group_by in global_window, fixed_window, sliding_window and session_window. A field with type `TIMESTAMP` is required to specify fixed_window/sliding_window/session window, which is used as event timestamp for rows. See below for several examples:

```
//fixed window, one hour in duration
SELECT f_int, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int, TUMBLE(f_timestamp, INTERVAL '1' HOUR)

//sliding window, one hour in duration and 30 minutes period
SELECT f_int, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int, HOP(f_timestamp, INTERVAL '1' HOUR, INTERVAL '30' MINUTE)

//session window, with 5 minutes gap duration
SELECT f_int, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int, SESSION(f_timestamp, INTERVAL '5' MINUTE)
```

Note: distinct aggregation is not supported yet.

**4. Join (inner, left_outer, right_outer);**

The scenarios of join can be categorized into 3 cases:

1. BoundedTable JOIN BoundedTable
2. UnboundedTable JOIN UnboundedTable
3. BoundedTable JOIN UnboundedTable

For case 1 and case 2, a standard join is utilized as long as the windowFn of the both sides match. For case 3, sideInput is utilized to implement the join. So there are some constraints:

* Only equal-join is supported, CROSS JOIN is not supported;
* FULL OUTER JOIN is not supported;
* If it's a LEFT OUTER JOIN, the unbounded table should on the left side; If it's a RIGHT OUTER JOIN, the unbounded table should on the right side;

**5. built-in SQL functions**

**6. User Defined Function (UDF) and User Defined Aggregate Function (UDAF);**

If the required function is not available, developers can register their own UDF(for scalar function) and UDAF(for aggregation function).

**create and specify User Defined Function (UDF)**

A UDF can be any Java method that takes zero or more scalar fields, and return one scalar value. Below is an example of UDF and how to use it in DSL:

```
/**
 * A example UDF for test.
 */
public static class CubicInteger{
  public static Integer cubic(Integer input){
    return input * input * input;
  }
}

// register and call in SQL
String sql = "SELECT f_int, cubic(f_int) as cubicvalue FROM PCOLLECTION WHERE f_int = 2";
PCollection<BeamSqlRow> result =
    input.apply("udfExample",
        BeamSql.simpleQuery(sql).withUdf("cubic", CubicInteger.class, "cubic"));
```

**create and specify User Defined Aggregate Function (UDAF)**

A UDAF aggregates a set of grouped scalar values, and output a single scalar value. To create a UDAF function, it's required to extend `org.apache.beam.dsls.sql.schema.BeamSqlUdaf<InputT, AccumT, OutputT>`, which defines 4 methods to process an aggregation:

1. init(), to create an initial accumulate value;
2. add(), to apply a new value to existing accumulate value;
3. merge(), to merge accumulate values from parallel operators;
4. result(), to generate the final result from accumulate value;

Here's an example of UDAF:

```
/**
 * UDAF for test, which returns the sum of square.
 */
public static class SquareSum extends BeamSqlUdaf<Integer, Integer, Integer> {
  public SquareSum() {
  }
  
  // @Override
  public Integer init() {
    return 0;
  }
  
  // @Override
  public Integer add(Integer accumulator, Integer input) {
    return accumulator + input * input;
  }
  
  // @Override
  public Integer merge(Iterable<Integer> accumulators) {
    int v = 0;
    Iterator<Integer> ite = accumulators.iterator();
    while (ite.hasNext()) {
      v += ite.next();
    }
    return v;
  }

  // @Override
  public Integer result(Integer accumulator) {
    return accumulator;
  }
}

//register and call in SQL
String sql = "SELECT f_int1, squaresum(f_int2) AS `squaresum` FROM PCOLLECTION GROUP BY f_int2";
PCollection<BeamSqlRow> result =
    input.apply("udafExample",
        BeamSql.simpleQuery(sql).withUdaf("squaresum", SquareSum.class));
```
 
## <a name="data-type"></a>4.2. Data Types
Each type in Beam SQL maps to a Java class to holds the value in `BeamSqlRow`. The following table lists the relation between SQL types and Java classes, which are supported in current repository:

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

## <a name="built-in-functions"></a>4.3. built-in SQL functions

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
