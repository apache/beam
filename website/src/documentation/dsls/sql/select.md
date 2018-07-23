---
layout: section
title: "Beam SQL: SELECT Statement"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/select/
redirect_from: /documentation/dsls/sql/statements/select/
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

# SELECT

The main functionality of Beam SQL is the `SELECT` statement. This is how you
query and join data. The operations supported are a subset of
[Apache Calcite SQL](http://calcite.apache.org/docs/reference.html#grammar).

Generally, the semantics of queries is standard. Please see the following
sections to learn about extensions for supporting Beam's unified
batch/streaming model:

 - [Joins]({{ site.baseurl}}/documentation/dsls/sql/joins)
 - [Windowing & Triggering]({{ site.baseurl}}/documentation/dsls/sql/windowing-and-triggering/)

Query statements scan one or more tables or expressions and return the computed
result rows. This topic describes the syntax for SQL queries in BigQuery.

## SQL Syntax

    query_statement:
        [ WITH with_query_name AS ( query_expr ) [, ...] ]
        query_expr

    query_expr:
        { select | ( query_expr ) | query_expr set_op query_expr }
        [ ORDER BY expression [{ ASC | DESC }] [, ...] ]
        [ LIMIT count [ OFFSET skip_rows ] ]

    select:
        SELECT  [{ ALL | DISTINCT }]
            { [ expression. ]* [ EXCEPT ( column_name [, ...] ) ]
                [ REPLACE ( expression [ AS ] column_name [, ...] ) ]
            | expression [ [ AS ] alias ] } [, ...]
        [ FROM from_item  [, ...] ]
        [ WHERE bool_expression ]
        [ GROUP BY { expression [, ...] | ROLLUP ( expression [, ...] ) } ]
        [ HAVING bool_expression ]
        [ WINDOW window_name AS ( window_definition ) [, ...] ]

    set_op:
        UNION { ALL | DISTINCT } | INTERSECT DISTINCT | EXCEPT DISTINCT

    from_item: {
        table_name [ [ AS ] alias ] [ FOR SYSTEM TIME AS OF timestamp_expression ]  |
        join |
        ( query_expr ) [ [ AS ] alias ] |
        field_path |
        { UNNEST( array_expression ) | UNNEST( array_path ) | array_path }
            [ [ AS ] alias ] [ WITH OFFSET [ [ AS ] alias ] ] |
        with_query_name [ [ AS ] alias ]
    }

    join:
        from_item [ join_type ] JOIN from_item
        [ { ON bool_expression | USING ( join_column [, ...] ) } ]

    join_type:
        { INNER | CROSS | FULL [OUTER] | LEFT [OUTER] | RIGHT [OUTER] }

Notation:

-   Square brackets "\[ \]" indicate optional clauses.
-   Parentheses "( )" indicate literal parentheses.
-   The vertical bar "|" indicates a logical OR.
-   Curly braces "{ }" enclose a set of options.
-   A comma followed by an ellipsis within square brackets "\[, ... \]"
    indicates that the preceding item can repeat in a comma-separated list.

## SELECT list

Syntax:

    SELECT  [{ ALL | DISTINCT }]
        { [ expression. ]* [ EXCEPT ( column_name [, ...] ) ]
            [ REPLACE ( expression [ AS ] column_name [, ...] ) ]
        | expression [ [ AS ] alias ] } [, ...]

The `SELECT` list defines the columns that the query will return. Expressions in
the `SELECT` list can refer to columns in any of the `from_item`s in its
corresponding `FROM` clause.

Each item in the `SELECT` list is one of:

-   \*
-   `expression`
-   `expression.*`

### SELECT \*

`SELECT *`, often referred to as *select star*, produces one output column for
each column that is visible after executing the full query.

``` {.codehilite}
SELECT * FROM (SELECT "apple" AS fruit, "carrot" AS vegetable);

+-------+-----------+
| fruit | vegetable |
+-------+-----------+
| apple | carrot    |
+-------+-----------+
```

### SELECT `expression`

Items in a `SELECT` list can be expressions. These expressions evaluate to a
single value and produce one output column, with an optional explicit `alias`.

If the expression does not have an explicit alias, it receives an implicit alias
according to the rules for [implicit aliases](#implicit_aliases), if possible.
Otherwise, the column is anonymous and you cannot refer to it by name elsewhere
in the query.

### SELECT `expression.*` {#select-expression_1}

An item in a `SELECT` list can also take the form of `expression.*`. This
produces one output column for each column or top-level field of `expression`.
The expression must either be a table alias or evaluate to a single value of a
data type with fields, such as a STRUCT.

The following query produces one output column for each column in the table
`groceries`, aliased as `g`.

``` {.codehilite}
WITH groceries AS
  (SELECT "milk" AS dairy,
   "eggs" AS protein,
   "bread" AS grain)
SELECT g.*
FROM groceries AS g;

+-------+---------+-------+
| dairy | protein | grain |
+-------+---------+-------+
| milk  | eggs    | bread |
+-------+---------+-------+
```

More examples:

``` {.codehilite}
WITH locations AS
  (SELECT STRUCT("Seattle" AS city, "Washington" AS state) AS location
  UNION ALL
  SELECT STRUCT("Phoenix" AS city, "Arizona" AS state) AS location)
SELECT l.location.*
FROM locations l;

+---------+------------+
| city    | state      |
+---------+------------+
| Seattle | Washington |
| Phoenix | Arizona    |
+---------+------------+
```

``` {.codehilite}
WITH locations AS
  (SELECT ARRAY<STRUCT<city STRING, state STRING>>[("Seattle", "Washington"),
    ("Phoenix", "Arizona")] AS location)
SELECT l.LOCATION[offset(0)].*
FROM locations l;

+---------+------------+
| city    | state      |
+---------+------------+
| Seattle | Washington |
+---------+------------+
```

### SELECT modifiers

You can modify the results returned from a `SELECT` query, as follows.

#### SELECT DISTINCT

A `SELECT DISTINCT` statement discards duplicate rows and returns only the
remaining rows. `SELECT DISTINCT` cannot return columns of the following types:

-   STRUCT
-   ARRAY

#### SELECT \* EXCEPT

A `SELECT * EXCEPT` statement specifies the names of one or more columns to
exclude from the result. All matching column names are omitted from the output.

``` {.codehilite}
WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * EXCEPT (order_id)
FROM orders;

+-----------+----------+
| item_name | quantity |
+-----------+----------+
| sprocket  | 200      |
+-----------+----------+
```

**Note:** `SELECT * EXCEPT` does not exclude columns that do not have names.

#### SELECT \* REPLACE

A `SELECT * REPLACE` statement specifies one or more `expression AS identifier`
clauses. Each identifier must match a column name from the `SELECT *` statement.
In the output column list, the column that matches the identifier in a `REPLACE`
clause is replaced by the expression in that `REPLACE` clause.

A `SELECT * REPLACE` statement does not change the names or order of columns.
However, it can change the value and the value type.

``` {.codehilite}
WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * REPLACE ("widget" AS item_name)
FROM orders;

+----------+-----------+----------+
| order_id | item_name | quantity |
+----------+-----------+----------+
| 5        | widget    | 200      |
+----------+-----------+----------+

WITH orders AS
  (SELECT 5 as order_id,
  "sprocket" as item_name,
  200 as quantity)
SELECT * REPLACE (quantity/2 AS quantity)
FROM orders;

+----------+-----------+----------+
| order_id | item_name | quantity |
+----------+-----------+----------+
| 5        | sprocket  | 100      |
+----------+-----------+----------+
```

**Note:** `SELECT * REPLACE` does not replace columns that do not have names.

#### SELECT ALL

A `SELECT ALL` statement returns all rows, including duplicate rows. `SELECT
ALL` is the default behavior of `SELECT`.

### Aliases

See [Aliases](#using_aliases) for information on syntax and visibility for
`SELECT` list aliases.

[]{#analytic_functions}

## Analytic functions

Clauses related to analytic functions are documented elsewhere.

-   `OVER` Clause and `PARTITION BY`: See
    [Analytic Functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#analytic-functions).

-   `WINDOW` Clause and Window Functions: See
    [WINDOW Clause](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#window-clause).

## FROM clause

The `FROM` clause indicates the table or tables from which to retrieve rows, and
specifies how to join those rows together to produce a single stream of rows for
processing in the rest of the query.

### Syntax

    from_item: {
        table_name [ [ AS ] alias ] [ FOR SYSTEM TIME AS OF timestamp_expression ]  |
        join |
        ( query_expr ) [ [ AS ] alias ] |
        field_path |
        { UNNEST( array_expression ) | UNNEST( array_path ) | array_path }
            [ [ AS ] alias ] [ WITH OFFSET [ [ AS ] alias ] ] |
        with_query_name [ [ AS ] alias ]
    }

#### table\_name

The name (optionally qualified) of an existing table.

    SELECT * FROM Roster;
    SELECT * FROM dataset.Roster;
    SELECT * FROM project.dataset.Roster;

#### FOR SYSTEM TIME AS OF

`FOR SYSTEM TIME AS OF` references the historical versions of the table
definition and rows that were current at `timestamp_expression`.

Limitations:

The source table in the `FROM` clause containing `FOR SYSTEM TIME AS OF` must
not be any of the following:

-   An `ARRAY` scan, including a
    [flattened array](arrays#flattening-arrays-and-repeated-fields) or the
    output of the `UNNEST` operator.
-   A common table expression defined by a `WITH` clause.

`timestamp_expression` must be a constant expression. It cannot contain the
following:

-   Subqueries.
-   Correlated references (references to columns of a table that appear at a
    higher level of the query statement, such as in the `SELECT` list).

-   User-defined functions (UDFs).

The value of `timestamp_expression` cannot fall into the following ranges:

-   After the current timestamp (in the future).
-   More than seven (7) days before the current timestamp.

A single query statement cannot reference a single table at more than one point
in time, including the current time. That is, a query can reference a table
multiple times at the same timestamp, but not the current version and a
historical version, or two different historical versions.

Examples:

The following query returns a historical version of the table from one hour ago.

``` {.codehilite}
SELECT *
FROM t
  FOR SYSTEM TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

The following query returns a historical version of the table at an absolute
point in time.

``` {.codehilite}
SELECT *
FROM t
  FOR SYSTEM TIME AS OF '2017-01-01 10:00:00-07:00';
```

The following query returns an error because the `timestamp_expression` contains
a correlated reference to a column in the containing query.

``` {.codehilite}
SELECT *
FROM t1
WHERE t1.a IN (SELECT t2.a
               FROM t2 FOR SYSTEM TIME AS OF t1.timestamp_column);
```

#### join

See [JOIN Types](#join_types) below.

#### select {#select_1}

`( select ) [ [ AS ] alias ]` is a table [subquery](#subqueries).

#### field\_path

In the `FROM` clause, `field_path` is any path that resolves to a field within a
data type. `field_path` can go arbitrarily deep into a nested data structure.

Some examples of valid `field_path` values include:

    SELECT * FROM T1 t1, t1.array_column;

    SELECT * FROM T1 t1, t1.struct_column.array_field;

    SELECT (SELECT ARRAY_AGG(c) FROM t1.array_column c) FROM T1 t1;

    SELECT a.struct_field1 FROM T1 t1, t1.array_of_structs a;

    SELECT (SELECT STRING_AGG(a.struct_field1) FROM t1.array_of_structs a) FROM T1 t1;

Field paths in the FROM clause must end in an array field. In addition, field
paths cannot contain arrays before the end of the path. For example, the path
`array_column.some_array.some_array_field` is invalid because it contains an
array before the end of the path.

Note: If a path has only one name, it is interpreted as a table. To work around
this, wrap the path using `UNNEST`, or use the fully-qualified path.

#### UNNEST

The `UNNEST` operator takes an `ARRAY` and returns a table, with one row for
each element in the `ARRAY`. You can also use `UNNEST` outside of the `FROM`
clause with the
[`IN` operator](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#in-operators).

For input `ARRAY`s of most element types, the output of `UNNEST` generally has
one column. This single column has an optional `alias`, which you can use to
refer to the column elsewhere in the query. `ARRAYS` with these element types
return multiple columns:

-   STRUCT

`UNNEST` destroys the order of elements in the input `ARRAY`. Use the optional
`WITH OFFSET` clause to return a second column with the array element indexes
(see below).

For an input `ARRAY` of `STRUCT`s, `UNNEST` returns a row for each `STRUCT`,
with a separate column for each field in the `STRUCT`. The alias for each column
is the name of the corresponding `STRUCT` field.

**Example**

``` {.codehilite}
SELECT *
FROM UNNEST(ARRAY<STRUCT<x INT64, y STRING>>[(1, 'foo'), (3, 'bar')]);

+---+-----+
| x | y   |
+---+-----+
| 3 | bar |
| 1 | foo |
+---+-----+
```

Because the `UNNEST` operator returns a
[value table](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#value-tables),
you can alias `UNNEST` to define a range variable that you can reference
elsewhere in the query. If you reference the range variable in the `SELECT`
list, the query returns a `STRUCT` containing all of the fields of the original
`STRUCT` in the input table.

**Example**

``` {.codehilite}
SELECT *, struct_value
FROM UNNEST(ARRAY<STRUCT<x INT64, y STRING>>[(1, 'foo'), (3, 'bar')])
       AS struct_value;

+---+-----+--------------+
| x | y   | struct_value |
+---+-----+--------------+
| 3 | bar | {3, bar}     |
| 1 | foo | {1, foo}     |
+---+-----+--------------+
```

ARRAY unnesting can be either explicit or implicit. In explicit unnesting,
`array_expression` must return an ARRAY value but does not need to resolve to an
ARRAY, and the `UNNEST` keyword is required.

Example:

    SELECT * FROM UNNEST ([1, 2, 3]);

In implicit unnesting, `array_path` must resolve to an ARRAY and the `UNNEST`
keyword is optional.

Example:

    SELECT x
    FROM mytable AS t,
      t.struct_typed_column.array_typed_field1 AS x;

In this scenario, `array_path` can go arbitrarily deep into a data structure,
but the last field must be ARRAY-typed. No previous field in the expression can
be ARRAY-typed because it is not possible to extract a named field from an
ARRAY.

`UNNEST` treats NULLs as follows:

-   NULL and empty ARRAYs produces zero rows.
-   An ARRAY containing NULLs produces rows containing NULL values.

The optional `WITH` `OFFSET` clause returns a separate column containing the
"offset" value (i.e. counting starts at zero) for each row produced by the
`UNNEST` operation. This column has an optional `alias`; the default alias is
offset.

Example:

    SELECT * FROM UNNEST ( ) WITH OFFSET AS num;

See the
[`Arrays topic`](https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays)
for more ways to use `UNNEST`, including construction, flattening, and
filtering.

#### with\_query\_name

The query names in a `WITH` clause (see [WITH Clause](#with_clause)) act like
names of temporary tables that you can reference anywhere in the `FROM` clause.
In the example below, `subQ1` and `subQ2` are `with_query_names`.

Example:

    WITH
      subQ1 AS (SELECT * FROM Roster WHERE SchoolID = 52),
      subQ2 AS (SELECT SchoolID FROM subQ1)
    SELECT DISTINCT * FROM subQ2;

The `WITH` clause hides any permanent tables with the same name for the duration
of the query, unless you qualify the table name, e.g. `dataset.Roster` or
`project.dataset.Roster`.

[]{#subqueries}

### Subqueries

A subquery is a query that appears inside another statement, and is written
inside parentheses. These are also referred to as "sub-SELECTs" or "nested
SELECTs". The full `SELECT` syntax is valid in subqueries.

There are two types of subquery:

-   [Expression Subqueries](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#expression-subqueries),
    which you can use in a query wherever expressions are valid. Expression
    subqueries return a single value.
-   Table subqueries, which you can use only in a `FROM` clause. The outer query
    treats the result of the subquery as a table.

Note that there must be parentheses around both types of subqueries.

Example:

``` {.codehilite}
SELECT AVG ( PointsScored )
FROM
( SELECT PointsScored
  FROM Stats
  WHERE SchoolID = 77 )
```

Optionally, a table subquery can have an alias.

Example:

``` {.codehilite}
SELECT r.LastName
FROM
( SELECT * FROM Roster) AS r;
```

### Aliases {#aliases_1}

See [Aliases](#using_aliases) for information on syntax and visibility for
`FROM` clause aliases.

[]{#join_types}

## JOIN types

### Syntax {#syntax_1}

    join:
        from_item [ join_type ] JOIN from_item
        [ ON bool_expression | USING ( join_column [, ...] ) ]

    join_type:
        { INNER | CROSS | FULL [OUTER] | LEFT [OUTER] | RIGHT [OUTER] }

The `JOIN` clause merges two `from_item`s so that the `SELECT` clause can query
them as one source. The `join_type` and `ON` or `USING` clause (a "join
condition") specify how to combine and discard rows from the two `from_item`s to
form a single source.

All `JOIN` clauses require a `join_type`.

A `JOIN` clause requires a join condition unless one of the following conditions
is true:

-   `join_type` is `CROSS`.
-   One or both of the `from_item`s is not a table, e.g. an `array_path` or
    `field_path`.

### \[INNER\] JOIN

An `INNER JOIN`, or simply `JOIN`, effectively calculates the Cartesian product
of the two `from_item`s and discards all rows that do not meet the join
condition. "Effectively" means that it is possible to implement an `INNER JOIN`
without actually calculating the Cartesian product.

### CROSS JOIN

`CROSS JOIN` returns the Cartesian product of the two `from_item`s. In other
words, it retains all rows from both `from_item`s and combines each row from the
first `from_item`s with each row from the second `from_item`s.

**Comma cross joins**

`CROSS JOIN`s can be written explicitly (see directly above) or implicitly using
a comma to separate the `from_item`s.

Example of an implicit "comma cross join":

``` {.codehilite}
SELECT * FROM Roster, TeamMascot;
```

Here is the explicit cross join equivalent:

``` {.codehilite}
SELECT * FROM Roster CROSS JOIN TeamMascot;
```

You cannot write comma cross joins inside parentheses.

Invalid - comma cross join inside parentheses:

``` {.codehilite}
SELECT * FROM t CROSS JOIN (Roster, TeamMascot);  // INVALID.
```

See [Sequences of JOINs](#sequences_of_joins) for details on how a comma cross
join behaves in a sequence of JOINs.

### FULL \[OUTER\] JOIN

A `FULL OUTER JOIN` (or simply `FULL JOIN`) returns all fields for all rows in
both `from_item`s that meet the join condition.

`FULL` indicates that *all rows* from both `from_item`s are returned, even if
they do not meet the join condition.

`OUTER` indicates that if a given row from one `from_item` does not join to any
row in the other `from_item`, the row will return with NULLs for all columns
from the other `from_item`.

### LEFT \[OUTER\] JOIN

The result of a `LEFT OUTER JOIN` (or simply `LEFT JOIN`) for two `from_item`s
always retains all rows of the left `from_item` in the `JOIN` clause, even if no
rows in the right `from_item` satisfy the join predicate.

`LEFT` indicates that all rows from the *left* `from_item` are returned; if a
given row from the left `from_item` does not join to any row in the *right*
`from_item`, the row will return with NULLs for all columns from the right
`from_item`. Rows from the right `from_item` that do not join to any row in the
left `from_item` are discarded.

### RIGHT \[OUTER\] JOIN

The result of a `RIGHT OUTER JOIN` (or simply `RIGHT JOIN`) is similar and
symmetric to that of `LEFT OUTER JOIN`.

[]{#on_clause}

### ON clause

The `ON` clause contains a `bool_expression`. A combined row (the result of
joining two rows) meets the join condition if `bool_expression` returns TRUE.

Example:

``` {.codehilite}
SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

[]{#using_clause}

### USING clause

The `USING` clause requires a `column_list` of one or more columns which occur
in both input tables. It performs an equality comparison on that column, and the
rows meet the join condition if the equality comparison returns TRUE.

In most cases, a statement with the `USING` keyword is equivalent to using the
`ON` keyword. For example, the statement:

``` {.codehilite}
SELECT FirstName
FROM Roster INNER JOIN PlayerStats
USING (LastName);
```

is equivalent to:

``` {.codehilite}
SELECT FirstName
FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

The results from queries with `USING` do differ from queries that use `ON` when
you use `SELECT *`. To illustrate this, consider the query:

``` {.codehilite}
SELECT * FROM Roster INNER JOIN PlayerStats
USING (LastName);
```

This statement returns the rows from `Roster` and `PlayerStats` where
`Roster.LastName` is the same as `PlayerStats.LastName`. The results include a
single `LastName` column.

By contrast, consider the following query:

``` {.codehilite}
SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

This statement returns the rows from `Roster` and `PlayerStats` where
`Roster.LastName` is the same as `PlayerStats.LastName`. The results include two
`LastName` columns; one from `Roster` and one from `PlayerStats`.

[]{#sequences_of_joins}

### Sequences of JOINs

The `FROM` clause can contain multiple `JOIN` clauses in sequence.

Example:

``` {.codehilite}
SELECT * FROM a LEFT JOIN b ON TRUE LEFT JOIN c ON TRUE;
```

where `a`, `b`, and `c` are any `from_item`s. JOINs are bound from left to
right, but you can insert parentheses to group them in a different order.

Consider the following queries: A (without parentheses) and B (with parentheses)
are equivalent to each other but not to C. The `FULL JOIN` in **bold** binds
first.

A.

    SELECT * FROM Roster FULL JOIN TeamMascot USING (SchoolID)
    FULL JOIN PlayerStats USING (LastName);

B.

    SELECT * FROM ( (Roster FULL JOIN TeamMascot USING (SchoolID))
    FULL JOIN PlayerStats USING (LastName));

C.

    SELECT * FROM (Roster FULL JOIN (TeamMascot FULL JOIN PlayerStats USING
    (LastName)) USING (SchoolID)) ;

When comma cross joins are present in a query with a sequence of JOINs, they
group from left to right like other `JOIN` types.

Example:

``` {.codehilite}
SELECT * FROM a JOIN b ON TRUE, b JOIN c ON TRUE;
```

The query above is equivalent to

``` {.codehilite}
SELECT * FROM ((a JOIN b ON TRUE) CROSS JOIN b) JOIN c ON TRUE);
```

There cannot be a `RIGHT JOIN` or `FULL JOIN` after a comma join.

Invalid - `RIGHT JOIN` after a comma cross join:

``` {.codehilite}
SELECT * FROM Roster, TeamMascot RIGHT JOIN PlayerStats ON TRUE;  // INVALID.
```

[]{#where_clause}

## WHERE clause

### Syntax {#syntax_2}

``` {.codehilite}
WHERE bool_expression
```

The `WHERE` clause filters out rows by evaluating each row against
`bool_expression`, and discards all rows that do not return TRUE (that is, rows
that return FALSE or NULL).

Example:

``` {.codehilite}
SELECT * FROM Roster
WHERE SchoolID = 52;
```

The `bool_expression` can contain multiple sub-conditions.

Example:

``` {.codehilite}
SELECT * FROM Roster
WHERE STARTS_WITH(LastName, "Mc") OR STARTS_WITH(LastName, "Mac");
```

You cannot reference column aliases from the `SELECT` list in the `WHERE`
clause.

Expressions in an `INNER JOIN` have an equivalent expression in the `WHERE`
clause. For example, a query using `INNER` `JOIN` and `ON` has an equivalent
expression using `CROSS JOIN` and `WHERE`.

Example - this query:

``` {.codehilite}
SELECT * FROM Roster INNER JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

is equivalent to:

``` {.codehilite}
SELECT * FROM Roster CROSS JOIN TeamMascot
WHERE Roster.SchoolID = TeamMascot.SchoolID;
```

[]{#group_by_clause}

## GROUP BY clause

### Syntax {#syntax_3}

    GROUP BY { expression [, ...] | ROLLUP ( expression [, ...] ) }

The `GROUP BY` clause groups together rows in a table with non-distinct values
for the `expression` in the `GROUP BY` clause. For multiple rows in the source
table with non-distinct values for `expression`, the `GROUP BY` clause produces
a single combined row. `GROUP BY` is commonly used when aggregate functions are
present in the `SELECT` list, or to eliminate redundancy in the output. The data
type of `expression` must be
[groupable](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#data-type-properties).

Example:

``` {.codehilite}
SELECT SUM(PointsScored), LastName
FROM PlayerStats
GROUP BY LastName;
```

The `GROUP BY` clause can refer to expression names in the `SELECT` list. The
`GROUP BY` clause also allows ordinal references to expressions in the `SELECT`
list using integer values. `1` refers to the first expression in the `SELECT`
list, `2` the second, and so forth. The expression list can combine ordinals and
expression names.

Example:

``` {.codehilite}
SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY LastName, FirstName;
```

The query above is equivalent to:

``` {.codehilite}
SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY 2, FirstName;
```

`GROUP BY` clauses may also refer to aliases. If a query contains aliases in the
`SELECT` clause, those aliases override names in the corresponding `FROM`
clause.

Example:

``` {.codehilite}
SELECT SUM(PointsScored), LastName as last_name
FROM PlayerStats
GROUP BY last_name;
```

`GROUP BY ROLLUP` returns the results of `GROUP BY` for prefixes of the
expressions in the `ROLLUP` list, each of which is known as a *grouping set*.
For the `ROLLUP` list `(a, b, c)`, the grouping sets are `(a, b, c)`, `(a, b)`,
`(a)`, `()`. When evaluating the results of `GROUP BY` for a particular grouping
set, `GROUP BY ROLLUP` treats expressions that are not in the grouping set as
having a `NULL` value. A `SELECT` statement like this one:

``` {.codehilite}
SELECT a,    b,    SUM(c) FROM Input GROUP BY ROLLUP(a, b);
```

uses the rollup list `(a, b)`. The result will include the results of `GROUP BY`
for the grouping sets `(a, b)`, `(a)`, and `()`, which includes all rows. This
returns the same rows as:

``` {.codehilite}
SELECT NULL, NULL, SUM(c) FROM Input               UNION ALL
SELECT a,    NULL, SUM(c) FROM Input GROUP BY a    UNION ALL
SELECT a,    b,    SUM(c) FROM Input GROUP BY a, b;
```

This allows the computation of aggregates for the grouping sets defined by the
expressions in the `ROLLUP` list and the prefixes of that list.

Example:

``` {.codehilite}
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(day);
```

The query above outputs a row for each day in addition to the rolled up total
across all days, as indicated by a `NULL` day:

``` {.codehilite}
+------+-------+
| day  | total |
+------+-------+
| NULL | 39.77 |
|    1 | 23.54 |
|    2 |  9.99 |
|    3 |  6.24 |
+------+-------+
```

Example:

``` {.codehilite}
WITH Sales AS (
  SELECT 123 AS sku, 1 AS day, 9.99 AS price UNION ALL
  SELECT 123, 1, 8.99 UNION ALL
  SELECT 456, 1, 4.56 UNION ALL
  SELECT 123, 2, 9.99 UNION ALL
  SELECT 789, 3, 1.00 UNION ALL
  SELECT 456, 3, 4.25 UNION ALL
  SELECT 789, 3, 0.99
)
SELECT
  sku,
  day,
  SUM(price) AS total
FROM Sales
GROUP BY ROLLUP(sku, day)
ORDER BY sku, day;
```

The query above returns rows grouped by the following grouping sets:

-   sku and day
-   sku (day is `NULL`)
-   The empty grouping set (day and sku are `NULL`)

The sums for these grouping sets correspond to the total for each distinct
sku-day combination, the total for each sku across all days, and the grand
total:

``` {.codehilite}
+------+------+-------+
| sku  | day  | total |
+------+------+-------+
| NULL | NULL | 39.77 |
|  123 | NULL | 28.97 |
|  123 |    1 | 18.98 |
|  123 |    2 |  9.99 |
|  456 | NULL |  8.81 |
|  456 |    1 |  4.56 |
|  456 |    3 |  4.25 |
|  789 |    3 |  1.99 |
|  789 | NULL |  1.99 |
+------+------+-------+
```

[]{#having_clause}

## HAVING clause

### Syntax {#syntax_4}

``` {.codehilite}
HAVING bool_expression
```

The `HAVING` clause is similar to the `WHERE` clause: it filters out rows that
do not return TRUE when they are evaluated against the `bool_expression`.

As with the `WHERE` clause, the `bool_expression` can be any expression that
returns a boolean, and can contain multiple sub-conditions.

The `HAVING` clause differs from the `WHERE` clause in that:

-   The `HAVING` clause requires `GROUP BY` or aggregation to be present in the
    query.
-   The `HAVING` clause occurs after `GROUP BY` and aggregation, and before
    `ORDER BY`. This means that the `HAVING` clause is evaluated once for every
    aggregated row in the result set. This differs from the `WHERE` clause,
    which is evaluated before `GROUP BY` and aggregation.

The `HAVING` clause can reference columns available via the `FROM` clause, as
well as `SELECT` list aliases. Expressions referenced in the `HAVING` clause
must either appear in the `GROUP BY` clause or they must be the result of an
aggregate function:

``` {.codehilite}
SELECT LastName
FROM Roster
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

If a query contains aliases in the `SELECT` clause, those aliases override names
in a `FROM` clause.

``` {.codehilite}
SELECT LastName, SUM(PointsScored) AS ps
FROM Roster
GROUP BY LastName
HAVING ps > 0;
```

[]{#mandatory_aggregation}

### Mandatory aggregation

Aggregation does not have to be present in the `HAVING` clause itself, but
aggregation must be present in at least one of the following forms:

#### Aggregation function in the `SELECT` list. {#aggregation-function-in-the-select-list}

``` {.codehilite}
SELECT LastName, SUM(PointsScored) AS total
FROM PlayerStats
GROUP BY LastName
HAVING total > 15;
```

#### Aggregation function in the 'HAVING' clause. {#aggregation-function-in-the-having-clause}

``` {.codehilite}
SELECT LastName
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

#### Aggregation in both the `SELECT` list and `HAVING` clause. {#aggregation-in-both-the-select-list-and-having-clause}

When aggregation functions are present in both the `SELECT` list and `HAVING`
clause, the aggregation functions and the columns they reference do not need to
be the same. In the example below, the two aggregation functions, `COUNT()` and
`SUM()`, are different and also use different columns.

``` {.codehilite}
SELECT LastName, COUNT(*)
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

[]{#order_by_clause}

## ORDER BY clause

### Syntax {#syntax_5}

    ORDER BY expression [{ ASC | DESC }] [, ...]

The `ORDER BY` clause specifies a column or expression as the sort criterion for
the result set. If an ORDER BY clause is not present, the order of the results
of a query is not defined. The default sort direction is `ASC`, which sorts the
results in ascending order of `expression` values. `DESC` sorts the results in
descending order. Column aliases from a `FROM` clause or `SELECT` list are
allowed. If a query contains aliases in the `SELECT` clause, those aliases
override names in the corresponding `FROM` clause.

It is possible to order by multiple columns. In the example below, the result
set is ordered first by `SchoolID` and then by `LastName`:

``` {.codehilite}
SELECT LastName, PointsScored, OpponentID
FROM PlayerStats
ORDER BY SchoolID, LastName;
```

The following rules apply when ordering values:

-   NULLs: In the context of the `ORDER BY` clause, NULLs are the minimum
    possible value; that is, NULLs appear first in `ASC` sorts and last in
    `DESC` sorts.
-   Floating point data types: see
    [Floating Point Semantics](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating-point-semantics)
    on ordering and grouping.

When used in conjunction with [set operators](#set-operators), the `ORDER BY`
clause applies to the result set of the entire query; it does not apply only to
the closest `SELECT` statement. For this reason, it can be helpful (though it is
not required) to use parentheses to show the scope of the `ORDER BY`.

This query without parentheses:

``` {.codehilite}
SELECT * FROM Roster
UNION ALL
SELECT * FROM TeamMascot
ORDER BY SchoolID;
```

is equivalent to this query with parentheses:

``` {.codehilite}
( SELECT * FROM Roster
  UNION ALL
  SELECT * FROM TeamMascot )
ORDER BY SchoolID;
```

but is not equivalent to this query, where the `ORDER BY` clause applies only to
the second `SELECT` statement:

``` {.codehilite}
SELECT * FROM Roster
UNION ALL
( SELECT * FROM TeamMascot
  ORDER BY SchoolID );
```

You can also use integer literals as column references in `ORDER BY` clauses. An
integer literal becomes an ordinal (for example, counting starts at 1) into the
`SELECT` list.

Example - the following two queries are equivalent:

``` {.codehilite}
SELECT SUM(PointsScored), LastName
FROM PlayerStats
ORDER BY LastName;
```

``` {.codehilite}
SELECT SUM(PointsScored), LastName
FROM PlayerStats
ORDER BY 2;
```

[]{#set_operators}

## Set operators

### Syntax {#syntax_6}

    UNION { ALL | DISTINCT } | INTERSECT DISTINCT | EXCEPT DISTINCT

Set operators combine results from two or more input queries into a single
result set. You must specify `ALL` or `DISTINCT`; if you specify `ALL`, then all
rows are retained. If `DISTINCT` is specified, duplicate rows are discarded.

If a given row R appears exactly m times in the first input query and n times in
the second input query (m &gt;= 0, n &gt;= 0):

-   For `UNION ALL`, R appears exactly m + n times in the result.
-   For `UNION DISTINCT`, the `DISTINCT` is computed after the `UNION` is
    computed, so R appears exactly one time.
-   For `INTERSECT DISTINCT`, the `DISTINCT` is computed after the result above
    is computed.
-   For `EXCEPT DISTINCT`, row R appears once in the output if m &gt; 0 and
    n = 0.
-   If there are more than two input queries, the above operations generalize
    and the output is the same as if the inputs were combined incrementally from
    left to right.

The following rules apply:

-   For set operations other than `UNION ALL`, all column types must support
    equality comparison.
-   The input queries on each side of the operator must return the same number
    of columns.
-   The operators pair the columns returned by each input query according to the
    columns' positions in their respective `SELECT` lists. That is, the first
    column in the first input query is paired with the first column in the
    second input query.
-   The result set always uses the column names from the first input query.
-   The result set always uses the supertypes of input types in corresponding
    columns, so paired columns must also have either the same data type or a
    common supertype.
-   You must use parentheses to separate different set operations; for this
    purpose, set operations such as `UNION ALL` and `UNION DISTINCT` are
    different. If the statement only repeats the same set operation, parentheses
    are not necessary.

Examples:

``` {.codehilite}
query1 UNION ALL (query2 UNION DISTINCT query3)
query1 UNION ALL query2 UNION ALL query3
```

Invalid:

    query1 UNION ALL query2 UNION DISTINCT query3
    query1 UNION ALL query2 INTERSECT ALL query3;  // INVALID.

[]{#union}

### UNION

The `UNION` operator combines the result sets of two or more input queries by
pairing columns from the result set of each query and vertically concatenating
them.

[]{#intersect}

### INTERSECT

The `INTERSECT` operator returns rows that are found in the result sets of both
the left and right input queries. Unlike `EXCEPT`, the positioning of the input
queries (to the left vs. right of the `INTERSECT` operator) does not matter.

[]{#except}

### EXCEPT

The `EXCEPT` operator returns rows from the left input query that are not
present in the right input query.

[]{#limit-clause_and_offset_clause}

## LIMIT clause and OFFSET clause

### Syntax {#syntax_7}

``` {.codehilite}
LIMIT count [ OFFSET skip_rows ]
```

`LIMIT` specifies a non-negative `count` of type INT64, and no more than `count`
rows will be returned. `LIMIT` `0` returns 0 rows. If there is a set operation,
`LIMIT` is applied after the set operation is evaluated.

`OFFSET` specifies a non-negative `skip_rows` of type INT64, and only rows from
that offset in the table will be considered.

These clauses accept only literal or parameter values.

The rows that are returned by `LIMIT` and `OFFSET` is unspecified unless these
operators are used after `ORDER BY`.

[]{#with_clause}

## WITH clause

The `WITH` clause contains one or more named subqueries which execute every time
a subsequent `SELECT` statement references them. Any clause or subquery can
reference subqueries you define in the `WITH` clause. This includes any `SELECT`
statements on either side of a set operator, such as `UNION`.

The `WITH` clause is useful primarily for readability, because BigQuery does not
materialize the result of the queries inside the `WITH` clause. If a query
appears in more than one `WITH` clause, it executes in each clause.

Example:

``` {.codehilite}
WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2;
```

Another useful role of the `WITH` clause is to break up more complex queries
into a `WITH` `SELECT` statement and `WITH` clauses, where the less desirable
alternative is writing nested table subqueries. If a `WITH` clause contains
multiple subqueries, the subquery names cannot repeat.

BigQuery supports `WITH` clauses in subqueries, such as table subqueries,
expression subqueries, and so on.

``` {.codehilite}
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1) SELECT * FROM q2)
```

The following are scoping rules for `WITH` clauses:

-   Aliases are scoped so that the aliases introduced in a `WITH` clause are
    visible only in the later subqueries in the same `WITH` clause, and in the
    query under the `WITH` clause.
-   Aliases introduced in the same `WITH` clause must be unique, but the same
    alias can be used in multiple `WITH` clauses in the same query. The local
    alias overrides any outer aliases anywhere that the local alias is visible.
-   Aliased subqueries in a `WITH` clause can never be correlated. No columns
    from outside the query are visible. The only names from outside that are
    visible are other `WITH` aliases that were introduced earlier in the same
    `WITH` clause.

Here's an example of a statement that uses aliases in `WITH` subqueries:

``` {.codehilite}
WITH q1 AS (my_query)
SELECT *
FROM
  (WITH q2 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q3 AS (SELECT * FROM q1),  # q1 resolves to my_query
        q1 AS (SELECT * FROM q1),  # q1 (in the query) resolves to my_query
        q4 AS (SELECT * FROM q1)   # q1 resolves to the WITH subquery
                                   # on the previous line.
    SELECT * FROM q1)  # q1 resolves to the third inner WITH subquery.
```

BigQuery does not support `WITH RECURSIVE`.

[]{#using_aliases}

## Aliases {#aliases_2}

An alias is a temporary name given to a table, column, or expression present in
a query. You can introduce explicit aliases in the `SELECT` list or `FROM`
clause, or BigQuery will infer an implicit alias for some expressions.
Expressions with neither an explicit nor implicit alias are anonymous and the
query cannot reference them by name.

[]{#explicit_alias_syntax}

### Explicit alias syntax

You can introduce explicit aliases in either the `FROM` clause or the `SELECT`
list.

In a `FROM` clause, you can introduce explicit aliases for any item, including
tables, arrays, subqueries, and `UNNEST` clauses, using `[AS] alias`. The `AS`
keyword is optional.

Example:

``` {.codehilite}
SELECT s.FirstName, s2.SongName
FROM Singers AS s, (SELECT * FROM Songs) AS s2;
```

You can introduce explicit aliases for any expression in the `SELECT` list using
`[AS] alias`. The `AS` keyword is optional.

Example:

``` {.codehilite}
SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname
FROM Singers s;
```

[]{#alias_visibility}

### Explicit alias visibility

After you introduce an explicit alias in a query, there are restrictions on
where else in the query you can reference that alias. These restrictions on
alias visibility are the result of BigQuery's name scoping rules.

[]{#from_clause_aliases}

#### FROM clause aliases

BigQuery processes aliases in a `FROM` clause from left to right, and aliases
are visible only to subsequent path expressions in a `FROM` clause.

Example:

Assume the `Singers` table had a `Concerts` column of `ARRAY` type.

``` {.codehilite}
SELECT FirstName
FROM Singers AS s, s.Concerts;
```

Invalid:

``` {.codehilite}
SELECT FirstName
FROM s.Concerts, Singers AS s;  // INVALID.
```

`FROM` clause aliases are **not** visible to subqueries in the same `FROM`
clause. Subqueries in a `FROM` clause cannot contain correlated references to
other tables in the same `FROM` clause.

Invalid:

``` {.codehilite}
SELECT FirstName
FROM Singers AS s, (SELECT (2020 - ReleaseDate) FROM s)  // INVALID.
```

You can use any column name from a table in the `FROM` as an alias anywhere in
the query, with or without qualification with the table name.

Example:

``` {.codehilite}
SELECT FirstName, s.ReleaseDate
FROM Singers s WHERE ReleaseDate = 1975;
```

If the `FROM` clause contains an explicit alias, you must use the explicit alias
instead of the implicit alias for the remainder of the query (see
[Implicit Aliases](#implicit_aliases)). A table alias is useful for brevity or
to eliminate ambiguity in cases such as self-joins, where the same table is
scanned multiple times during query processing.

Example:

``` {.codehilite}
SELECT * FROM Singers as s, Songs as s2
ORDER BY s.LastName
```

Invalid — `ORDER BY` does not use the table alias:

``` {.codehilite}
SELECT * FROM Singers as s, Songs as s2
ORDER BY Singers.LastName;  // INVALID.
```

[]{#select-list_aliases}

#### SELECT list aliases

Aliases in the `SELECT` list are **visible only** to the following clauses:

-   `GROUP BY` clause
-   `ORDER BY` clause
-   `HAVING` clause

Example:

``` {.codehilite}
SELECT LastName AS last, SingerID
FROM Singers
ORDER BY last;
```

[]{#aliases_clauses}

### Explicit aliases in GROUP BY, ORDER BY, and HAVING clauses

These three clauses, `GROUP BY`, `ORDER BY`, and `HAVING`, can refer to only the
following values:

-   Tables in the `FROM` clause and any of their columns.
-   Aliases from the `SELECT` list.

`GROUP BY` and `ORDER BY` can also refer to a third group:

-   Integer literals, which refer to items in the `SELECT` list. The integer `1`
    refers to the first item in the `SELECT` list, `2` refers to the second
    item, etc.

Example:

``` {.codehilite}
SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY 1
ORDER BY 2 DESC;
```

The query above is equivalent to:

``` {.codehilite}
SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY sid
ORDER BY s2id DESC;
```

[]{#ambiguous_aliases}

### Ambiguous aliases

BigQuery provides an error if a name is ambiguous, meaning it can resolve to
more than one unique object.

Examples:

This query contains column names that conflict between tables, since both
`Singers` and `Songs` have a column named `SingerID`:

``` {.codehilite}
SELECT SingerID
FROM Singers, Songs;
```

This query contains aliases that are ambiguous in the `GROUP BY` clause because
they are duplicated in the `SELECT` list:

``` {.codehilite}
SELECT FirstName AS name, LastName AS name,
FROM Singers
GROUP BY name;
```

Ambiguity between a `FROM` clause column name and a `SELECT` list alias in
`GROUP BY`:

``` {.codehilite}
SELECT UPPER(LastName) AS LastName
FROM Singers
GROUP BY LastName;
```

The query above is ambiguous and will produce an error because `LastName` in the
`GROUP BY` clause could refer to the original column `LastName` in `Singers`, or
it could refer to the alias `AS LastName`, whose value is `UPPER(LastName)`.

The same rules for ambiguity apply to path expressions. Consider the following
query where `table` has columns `x` and `y`, and column `z` is of type STRUCT
and has fields `v`, `w`, and `x`.

Example:

``` {.codehilite}
SELECT x, z AS T
FROM table T
GROUP BY T.x;
```

The alias `T` is ambiguous and will produce an error because `T.x` in the `GROUP
BY` clause could refer to either `table.x` or `table.z.x`.

A name is **not** ambiguous in `GROUP BY`, `ORDER BY` or `HAVING` if it is both
a column name and a `SELECT` list alias, as long as the name resolves to the
same underlying object.

Example:

``` {.codehilite}
SELECT LastName, BirthYear AS BirthYear
FROM Singers
GROUP BY BirthYear;
```

The alias `BirthYear` is not ambiguous because it resolves to the same
underlying column, `Singers.BirthYear`.

[]{#implicit_aliases}

### Implicit aliases

In the `SELECT` list, if there is an expression that does not have an explicit
alias, BigQuery assigns an implicit alias according to the following rules.
There can be multiple columns with the same alias in the `SELECT` list.

-   For identifiers, the alias is the identifier. For example, `SELECT abc`
    implies `AS abc`.
-   For path expressions, the alias is the last identifier in the path. For
    example, `SELECT abc.def.ghi` implies `AS ghi`.
-   For field access using the "dot" member field access operator, the alias is
    the field name. For example, `SELECT (struct_function()).fname` implies `AS
    fname`.

In all other cases, there is no implicit alias, so the column is anonymous and
cannot be referenced by name. The data from that column will still be returned
and the displayed query results may have a generated label for that column, but
the label cannot be used like an alias.

In a `FROM` clause, `from_item`s are not required to have an alias. The
following rules apply:

If there is an expression that does not have an explicit alias, BigQuery assigns
an implicit alias in these cases:

-   For identifiers, the alias is the identifier. For example, `FROM abc`
    implies `AS abc`.
-   For path expressions, the alias is the last identifier in the path. For
    example, `FROM abc.def.ghi` implies `AS ghi`
-   The column produced using `WITH OFFSET` has the implicit alias `offset`.

Table subqueries do not have implicit aliases.

`FROM UNNEST(x)` does not have an implicit alias.

[]{#appendix_a_examples_with_sample_data}

## Appendix A: examples with sample data

[]{#sample_tables}

### Sample tables

The following three tables contain sample data about athletes, their schools,
and the points they score during the season. These tables will be used to
illustrate the behavior of different query clauses.

Table Roster:

LastName SchoolID

--------------------------------------------------------------------------------

Adams 50 Buchanan 52 Coolidge 52 Davis 51 Eisenhower 77

The Roster table includes a list of player names (LastName) and the unique ID
assigned to their school (SchoolID).

Table PlayerStats:

LastName OpponentID PointsScored

--------------------------------------------------------------------------------

Adams 51 3 Buchanan 77 0 Coolidge 77 1 Adams 52 4 Buchanan 50 13

The PlayerStats table includes a list of player names (LastName) and the unique
ID assigned to the opponent they played in a given game (OpponentID) and the
number of points scored by the athlete in that game (PointsScored).

Table TeamMascot:

SchoolId Mascot

--------------------------------------------------------------------------------

50 Jaguars 51 Knights 52 Lakers 53 Mustangs

The TeamMascot table includes a list of unique school IDs (SchoolID) and the
mascot for that school (Mascot).

[]{#join_types_examples}

### JOIN types {#join-types_1}

1\) \[INNER\] JOIN

Example:

``` {.codehilite}
SELECT * FROM Roster JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

LastName Roster.SchoolId TeamMascot.SchoolId Mascot

--------------------------------------------------------------------------------

Adams 50 50 Jaguars Buchanan 52 52 Lakers Coolidge 52 52 Lakers Davis 51 51
Knights

2\) CROSS JOIN

Example:

``` {.codehilite}
SELECT * FROM Roster CROSS JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

LastName Roster.SchoolId TeamMascot.SchoolId Mascot

--------------------------------------------------------------------------------

Adams 50 50 Jaguars Adams 50 51 Knights Adams 50 52 Lakers Adams 50 53 Mustangs
Buchanan 52 50 Jaguars Buchanan 52 51 Knights Buchanan 52 52 Lakers Buchanan 52
53 Mustangs Coolidge 52 50 Jaguars Coolidge 52 51 Knights Coolidge 52 52 Lakers
Coolidge 52 53 Mustangs Davis 51 50 Jaguars Davis 51 51 Knights Davis 51 52
Lakers Davis 51 53 Mustangs Eisenhower 77 50 Jaguars Eisenhower 77 51 Knights
Eisenhower 77 52 Lakers Eisenhower 77 53 Mustangs

3\) FULL \[OUTER\] JOIN

Example:

``` {.codehilite}
SELECT * FROM Roster FULL JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

LastName Roster.SchoolId TeamMascot.SchoolId Mascot

--------------------------------------------------------------------------------

Adams 50 50 Jaguars Buchanan 52 52 Lakers Coolidge 52 52 Lakers Davis 51 51
Knights Eisenhower 77 NULL NULL NULL NULL 53 Mustangs

4\) LEFT \[OUTER\] JOIN

Example:

``` {.codehilite}
SELECT * FROM Roster LEFT JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

LastName Roster.SchoolId TeamMascot.SchoolId Mascot

--------------------------------------------------------------------------------

Adams 50 50 Jaguars Buchanan 52 52 Lakers Coolidge 52 52 Lakers Davis 51 51
Knights Eisenhower 77 NULL NULL

5\) RIGHT \[OUTER\] JOIN

Example:

``` {.codehilite}
SELECT * FROM Roster RIGHT JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

Results:

LastName Roster.SchoolId TeamMascot.SchoolId Mascot

--------------------------------------------------------------------------------

Adams 50 50 Jaguars Davis 51 51 Knights Coolidge 52 52 Lakers Buchanan 52 52
Lakers NULL NULL 53 Mustangs

[]{#group_by_clause}

### GROUP BY clause {#group-by-clause_1}

Example:

``` {.codehilite}
SELECT LastName, SUM(PointsScored)
FROM PlayerStats
GROUP BY LastName;
```

LastName SUM

--------------------------------------------------------------------------------

Adams 7 Buchanan 13 Coolidge 1

[]{#set_operators}

### Set operators {#set-operators_1}

[]{#union}

#### UNION {#union_1}

The `UNION` operator combines the result sets of two or more `SELECT` statements
by pairing columns from the result set of each `SELECT` statement and vertically
concatenating them.

Example:

``` {.codehilite}
SELECT Mascot AS X, SchoolID AS Y
FROM TeamMascot
UNION ALL
SELECT LastName, PointsScored
FROM PlayerStats;
```

Results:

X Y

--------------------------------------------------------------------------------

Mustangs 50 Knights 51 Lakers 52 Mustangs 53 Adams 3 Buchanan 0 Coolidge 1 Adams
4 Buchanan 13

[]{#intersect}

#### INTERSECT {#intersect_1}

This query returns the last names that are present in both Roster and
PlayerStats.

``` {.codehilite}
SELECT LastName
FROM Roster
INTERSECT DISTINCT
SELECT LastName
FROM PlayerStats;
```

Results:

LastName

--------------------------------------------------------------------------------

Adams Coolidge Buchanan

[]{#except}

#### EXCEPT {#except_1}

The query below returns last names in Roster that are **not** present in
PlayerStats.

``` {.codehilite}
SELECT LastName
FROM Roster
EXCEPT DISTINCT
SELECT LastName
FROM PlayerStats;
```

Results:

LastName

--------------------------------------------------------------------------------

Eisenhower Davis

Reversing the order of the `SELECT` statements will return last names in
PlayerStats that are **not** present in Roster:

``` {.codehilite}
SELECT LastName
FROM PlayerStats
EXCEPT DISTINCT
SELECT LastName
FROM Roster;
```

Results:

``` {.codehilite}
(empty)
```

> Portions of this page are modifications based on work created and
> [shared by Google](https://developers.google.com/terms/site-policies)
> and used according to terms described in the [Creative Commons 3.0
> Attribution License](http://creativecommons.org/licenses/by/3.0/).
