---
type: languages
title: "Beam Calcite SQL query syntax"
aliases:
    - /documentation/dsls/sql/statements/select/
    - /documentation/dsls/sql/select/
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

# Beam Calcite SQL query syntax

Query statements scan one or more tables or expressions and return the computed
result rows.

Generally, the semantics of queries is standard. See the following
sections to learn about extensions for supporting Beam's unified
batch/streaming model:

 - [Joins](/documentation/dsls/sql/extensions/joins)
 - [Windowing & Triggering](/documentation/dsls/sql/windowing-and-triggering/)

The main functionality of Beam SQL is the `SELECT` statement. This is how you
query and join data. The operations supported are a subset of
[Apache Calcite SQL](https://calcite.apache.org/docs/reference.html#grammar).

## SQL Syntax

    query_statement:
        [ WITH with_query_name AS ( query_expr ) [, ...] ]
        query_expr

    query_expr:
        { select | ( query_expr ) | query_expr set_op query_expr }
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

    set_op:
        UNION { ALL | DISTINCT } | INTERSECT DISTINCT | EXCEPT DISTINCT

    from_item: {
        table_name [ [ AS ] alias ] |
        join |
        ( query_expr ) [ [ AS ] alias ]
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
        { [ expression. ]*
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

```
SELECT * FROM (SELECT 'apple' AS fruit, 'carrot' AS vegetable);

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
according to the rules for [implicit aliases](#implicit-aliases), if possible.
Otherwise, the column is anonymous and you cannot refer to it by name elsewhere
in the query.

### SELECT `expression.*` {#select-expression_1}

An item in a `SELECT` list can also take the form of `expression.*`. This
produces one output column for each column or top-level field of `expression`.
The expression must be a table alias.

The following query produces one output column for each column in the table
`groceries`, aliased as `g`.

```
WITH groceries AS
  (SELECT 'milk' AS dairy,
   'eggs' AS protein,
   'bread' AS grain)
SELECT g.*
FROM groceries AS g;

+-------+---------+-------+
| dairy | protein | grain |
+-------+---------+-------+
| milk  | eggs    | bread |
+-------+---------+-------+
```

### SELECT modifiers

You can modify the results returned from a `SELECT` query, as follows.

#### SELECT DISTINCT

A `SELECT DISTINCT` statement discards duplicate rows and returns only the
remaining rows. `SELECT DISTINCT` cannot return columns of the following types:

-   STRUCT
-   ARRAY

#### SELECT ALL

A `SELECT ALL` statement returns all rows, including duplicate rows. `SELECT
ALL` is the default behavior of `SELECT`.

### Aliases

See [Aliases](#aliases_2) for information on syntax and visibility for
`SELECT` list aliases.

## FROM clause

The `FROM` clause indicates the table or tables from which to retrieve rows, and
specifies how to join those rows together to produce a single stream of rows for
processing in the rest of the query.

### Syntax

    from_item: {
        table_name [ [ AS ] alias ] |
        join |
        ( query_expr ) [ [ AS ] alias ] |
        with_query_name [ [ AS ] alias ]
    }

#### table\_name

The name (optionally qualified) of an existing table.

    SELECT * FROM Roster;
    SELECT * FROM beam.Roster;

#### join

See [JOIN Types](#join-types) below and [Joins](/documentation/dsls/sql/extensions/joins).

#### select {#select_1}

`( select ) [ [ AS ] alias ]` is a table [subquery](#subqueries).

#### with\_query\_name

The query names in a `WITH` clause (see [WITH Clause](#with-clause)) act like
names of temporary tables that you can reference anywhere in the `FROM` clause.
In the example below, `subQ1` and `subQ2` are `with_query_names`.

Example:

    WITH
      subQ1 AS (SELECT * FROM Roster WHERE SchoolID = 52),
      subQ2 AS (SELECT SchoolID FROM subQ1)
    SELECT DISTINCT * FROM subQ2;

The `WITH` clause hides any permanent tables with the same name for the duration
of the query, unless you qualify the table name, e.g. `beam.Roster`.

### Subqueries

A subquery is a query that appears inside another statement, and is written
inside parentheses. These are also referred to as "sub-SELECTs" or "nested
SELECTs". The full `SELECT` syntax is valid in subqueries.

There are two types of subquery:

-   Expression Subqueries
    which you can use in a query wherever expressions are valid. Expression
    subqueries return a single value.
-   Table subqueries, which you can use only in a `FROM` clause. The outer query
    treats the result of the subquery as a table.

Note that there must be parentheses around both types of subqueries.

Example:

```
SELECT AVG ( PointsScored )
FROM
( SELECT PointsScored
  FROM Stats
  WHERE SchoolID = 77 )
```

Optionally, a table subquery can have an alias.

Example:

```
SELECT r.LastName
FROM
( SELECT * FROM Roster) AS r;
```

### Aliases {#aliases_1}

See [Aliases](#aliases_2) for information on syntax and visibility for
`FROM` clause aliases.

## JOIN types

Also see [Joins](/documentation/dsls/sql/extensions/joins).

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

`CROSS JOIN` is generally not yet supported.

### FULL \[OUTER\] JOIN

A `FULL OUTER JOIN` (or simply `FULL JOIN`) returns all fields for all rows in
both `from_item`s that meet the join condition.

`FULL` indicates that *all rows* from both `from_item`s are returned, even if
they do not meet the join condition. For streaming jobs, all rows that are
not late according to default trigger and belonging to the same window
if there's non-global window applied.

`OUTER` indicates that if a given row from one `from_item` does not join to any
row in the other `from_item`, the row will return with NULLs for all columns
from the other `from_item`.

Also see [Joins](/documentation/dsls/sql/extensions/joins).

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

### ON clause

The `ON` clause contains a `bool_expression`. A combined row (the result of
joining two rows) meets the join condition if `bool_expression` returns TRUE.

Example:

```
SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

### USING clause

The `USING` clause requires a `column_list` of one or more columns which occur
in both input tables. It performs an equality comparison on that column, and the
rows meet the join condition if the equality comparison returns TRUE.

In most cases, a statement with the `USING` keyword is equivalent to using the
`ON` keyword. For example, the statement:

```
SELECT FirstName
FROM Roster INNER JOIN PlayerStats
USING (LastName);
```

is equivalent to:

```
SELECT FirstName
FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

The results from queries with `USING` do differ from queries that use `ON` when
you use `SELECT *`. To illustrate this, consider the query:

```
SELECT * FROM Roster INNER JOIN PlayerStats
USING (LastName);
```

This statement returns the rows from `Roster` and `PlayerStats` where
`Roster.LastName` is the same as `PlayerStats.LastName`. The results include a
single `LastName` column.

By contrast, consider the following query:

```
SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;
```

This statement returns the rows from `Roster` and `PlayerStats` where
`Roster.LastName` is the same as `PlayerStats.LastName`. The results include two
`LastName` columns; one from `Roster` and one from `PlayerStats`.

### Sequences of JOINs

The `FROM` clause can contain multiple `JOIN` clauses in sequence.

Example:

```
SELECT * FROM a LEFT JOIN b ON TRUE LEFT JOIN c ON TRUE;
```

where `a`, `b`, and `c` are any `from_item`s. JOINs are bound from left to
right, but you can insert parentheses to group them in a different order.

## WHERE clause

### Syntax {#syntax_2}

```
WHERE bool_expression
```

The `WHERE` clause filters out rows by evaluating each row against
`bool_expression`, and discards all rows that do not return TRUE (that is, rows
that return FALSE or NULL).

Example:

```
SELECT * FROM Roster
WHERE SchoolID = 52;
```

The `bool_expression` can contain multiple sub-conditions.

Example:

```
SELECT * FROM Roster
WHERE LastName LIKE 'Mc%' OR LastName LIKE 'Mac%';
```

You cannot reference column aliases from the `SELECT` list in the `WHERE`
clause.

Expressions in an `INNER JOIN` have an equivalent expression in the `WHERE`
clause. For example, a query using `INNER` `JOIN` and `ON` has an equivalent
expression using `CROSS JOIN` and `WHERE`.

Example - this query:

```
SELECT * FROM Roster INNER JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;
```

is equivalent to:

```
SELECT * FROM Roster CROSS JOIN TeamMascot
WHERE Roster.SchoolID = TeamMascot.SchoolID;
```

## GROUP BY clause

Also see [Windowing & Triggering](/documentation/dsls/sql/windowing-and-triggering/)

### Syntax {#syntax_3}

    GROUP BY { expression [, ...] | ROLLUP ( expression [, ...] ) }

The `GROUP BY` clause groups together rows in a table with non-distinct values
for the `expression` in the `GROUP BY` clause. For multiple rows in the source
table with non-distinct values for `expression`, the `GROUP BY` clause produces
a single combined row. `GROUP BY` is commonly used when aggregate functions are
present in the `SELECT` list, or to eliminate redundancy in the output.

Example:

```
SELECT SUM(PointsScored), LastName
FROM PlayerStats
GROUP BY LastName;
```

## HAVING clause

### Syntax {#syntax_4}

```
HAVING bool_expression
```

The `HAVING` clause is similar to the `WHERE` clause: it filters out rows that
do not return TRUE when they are evaluated against the `bool_expression`.

As with the `WHERE` clause, the `bool_expression` can be any expression that
returns a boolean, and can contain multiple sub-conditions.

The `HAVING` clause differs from the `WHERE` clause in that:

-   The `HAVING` clause requires `GROUP BY` or aggregation to be present in the
    query.
-   The `HAVING` clause occurs after `GROUP BY` and aggregation.
    This means that the `HAVING` clause is evaluated once for every
    aggregated row in the result set. This differs from the `WHERE` clause,
    which is evaluated before `GROUP BY` and aggregation.

The `HAVING` clause can reference columns available via the `FROM` clause, as
well as `SELECT` list aliases. Expressions referenced in the `HAVING` clause
must either appear in the `GROUP BY` clause or they must be the result of an
aggregate function:

```
SELECT LastName
FROM Roster
GROUP BY LastName
HAVING SUM(PointsScored) > 15;
```

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

```
query1 UNION ALL (query2 UNION DISTINCT query3)
query1 UNION ALL query2 UNION ALL query3
```

Invalid:

    query1 UNION ALL query2 UNION DISTINCT query3
    query1 UNION ALL query2 INTERSECT ALL query3;  // INVALID.

### UNION

The `UNION` operator combines the result sets of two or more input queries by
pairing columns from the result set of each query and vertically concatenating
them.

### INTERSECT

The `INTERSECT` operator returns rows that are found in the result sets of both
the left and right input queries. Unlike `EXCEPT`, the positioning of the input
queries (to the left vs. right of the `INTERSECT` operator) does not matter.

### EXCEPT

The `EXCEPT` operator returns rows from the left input query that are not
present in the right input query.

## LIMIT clause and OFFSET clause

### Syntax {#syntax_7}

```
LIMIT count [ OFFSET skip_rows ]
```

`LIMIT` specifies a non-negative `count` of type INTEGER, and no more than `count`
rows will be returned. `LIMIT` `0` returns 0 rows. If there is a set operation,
`LIMIT` is applied after the set operation is evaluated.

`OFFSET` specifies a non-negative `skip_rows` of type INTEGER, and only rows from
that offset in the table will be considered.

These clauses accept only literal or parameter values.

The rows that are returned by `LIMIT` and `OFFSET` is unspecified.

## WITH clause

The `WITH` clause contains one or more named subqueries which execute every time
a subsequent `SELECT` statement references them. Any clause or subquery can
reference subqueries you define in the `WITH` clause. This includes any `SELECT`
statements on either side of a set operator, such as `UNION`.

Example:

```
WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2;
```

## Aliases {#aliases_2}

An alias is a temporary name given to a table, column, or expression present in
a query. You can introduce explicit aliases in the `SELECT` list or `FROM`
clause, or Beam will infer an implicit alias for some expressions.
Expressions with neither an explicit nor implicit alias are anonymous and the
query cannot reference them by name.

### Explicit alias syntax

You can introduce explicit aliases in either the `FROM` clause or the `SELECT`
list.

In a `FROM` clause, you can introduce explicit aliases for any item, including
tables, arrays, subqueries, and `UNNEST` clauses, using `[AS] alias`. The `AS`
keyword is optional.

Example:

```
SELECT s.FirstName, s2.SongName
FROM Singers AS s JOIN Songs AS s2 ON s.SingerID = s2.SingerID;
```

You can introduce explicit aliases for any expression in the `SELECT` list using
`[AS] alias`. The `AS` keyword is optional.

Example:

```
SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname
FROM Singers s;
```

### Explicit alias visibility

After you introduce an explicit alias in a query, there are restrictions on
where else in the query you can reference that alias. These restrictions on
alias visibility are the result of Beam's name scoping rules.

#### FROM clause aliases

Beam processes aliases in a `FROM` clause from left to right, and aliases
are visible only to subsequent `JOIN` clauses.

### Ambiguous aliases

Beam provides an error if a name is ambiguous, meaning it can resolve to
more than one unique object.

Examples:

This query contains column names that conflict between tables, since both
`Singers` and `Songs` have a column named `SingerID`:

```
SELECT SingerID
FROM Singers, Songs;
```

### Implicit aliases

In the `SELECT` list, if there is an expression that does not have an explicit
alias, Beam assigns an implicit alias according to the following rules.
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

If there is an expression that does not have an explicit alias, Beam assigns
an implicit alias in these cases:

-   For identifiers, the alias is the identifier. For example, `FROM abc`
    implies `AS abc`.
-   For path expressions, the alias is the last identifier in the path. For
    example, `FROM abc.def.ghi` implies `AS ghi`

Table subqueries do not have implicit aliases.

`FROM UNNEST(x)` does not have an implicit alias.

> Portions of this page are modifications based on
> [work](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
> created and
> [shared by Google](https://developers.google.com/terms/site-policies)
> and used according to terms described in the [Creative Commons 3.0
> Attribution License](https://creativecommons.org/licenses/by/3.0/).
