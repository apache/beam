---
type: languages
title: "Beam ZetaSQL query syntax"
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

# Beam ZetaSQL query syntax

<p>Query statements scan one or more tables, streams, or expressions and return
  the computed result rows.</p>

## SQL Syntax

<pre>
<span class="var">query_statement</span>:
    <span class="var">query_expr</span>

<span class="var">query_expr</span>:
    [ <a href="#with-clause">WITH</a> <span class="var"><a href="#with_query_name">with_query_name</a></span> AS ( <span class="var">query_expr</span> ) [, ...] ]
    { <span class="var">select</span> | ( <span class="var">query_expr</span> ) | <span class="var">query_expr</span> <span class="var">set_op</span> <span class="var">query_expr</span> }
    [ [ <a href="#order-by-clause">ORDER</a> BY <span class="var">expression</span> [{ ASC | DESC }] [, ...] ] <a href="#limit-clause-and-offset-clause">LIMIT</a> <span class="var">count</span> [ OFFSET <span class="var">skip_rows</span> ] ]

<span class="var">select</span>:
    <a href="#select-list">SELECT</a>  [ ALL | DISTINCT ] { * | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
    [ <a href="#from-clause">FROM</a> <span class="var">from_item</span> ]
    [ <a href="#where-clause">WHERE</a> <span class="var">bool_expression</span> ]
    [ <a href="#group-by-clause">GROUP</a> BY <span class="var">expression</span> [, ...] ]
    [ <a href="#having-clause">HAVING</a> <span class="var">bool_expression</span> ]

<span class="var">set_op</span>:
    <a href="#union">UNION</a> { ALL | DISTINCT } | <a href="#intersect">INTERSECT</a> { ALL | DISTINCT } | <a href="#except">EXCEPT</a> { ALL | DISTINCT }

<span class="var">from_item</span>: {
    <span class="var">table_name</span> [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">join</span> |
    ( <span class="var">query_expr</span> ) [ [ AS ] <span class="var">alias</span> ] |
    <span class="var"><a href="#with_query_name">with_query_name</a></span> [ [ AS ] <span class="var">alias</span> ]
}
<span class="var">table_name</span>:
    <a href="/documentation/dsls/sql/zetasql/lexical#identifiers"><span class="var">identifier</span></a> [ . <a href="/documentation/dsls/sql/zetasql/lexical#identifiers"><span class="var">identifier</span></a> ...]

<span class="var">join</span>:
    <span class="var">from_item</span> [ <span class="var">join_type</span> ] <a href="#join-types">JOIN</a> <span class="var">from_item</span>
    <a href="#on-clause">ON</a> <span class="var">bool_expression</span>

<span class="var">join_type</span>:
    { <a href="#inner-join">INNER</a> | <a href="#full-outer-join">FULL [OUTER]</a> | <a href="#left-outer-join">LEFT [OUTER]</a> | <a href="#right-outer-join">RIGHT [OUTER]</a> }

</pre>

<p>Notation:</p>

<ul>
<li>Square brackets "[ ]" indicate optional clauses.</li>
<li>Parentheses "( )" indicate literal parentheses.</li>
<li>The vertical bar "|" indicates a logical OR.</li>
<li>Curly braces "{ }" enclose a set of options.</li>
<li>A comma followed by an ellipsis within square brackets "[, ... ]" indicates that
  the preceding item can repeat in a comma-separated list.</li>
</ul>

## SELECT list

<p>Syntax:</p>

<pre>
SELECT  [ ALL ]
    { * | <span class="var">expression</span> [ [ AS ] <span class="var">alias</span> ] } [, ...]
</pre>

<p>The <code>SELECT</code> list defines the columns that the query will return. Expressions in
the <code>SELECT</code> list can refer to columns in any of the <code>from_item</code>s in its
corresponding <code>FROM</code> clause.</p>

<p>Each item in the <code>SELECT</code> list is one of:</p>

<ul>
<li>*</li>
<li><code>expression</code></li>
</ul>

### SELECT *

<p><code>SELECT *</code>, often referred to as <em>select star</em>, produces one output column for
each column that is visible after executing the full query.</p>

<pre class="codehilite"><code>SELECT * FROM (SELECT "apple" AS fruit, "carrot" AS vegetable);

+-------+-----------+
| fruit | vegetable |
+-------+-----------+
| apple | carrot    |
+-------+-----------+</code></pre>

### SELECT <code>expression</code>
<p><strong>Caution:</strong> In the top-level
  <code>SELECT</code>, you must either use an explicitly selected column name,
  or if you are using an expression, you must use an explicit alias.</p>

<p>Items in a <code>SELECT</code> list can be expressions. These expressions evaluate to a
single value and produce one output column, with an optional explicit <code>alias</code>.</p>

<p>If the expression does not have an explicit alias, it receives an implicit alias
according to the rules for implicit aliases, if possible.
Otherwise, the column is anonymous and you cannot refer to it by name elsewhere
in the query.</p>

### SELECT modifiers

<p>You can modify the results returned from a <code>SELECT</code> query, as follows.</p>

<h4 id="select-all">SELECT ALL</h4>

<p>A <code>SELECT ALL</code> statement returns all rows, including duplicate rows.
<code>SELECT ALL</code> is the default behavior of <code>SELECT</code>.</p>

### Aliases

<p>See <a href="#using_aliases">Aliases</a> for information on syntax and visibility for
<code>SELECT</code> list aliases.</p>

## FROM clause

<p>The <code>FROM</code> clause indicates the tables or streams from which to retrieve rows, and
specifies how to join those rows together to produce a single stream of
rows for processing in the rest of the query.</p>

### Syntax

<pre>
<span class="var">from_item</span>: {
    <span class="var">table_name</span> [ [ AS ] <span class="var">alias</span> ] |
    <span class="var">join</span> |
    ( <span class="var">query_expr</span> ) [ [ AS ] <span class="var">alias</span> ] |
    <span class="var"><a href="#with_query_name">with_query_name</a></span> [ [ AS ] <span class="var">alias</span> ]
}
</pre>

<h4 id="table_name">table_name</h4>

The fully-qualified SQL name of a data source queryable by Beam
  SQL, specified by a dot-separated list of identifiers using
  [Standard SQL lexical structure](/documentation/dsls/sql/zetasql/lexical). You
  must use backticks to enclose identifiers that contain characters which
  are not letters, numbers, or underscores.

<pre>
SELECT * FROM bigquery.table.`my-project`.baseball.roster;
SELECT * FROM pubsub.topic.`my-project`.incoming_events;
</pre>

<h4 id="join">join</h4>

<p>See <a href="#join_types">JOIN Types</a> below.</p>

<h4 id="select_1">select</h4>

<p><code>( select ) [ [ AS ] alias ]</code> is a table <a href="#subqueries">subquery</a>.</p>

<h4 id="with_query_name">with_query_name</h4>

<p>The query names in a <code>WITH</code> clause (see <a href="#with_clause">WITH Clause</a>) act like names of temporary tables that you
can reference anywhere in the <code>FROM</code> clause. In the example below,
<code>subQ1</code> and <code>subQ2</code> are <code>with_query_names</code>.</p>

<p>Example:</p>

<pre>
WITH
  subQ1 AS (SELECT * FROM Roster WHERE SchoolID = 52),
  subQ2 AS (SELECT SchoolID FROM subQ1)
SELECT DISTINCT * FROM subQ2;
</pre>

<p>The <code>WITH</code> clause hides any permanent tables with the same name
for the duration of the query, unless you qualify the table name, e.g.

 <code>db.Roster</code>.

</p>

<p><a id="subqueries"></a></p>

### Subqueries

<p>A subquery is a query that appears inside another statement, and is written
inside parentheses. These are also referred to as "sub-SELECTs" or
"nested SELECTs". The full <code>SELECT</code> syntax is valid in
subqueries.</p>

<p>There are two types of subquery:</p>

<ul>
<li>Expression subqueries,
   which you can use in a query wherever expressions are valid. Expression
   subqueries return a single value.</li>
<li>Table subqueries, which you can use only in a <code>FROM</code> clause. The outer
query treats the result of the subquery as a table.</li>
</ul>

<p>Note that there must be parentheses around both types of subqueries.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT AVG ( PointsScored )
FROM
( SELECT PointsScored
  FROM Stats
  WHERE SchoolID = 77 )</code></pre>

<p>Optionally, a table subquery can have an alias.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT r.LastName
FROM
( SELECT * FROM Roster) AS r;</code></pre>

### Aliases

<p>See <a href="#using_aliases">Aliases</a> for information on syntax and visibility for
<code>FROM</code> clause aliases.</p>

<p><a id="join_types"></a></p>

## JOIN types

### Syntax

<pre>
<span class="var">join</span>:
    <span class="var">from_item</span> [ <span class="var">join_type</span> ] JOIN <span class="var">from_item</span>
    <a href="#on-clause">ON</a> <span class="var">bool_expression</span>

<span class="var">join_type</span>:
    { <a href="#inner-join">INNER</a> | <a href="#full-outer-join">FULL [OUTER]</a> | <a href="#left-outer-join">LEFT [OUTER]</a> | <a href="#right-outer-join">RIGHT [OUTER]</a> }
</pre>

<p>The <code>JOIN</code> clause merges two <code>from_item</code>s so that the <code>SELECT</code> clause can
query them as one source. The <code>join_type</code> and <code>ON</code> clause (a
"join condition") specify how to combine and discard rows from the two
<code>from_item</code>s to form a single source.</p>

<p>All <code>JOIN</code> clauses require a <code>join_type</code>.</p>

<aside class="caution"> <strong>Caution:</strong> <p>Currently, only equi-join
is supported. Joins must use the following form:</p>

<pre>
<span class="var">join condition</span>: conjunction_clause [AND ...]
<span class="var">conjunction_clause</span>: { column | field_access } = { column | field_access }
</pre>
</aside>

### [INNER] JOIN

<p>An <code>INNER JOIN</code>, or simply <code>JOIN</code>, effectively calculates the Cartesian product
of the two <code>from_item</code>s and discards all rows that do not meet the join
condition. "Effectively" means that it is possible to implement an <code>INNER JOIN</code>
without actually calculating the Cartesian product.</p>

### FULL [OUTER] JOIN

<p>A <code>FULL OUTER JOIN</code> (or simply <code>FULL JOIN</code>) returns all fields for all rows in
both <code>from_item</code>s that meet the join condition.</p>

<p><code>FULL</code> indicates that <em>all rows</em> from both <code>from_item</code>s are
returned, even if they do not meet the join condition.</p>

<p><code>OUTER</code> indicates that if a given row from one <code>from_item</code> does not
join to any row in the other <code>from_item</code>, the row will return with NULLs
for all columns from the other <code>from_item</code>.</p>

### LEFT [OUTER] JOIN

<p>The result of a <code>LEFT OUTER JOIN</code> (or simply <code>LEFT JOIN</code>) for two
<code>from_item</code>s always retains all rows of the left <code>from_item</code> in the
<code>JOIN</code> clause, even if no rows in the right <code>from_item</code> satisfy the join
predicate.</p>

<p><code>LEFT</code> indicates that all rows from the <em>left</em> <code>from_item</code> are
returned; if a given row from the left <code>from_item</code> does not join to any row
in the <em>right</em> <code>from_item</code>, the row will return with NULLs for all
columns from the right <code>from_item</code>.  Rows from the right <code>from_item</code> that
do not join to any row in the left <code>from_item</code> are discarded.</p>

### RIGHT [OUTER] JOIN

<p>The result of a <code>RIGHT OUTER JOIN</code> (or simply <code>RIGHT JOIN</code>) is similar and
symmetric to that of <code>LEFT OUTER JOIN</code>.</p>

<p><a id="on_clause"></a></p>

### ON clause

<p>The <code>ON</code> clause contains a <code>bool_expression</code>. A combined row (the result of
joining two rows) meets the join condition if <code>bool_expression</code> returns
TRUE.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT * FROM Roster INNER JOIN PlayerStats
ON Roster.LastName = PlayerStats.LastName;</code></pre>


<p><a id="sequences_of_joins"></a></p>

### Sequences of JOINs

<p>The <code>FROM</code> clause can contain multiple <code>JOIN</code> clauses in sequence.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT * FROM a LEFT JOIN b ON TRUE LEFT JOIN c ON TRUE;</code></pre>

<p>where <code>a</code>, <code>b</code>, and <code>c</code> are any <code>from_item</code>s. JOINs are bound from left to
right, but you can insert parentheses to group them in a different order.</p>

<p><a id="where_clause"></a></p>

## WHERE clause

### Syntax

<pre class="codehilite"><code>WHERE bool_expression</code></pre>

<p>The <code>WHERE</code> clause filters out rows by evaluating each row against
<code>bool_expression</code>, and discards all rows that do not return TRUE (that is,
rows that return FALSE or NULL).</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT * FROM Roster
WHERE SchoolID = 52;</code></pre>

<p>The <code>bool_expression</code> can contain multiple sub-conditions.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT * FROM Roster
WHERE STARTS_WITH(LastName, "Mc") OR STARTS_WITH(LastName, "Mac");</code></pre>

<p>You cannot reference column aliases from the <code>SELECT</code> list in the <code>WHERE</code>
clause.</p>

<p><a id="group_by_clause"></a></p>

## GROUP BY clause

### Syntax

<pre>
GROUP BY <span class="var">expression</span> [, ...]
</pre>

<p>The <code>GROUP BY</code> clause groups together rows in a table with non-distinct values
for the <code>expression</code> in the <code>GROUP BY</code> clause. For multiple rows in the
source table with non-distinct values for <code>expression</code>, the
<code>GROUP BY</code> clause produces a single combined row. <code>GROUP BY</code> is commonly used
when aggregate functions are present in the <code>SELECT</code> list, or to eliminate
redundancy in the output. The data type of <code>expression</code> must be <a href="/documentation/dsls/sql/zetasql/data-types#data-type-properties">groupable</a>.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT SUM(PointsScored), LastName
FROM PlayerStats
GROUP BY LastName;</code></pre>

<p>The <code>GROUP BY</code> clause can refer to expression names in the <code>SELECT</code> list. The
<code>GROUP BY</code> clause also allows ordinal references to expressions in the <code>SELECT</code>
list using integer values. <code>1</code> refers to the first expression in the
<code>SELECT</code> list, <code>2</code> the second, and so forth. The expression list can combine
ordinals and expression names.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY LastName, FirstName;</code></pre>

<p>The query above is equivalent to:</p>

<pre class="codehilite"><code>SELECT SUM(PointsScored), LastName, FirstName
FROM PlayerStats
GROUP BY 2, FirstName;</code></pre>

<p><code>GROUP BY</code> clauses may also refer to aliases. If a query contains aliases in
the <code>SELECT</code> clause, those aliases override names in the corresponding <code>FROM</code>
clause.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT SUM(PointsScored), LastName as last_name
FROM PlayerStats
GROUP BY last_name;</code></pre>

<p><a id="having_clause"></a></p>

## HAVING clause

### Syntax

<pre class="codehilite"><code>HAVING bool_expression</code></pre>

<p>The <code>HAVING</code> clause is similar to the <code>WHERE</code> clause: it filters out rows that
do not return TRUE when they are evaluated against the <code>bool_expression</code>.</p>

<p>As with the <code>WHERE</code> clause, the <code>bool_expression</code> can be any expression
that returns a boolean, and can contain multiple sub-conditions.</p>

<p>The <code>HAVING</code> clause differs from the <code>WHERE</code> clause in that:</p>

<ul>
<li>The <code>HAVING</code> clause requires <code>GROUP BY</code> or aggregation to be present in the
     query.</li>
<li>The <code>HAVING</code> clause occurs after <code>GROUP BY</code> and aggregation, and before
     <code>ORDER BY</code>. This means that the <code>HAVING</code> clause is evaluated once for every
     aggregated row in the result set. This differs from the <code>WHERE</code> clause,
     which is evaluated before <code>GROUP BY</code> and aggregation.</li>
</ul>

<p>The <code>HAVING</code> clause can reference columns available via the <code>FROM</code> clause, as
well as <code>SELECT</code> list aliases. Expressions referenced in the <code>HAVING</code> clause
must either appear in the <code>GROUP BY</code> clause or they must be the result of an
aggregate function:</p>

<pre class="codehilite"><code>SELECT LastName
FROM Roster
GROUP BY LastName
HAVING SUM(PointsScored) &gt; 15;</code></pre>

<p>If a query contains aliases in the <code>SELECT</code> clause, those aliases override names
in a <code>FROM</code> clause.</p>

<pre class="codehilite"><code>SELECT LastName, SUM(PointsScored) AS ps
FROM Roster
GROUP BY LastName
HAVING ps &gt; 0;</code></pre>

<p><a id="mandatory_aggregation"></a></p>

### Mandatory aggregation

<p>Aggregation does not have to be present in the <code>HAVING</code> clause itself, but
aggregation must be present in at least one of the following forms:</p>

<h4 id="aggregation-function-in-the-select-list">Aggregation function in the <code>SELECT</code> list.</h4>

<pre class="codehilite"><code>SELECT LastName, SUM(PointsScored) AS total
FROM PlayerStats
GROUP BY LastName
HAVING total &gt; 15;</code></pre>

<h4 id="aggregation-function-in-the-having-clause">Aggregation function in the 'HAVING' clause.</h4>

<pre class="codehilite"><code>SELECT LastName
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) &gt; 15;</code></pre>

<h4 id="aggregation-in-both-the-select-list-and-having-clause">Aggregation in both the <code>SELECT</code> list and <code>HAVING</code> clause.</h4>

<p>When aggregation functions are present in both the <code>SELECT</code> list and <code>HAVING</code>
clause, the aggregation functions and the columns they reference do not need
to be the same. In the example below, the two aggregation functions,
<code>COUNT()</code> and <code>SUM()</code>, are different and also use different columns.</p>

<pre class="codehilite"><code>SELECT LastName, COUNT(*)
FROM PlayerStats
GROUP BY LastName
HAVING SUM(PointsScored) &gt; 15;</code></pre>

<p><a id="limit-clause_and_offset_clause"></a></p>

## LIMIT clause and OFFSET clause

### Syntax

<pre class="codehilite"><code>[ ORDER BY expression [{ASC | DESC}] [,...] ] LIMIT count [ OFFSET skip_rows ]</code></pre>

<p>The <code>ORDER BY</code> clause specifies a column or expression as the sort criterion for
the result set. If an ORDER BY clause is not present, the order of the results
of a query is not defined. The default sort direction is <code>ASC</code>, which sorts the
results in ascending order of <code>expression</code> values. <code>DESC</code> sorts the results in
descending order. Column aliases from a <code>FROM</code> clause or <code>SELECT</code> list are
allowed. If a query contains aliases in the <code>SELECT</code> clause, those aliases
override names in the corresponding <code>FROM</code> clause.</p>

<p>It is possible to order by multiple columns.</p>

<p>The following rules apply when ordering values:</p>

<ul>
<li>NULLs: In the context of the <code>ORDER BY</code> clause, NULLs are the minimum
   possible value; that is, NULLs appear first in <code>ASC</code> sorts and last in <code>DESC</code>
   sorts.</li>
</ul>

<p><code>LIMIT</code> specifies a non-negative <code>count</code> of type INT64,
and no more than <code>count</code> rows will be returned. <code>LIMIT</code> <code>0</code> returns 0 rows. If
there is a set
operation, <code>LIMIT</code> is applied after the
set operation
is evaluated.</p>

<p><code>OFFSET</code> specifies a non-negative <code>skip_rows</code> of type
INT64, and only rows from
that offset in the table will be considered.</p>

<p>These clauses accept only literal or parameter values.</p>

<p>The rows that are returned by <code>LIMIT</code> and <code>OFFSET</code> is unspecified unless these
operators are used after <code>ORDER BY</code>.</p>

<p><a id="with_clause"></a></p>

## WITH clause

<p>The <code>WITH</code> clause binds the results of one or more named subqueries to temporary
table names.  Each introduced table name is visible in subsequent <code>SELECT</code>
expressions within the same query expression. This includes the following kinds
of <code>SELECT</code> expressions:</p>

<ul>
<li>Any <code>SELECT</code> expressions in subsequent <code>WITH</code> bindings</li>
<li>Top level <code>SELECT</code> expressions in the query expression on both sides of a set
  operator such as <code>UNION</code></li>
<li><code>SELECT</code> expressions inside subqueries within the same query expression</li>
</ul>

<p>Example:</p>

<pre class="codehilite"><code>WITH subQ1 AS (SELECT SchoolID FROM Roster),
     subQ2 AS (SELECT OpponentID FROM PlayerStats)
SELECT * FROM subQ1
UNION ALL
SELECT * FROM subQ2;</code></pre>

<p>The following are scoping rules for <code>WITH</code> clauses:</p>

<ul>
<li>Aliases are scoped so that the aliases introduced in a <code>WITH</code> clause are
  visible only in the later subqueries in the same <code>WITH</code> clause, and in the
  query under the <code>WITH</code> clause.</li>
<li>Aliases introduced in the same <code>WITH</code> clause must be unique, but the same
  alias can be used in multiple <code>WITH</code> clauses in the same query.  The local
  alias overrides any outer aliases anywhere that the local alias is visible.</li>
</ul>

<p>Beam SQL does not support <code>WITH RECURSIVE</code>.</p>

<p><a name="using_aliases"></a></p>

## Aliases

<p>An alias is a temporary name given to a table, column, or expression present in
a query. You can introduce explicit aliases in the <code>SELECT</code> list or <code>FROM</code>
clause.</p>

<p><a id="explicit_alias_syntax"></a></p>

### Explicit alias syntax

<p>You can introduce explicit aliases in either the <code>FROM</code> clause or the <code>SELECT</code>
list.</p>

<p>In a <code>FROM</code> clause, you can introduce explicit aliases for any item, including
tables, arrays and subqueries, using <code>[AS] alias</code>.  The <code>AS</code>
keyword is optional.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT s.FirstName, s2.SongName
FROM Singers AS s, (SELECT * FROM Songs) AS s2;</code></pre>

<p>You can introduce explicit aliases for any expression in the <code>SELECT</code> list using
<code>[AS] alias</code>. The <code>AS</code> keyword is optional.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT s.FirstName AS name, LOWER(s.FirstName) AS lname
FROM Singers s;</code></pre>

<p><a id="alias_visibility"></a></p>

### Explicit alias visibility

<p>After you introduce an explicit alias in a query, there are restrictions on
where else in the query you can reference that alias. These restrictions on
alias visibility are the result of Beam SQL's name scoping rules.</p>

<p><a id="from_clause_aliases"></a></p>

<h4 id="from-clause-aliases">FROM clause aliases</h4>

<p>Beam SQL processes aliases in a <code>FROM</code> clause from left to right,
and aliases are visible only to subsequent path expressions in a <code>FROM</code>
clause.</p>

<p>Example:</p>

<p>Assume the <code>Singers</code> table had a <code>Concerts</code> column of <code>ARRAY</code> type.</p>

<pre class="codehilite"><code>SELECT FirstName
FROM Singers AS s, s.Concerts;</code></pre>

<p>Invalid:</p>

<pre class="codehilite"><code>SELECT FirstName
FROM s.Concerts, Singers AS s;  // INVALID.</code></pre>

<p><code>FROM</code> clause aliases are <strong>not</strong> visible to subqueries in the same <code>FROM</code>
clause. Subqueries in a <code>FROM</code> clause cannot contain correlated references to
other tables in the same <code>FROM</code> clause.</p>

<p>Invalid:</p>

<pre class="codehilite"><code>SELECT FirstName
FROM Singers AS s, (SELECT (2020 - ReleaseDate) FROM s)  // INVALID.</code></pre>

<p>You can use any column name from a table in the <code>FROM</code> as an alias anywhere in
the query, with or without qualification with the table name.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT FirstName, s.ReleaseDate
FROM Singers s WHERE ReleaseDate = 1975;</code></pre>

<p><a id="select-list_aliases"></a></p>

<h4 id="select-list-aliases">SELECT list aliases</h4>

<p>Aliases in the <code>SELECT</code> list are <strong>visible only</strong> to the following clauses:</p>

<ul>
<li><code>GROUP BY</code> clause</li>
<li><code>ORDER BY</code> clause</li>
<li><code>HAVING</code> clause</li>
</ul>

<p>Example:</p>

<pre class="codehilite"><code>SELECT LastName AS last, SingerID
FROM Singers
ORDER BY last;</code></pre>

<p><a id="aliases_clauses"></a></p>

### Explicit aliases in GROUP BY, ORDER BY, and HAVING clauses

<p>These three clauses, <code>GROUP BY</code>, <code>ORDER BY</code>, and <code>HAVING</code>, can refer to only the
following values:</p>

<ul>
<li>Tables in the <code>FROM</code> clause and any of their columns.</li>
<li>Aliases from the <code>SELECT</code> list.</li>
</ul>

<p><code>GROUP BY</code> and <code>ORDER BY</code> can also refer to a third group:</p>

<ul>
<li>Integer literals, which refer to items in the <code>SELECT</code> list. The integer <code>1</code>
   refers to the first item in the <code>SELECT</code> list, <code>2</code> refers to the second item,
   etc.</li>
</ul>

<p>Example:</p>

<pre class="codehilite"><code>SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY 1
ORDER BY 2 DESC LIMIT 10;</code></pre>

<p>The query above is equivalent to:</p>

<pre class="codehilite"><code>SELECT SingerID AS sid, COUNT(Songid) AS s2id
FROM Songs
GROUP BY sid
ORDER BY s2id DESC LIMIT 10;</code></pre>

<p><a id="ambiguous_aliases"></a></p>

### Ambiguous aliases

<p>Beam SQL provides an error if a name is ambiguous, meaning it can
resolve to more than one unique object.</p>

<p>Examples:</p>

<p>This query contains column names that conflict between tables, since both
<code>Singers</code> and <code>Songs</code> have a column named <code>SingerID</code>:</p>

<pre class="codehilite"><code>SELECT SingerID
FROM Singers, Songs;</code></pre>

<p>This query contains aliases that are ambiguous in the <code>GROUP BY</code> clause because
they are duplicated in the <code>SELECT</code> list:</p>

<pre class="codehilite"><code>SELECT FirstName AS name, LastName AS name,
FROM Singers
GROUP BY name;</code></pre>

<p>Ambiguity between a <code>FROM</code> clause column name and a <code>SELECT</code> list alias in
<code>GROUP BY</code>:</p>

<pre class="codehilite"><code>SELECT UPPER(LastName) AS LastName
FROM Singers
GROUP BY LastName;</code></pre>

<p>The query above is ambiguous and will produce an error because <code>LastName</code> in the
<code>GROUP BY</code> clause could refer to the original column <code>LastName</code> in <code>Singers</code>, or
it could refer to the alias <code>AS LastName</code>, whose value is <code>UPPER(LastName)</code>.</p>

<p>The same rules for ambiguity apply to path expressions. Consider the following
query where <code>table</code> has columns <code>x</code> and <code>y</code>, and column <code>z</code> is of type STRUCT
and has fields <code>v</code>, <code>w</code>, and <code>x</code>.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT x, z AS T
FROM table T
GROUP BY T.x;</code></pre>

<p>The alias <code>T</code> is ambiguous and will produce an error because <code>T.x</code> in the <code>GROUP
BY</code> clause could refer to either <code>table.x</code> or <code>table.z.x</code>.</p>

<p>A name is <strong>not</strong> ambiguous in <code>GROUP BY</code>, <code>ORDER BY</code> or <code>HAVING</code> if it is both
a column name and a <code>SELECT</code> list alias, as long as the name resolves to the
same underlying object.</p>

<p>Example:</p>

<pre class="codehilite"><code>SELECT LastName, BirthYear AS BirthYear
FROM Singers
GROUP BY BirthYear;</code></pre>

<p>The alias <code>BirthYear</code> is not ambiguous because it resolves to the same
underlying column, <code>Singers.BirthYear</code>.</p>

<p><a id="appendix_a_examples_with_sample_data"></a></p>

## Appendix A: examples with sample data

<p><a id="sample_tables"></a></p>

### Sample tables

<p>The following three tables contain sample data about athletes, their schools,
and the points they score during the season. These tables will be used to
illustrate the behavior of different query clauses.</p>
<p>Table Roster:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>SchoolID</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>The Roster table includes a list of player names (LastName) and the unique ID
assigned to their school (SchoolID).</p>
<p>Table PlayerStats:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>OpponentID</th>
<th>PointsScored</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>51</td>
<td>3</td>
</tr>
<tr>
<td>Buchanan</td>
<td>77</td>
<td>0</td>
</tr>
<tr>
<td>Coolidge</td>
<td>77</td>
<td>1</td>
</tr>
<tr>
<td>Adams</td>
<td>52</td>
<td>4</td>
</tr>
<tr>
<td>Buchanan</td>
<td>50</td>
<td>13</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>The PlayerStats table includes a list of player names (LastName) and the unique
ID assigned to the opponent they played in a given game (OpponentID) and the
number of points scored by the athlete in that game (PointsScored).</p>
<p>Table TeamMascot:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>The TeamMascot table includes a list of unique school IDs (SchoolID) and the
mascot for that school (Mascot).</p>
<p><a id="join_types_examples"></a></p>

### JOIN types

<p>1) [INNER] JOIN</p>
<p>Example:</p>
<pre class="codehilite"><code>SELECT * FROM Roster JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;</code></pre>
<p>Results:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>2) FULL [OUTER] JOIN</p>
<p>Example:</p>
<pre class="codehilite"><code>SELECT * FROM Roster FULL JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;</code></pre>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>NULL</td>
<td>NULL</td>
</tr>
<tr>
<td>NULL</td>
<td>NULL</td>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>3) LEFT [OUTER] JOIN</p>
<p>Example:</p>
<pre class="codehilite"><code>SELECT * FROM Roster LEFT JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;</code></pre>
<p>Results:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Eisenhower</td>
<td>77</td>
<td>NULL</td>
<td>NULL</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>4) RIGHT [OUTER] JOIN</p>
<p>Example:</p>
<pre class="codehilite"><code>SELECT * FROM Roster RIGHT JOIN TeamMascot
ON Roster.SchoolID = TeamMascot.SchoolID;</code></pre>
<p>Results:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>Roster.SchoolId</th>
<th>TeamMascot.SchoolId</th>
<th>Mascot</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>50</td>
<td>50</td>
<td>Jaguars</td>
</tr>
<tr>
<td>Davis</td>
<td>51</td>
<td>51</td>
<td>Knights</td>
</tr>
<tr>
<td>Coolidge</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>Buchanan</td>
<td>52</td>
<td>52</td>
<td>Lakers</td>
</tr>
<tr>
<td>NULL</td>
<td>NULL</td>
<td>53</td>
<td>Mustangs</td>
</tr>
</tbody>
</table>
{{< /table >}}

### GROUP BY clause
<p>Example:</p>
<pre class="codehilite"><code>SELECT LastName, SUM(PointsScored)
FROM PlayerStats
GROUP BY LastName;</code></pre>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
<th>SUM</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
<td>7</td>
</tr>
<tr>
<td>Buchanan</td>
<td>13</td>
</tr>
<tr>
<td>Coolidge</td>
<td>1</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p><a id="set_operators"></a></p>

### Set operators

<p><a id="union"></a></p>
<h4 id="union_1">UNION</h4>
<p>The <code>UNION</code> operator combines the result sets of two or more <code>SELECT</code> statements
by pairing columns from the result set of each <code>SELECT</code> statement and vertically
concatenating them.</p>
<p>Example:</p>
<pre class="codehilite"><code>SELECT Mascot AS X, SchoolID AS Y
FROM TeamMascot
UNION ALL
SELECT LastName, PointsScored
FROM PlayerStats;</code></pre>
<p>Results:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>X</th>
<th>Y</th>
</tr>
</thead>
<tbody>
<tr>
<td>Jaguars</td>
<td>50</td>
</tr>
<tr>
<td>Knights</td>
<td>51</td>
</tr>
<tr>
<td>Lakers</td>
<td>52</td>
</tr>
<tr>
<td>Mustangs</td>
<td>53</td>
</tr>
<tr>
<td>Adams</td>
<td>3</td>
</tr>
<tr>
<td>Buchanan</td>
<td>0</td>
</tr>
<tr>
<td>Coolidge</td>
<td>1</td>
</tr>
<tr>
<td>Adams</td>
<td>4</td>
</tr>
<tr>
<td>Buchanan</td>
<td>13</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p><a id="intersect"></a></p>
<h4 id="intersect_1">INTERSECT</h4>
<p>This query returns the last names that are present in both Roster and
PlayerStats.</p>
<pre class="codehilite"><code>SELECT LastName
FROM Roster
INTERSECT ALL
SELECT LastName
FROM PlayerStats;</code></pre>
<p>Results:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>Adams</td>
</tr>
<tr>
<td>Coolidge</td>
</tr>
<tr>
<td>Buchanan</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p><a id="except"></a></p>
<h4 id="except_1">EXCEPT</h4>
<p>The query below returns last names in Roster that are <strong>not </strong>present in
PlayerStats.</p>
<pre class="codehilite"><code>SELECT LastName
FROM Roster
EXCEPT DISTINCT
SELECT LastName
FROM PlayerStats;</code></pre>
<p>Results:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>LastName</th>
</tr>
</thead>
<tbody>
<tr>
<td>Eisenhower</td>
</tr>
<tr>
<td>Davis</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>Reversing the order of the <code>SELECT</code> statements will return last names in
PlayerStats that are <strong>not</strong> present in Roster:</p>
<pre class="codehilite"><code>SELECT LastName
FROM PlayerStats
EXCEPT DISTINCT
SELECT LastName
FROM Roster;</code></pre>
<p>Results:</p>
<pre class="codehilite"><code>(empty)</code></pre>