---
type: languages
title: "Beam Calcite SQL overview"
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
# Beam Calcite SQL overview

[Apache Calcite](https://calcite.apache.org) is a widespread SQL dialect used in
big data processing with some streaming enhancements. Beam Calcite SQL is the default Beam SQL dialect.

Beam SQL has additional extensions leveraging Beam’s unified batch/streaming model and processing complex data types. You can use these extensions with all Beam SQL dialects, including Beam Calcite SQL.

## Query syntax
Query statements scan one or more tables or expressions and return the computed result rows. For more information about query statements in Beam Calcite SQL, see the [Query syntax](/documentation/dsls/sql/calcite/query-syntax) reference.

## Lexical structure
A Beam SQL statement comprises a series of tokens. For more information about tokens in Beam Calcite SQL, see the [Lexical structure](/documentation/dsls/sql/calcite/lexical) reference.

## Data types
Beam SQL supports standard SQL scalar data types as well as extensions including arrays, maps, and nested rows. For more information about scalar data in Beam Calcite SQL, see the [Data types](/documentation/dsls/sql/calcite/data-types) reference.

## Functions and operators
The following table summarizes the Apache Calcite functions and operators supported by Beam Calcite SQL.

<table class="table-bordered table-striped">
  <tr><th>Operators and functions</th><th>Beam SQL support status</th></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#operator-precedence">Operator precedence</a></td><td>Yes</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#comparison-operators">Comparison operators</a></td><td class="style1">See Beam SQL <a href="/documentation/dsls/sql/calcite/scalar-functions/#comparison-functions-and-operators">scalar functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#logical-operators">Logical operators</a></td><td>See Beam SQL <a href="/documentation/dsls/sql/calcite/scalar-functions/#logical-functions-and-operators">scalar functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#arithmetic-operators-and-functions">Arithmetic operators and functions</a></td><td>See Beam SQL <a href="/documentation/dsls/sql/calcite/scalar-functions/#arithmetic-expressions">scalar functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#character-string-operators-and-functions">Character string operators and functions</a></td><td>See Beam SQL <a href="/documentation/dsls/sql/calcite/scalar-functions/#string-functions">scalar functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#binary-string-operators-and-functions">Binary string operators and functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#datetime-functions">Date/time functions</a></td><td>See Beam SQL <a href="/documentation/dsls/sql/calcite/scalar-functions/#date-functions">scalar functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#system-functions">System functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#conditional-functions-and-operators">Conditional functions and operators</a></td><td>See Beam SQL <a href="/documentation/dsls/sql/calcite/scalar-functions/#conditional-functions">scalar functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#type-conversion">Type conversion</a></td><td>Yes</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#value-constructors">Value constructors</a></td><td>No, except array</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#collection-functions">Collection functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#period-predicates">Period predicates</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#jdbc-function-escape">JDBC function escape</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#aggregate-functions">Aggregate functions</a></td>
<td>See Beam SQL extension <a href="/documentation/dsls/sql/calcite/aggregate-functions/">aggregate functions</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#window-functions">Window functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#grouping-functions">Grouping functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#grouped-window-functions">Grouped window functions</a></td><td>See Beam SQL extension <a href="/documentation/dsls/sql/windowing-and-triggering/">windowing and triggering</a></td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#grouped-auxiliary-functions">Grouped auxiliary functions</a></td><td>Yes, except SESSION_END</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#spatial-functions">Spatial functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#geometry-creation-functions-3d">Geometry creation functions (3D)</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#geometry-predicates">Geometry predicates</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#json-functions">JSON functions</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#user-defined-functions">User-defined functions</a></td>
<td>See Beam SQL extension <a href="/documentation/dsls/sql/user-defined-functions/">user-defined functions</a>. You cannot call functions with <a href="https://calcite.apache.org/docs/reference.html#calling-functions-with-named-and-optional-parameters">named and optional parameters</a>.</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#match_recognize">MATCH_RECOGNIZE</a></td><td>No</td></tr>
<tr><td><a href="https://calcite.apache.org/docs/reference.html#ddl-extensions">DDL Extensions</a></td><td>See Beam SQL extension <a href="/documentation/dsls/sql/create-external-table/">CREATE EXTERNAL TABLE</a></td></tr>
</table>
