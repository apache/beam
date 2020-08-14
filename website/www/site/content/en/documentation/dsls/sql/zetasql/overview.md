---
type: languages
title: "Beam ZetaSQL overview"
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
# Beam ZetaSQL overview
Beam SQL supports a varient of the [ZetaSQL](https://github.com/google/zetasql) language. ZetaSQL is similar to the language in BigQuery's SQL framework. This Beam SQL dialect is especially useful in pipelines that [write to or read from BigQuery tables](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html).

Beam SQL has additional extensions leveraging Beam’s unified batch/streaming model and processing complex data types. You can use these extensions with all Beam SQL dialects, including Beam ZetaSQL.

## Query syntax
Query statements scan tables or expressions and return the computed result rows. For more information about query statements in Beam ZetaSQL, see the [Query syntax](/documentation/dsls/sql/zetasql/query-syntax) reference and [Function call rules](/documentation/dsls/sql/zetasql/syntax).

## Lexical structure
A Beam SQL statement comprises a series of tokens. For more information about tokens in Beam ZetaSQL, see the [Lexical structure](/documentation/dsls/sql/zetasql/lexical) reference.

## Data types
Beam SQL supports standard SQL scalar data types as well as extensions including arrays, maps, and nested rows. For more information about scalar data in Beam ZetaSQL, see the [Data types](/documentation/dsls/sql/zetasql/data-types) reference.

## Functions and operators
The following table summarizes the [ZetaSQL functions and operators](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md) supported by Beam ZetaSQL.
<table class="table-bordered table-striped">
  <tr><th>Operators and functions</th><th>Beam ZetaSQL support</th></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/conversion_rules.md">Type conversion</a></td><td>Yes</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/aggregate_functions.md">Aggregate functions</a></td><td>See Beam SQL <a href="/documentation/dsls/sql/zetasql/aggregate-functions">aggregate functions</a></td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/statistical_aggregate_functions.md">Statistical aggregate functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/approximate_aggregate_functions.md">Approximate aggregate functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/hll_functions.md">HyperLogLog++ functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md#kll16-quantile-functions">KLL16 quantile functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/numbering_functions.md">Numbering functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/bit_functions.md">Bit functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/mathematical_functions.md">Mathematical functions</a></td><td>See <a href="/documentation/dsls/sql/zetasql/math-functions">mathematical functions</a></td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/navigation_functions.md">Navigation functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/aggregate_analytic_functions.md">Aggregate analytic functions</a></td><td>See <a href="/documentation/dsls/sql/zetasql/aggregate-functions">aggregate functions</a></td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/hash_functions.md">Hash functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/string_functions.md">String functions</a></td><td>See <a href="/documentation/dsls/sql/zetasql/string-functions">string functions</a></td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/json_functions.md">JSON functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/array_functions.md">Array functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/date_functions.md">Date functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/datetime_functions.md">DateTime functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/time_functions.md">Time functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/timestamp_functions.md">Timestamp functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/protocol-buffers.md">Protocol buffer functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/security_functions.md">Security functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/net_functions.md">Net functions</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/operators.md">Operator precedence</a></td><td>Yes</td></tr>
  <tr><td><a href="">Conditional expressions</a></td><td>See <a href="/documentation/dsls/sql/zetasql/conditional-expressions">conditional expressions</a></td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/expression_subqueries.md">Expression subqueries</a></td><td>No</td></tr>
  <tr><td><a href="https://github.com/google/zetasql/blob/master/docs/debugging_functions.md">Debugging functions</a></td><td>No</td></tr>
</table>
