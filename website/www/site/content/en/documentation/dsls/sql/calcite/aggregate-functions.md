---
type: languages
title: "Beam Calcite SQL aggregate functions"
aliases: /documentation/dsls/sql/aggregate-functions/
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

# Beam Calcite SQL aggregate functions

This page documents Apache Calcite aggregate functions supported by Beam Calcite SQL.

{{< table >}}
| Operator syntax | Description |
| ---- | ---- |
| COUNT(*) | Returns the number of input rows |
| AVG(numeric) | Returns the average (arithmetic mean) of numeric across all input values |
| SUM(numeric) | Returns the sum of numeric across all input values |
| MAX(value) | Returns the maximum value of value across all input values |
| MIN(value) | Returns the minimum value of value across all input values |
{{< /table >}}
