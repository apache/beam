---
type: languages
title: "Beam ZetaSQL mathematical functions"
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

# Beam ZetaSQL mathematical functions

This page documents ZetaSQL scalar functions supported by Beam ZetaSQL.

All mathematical functions return `NULL` if any of the input parameters is `NULL`.

{{< table >}}
| Operator syntax | Description |
| ---- | ---- |
| MOD(X, Y) | Returns the remainder of the division of X by Y |
| CEIL(X) | Returns the smallest integral value (with FLOAT64 type) that is not less than X |
| CEILING(X) | Synonym of CEIL(X) |
| FLOOR(X) | Returns the largest integral value (with FLOAT64 type) that is not greater than X |
{{< /table >}}

## MOD

```
MOD(X, Y)
```

**Description**

Modulo function: returns the remainder of the division of X by Y. Returned value
has the same sign as X.

## CEIL

```
CEIL(X)
```

**Description**

Returns the smallest integral value (with FLOAT64
type) that is not less than X.

## CEILING

```
CEILING(X)
```

**Description**

Synonym of CEIL(X)

## FLOOR

```
FLOOR(X)
```

**Description**

Returns the largest integral value (with FLOAT64
type) that is not greater than X.

### Example rounding function behavior
Example behavior of Cloud Dataflow SQL rounding functions:

{{< table >}}
<table>
<thead>
<tr>
<th>Input "X"</th>
<th>CEIL(X)</th>
<th>FLOOR(X)</th>
</tr>
</thead>
<tbody>
<tr>
<td>2.0</td>
<td>2.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.3</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.8</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>2.5</td>
<td>3.0</td>
<td>2.0</td>
</tr>
<tr>
<td>-2.3</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>-2.8</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>-2.5</td>
<td>-2.0</td>
<td>-3.0</td>
</tr>
<tr>
<td>0</td>
<td>0</td>
<td>0</td>
</tr>
</tbody>
</table>
{{< /table >}}