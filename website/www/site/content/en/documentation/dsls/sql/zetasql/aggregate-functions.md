---
type: languages
title: "Beam ZetaSQL aggregate functions"
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

# Beam ZetaSQL aggregate functions

This page documents the ZetaSQL aggregate functions supported by Beam ZetaSQL.

{{< table >}}
| Operator syntax | Description |
| ---- | ---- |
| [COUNT(*)](#count) | Returns the number of input rows |
| [AVG(FLOAT64)](#avg) | Returns the average of non-`NULL` input values |
| [SUM(numeric)](#sum) | Returns the sum of non-`NULL` values |
| [MAX(value)](#max) | Returns the maximum non-`NULL` value |
| [MIN(value)](#min) | Returns the minimum non-`NULL` value |
{{< /table >}}

## AVG

```
AVG(expression)
```

**Description**

Returns the average of non-`NULL` input values.

**Supported Argument Types**

FLOAT64. Note that, for floating point input types, the return result
is non-deterministic, which means you might receive a different result each time
you use this function.

**Returned Data Types**

+ FLOAT64


**Examples**

```
SELECT AVG(x) as avg
FROM UNNEST([0, 2, NULL, 4, 4, 5]) as x;

+-----+
| avg |
+-----+
| 3   |
+-----+

```

## COUNT

1. `COUNT(*)`

2. `COUNT(expression)`

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

**Supported Argument Types**

`expression` can be any data type.

**Return Data Types**

INT64

**Examples**

```
SELECT COUNT(*) AS count_star, COUNT(x) AS count_x
FROM UNNEST([1, 4, NULL, 4, 5]) AS x;

+------------+---------+
| count_star | count_x |
+------------+---------+
| 5          | 4       |
+------------+---------+


```

## MAX
```
MAX(expression)
```

**Description**

Returns the maximum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

Any data type except:
+ `ARRAY`
+ `STRUCT`

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```
SELECT MAX(x) AS max
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+-----+
| max |
+-----+
| 55  |
+-----+


```

## MIN
```
MIN(expression)
```

**Description**

Returns the minimum value of non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

**Supported Argument Types**

Any data type except:
+ `ARRAY`
+ `STRUCT`

**Return Data Types**

Same as the data type used as the input values.

**Examples**

```
SELECT MIN(x) AS min
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

+-----+
| min |
+-----+
| 4   |
+-----+


```

## SUM
```
SUM(expression)
```

**Description**

Returns the sum of non-null values.

If the expression is a floating point value, the sum is non-deterministic, which means you might receive a different result each time you use this function.

**Supported Argument Types**

Any supported numeric data types.

**Return Data Types**

+ Returns INT64 if the input is an integer.
+ Returns FLOAT64 if the input is a floating point
value.

Returns `NULL` if the input contains only `NULL`s.

**Examples**

```
SELECT SUM(x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

+-----+
| sum |
+-----+
| 25  |
+-----+


```