---
type: languages
title: "Beam ZetaSQL string functions"
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

# Beam ZetaSQL string functions

This page documents the ZetaSQL string functions supported by Beam ZetaSQL.

These string functions work on STRING data. STRING values must be well-formed UTF-8. All string comparisons are done byte-by-byte, without regard to Unicode
canonical equivalence.

{{< table >}}
| Operator syntax | Description |
| ---- | ---- |
| [CHAR_LENGTH(value)](#char_length) | Returns the length of the string in characters |
| [CHARACTER_LENGTH(value)](#character_length) | Synonym for CHAR_LENGTH |
| [CONCAT(value1[, ...])](#concat) | Concatenates up to five values into a single result |
| [ENDS_WITH(value1, value2)](#ends_with) | Returns TRUE if the second value is a suffix of the first |
| [LTRIM(value1[, value2])](#ltrim) | Identical to TRIM, but only removes leading characters. |
| [REPLACE(original_value, from_value, to_value)](#replace) | Replaces all occurrences of `from_value` with `to_value` in `original_value` |
| [REVERSE(value)](#reverse) | Returns the reverse of the input string |
| [RTRIM(value1[, value2])](#rtrim) | Identical to TRIM, but only removes trailing characters |
| [STARTS_WITH(value1, value2)](#starts_with) | Returns TRUE if the second value is a prefix of the first. |
| [SUBSTR(value, position[, length])](#substr) | Returns a substring of the supplied value |
| [TRIM(value1[, value2])](#trim) | Removes all leading and trailing characters that match `value2` |
{{< /table >}}

## CHAR_LENGTH

```
CHAR_LENGTH(value)
```

**Description**

Returns the length of the STRING in characters.

**Return type**

INT64


**Examples**

```

Table example:

+----------------+
| characters     |
+----------------+
| абвгд          |
+----------------+

SELECT
  characters,
  CHAR_LENGTH(characters) AS char_length_example
FROM example;

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+

```
## CHARACTER_LENGTH
```
CHARACTER_LENGTH(value)
```

**Description**

Synonym for [CHAR_LENGTH](#char_length).

**Return type**

INT64


**Examples**

```

Table example:

+----------------+
| characters     |
+----------------+
| абвгд          |
+----------------+

SELECT
  characters,
  CHARACTER_LENGTH(characters) AS char_length_example
FROM example;

+------------+---------------------+
| characters | char_length_example |
+------------+---------------------+
| абвгд      |                   5 |
+------------+---------------------+

```


## CONCAT
```
CONCAT(value1[, ...])
```

**Description**

Concatenates up to five values into a single
result.

**Return type**

STRING

**Examples**

```

Table Employees:

+-------------+-----------+
| first_name  | last_name |
+-------------+-----------+
| John        | Doe       |
| Jane        | Smith     |
| Joe         | Jackson   |
+-------------+-----------+

SELECT
  CONCAT(first_name, " ", last_name)
  AS full_name
FROM Employees;

+---------------------+
| full_name           |
+---------------------+
| John Doe            |
| Jane Smith          |
| Joe Jackson         |
+---------------------+
```

## ENDS_WITH
```
ENDS_WITH(value1, value2)
```

**Description**

Takes two values. Returns TRUE if the second value is a
suffix of the first.

**Return type**

BOOL


**Examples**

```

Table items:

+----------------+
| item           |
+----------------+
| apple          |
| banana         |
| orange         |
+----------------+

SELECT
  ENDS_WITH(item, "e") as example
FROM items;

+---------+
| example |
+---------+
|    True |
|   False |
|    True |
+---------+

```

## LTRIM
```
LTRIM(value1[, value2])
```

**Description**

Identical to [TRIM](#trim), but only removes leading characters.

**Return type**

STRING

**Examples**

```

Table items:

+----------------+
| item           |
+----------------+
|    apple       |
|    banana      |
|    orange      |
+----------------+

SELECT
  CONCAT("#", LTRIM(item), "#") as example
FROM items;

+-------------+
| example     |
+-------------+
| #apple   #  |
| #banana   # |
| #orange   # |
+-------------+


Table items:

+----------------+
| item           |
+----------------+
| ***apple***    |
| ***banana***   |
| ***orange***   |
+----------------+

SELECT
  LTRIM(item, "*") as example
FROM items;

+-----------+
| example   |
+-----------+
| apple***  |
| banana*** |
| orange*** |
+-----------+


Table items:

+----------------+
| item           |
+----------------+
| xxxapplexxx    |
| yyybananayyy   |
| zzzorangezzz   |
| xyzpearzyz     |
+----------------+

SELECT
  LTRIM(item, "xyz") as example
FROM items;

+-----------+
| example   |
+-----------+
| applexxx  |
| bananayyy |
| orangezzz |
| pearxyz   |
+-----------+
```

## REPLACE
```
REPLACE(original_value, from_value, to_value)
```

**Description**

Replaces all occurrences of `from_value` with `to_value` in `original_value`.
If `from_value` is empty, no replacement is made.

**Return type**

STRING

**Examples**

```sql

+--------------------+
| dessert            |
+--------------------+
| apple pie          |
| blackberry pie     |
| cherry pie         |
+--------------------+

SELECT
  REPLACE (dessert, "pie", "cobbler") as example
FROM desserts;

+--------------------+
| example            |
+--------------------+
| apple cobbler      |
| blackberry cobbler |
| cherry cobbler     |
+--------------------+
```

## REVERSE

```
REVERSE(value)
```

**Description**

Returns the reverse of the input STRING.

**Return type**

STRING

**Examples**

```
WITH example AS (
  SELECT "foo" AS sample_string UNION ALL
  SELECT "абвгд" AS sample_string
)
SELECT
  sample_string,
  REVERSE(sample_string) AS reverse_string
FROM example;

+---------------+----------------+
| sample_string | reverse_string |
+---------------+----------------+
| foo           | oof            |
| абвгд         | дгвба          |
+---------------+----------------+
```

## RTRIM
```
RTRIM(value1[, value2])
```

**Description**

Identical to [TRIM](#trim), but only removes trailing characters.

**Return type**

STRING

**Examples**

```

Table items:

+----------------+
| item           |
+----------------+
| ***apple***    |
| ***banana***   |
| ***orange***   |
+----------------+

SELECT
  RTRIM(item, "*") as example
FROM items;

+-----------+
| example   |
+-----------+
| ***apple  |
| ***banana |
| ***orange |
+-----------+


Table items:

+----------------+
| item           |
+----------------+
| applexxx       |
| bananayyy      |
| orangezzz      |
| pearxyz        |
+----------------+

SELECT
  RTRIM(item, "xyz") as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
| pear    |
+---------+
```

## STARTS_WITH
```
STARTS_WITH(value1, value2)
```

**Description**

Takes two values. Returns TRUE if the second value is a prefix
of the first.

**Return type**

BOOL

**Examples**

```

SELECT
  STARTS_WITH(item, "b") as example
FROM (
  SELECT "foo" as item
  UNION ALL SELECT "bar" as item
  UNION ALL SELECT "baz" as item) AS items;


+---------+
| example |
+---------+
|   False |
|    True |
|    True |
+---------+
```
## SUBSTR
```
SUBSTR(value, position[, length])
```

**Description**

Returns a substring of the supplied value. The
`position` argument is an integer specifying the starting position of the
substring, with position = 1 indicating the first character or byte. The
`length` argument is the maximum number of characters for STRING arguments.

If `position` is negative, the function counts from the end of `value`,
with -1 indicating the last character.

If `position` is a position off the left end of the
STRING (`position` = 0 or
`position` &lt; `-LENGTH(value)`), the function starts
from position = 1. If `length` exceeds the length of
`value`, returns fewer than `length` characters.

If `length` is less than 0, the function returns an error.

**Return type**

STRING

**Examples**

```

Table items:

+----------------+
| item           |
+----------------+
| apple          |
| banana         |
| orange         |
+----------------+

SELECT
  SUBSTR(item, 2) as example
FROM items;

+---------+
| example |
+---------+
| pple    |
| anana   |
| range   |
+---------+


Table items:

+----------------+
| item           |
+----------------+
| apple          |
| banana         |
| orange         |
+----------------+

SELECT
  SUBSTR(item, 2, 2) as example
FROM items;

+---------+
| example |
+---------+
| pp      |
| an      |
| ra      |
+---------+


Table items:

+----------------+
| item           |
+----------------+
| apple          |
| banana         |
| orange         |
+----------------+

SELECT
  SUBSTR(item, -2) as example
FROM items;

+---------+
| example |
+---------+
| le      |
| na      |
| ge      |
+---------+
```
## TRIM
```
TRIM(value1[, value2])
```

**Description**

Removes all leading and trailing characters that match `value2`. If `value2` is
not specified, all leading and trailing whitespace characters (as defined by the
Unicode standard) are removed.

If `value2` contains more than one character, the function removes all leading
or trailing characters contained in `value2`.

**Return type**

STRING

**Examples**

```

Table items:

+----------------+
| item           |
+----------------+
|    apple       |
|    banana      |
|    orange      |
+----------------+

SELECT
  CONCAT("#", TRIM(item), "#") as example
FROM items;

+----------+
| example  |
+----------+
| #apple#  |
| #banana# |
| #orange# |
+----------+


Table items:

+----------------+
| item           |
+----------------+
| ***apple***    |
| ***banana***   |
| ***orange***   |
+----------------+

SELECT
  TRIM(item, "*") as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
+---------+


Table items:

+----------------+
| item           |
+----------------+
| xxxapplexxx    |
| yyybananayyy   |
| zzzorangezzz   |
| xyzpearxyz     |
+----------------+

SELECT
  TRIM(item, "xyz") as example
FROM items;

+---------+
| example |
+---------+
| apple   |
| banana  |
| orange  |
| pear    |
+---------+
```