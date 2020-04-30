---
type: languages
title: "Beam Calcite SQL scalar functions"
aliases: /documentation/dsls/sql/scalar-functions/
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

# Beam Calcite SQL scalar functions

This page documents the Apache Calcite functions supported by Beam Calcite SQL.

## Comparison functions and operators

{{< table >}}
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
{{< /table >}}

## Logical functions and operators

{{< table >}}
| Operator syntax | Description |
| ---- | ---- |
| boolean1 OR boolean2 | Whether boolean1 is TRUE or boolean2 is TRUE |
| boolean1 AND boolean2 | Whether boolean1 and boolean2 are both TRUE |
| NOT boolean | Whether boolean is not TRUE; returns UNKNOWN if boolean is UNKNOWN |
{{< /table >}}

## Arithmetic expressions

{{< table >}}
| Operator syntax | Description|
| ---- | ---- |
| numeric1 + numeric2 | Returns numeric1 plus numeric2|
| numeric1 - numeric2 | Returns numeric1 minus numeric2|
| numeric1 * numeric2 | Returns numeric1 multiplied by numeric2|
| numeric1 / numeric2 | Returns numeric1 divided by numeric2|
| MOD(numeric, numeric) | Returns the remainder (modulus) of numeric1 divided by numeric2. The result is negative only if numeric1 is negative|
{{< /table >}}

## Math functions

{{< table >}}
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
{{< /table >}}

## Date functions

{{< table >}}
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
{{< /table >}}

## String functions

{{< table >}}
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
{{< /table >}}

## Conditional functions

{{< table >}}
| Operator syntax | Description |
| ---- | ---- |
| CASE value <br>WHEN value1 [, value11 ]* THEN result1 <br>[ WHEN valueN [, valueN1 ]* THEN resultN ]* <br>[ ELSE resultZ ] <br>END | Simple case |
| CASE <br>WHEN condition1 THEN result1 <br>[ WHEN conditionN THEN resultN ]* <br>[ ELSE resultZ ] <br>END | Searched case |
| NULLIF(value, value) | Returns NULL if the values are the same. For example, NULLIF(5, 5) returns NULL; NULLIF(5, 0) returns 5. |
| COALESCE(value, value [, value ]*) | Provides a value if the first value is null. For example, COALESCE(NULL, 5) returns 5. |
{{< /table >}}
