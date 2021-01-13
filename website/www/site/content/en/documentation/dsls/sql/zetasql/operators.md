---
type: languages
title: "Beam ZetaSQL operators"
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

# Beam ZetaSQL operators

Operators are represented by special characters or keywords; they do not use
function call syntax. An operator manipulates any number of data inputs, also
called operands, and returns a result.

Common conventions:

+  Unless otherwise specified, all operators return `NULL` when one of the
   operands is `NULL`.

The following table lists all supported operators from highest to
lowest precedence. Precedence determines the order in which operators will be evaluated within a statement.

{{< table >}}
<table>
  <thead>
    <tr>
      <th>Order of Precedence</th>
      <th>Operator</th>
      <th>Input Data Types</th>
      <th>Name</th>
      <th>Operator Arity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>.</td>
      <td><span> STRUCT</span><br></td>
      <td>Member field access operator</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[ ]</td>
      <td>ARRAY</td>
      <td>Array position. Must be used with OFFSET or ORDINAL&mdash.</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>2</td>
      <td>-</td>
      <td>All numeric types</td>
      <td>Unary minus</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>3</td>
      <td>*</td>
      <td>All numeric types</td>
      <td>Multiplication</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>/</td>
      <td>All numeric types</td>
      <td>Division</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>4</td>
      <td>+</td>
      <td>All numeric types</td>
      <td>Addition</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>-</td>
      <td>All numeric types</td>
      <td>Subtraction</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>5 (Comparison Operators)</td>
      <td>=</td>
      <td>Any comparable type. See
      <a href="/documentation/dsls/sql/zetasql/data-types">Data Types</a> for
      a complete list.</td>
      <td>Equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;</td>
      <td>Any comparable type. See
      <a href="/documentation/dsls/sql/zetasql/data-types">Data Types</a> for
      a complete list.</td>
      <td>Less than</td>
      <td>Binary</td>
      </tr>
      <tr>
      <td>&nbsp;</td>
      <td>&gt;</td>
      <td>Any comparable type. See
      <a href="/documentation/dsls/sql/zetasql/data-types">Data Types</a> for
      a complete list.</td>
      <td>Greater than</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&lt;=</td>
      <td>Any comparable type. See
      <a href="/documentation/dsls/sql/zetasql/data-types">Data Types</a> for
      a complete list.</td>
      <td>Less than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>&gt;=</td>
      <td>Any comparable type. See
      <a href="/documentation/dsls/sql/zetasql/data-types">Data Types</a> for
      a complete list.</td>
      <td>Greater than or equal to</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>!=, &lt;&gt;</td>
      <td>Any comparable type. See
      <a href="/documentation/dsls/sql/zetasql/data-types">Data Types</a> for
      a complete list.</td>
      <td>Not equal</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] LIKE</td>
      <td>STRING and byte</td>
      <td>Value does [not] match the pattern specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] BETWEEN</td>
      <td>Any comparable types. See Data Types for list.</td>
      <td>Value is [not] within the range specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>[NOT] IN</td>
      <td>Any comparable types. See Data Types for list.</td>
      <td>Value is [not] in the set of values specified</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] <code>NULL</code></td>
      <td>All</td>
      <td>Value is [not] <code>NULL</code></td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] TRUE</td>
      <td>BOOL</td>
      <td>Value is [not] TRUE.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>&nbsp;</td>
      <td>IS [NOT] FALSE</td>
      <td>BOOL</td>
      <td>Value is [not] FALSE.</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>6</td>
      <td>NOT</td>
      <td>BOOL</td>
      <td>Logical NOT</td>
      <td>Unary</td>
    </tr>
    <tr>
      <td>7</td>
      <td>AND</td>
      <td>BOOL</td>
      <td>Logical AND</td>
      <td>Binary</td>
    </tr>
    <tr>
      <td>8</td>
      <td>OR</td>
      <td>BOOL</td>
      <td>Logical OR</td>
      <td>Binary</td>
    </tr>
  </tbody>
</table>
{{< /table >}}

Operators with the same precedence are left associative. This means that those
operators are grouped together starting from the left and moving right. For
example, the expression:

`x AND y AND z`

is interpreted as

`( ( x AND y ) AND z )`

The expression:

```
x * y / z
```

is interpreted as:

```
( ( x * y ) / z )
```

All comparison operators have the same priority and are grouped using left
associativity. However, comparison operators are not associative. As a result,
it is recommended that you use parentheses to improve readability and ensure
expressions are resolved as desired. For example:

`(x < y) IS FALSE`

is recommended over:

`x < y IS FALSE`

## Element access operators

{{< table >}}
<table>
<thead>
<tr>
<th>Operator</th>
<th>Syntax</th>
<th>Input Data Types</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>.</td>
<td>expression.fieldname1...</td>
<td><span> STRUCT</span><br></td>
<td>Type T stored in fieldname1</td>
<td>Dot operator. Can be used to access nested fields,
e.g.expression.fieldname1.fieldname2...</td>
</tr>
<tr>
<td>[ ]</td>
<td>array_expression [position_keyword (int_expression ) ]</td>
<td>See ARRAY Functions.</td>
<td>Type T stored in ARRAY</td>
<td>position_keyword is either OFFSET or ORDINAL.</td>
</tr>
</tbody>
</table>
{{< /table >}}

## Arithmetic operators

All arithmetic operators accept input of numeric type T, and the result type
has type T unless otherwise indicated in the description below:

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
</tr>
</thead>
<tbody>
<tr>
<td>Addition</td>
<td>X + Y</td>
</tr>
<tr>
<td>Subtraction</td>
<td>X - Y</td>
</tr>
<tr>
<td>Multiplication</td>
<td>X * Y</td>
</tr>
<tr>
<td>Division</td>
<td>X / Y</td>
</tr>
<tr>
<td>Unary Minus</td>
<td>- X</td>
</tr>
</tbody>
</table>
{{< /table >}}

Result types for Addition and Multiplication:

{{< table >}}
<table>
<thead>
<tr><th>&nbsp;</th><th>INT64</th><th>FLOAT64</th></tr>
</thead>
<tbody><tr><td>INT64</td><td>INT64</td><td>FLOAT64</td></tr><tr><td>FLOAT64</td><td>FLOAT64</td><td>FLOAT64</td></tr></tbody>
</table>
{{< /table >}}

Result types for Subtraction:

{{< table >}}
<table>
<thead>
<tr><th>&nbsp;</th><th>INT64</th><th>FLOAT64</th></tr>
</thead>
<tbody><tr><td>INT64</td><td>INT64</td><td>FLOAT64</td></tr><tr><td>FLOAT64</td><td>FLOAT64</td><td>FLOAT64</td></tr></tbody>
</table>
{{< /table >}}

Result types for Division:

{{< table >}}
<table>
<thead>
  <tr><th>&nbsp;</th><th>INT64</th><th>FLOAT64</th></tr>
</thead>
<tbody><tr><td>INT64</td><td>FLOAT64</td><td>FLOAT64</td></tr><tr><td>FLOAT64</td><td>FLOAT64</td><td>FLOAT64</td></tr></tbody>
</table>
{{< /table >}}

Result types for Unary Minus:

{{< table >}}
<table>
<thead>
<tr>
<th>Input Data Type</th>
<th>Result Data Type</th>
</tr>
</thead>
<tbody>

<tr>
<td>INT64</td>
<td>INT64</td>
</tr>

<tr>
<td>FLOAT64</td>
<td>FLOAT64</td>
</tr>

</tbody>
</table>
{{< /table >}}

## Logical operators

All logical operators allow only BOOL input.

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Logical NOT</td>
<td nowrap>NOT X</td>
<td>Returns FALSE if input is TRUE. Returns TRUE if input is FALSE. Returns <code>NULL</code>
otherwise.</td>
</tr>
<tr>
<td>Logical AND</td>
<td nowrap>X AND Y</td>
<td>Returns FALSE if at least one input is FALSE. Returns TRUE if both X and Y
are TRUE. Returns <code>NULL</code> otherwise.</td>
</tr>
<tr>
<td>Logical OR</td>
<td nowrap>X OR Y</td>
<td>Returns FALSE if both X and Y are FALSE. Returns TRUE if at least one input
is TRUE. Returns <code>NULL</code> otherwise.</td>
</tr>
</tbody>
</table>
{{< /table >}}

## Comparison operators

Comparisons always return BOOL. Comparisons generally
require both operands to be of the same type. If operands are of different
types, and if Cloud Dataflow SQL can convert the values of those types to a
common type without loss of precision, Cloud Dataflow SQL will generally coerce
them to that common type for the comparison; Cloud Dataflow SQL will generally
[coerce literals to the type of non-literals](/documentation/dsls/sql/zetasql/conversion-rules/#coercion), where
present. Comparable data types are defined in
[Data Types](/documentation/dsls/sql/zetasql/data-types).



STRUCTs support only 4 comparison operators: equal
(=), not equal (!= and <>), and IN.

The following rules apply when comparing these data types:

+  FLOAT64
   : All comparisons with NaN return FALSE,
   except for `!=` and `<>`, which return TRUE.
+  BOOL: FALSE is less than TRUE.
+  STRING: Strings are
   compared codepoint-by-codepoint, which means that canonically equivalent
   strings are only guaranteed to compare as equal if
   they have been normalized first.
+  `NULL`: The convention holds here: any operation with a `NULL` input returns
   `NULL`.

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Syntax</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Less Than</td>
<td>X &lt; Y</td>
<td>Returns TRUE if X is less than Y.</td>
</tr>
<tr>
<td>Less Than or Equal To</td>
<td>X &lt;= Y</td>
<td>Returns TRUE if X is less than or equal to Y.</td>
</tr>
<tr>
<td>Greater Than</td>
<td>X &gt; Y</td>
<td>Returns TRUE if X is greater than Y.</td>
</tr>
<tr>
<td>Greater Than or Equal To</td>
<td>X &gt;= Y</td>
<td>Returns TRUE if X is greater than or equal to Y.</td>
</tr>
<tr>
<td>Equal</td>
<td>X = Y</td>
<td>Returns TRUE if X is equal to Y.</td>
</tr>
<tr>
<td>Not Equal</td>
<td>X != Y<br>X &lt;&gt; Y</td>
<td>Returns TRUE if X is not equal to Y.</td>
</tr>
<tr>
<td>BETWEEN</td>
<td>X [NOT] BETWEEN Y AND Z</td>
<td>Returns TRUE if X is [not] within the range specified. The result of "X
BETWEEN Y AND Z" is equivalent to "Y &lt;= X AND X &lt;= Z" but X is evaluated
only once in the former.</td>
</tr>
<tr>
<td>LIKE</td>
<td>X [NOT] LIKE Y</td>
<td>Checks if the STRING in the first operand X
matches a pattern specified by the second operand Y. Expressions can contain
these characters:
<ul>
<li>A percent sign "%" matches any number of characters or bytes</li>
<li>An underscore "_" matches a single character or byte</li>
<li>You can escape "\", "_", or "%" using two backslashes. For example, <code>
"\\%"</code>. If you are using raw strings, only a single backslash is
required. For example, <code>r"\%"</code>.</li>
</ul>
</td>
</tr>
<tr>
<td>IN</td>
<td>Multiple - see below</td>
<td>Returns FALSE if the right operand is empty. Returns <code>NULL</code> if the left
operand is <code>NULL</code>. Returns TRUE or <code>NULL</code>, never FALSE, if the right operand
contains <code>NULL</code>. Arguments on either side of IN are general expressions. Neither
operand is required to be a literal, although using a literal on the right is
most common. X is evaluated only once.</td>
</tr>
</tbody>
</table>
{{< /table >}}

When testing values that have a STRUCT data type for
equality, it's possible that one or more fields are `NULL`. In such cases:

+ If all non-NULL field values are equal, the comparison returns NULL.
+ If any non-NULL field values are not equal, the comparison returns false.

The following table demonstrates how STRUCT data
types are compared when they have fields that are `NULL` valued.

{{< table >}}
<table>
<thead>
<tr>
<th>Struct1</th>
<th>Struct2</th>
<th>Struct1 = Struct2</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>NULL</code></td>
</tr>
<tr>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>STRUCT(2, NULL)</code></td>
<td><code>FALSE</code></td>
</tr>
<tr>
<td><code>STRUCT(1,2)</code></td>
<td><code>STRUCT(1, NULL)</code></td>
<td><code>NULL</code></td>
</tr>
</tbody>
</table>
{{< /table >}}



## IS operators

IS operators return TRUE or FALSE for the condition they are testing. They never
return `NULL`, even for `NULL` inputs. If NOT is present, the output BOOL value
is inverted.

{{< table >}}
<table>
<thead>
<tr>
<th>Function Syntax</th>
<th>Input Data Type</th>
<th>Result Data Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
  <td><pre>X IS [NOT] NULL</pre></td>
<td>Any value type</td>
<td>BOOL</td>
<td>Returns TRUE if the operand X evaluates to <code>NULL</code>, and returns FALSE
otherwise.</td>
</tr>
<tr>
  <td><pre>X IS [NOT] TRUE</pre></td>
<td>BOOL</td>
<td>BOOL</td>
<td>Returns TRUE if the BOOL operand evaluates to TRUE. Returns FALSE
otherwise.</td>
</tr>
<tr>
  <td><pre>X IS [NOT] FALSE</pre></td>
<td>BOOL</td>
<td>BOOL</td>
<td>Returns TRUE if the BOOL operand evaluates to FALSE. Returns FALSE
otherwise.</td>
</tr>
</tbody>
</table>
{{< /table >}}