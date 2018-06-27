---
layout: section
title: "Beam SQL: Lexical Structure"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/lexical/
---

# Beam SQL Lexical Structure

A Beam SQL statement comprises a series of tokens. Tokens include
*identifiers,* *quoted identifiers, literals*, *keywords*, *operators*,
and *special characters*. Tokens can be separated by whitespace (space,
backspace, tab, newline) or comments.

Identifiers
-----------

Identifiers are names that are associated with columns, tables, and
other database objects.

Identifiers must begin with a letter or an underscore. Subsequent
characters can be letters, numbers, or underscores. Quoted identifiers
are identifiers enclosed by backtick (`` ` ``) characters and can contain
any character, such as spaces or symbols. However, quoted identifiers
cannot be empty. [Reserved Keywords](#reserved-keywords) can only be used
as identifiers if enclosed by backticks.

Syntax (defined here as a regular expression):

`[A-Za-z_][A-Za-z_0-9]*`

Examples:

```
Customers5
_dataField1
ADGROUP
```

Invalid examples:

```
5Customers
_dataField!
GROUP
```

`5Customers` begins with a number, not a letter or underscore.
`_dataField!` contains the special character "!" which is not a letter,
number, or underscore. `GROUP` is a reserved keyword, and therefore
cannot be used as an identifier without being enclosed by backtick
characters.

Both identifiers and quoted identifiers are case insensitive, with some
nuances. See [Case Sensitivity](#case-sensitivity) for further details.

Quoted identifiers have the same escape sequences as string literals,
defined below.

Literals
--------

A literal represents a constant value of a built-in data type. Some, but
not all, data types can be expressed as literals.

### String Literals

Both string and bytes literals must be *quoted* with single
(`'`) quotation mark.

**Quoted literals:**

<table>
<thead>
<tr>
<th>Literal</th>
<th>Examples</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Quoted string</td>
<td><ul><li><code>'it''s'</code></li><li><code>'Title: "Boy"'</code></li></ul></td>
<td>Quoted strings enclosed by single (<code>'</code>) quotes can contain unescaped double (<code>"</code>) quotes. <br />Two quotation marks (<code>''</code>) is the escape sequence.<br />Quoted strings can contain newlines.</td>
</tr>
</tbody>
</table>

### Integer Literals

Integer literals are either a sequence of decimal digits (0 through 9).
Integers can be prefixed by "`+`" or "`-`" to represent positive and
negative values, respectively.

Examples:

```
123
-123
```

An integer literal is interpreted as an `BIGINT`.

### Floating Point Literals

Syntax options:

```
[+-]DIGITS.[DIGITS][e[+-]DIGITS]
[DIGITS].DIGITS[e[+-]DIGITS]
DIGITSe[+-]DIGITS
```

`DIGITS` represents one or more decimal numbers (0 through 9) and `e`
represents the exponent marker (e or E).

Examples:

```
123.456e-67
.1E4
58.
4e2
```

Numeric literals that contain either a decimal point or an exponent
marker are presumed to be type double.

Implicit coercion of floating point literals to float type is possible
if the value is within the valid float range.

There is no literal representation of NaN or infinity.

### Array Literals

Array literals are a comma-separated lists of elements enclosed in
square brackets prefixed with the `ARRAY` keyword.

Examples:

```
ARRAY[1, 2, 3]
ARRAY['x', 'y', 'xy']
```

### Struct Literals

Syntax:

```
(elem[, elem...])
```

where `elem` is an element in the struct. `elem` must be a literal data
type, not an expression or column name.

The output type is an anonymous struct type (structs are not named
types) with anonymous fields with types matching the types of the input
expressions.

<table>
<thead>
<tr>
<th>Example</th>
<th>Output Type</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>(1, 2, 3)</code></td>
<td><code>STRUCT&lt;BIGINT,BIGINT,BIGINT&gt;</code></td>
</tr>
<tr>
<td><code>(1, 'abc')</code></td>
<td><code>STRUCT&lt;BIGINT,STRING&gt;</code></td>
</tr>
</tbody>
</table>

### Date Literals

Syntax:

```
DATE 'YYYY-M[M]-D[D]'
```

Date literals contain the `DATE` keyword followed by a string literal
that conforms to the canonical date format, enclosed in single quotation
marks. Date literals support a range between the years 1 and 9999,
inclusive. Dates outside of this range are invalid.

For example, the following date literal represents September 27, 2014:

```
DATE '2014-09-27'
```

String literals in canonical date format also implicitly coerce to DATE
type when used where a DATE-type expression is expected. For example, in
the query

```
SELECT * FROM foo WHERE date_col = "2014-09-27"
```

the string literal `"2014-09-27"` will be coerced to a date literal.

### Time Literals

Syntax:

```
TIME '[H]H:[M]M:[S]S[.DDDDDD]]'
```

TIME literals contain the `TIME` keyword and a string literal that
conforms to the canonical time format, enclosed in single quotation
marks.

For example, the following time represents 12:30 p.m.:

```
TIME '12:30:00.45'
```

### Timestamp literals

Syntax:

```
TIMESTAMP 'YYYY-[M]M-[D]D [[H]H:[M]M:[S]S[.DDDDDD]]'
```

Timestamp literals contain the `TIMESTAMP` keyword and a string literal
that conforms to the canonical timestamp format, enclosed in single
quotation marks.

Timestamp literals support a range between the years 1 and 9999,
inclusive. Timestamps outside of this range are invalid.

For example, the following timestamp represents 12:30 p.m. on September
27, 2014:

```
TIMESTAMP '2014-09-27 12:30:00.45'
```

Case Sensitivity
----------------

Beam SQL follows these rules for case sensitivity:

<table>
<thead>
<tr>
<th>Category</th>
<th>Case Sensitive?</th>
<th>Notes</th>
</tr>
</thead>
<tbody>
<tr>
<td>Keywords</td>
<td>No</td>
<td></td>
</tr>
<tr>
<td>Function names</td>
<td>No</td>
<td></td>
</tr>
<tr>
<td>Table names</td>
<td>Yes</td>
<td></td>
</tr>
<tr>
<td>Column names</td>
<td>Yes</td>
<td></td>
</tr>
<tr>
<td>String values</td>
<td>Yes</td>
<td></td>
</tr>
<tr>
<td>String comparisons</td>
<td>Yes</td>
<td></td>
</tr>
<tr>
<td>Aliases within a query</td>
<td>No</td>
<td></td>
</tr>
<tr>
<td>Regular expression matching</td>
<td>See Notes</td>
<td>Regular expression matching is case sensitive by default, unless the expression itself specifies that it should be case insensitive.</td>
</tr>
<tr>
<td><code>LIKE</code> matching</td>
<td>Yes</td>
<td>&nbsp;</td>
</tr>
</tbody>
</table>

Reserved Keywords
-----------------

Keywords are a group of tokens that have special meaning in the Beam SQL
language, and have the following characteristics:

-   Keywords cannot be used as identifiers unless enclosed by backtick
    (\`) characters.
-   Keywords are case insensitive.

Beam SQL has the following reserved keywords.

<table style="table-layout: fixed; width: 110%">
<tbody>
<tr>
<td>
ALL<br />
AND<br />
ANY<br />
ARRAY<br />
AS<br />
ASC<br />
ASSERT_ROWS_MODIFIED<br />
AT<br />
BETWEEN<br />
BY<br />
CASE<br />
CAST<br />
COLLATE<br />
CONTAINS<br />
CREATE<br />
CROSS<br />
CUBE<br />
CURRENT<br />
DEFAULT<br />
DEFINE<br />
DESC<br />
DISTINCT<br />
ELSE<br />
END<br />
</td>
<td>
ENUM<br />
ESCAPE<br />
EXCEPT<br />
EXCLUDE<br />
EXISTS<br />
EXTRACT<br />
FALSE<br />
FETCH<br />
FOLLOWING<br />
FOR<br />
FROM<br />
FULL<br />
GROUP<br />
GROUPING<br />
GROUPS<br />
HASH<br />
HAVING<br />
IF<br />
IGNORE<br />
IN<br />
INNER<br />
INTERSECT<br />
INTERVAL<br />
INTO<br />
</td>
<td>
IS<br />
JOIN<br />
LATERAL<br />
LEFT<br />
LIKE<br />
LIMIT<br />
LOOKUP<br />
MERGE<br />
NATURAL<br />
NEW<br />
NO<br />
NOT<br />
NULL<br />
NULLS<br />
OF<br />
ON<br />
OR<br />
ORDER<br />
OUTER<br />
OVER<br />
PARTITION<br />
PRECEDING<br />
PROTO<br />
RANGE<br />
</td>
<td>
RECURSIVE<br />
RESPECT<br />
RIGHT<br />
ROLLUP<br />
ROWS<br />
SELECT<br />
SET<br />
SOME<br />
STRUCT<br />
TABLESAMPLE<br />
THEN<br />
TO<br />
TREAT<br />
TRUE<br />
UNBOUNDED<br />
UNION<br />
UNNEST<br />
USING<br />
WHEN<br />
WHERE<br />
WINDOW<br />
WITH<br />
WITHIN<br />
</td>
</tr>
</tbody>
</table>

Terminating Semicolons
----------------------

Statements can optionally use a terminating semicolon (`;`) in the
context of a query string submitted through an Application Programming
Interface (API). Some interactive tools require statements to have a
terminating semicolon. In a request containing multiple statements,
statements must be separated by semicolons, but the semicolon is
optional for the final statement.

Comments
--------

Comments are sequences of characters that are ignored by the parser.
Beam SQL supports the following types of comments.

### Single line comments

Single line comments are supported by prepending `--` before the comment.

**Examples**

`SELECT x FROM T; --x is a field and T is a table`

Comment includes all characters from the '`--`' sequence to the end of
the line. You can optionally add a space after the '`--`'.

### Multiline comments

Multiline comments are supported by enclosing the comment using
`/* <comment> */`.

**Example:**

```
SELECT x FROM T /* x is a field and T is a table */
WHERE x = 3;
```

**Invalid example:**

```
SELECT x FROM T /* comment starts here
                /* comment ends on this line */
                this line is not considered a comment */
WHERE x = 3;
```

Comment includes all characters, including newlines, enclosed by the
first occurrence of '`/*`' and the first subsequent occurrence of
'`*/`'. Nested comments are not supported. The second example contains a
nested comment that renders the query invalid.

> Portions of this page are modifications based on work created and
> [shared by Google](https://developers.google.com/terms/site-policies)
> and used according to terms described in the [Creative Commons 3.0
> Attribution License](http://creativecommons.org/licenses/by/3.0/).
