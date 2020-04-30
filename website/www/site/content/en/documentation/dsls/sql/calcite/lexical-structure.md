---
type: languages
title: "Beam Calcite SQL lexical structure"
aliases: 
  - /documentation/dsls/sql/lexical/
  - /documentation/dsls/sql/calcite/lexical/
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

# Beam Calcite SQL lexical structure

A Beam Calcite SQL statements are comprised of a series of tokens. Tokens include
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
A<br />
ABS<br />
ABSOLUTE<br />
ACTION<br />
ADA<br />
ADD<br />
ADMIN<br />
AFTER<br />
ALL<br />
ALLOCATE<br />
ALLOW<br />
ALTER<br />
ALWAYS<br />
AND<br />
ANY<br />
APPLY<br />
ARE<br />
ARRAY<br />
ARRAY_MAX_CARDINALITY<br />
AS<br />
ASC<br />
ASENSITIVE<br />
ASSERTION<br />
ASSIGNMENT<br />
ASYMMETRIC<br />
AT<br />
ATOMIC<br />
ATTRIBUTE<br />
ATTRIBUTES<br />
AUTHORIZATION<br />
AVG<br />
BEFORE<br />
BEGIN<br />
BEGIN_FRAME<br />
BEGIN_PARTITION<br />
BERNOULLI<br />
BETWEEN<br />
BIGINT<br />
BINARY<br />
BIT<br />
BLOB<br />
BOOLEAN<br />
BOTH<br />
BREADTH<br />
BY<br />
C<br />
CALL<br />
CALLED<br />
CARDINALITY<br />
CASCADE<br />
CASCADED<br />
CASE<br />
CAST<br />
CATALOG<br />
CATALOG_NAME<br />
CEIL<br />
CEILING<br />
CENTURY<br />
CHAIN<br />
CHAR<br />
CHAR_LENGTH<br />
CHARACTER<br />
CHARACTER_LENGTH<br />
CHARACTER_SET_CATALOG<br />
CHARACTER_SET_NAME<br />
CHARACTER_SET_SCHEMA<br />
CHARACTERISTICS<br />
CHARACTERS<br />
CHECK<br />
CLASSIFIER<br />
CLASS_ORIGIN<br />
CLOB<br />
CLOSE<br />
COALESCE<br />
COBOL<br />
COLLATE<br />
COLLATION<br />
COLLATION_CATALOG<br />
COLLATION_NAME<br />
COLLATION_SCHEMA<br />
COLLECT<br />
COLUMN<br />
COLUMN_NAME<br />
COMMAND_FUNCTION<br />
COMMAND_FUNCTION_CODE<br />
COMMENT<br />
COMMIT<br />
COMMITTED<br />
CONDITION<br />
CONDITION_NUMBER<br />
CONNECT<br />
CONNECTION<br />
CONNECTION_NAME<br />
CONSTRAINT<br />
CONSTRAINT_CATALOG<br />
CONSTRAINT_NAME<br />
CONSTRAINT_SCHEMA<br />
CONSTRAINTS<br />
CONSTRUCTOR<br />
CONTAINS<br />
CONTINUE<br />
CONVERT<br />
CORR<br />
CORRESPONDING<br />
COUNT<br />
COVAR_POP<br />
COVAR_SAMP<br />
CREATE<br />
CROSS<br />
CUBE<br />
CUME_DIST<br />
CURRENT<br />
CURRENT_CATALOG<br />
CURRENT_DATE<br />
CURRENT_DEFAULT_TRANSFORM_GROUP<br />
CURRENT_PATH<br />
CURRENT_ROLE<br />
CURRENT_ROW<br />
CURRENT_SCHEMA<br />
CURRENT_TIME<br />
CURRENT_TIMESTAMP<br />
CURRENT_TRANSFORM_GROUP_FOR_TYPE<br />
CURRENT_USER<br />
CURSOR<br />
CURSOR_NAME<br />
CYCLE<br />
DATA<br />
DATABASE<br />
DATE<br />
DATETIME_INTERVAL_CODE<br />
DATETIME_INTERVAL_PRECISION<br />
DAY<br />
DEALLOCATE<br />
DEC<br />
DECADE<br />
DECIMAL<br />
DECLARE<br />
DEFAULT<br />
DEFAULTS<br />
DEFERRABLE<br />
DEFERRED<br />
DEFINE<br />
DEFINED<br />
DEFINER<br />
DEGREE<br />
DELETE<br />
DENSE_RANK<br />
DEPTH<br />
DEREF<br />
DERIVED<br />
DESC<br />
DESCRIBE<br />
DESCRIPTION<br />
DESCRIPTOR<br />
DETERMINISTIC<br />
DIAGNOSTICS<br />
DISALLOW<br />
DISCONNECT<br />
DISPATCH<br />
DISTINCT<br />
DOMAIN<br />
DOUBLE<br />
DOW<br />
DOY<br />
DROP<br />
DYNAMIC<br />
DYNAMIC_FUNCTION<br />
DYNAMIC_FUNCTION_CODE<br />
EACH<br />
ELEMENT<br />
ELSE<br />
EMPTY<br />
END<br />
END-EXEC<br />
END_FRAME<br />
END_PARTITION<br />
EPOCH<br />
EQUALS<br />
ESCAPE<br />
EVERY<br />
EXCEPT<br />
EXCEPTION<br />
EXCLUDE<br />
EXCLUDING<br />
EXEC<br />
EXECUTE<br />
EXISTS<br />
EXP<br />
EXPLAIN<br />
EXTEND<br />
EXTERNAL<br />
EXTRACT<br />
FALSE<br />
FETCH<br />
FILTER<br />
FINAL<br />
FIRST<br />
FIRST_VALUE<br />
FLOAT<br />
FLOOR<br />
FOLLOWING<br />
FOR<br />
FOREIGN<br />
FORTRAN<br />
FOUND<br />
FRAC_SECOND<br />
FRAME_ROW<br />
FREE<br />
FROM<br />
FULL<br />
FUNCTION<br />
FUSION<br />
G<br />
GENERAL<br />
GENERATED<br />
GEOMETRY<br />
GET<br />
GLOBAL<br />
GO<br />
GOTO<br />
GRANT<br />
GRANTED<br />
GROUP<br />
GROUPING<br />
GROUPS<br />
HAVING<br />
HIERARCHY<br />
HOLD<br />
HOUR<br />
IDENTITY<br />
IF<br />
IMMEDIATE<br />
IMMEDIATELY<br />
IMPLEMENTATION<br />
IMPORT<br />
IN<br />
INCLUDING<br />
INCREMENT<br />
INDICATOR<br />
INITIAL<br />
INITIALLY<br />
INNER<br />
INOUT<br />
INPUT<br />
INSENSITIVE<br />
INSERT<br />
INSTANCE<br />
INSTANTIABLE<br />
INT<br />
INTEGER<br />
INTERSECT<br />
INTERSECTION<br />
INTERVAL<br />
INTO<br />
INVOKER<br />
IS<br />
ISOLATION<br />
JAVA<br />
JOIN<br />
JSON<br />
K<br />
KEY<br />
KEY_MEMBER<br />
KEY_TYPE<br />
LABEL<br />
LAG<br />
LANGUAGE<br />
LARGE<br />
LAST<br />
LAST_VALUE<br />
LATERAL<br />
LEAD<br />
LEADING<br />
LEFT<br />
LENGTH<br />
LEVEL<br />
LIBRARY<br />
LIKE<br />
LIKE_REGEX<br />
LIMIT<br />
LN<br />
LOCAL<br />
LOCALTIME<br />
LOCALTIMESTAMP<br />
LOCATION<br />
LOCATOR<br />
LOWER<br />
M<br />
MAP<br />
MATCH<br />
MATCHED<br />
MATCHES<br />
MATCH_NUMBER<br />
MATCH_RECOGNIZE<br />
MAX<br />
MAXVALUE<br />
MEASURES<br />
MEMBER<br />
MERGE<br />
MESSAGE_LENGTH<br />
MESSAGE_OCTET_LENGTH<br />
MESSAGE_TEXT<br />
METHOD<br />
MICROSECOND<br />
MILLENNIUM<br />
MIN<br />
MINUTE<br />
MINVALUE<br />
MOD<br />
MODIFIES<br />
MODULE<br />
MONTH<br />
MORE<br />
MULTISET<br />
MUMPS<br />
NAME<br />
NAMES<br />
NATIONAL<br />
NATURAL<br />
NCHAR<br />
NCLOB<br />
NESTING<br />
</td>
<td>
NEW<br />
NEXT<br />
NO<br />
NONE<br />
NORMALIZE<br />
NORMALIZED<br />
NOT<br />
NTH_VALUE<br />
NTILE<br />
NULL<br />
NULLABLE<br />
NULLIF<br />
NULLS<br />
NUMBER<br />
NUMERIC<br />
OBJECT<br />
OCCURRENCES_REGEX<br />
OCTET_LENGTH<br />
OCTETS<br />
OF<br />
OFFSET<br />
OLD<br />
OMIT<br />
ON<br />
ONE<br />
ONLY<br />
OPEN<br />
OPTION<br />
OPTIONS<br />
OR<br />
ORDER<br />
ORDERING<br />
ORDINALITY<br />
OTHERS<br />
OUT<br />
OUTER<br />
OUTPUT<br />
OVER<br />
OVERLAPS<br />
OVERLAY<br />
OVERRIDING<br />
PAD<br />
PARAMETER<br />
PARAMETER_MODE<br />
PARAMETER_NAME<br />
PARAMETER_ORDINAL_POSITION<br />
PARAMETER_SPECIFIC_CATALOG<br />
PARAMETER_SPECIFIC_NAME<br />
PARAMETER_SPECIFIC_SCHEMA<br />
PARTIAL<br />
PARTITION<br />
PASCAL<br />
PASSTHROUGH<br />
PAST<br />
PATH<br />
PATTERN<br />
PER<br />
PERCENT<br />
PERCENTILE_CONT<br />
PERCENTILE_DISC<br />
PERCENT_RANK<br />
PERIOD<br />
PERMUTE<br />
PLACING<br />
PLAN<br />
PLI<br />
PORTION<br />
POSITION<br />
POSITION_REGEX<br />
POWER<br />
PRECEDES<br />
PRECEDING<br />
PRECISION<br />
PREPARE<br />
PRESERVE<br />
PREV<br />
PRIMARY<br />
PRIOR<br />
PRIVILEGES<br />
PROCEDURE<br />
PUBLIC<br />
QUARTER<br />
RANGE<br />
RANK<br />
READ<br />
READS<br />
REAL<br />
RECURSIVE<br />
REF<br />
REFERENCES<br />
REFERENCING<br />
REGR_AVGX<br />
REGR_AVGY<br />
REGR_COUNT<br />
REGR_INTERCEPT<br />
REGR_R2<br />
REGR_SLOPE<br />
REGR_SXX<br />
REGR_SXY<br />
REGR_SYY<br />
RELATIVE<br />
RELEASE<br />
REPEATABLE<br />
REPLACE<br />
RESET<br />
RESTART<br />
RESTRICT<br />
RESULT<br />
RETURN<br />
RETURNED_CARDINALITY<br />
RETURNED_LENGTH<br />
RETURNED_OCTET_LENGTH<br />
RETURNED_SQLSTATE<br />
RETURNS<br />
REVOKE<br />
RIGHT<br />
ROLE<br />
ROLLBACK<br />
ROLLUP<br />
ROUTINE<br />
ROUTINE_CATALOG<br />
ROUTINE_NAME<br />
ROUTINE_SCHEMA<br />
ROW<br />
ROW_COUNT<br />
ROW_NUMBER<br />
ROWS<br />
RUNNING<br />
SAVEPOINT<br />
SCALE<br />
SCHEMA<br />
SCHEMA_NAME<br />
SCOPE<br />
SCOPE_CATALOGS<br />
SCOPE_NAME<br />
SCOPE_SCHEMA<br />
SCROLL<br />
SEARCH<br />
SECOND<br />
SECTION<br />
SECURITY<br />
SEEK<br />
SELECT<br />
SELF<br />
SENSITIVE<br />
SEQUENCE<br />
SERIALIZABLE<br />
SERVER<br />
SERVER_NAME<br />
SESSION<br />
SESSION_USER<br />
SET<br />
SETS<br />
MINUS<br />
SHOW<br />
SIMILAR<br />
SIMPLE<br />
SIZE<br />
SKIP<br />
SMALLINT<br />
SOME<br />
SOURCE<br />
SPACE<br />
SPECIFIC<br />
SPECIFIC_NAME<br />
SPECIFICTYPE<br />
SQL<br />
SQLEXCEPTION<br />
SQLSTATE<br />
SQLWARNING<br />
SQL_BIGINT<br />
SQL_BINARY<br />
SQL_BIT<br />
SQL_BLOB<br />
SQL_BOOLEAN<br />
SQL_CHAR<br />
SQL_CLOB<br />
SQL_DATE<br />
SQL_DECIMAL<br />
SQL_DOUBLE<br />
SQL_FLOAT<br />
SQL_INTEGER<br />
SQL_INTERVAL_DAY<br />
SQL_INTERVAL_DAY_TO_HOUR<br />
SQL_INTERVAL_DAY_TO_MINUTE<br />
SQL_INTERVAL_DAY_TO_SECOND<br />
SQL_INTERVAL_HOUR<br />
SQL_INTERVAL_HOUR_TO_MINUTE<br />
SQL_INTERVAL_HOUR_TO_SECOND<br />
SQL_INTERVAL_MINUTE<br />
SQL_INTERVAL_MINUTE_TO_SECOND<br />
SQL_INTERVAL_MONTH<br />
SQL_INTERVAL_SECOND<br />
SQL_INTERVAL_YEAR<br />
SQL_INTERVAL_YEAR_TO_MONTH<br />
SQL_LONGVARBINARY<br />
SQL_LONGVARCHAR<br />
SQL_LONGVARNCHAR<br />
SQL_NCHAR<br />
SQL_NCLOB<br />
SQL_NUMERIC<br />
SQL_NVARCHAR<br />
SQL_REAL<br />
SQL_SMALLINT<br />
SQL_TIME<br />
SQL_TIMESTAMP<br />
SQL_TINYINT<br />
SQL_TSI_DAY<br />
SQL_TSI_FRAC_SECOND<br />
SQL_TSI_HOUR<br />
SQL_TSI_MICROSECOND<br />
SQL_TSI_MINUTE<br />
SQL_TSI_MONTH<br />
SQL_TSI_QUARTER<br />
SQL_TSI_SECOND<br />
SQL_TSI_WEEK<br />
SQL_TSI_YEAR<br />
SQL_VARBINARY<br />
SQL_VARCHAR<br />
SQRT<br />
START<br />
STATE<br />
STATEMENT<br />
STATIC<br />
STDDEV_POP<br />
STDDEV_SAMP<br />
STREAM<br />
STRUCTURE<br />
STYLE<br />
SUBCLASS_ORIGIN<br />
SUBMULTISET<br />
SUBSET<br />
SUBSTITUTE<br />
SUBSTRING<br />
SUBSTRING_REGEX<br />
SUCCEEDS<br />
SUM<br />
SYMMETRIC<br />
SYSTEM<br />
SYSTEM_TIME<br />
SYSTEM_USER<br />
TABLE<br />
TABLE_NAME<br />
TABLESAMPLE<br />
TBLPROPERTIES<br />
TEMPORARY<br />
THEN<br />
TIES<br />
TIME<br />
TIMESTAMP<br />
TIMESTAMPADD<br />
TIMESTAMPDIFF<br />
TIMEZONE_HOUR<br />
TIMEZONE_MINUTE<br />
TINYINT<br />
TO<br />
TOP_LEVEL_COUNT<br />
TRAILING<br />
TRANSACTION<br />
TRANSACTIONS_ACTIVE<br />
TRANSACTIONS_COMMITTED<br />
TRANSACTIONS_ROLLED_BACK<br />
TRANSFORM<br />
TRANSFORMS<br />
TRANSLATE<br />
TRANSLATE_REGEX<br />
TRANSLATION<br />
TREAT<br />
TRIGGER<br />
TRIGGER_CATALOG<br />
TRIGGER_NAME<br />
TRIGGER_SCHEMA<br />
TRIM<br />
TRIM_ARRAY<br />
TRUE<br />
TRUNCATE<br />
TYPE<br />
UESCAPE<br />
UNBOUNDED<br />
UNCOMMITTED<br />
UNDER<br />
UNION<br />
UNIQUE<br />
UNKNOWN<br />
UNNAMED<br />
UNNEST<br />
UPDATE<br />
UPPER<br />
UPSERT<br />
USAGE<br />
USER<br />
USER_DEFINED_TYPE_CATALOG<br />
USER_DEFINED_TYPE_CODE<br />
USER_DEFINED_TYPE_NAME<br />
USER_DEFINED_TYPE_SCHEMA<br />
USING<br />
VALUE<br />
VALUES<br />
VALUE_OF<br />
VAR_POP<br />
VAR_SAMP<br />
VARBINARY<br />
VARCHAR<br />
VARYING<br />
VERSION<br />
VERSIONING<br />
VIEW<br />
WEEK<br />
WHEN<br />
WHENEVER<br />
WHERE<br />
WIDTH_BUCKET<br />
WINDOW<br />
WITH<br />
WITHIN<br />
WITHOUT<br />
WORK<br />
WRAPPER<br />
WRITE<br />
XML<br />
YEAR<br />
ZONE<br />
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
> Attribution License](https://creativecommons.org/licenses/by/3.0/).
