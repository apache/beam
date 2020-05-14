---
type: languages
title: "Beam ZetaSQL lexical structure"
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

# Beam ZetaSQL lexical structure

<p>Beam ZetaSQL statements are comprised of a series of tokens. Tokens include
<em>identifiers,</em> <em>quoted identifiers, literals</em>, <em>keywords</em>, <em>operators</em>, and
<em>special characters</em>. Tokens can be separated by whitespace (space, backspace,
tab, newline) or comments.</p>

<p><a id="identifiers"></a></p>

## Identifiers

<p>Identifiers are names that are associated with columns, tables, and other
database objects.</p>

<p>There are two ways to specify an identifier: unquoted or quoted:</p>

<ul>
  <li>Unquoted identifiers must begin with a letter or an underscore.
      Subsequent characters can be letters, numbers, or underscores.</li>
  <li>Quoted identifiers are enclosed by backtick (`) characters and can
      contain any character, such as spaces or symbols. However, quoted identifiers
      cannot be empty. <a href="#reserved_keywords">Reserved Keywords</a> can only be used as
      identifiers if enclosed by backticks.</li>
</ul>

Syntax (presented as a grammar with regular expressions, ignoring whitespace):

<pre>
<span class="var">identifier</span>: { quoted_identifier | unquoted_identifier }
<span class="var">unquoted_identifier</span>: <code>[A-Za-z_][A-Za-z_0-9]*</code>
<span class="var">quoted_identifier</span>: <code>\`[^\\\`\r\n]</code> <span class="var">any_escape*</span> <code>\`</code>
<span class="var">any_escape</span>: <code>\\(. | \n | \r | \r\n)</code>
</pre>

<p>Examples:</p>

<pre class="codehilite"><code>Customers5
_dataField1
ADGROUP</code></pre>

<p>Invalid examples:</p>

<pre class="codehilite"><code>5Customers
_dataField!
GROUP</code></pre>

<p><code>5Customers</code> begins with a number, not a letter or underscore. <code>_dataField!</code>
contains the special character "!" which is not a letter, number, or underscore.
<code>GROUP</code> is a reserved keyword, and therefore cannot be used as an identifier
without being enclosed by backtick characters.</p>

<p>Both identifiers and quoted identifiers are case insensitive, with some
nuances. See <a href="#case_sensitivity">Case Sensitivity</a> for further details.</p>

<p>Quoted identifiers have the same escape sequences as string literals,
defined below.</p>

<p><a id="literals"></a></p>

## Literals

<p>A literal represents a constant value of a built-in data type. Some, but not
all, data types can be expressed as literals.</p>

<p><a id="string_and_bytes_literals"></a></p>

### String and Bytes Literals

<p>Both string and bytes literals must be <em>quoted</em>, either with single (<code>'</code>) or
double (<code>"</code>) quotation marks, or <em>triple-quoted</em> with groups of three single
(<code>'''</code>) or three double (<code>"""</code>) quotation marks.</p>

<p><strong>Quoted literals:</strong></p>

{{< table >}}
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
<td><ul><li><code>"abc"</code></li><li><code>"it's"</code></li><li><code>'it\'s'</code></li><li><code>'Title: "Boy"'</code></li></ul></td>
<td>Quoted strings enclosed by single (<code>'</code>) quotes can contain unescaped double (<code>"</code>) quotes, and vice versa. <br>Backslashes (<code>\</code>) introduce escape sequences. See Escape Sequences table below.<br>Quoted strings cannot contain newlines, even when preceded by a backslash (<code>\</code>).</br></br></td>
</tr>
<tr>
<td>Triple-quoted string</td>
<td><ul><li><code>"""abc"""</code></li><li><code>'''it's'''</code></li><li><code>'''Title:"Boy"'''</code></li><li><code>'''two<br>lines'''</br></code></li><li><code>'''why\?'''</code></li></ul></td>
<td>Embedded newlines and quotes are allowed without escaping - see fourth example.<br>Backslashes (<code>\</code>) introduce escape sequences. See Escape Sequences table below.<br>A trailing unescaped backslash (<code>\</code>) at the end of a line is not allowed.<br>Three unescaped quotes in a row which match the starting quotes will end the string.</br></br></br></td>
</tr>
<tr>
<td>Raw string</td>
<td><ul><li><code>R"abc+"</code></li><li> <code>r'''abc+'''</code></li><li> <code>R"""abc+"""</code></li><li><code>r'f\(abc,(.*),def\)'</code></li></ul></td>
<td>Quoted or triple-quoted literals that have the raw string literal prefix (<code>r</code> or <code>R</code>) are interpreted as raw/regex strings.<br>Backslash characters (<code>\</code>) do not act as escape characters. If a backslash followed by another character occurs inside the string literal, both characters are preserved.<br>A raw string cannot end with an odd number of backslashes.<br>Raw strings are useful for constructing regular expressions.</br></br></br></td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>Prefix characters (<code>r</code>, <code>R</code>, <code>b</code>, <code>B)</code> are optional for quoted or triple-quoted strings, and indicate that the string is a raw/regex string or a byte sequence, respectively. For
example, <code>b'abc'</code> and <code>b'''abc'''</code> are both interpreted as type bytes. Prefix characters are case insensitive.</p>

<p><strong>Quoted literals with prefixes:</strong></p>

<p>The table below lists all valid escape sequences for representing non-alphanumeric characters in string literals.
Any sequence not in this table produces an error.</p>

{{< table >}}
<table>
<thead>
<tr>
<th>Escape Sequence</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>\a</code></td>
<td>Bell</td>
</tr>
<tr>
<td><code>\b</code></td>
<td>Backspace</td>
</tr>
<tr>
<td><code>\f</code></td>
<td>Formfeed</td>
</tr>
<tr>
<td><code>\n</code></td>
<td>Newline</td>
</tr>
<tr>
<td><code>\r</code></td>
<td>Carriage Return</td>
</tr>
<tr>
<td><code>\t</code></td>
<td>Tab</td>
</tr>
<tr>
<td><code>\v</code></td>
<td>Vertical Tab</td>
</tr>
<tr>
<td><code>\\</code></td>
<td>Backslash (<code>\</code>)</td>
</tr>
<tr>
<td><code>\?</code></td>
<td>Question Mark (<code>?</code>)</td>
</tr>
<tr>
<td><code>\"</code></td>
<td>Double Quote (<code>"</code>)</td>
</tr>
<tr>
<td><code>\'</code></td>
<td>Single Quote (<code>'</code>)</td>
</tr>
<tr>
<td><code>\`</code></td>
<td>Backtick (<code>`</code>)</td>
</tr>
<tr>
<td><code>\ooo</code></td>
<td>Octal escape, with exactly three digits (in the range 0-7). Decodes to a single Unicode character (in string literals).</td>
</tr>
<tr>
<td><code>\xhh</code> or <code>\Xhh</code></td>
<td>Hex escape, with exactly two hex digits (0-9 or A-F or a-f). Decodes to a single Unicode character (in string literals). Examples:<ul style="list-style-type:none"><li><code>'\x41'</code> == <code>'A'</code></li><li><code>'\x41B'</code> is <code>'AB'</code></li><li><code>'\x4'</code> is an error</li></ul></td>
</tr>
<tr>
<td><code>\uhhhh</code></td>
<td>Unicode escape, with lowercase 'u' and exactly four hex digits. Valid only in string literals or identifiers.<br/>Note that the range D800-DFFF is not allowed, as these are surrogate unicode values.</td>
</tr>
<tr>
<td><code>\Uhhhhhhhh</code></td>
<td>Unicode escape, with uppercase 'U' and exactly eight hex digits. Valid only in string literals or identifiers.<br/>Note that the range D800-DFFF is not allowed, as these are surrogate unicode values. Also, values greater than 10FFFF are not allowed.</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p><a id="integer_literals"></a></p>

### Integer Literals

<p>Integer literals are either a sequence of decimal digits (0 through
9) or a hexadecimal value that is prefixed with "<code>0x</code>". Integers can be prefixed
by "<code>+</code>" or "<code>-</code>" to represent positive and negative values, respectively.</p>

<p>Examples:</p>

<pre class="codehilite"><code>123
0xABC
-123</code></pre>

<p>An integer literal is interpreted as an <code>INT64</code>.</p>

<p><a id="floating_point_literals"></a></p>

### Floating Point Literals

<p>Syntax options:</p>

<pre class="codehilite"><code>[+-]DIGITS.[DIGITS][e[+-]DIGITS]
[DIGITS].DIGITS[e[+-]DIGITS]
DIGITSe[+-]DIGITS</code></pre>

<p><code>DIGITS</code> represents one or more decimal numbers (0 through 9) and <code>e</code> represents the exponent marker (e or E).</p>

<p>Examples:</p>

<pre class="codehilite"><code>123.456e-67
.1E4
58.
4e2</code></pre>

<p>Numeric literals that contain
either a decimal point or an exponent marker are presumed to be type double.</p>

<p>Implicit coercion of floating point literals to float type is possible if the
value is within the valid float range.</p>

<p>There is no literal
representation of NaN or infinity.</p>

<p><a id="array_literals"></a></p>

### Array Literals

<p>Array literals are a comma-separated lists of elements
enclosed in square brackets. The <code>ARRAY</code> keyword is optional, and an explicit
element type T is also optional.</p>

<p>Examples:</p>

<pre class="codehilite"><code>[1, 2, 3]
['x', 'y', 'xy']
ARRAY[1, 2, 3]
ARRAY&lt;string&gt;['x', 'y', 'xy']
</code></pre>

### Timestamp literals

<p>Syntax:</p>

<pre class="codehilite"><code>TIMESTAMP 'YYYY-[M]M-[D]D [[H]H:[M]M:[S]S[.DDDDDD]] [timezone]'</code></pre>

<p>Timestamp literals contain the <code>TIMESTAMP</code> keyword and a string literal that
conforms to the canonical timestamp format, enclosed in single quotation marks.</p>

<p>Timestamp literals support a range between the years 1 and 9999, inclusive.
Timestamps outside of this range are invalid.</p>

<p>A timestamp literal can include a numerical suffix to indicate the timezone:</p>

<pre class="codehilite"><code>TIMESTAMP '2014-09-27 12:30:00.45-08'</code></pre>

<p>If this suffix is absent, the default timezone, UTC, is used.</p>

<p>For example, the following timestamp represents 12:30 p.m. on September 27,
2014, using the timezone, UTC:</p>

<pre class="codehilite"><code>TIMESTAMP '2014-09-27 12:30:00.45'</code></pre>

<p>For more information on timezones, see <a href="#timezone">Timezone</a>.</p>

<p>String literals with the canonical timestamp format, including those with
timezone names, implicitly coerce to a timestamp literal when used where a
timestamp expression is expected.  For example, in the following query, the
string literal <code>"2014-09-27 12:30:00.45 America/Los_Angeles"</code> is coerced
to a timestamp literal.</p>

<pre class="codehilite"><code>SELECT * FROM foo
WHERE timestamp_col = "2014-09-27 12:30:00.45 America/Los_Angeles"</code></pre>

<h4 id="timezone">Timezone</h4>

<p>Since timestamp literals must be mapped to a specific point in time, a timezone
is necessary to correctly interpret a literal. If a timezone is not specified
as part of the literal itself, then the default timezone value, which is set by
the Beam SQL implementation, is used.</p>

<p>Timezones are represented by strings in the following canonical format, which
represents the offset from Coordinated Universal Time (UTC).</p>

<p>Format:</p>

<pre class="codehilite"><code>(+|-)H[H][:M[M]]</code></pre>

<p>Examples:</p>

<pre class="codehilite"><code>'-08:00'
'-8:15'
'+3:00'
'+07:30'
'-7'</code></pre>

<p>Timezones can also be expressed using string timezone names from the
<a href="https://www.iana.org/time-zones">tz database</a>. For a less comprehensive but
simpler reference, see the
<a href="https://en.wikipedia.org/wiki/List_of_tz_database_time_zones">List of tz database timezones</a>
on Wikipedia. Canonical timezone names have the format
<code>&lt;continent/[region/]city&gt;</code>, such as <code>America/Los_Angeles</code>.</p>

<p>Note that not all timezone names are interchangeable even if they do happen to
report the same time during a given part of the year. For example, <code>America/Los_Angeles</code> reports the same time as <code>UTC-7:00</code> during Daylight Savings Time, but reports the same time as <code>UTC-8:00</code> outside of Daylight Savings Time.</p>

<p>Example:</p>

<pre class="codehilite"><code>TIMESTAMP '2014-09-27 12:30:00 America/Los_Angeles'
TIMESTAMP '2014-09-27 12:30:00 America/Argentina/Buenos_Aires'</code></pre>

<p><a id="case_sensitivity"></a></p>

## Case Sensitivity

<p>Beam SQL follows these rules for case sensitivity:</p>

{{< table >}}
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
<td> </td>
</tr>
<tr>
<td>Function names</td>
<td>No</td>
<td> </td>
</tr>
<tr>
<td>Table names</td>
<td>See Notes</td>
<td>Table names are usually case insensitive, but may be case sensitive when querying a database that uses case sensitive table names.</td>
</tr>
<tr>
<td>Column names</td>
<td>No</td>
<td> </td>
</tr>
<tr>
<td>String values</td>
<td>Yes</td>
<td></td>
</tr>
<tr>
<td>String comparisons</td>
<td>Yes</td>
<td> </td>
</tr>
<tr>
<td>Aliases within a query</td>
<td>No</td>
<td> </td>
</tr>
<tr>
<td>Regular expression matching</td>
<td>See Notes</td>
<td>Regular expression matching is case sensitive by default, unless the expression itself specifies that it should be case insensitive.</td>
</tr>
<tr>
<td><code>LIKE</code> matching</td>
<td>Yes</td>
<td> </td>
</tr>
</tbody>
</table>
{{< /table >}}

<p><a id="reserved_keywords"></a></p>

## Reserved Keywords

<p>Keywords are a group of tokens that have special meaning in the Beam SQL
language, and  have the following characteristics:</p>

<ul>
<li>Keywords cannot be used as identifiers unless enclosed by backtick (`) characters.</li>
<li>Keywords are case insensitive.</li>
</ul>

<p>Beam SQL has the following reserved keywords.</p>

{{< table >}}
<table style="table-layout: fixed; width: 110%">
<tbody>
<tr>
<td>
ALL<br/>
AND<br/>
ANY<br/>
ARRAY<br/>
AS<br/>
ASC<br/>
ASSERT_ROWS_MODIFIED<br/>
AT<br/>
BETWEEN<br/>
BY<br/>
CASE<br/>
CAST<br/>
COLLATE<br/>
CONTAINS<br/>
CREATE<br/>
CROSS<br/>
CUBE<br/>
CURRENT<br/>
DEFAULT<br/>
DEFINE<br/>
DESC<br/>
DISTINCT<br/>
ELSE<br/>
END<br/>
</td>
<td>
ENUM<br/>
ESCAPE<br/>
EXCEPT<br/>
EXCLUDE<br/>
EXISTS<br/>
EXTRACT<br/>
FALSE<br/>
FETCH<br/>
FOLLOWING<br/>
FOR<br/>
FROM<br/>
FULL<br/>
GROUP<br/>
GROUPING<br/>
GROUPS<br/>
HASH<br/>
HAVING<br/>
IF<br/>
IGNORE<br/>
IN<br/>
INNER<br/>
INTERSECT<br/>
INTERVAL<br/>
INTO<br/>
</td>
<td>
IS<br/>
JOIN<br/>
LATERAL<br/>
LEFT<br/>
LIKE<br/>
LIMIT<br/>
LOOKUP<br/>
MERGE<br/>
NATURAL<br/>
NEW<br/>
NO<br/>
NOT<br/>
NULL<br/>
NULLS<br/>
OF<br/>
ON<br/>
OR<br/>
ORDER<br/>
OUTER<br/>
OVER<br/>
PARTITION<br/>
PRECEDING<br/>
PROTO<br/>
RANGE<br/>
</td>
<td>
RECURSIVE<br/>
RESPECT<br/>
RIGHT<br/>
ROLLUP<br/>
ROWS<br/>
SELECT<br/>
SET<br/>
SOME<br/>
STRUCT<br/>
TABLESAMPLE<br/>
THEN<br/>
TO<br/>
TREAT<br/>
TRUE<br/>
UNBOUNDED<br/>
UNION<br/>
UNNEST<br/>
USING<br/>
WHEN<br/>
WHERE<br/>
WINDOW<br/>
WITH<br/>
WITHIN<br/>
</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p><a id="terminating_semicolons"></a></p>

## Terminating Semicolons

<p>Statements can optionally use a terminating semicolon (<code>;</code>) in the context of a query string submitted through an Application Programming Interface (API). Some interactive tools require statements to have a terminating semicolon.
In a request containing multiple statements, statements must be separated by semicolons, but the semicolon is optional for the final statement.</p>

## Comments

<p>Comments are sequences of characters that are ignored by the parser. Beam SQL
supports the following types of comments.</p>

### Single line comments

<p>Single line comments are supported by prepending <code>#</code> or <code>--</code> before the
comment.</p>

<p><strong>Examples</strong></p>

<p><code>SELECT x FROM T; # x is a field and T is a table</code></p>

<p>Comment includes all characters from the '#' character to the end of the line.</p>

<p><code>SELECT x FROM T; --x is a field and T is a table</code></p>

<p>Comment includes all characters from the '<code>--</code>' sequence to the end of the line. You can optionally add a space after the '<code>--</code>'.</p>

### Multiline comments

<p>Multiline comments are supported by enclosing the comment using <code>/* &lt;comment&gt; */</code>.</p>

<p><strong>Example:</strong></p>

<pre class="codehilite"><code>SELECT x FROM T /* x is a field and T is a table */
WHERE x = 3;</code></pre>

<p><strong>Invalid example:</strong></p>

<pre class="codehilite"><code>SELECT x FROM T /* comment starts here
                /* comment ends on this line */
                this line is not considered a comment */
WHERE x = 3;</code></pre>

<p>Comment includes all characters, including newlines, enclosed by the first
occurrence of '<code>/*</code>' and the first subsequent occurrence of '<code>*/</code>'. Nested
comments are not supported. The second example contains a nested comment that
renders the query invalid.</p>