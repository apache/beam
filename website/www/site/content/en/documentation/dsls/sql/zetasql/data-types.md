---
type: languages
title: "Beam ZetaSQL data types"
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

<p>
Beam ZetaSQL supports standard SQL scalar data types as well as extensions including arrays, maps, and nested rows. This page documents the ZetaSQL data types supported in Beam ZetaSQL.
</p>

## Data type properties

<p>The following table contains data type properties and the data types that
each property applies to:</p>

{{< table >}}
<table>
<thead>
<tr>
<th>Property</th>
<th>Description</th>
<th>Applies To</th>
</tr>
</thead>
<tbody>
<tr>
<td>Nullable</td>
<td nowrap=""><code>NULL</code> is a valid value.</td>
<td>
All data types, with the following exceptions:
<ul>
<li>ARRAYs cannot be <code>NULL</code>.</li>
<li><code>NULL ARRAY</code> elements cannot persist to a table.</li>
<li>Queries cannot handle <code>NULL ARRAY</code> elements.</li>
</ul>
</td>
</tr>
<tr>
<td>Orderable</td>
<td nowrap="">Can be used in an <code>ORDER BY</code> clause.</td>
<td>All data types except for:
<ul>
<li>ARRAY</li>
<li>STRUCT</li>
</ul></td>
</tr>
<tr>
<td>Groupable</td>
<td nowrap="">Can generally appear in an expression following<br>
<code>GROUP BY</code>, <code>DISTINCT</code>, or <code>PARTITION BY</code>.<br>
  However, <code>PARTITION BY</code> expressions cannot include<br>
  the floating point types <code>FLOAT</code> and <code>DOUBLE</code>.</br></br></br></td>
<td>All data types except for:<ul>
<li>ARRAY</li>
<li>STRUCT</li>
<li>FLOAT64</li>
</ul>
</td>
</tr>
<tr>
<td>Comparable</td>
<td>Values of the same type can be compared to each other.</td>
<td>All data types, with the following exceptions:

ARRAY comparisons are not supported.

<br/><br/>
Equality comparisons for STRUCTs are supported field by field, in field order.
Field names are ignored. Less than and greater than comparisons are not
supported.

<br/><br/>
<br/><br/>
All types that support comparisons
can be used in a <code>JOIN</code> condition. See
<a href="/documentation/dsls/sql/zetasql/query-syntax#join_types">JOIN
Types</a> for an explanation of join conditions.
</td>
</tr>
</tbody>
</table>
{{< /table >}}

## Numeric types

<p>Numeric types include integer types and floating point types.</p>

### Integer type

<p>Integers are numeric values that do not have fractional components.</p>

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Storage Size</th>
<th>Range</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>INT64</code></td>
<td>8 bytes</td>
<td>-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807</td>
</tr>
</tbody>
</table>
{{< /table >}}

### Floating point type

<p>Floating point values are approximate numeric values with fractional components.</p>

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Storage Size</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>FLOAT64</code></td>
<td>8 bytes</td>
<td>Double precision (approximate) decimal values.</td>
</tr>
</tbody>
</table>
{{< /table >}}

## Boolean type

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>BOOL</code></td>
<td>Boolean values are represented by the keywords <code>TRUE</code> and
<code>FALSE</code> (case insensitive).</td>
</tr>
</tbody>
</table>
{{< /table >}}

## String type

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRING</code></td>
<td>Variable-length character (Unicode) data.</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>Input STRING values must be UTF-8 encoded and output STRING values will be UTF-8
encoded. Alternate encodings like CESU-8 and Modified UTF-8 are not treated as
valid UTF-8.</p>
<p>All functions and operators that act on STRING values operate on Unicode
characters rather than bytes. For example, when functions like <code>SUBSTR</code> and <code>LENGTH</code>
are applied to STRING input, the functions count Unicode characters, not bytes. Comparisons are
defined on Unicode characters. Comparisons for less than and <code>ORDER BY</code> compare
character by character, and lower unicode code points are considered lower
characters.</p>

## Bytes type

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>BYTES</code></td>
<td>Variable-length binary data.</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>STRING and BYTES are separate types that cannot be used interchangeably. Casts between STRING and BYTES enforce
that the bytes are encoded using UTF-8.</p>

## Timestamp type

Caution: SQL has millisecond `TIMESTAMP` precision. If a
`TIMESTAMP` field has sub-millisecond precision, SQL
throws an `IllegalArgumentException`.

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
<th>Range</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>TIMESTAMP</code></td>
<td>Represents an absolute point in time, with
 millisecond
precision.</td>
<td>0001-01-01 00:00:00 to 9999-12-31 23:59:59.999 UTC.</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>A timestamp represents an absolute point in time, independent of any time zone
or convention such as Daylight Savings Time.</p>

### Canonical format
<pre class="codehilite"><code>YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDD]][time zone]</code></pre>
<ul>
<li><code>YYYY</code>: Four-digit year</li>
<li><code>[M]M</code>: One or two digit month</li>
<li><code>[D]D</code>: One or two digit day</li>
<li><code>( |T)</code>: A space or a <code>T</code> separator</li>
<li><code>[H]H</code>: One or two digit hour (valid values from 00 to 23)</li>
<li><code>[M]M</code>: One or two digit minutes (valid values from 00 to 59)</li>
<li><code>[S]S</code>: One or two digit seconds (valid values from 00 to 59)</li>
<li><code>[.DDD]</code>: Up to three fractional digits (i.e. up to millisecond precision)</li>
<li><code>[time zone]</code>: String representing the time zone. See the <a href="#time-zones">time zones</a>
  section for details.</li>
</ul>
<p>Time zones are used when parsing timestamps or formatting timestamps for display.
The timestamp value itself does not store a specific time zone.  A
string-formatted timestamp may include a time zone.  When a time zone is not
explicitly specified, the default time zone, UTC, is used.</p>

### Time zones

<p>Time zones are represented by strings in one of these two canonical formats:</p>
<ul>
<li>Offset from Coordinated Universal Time (UTC), or the letter <code>Z</code> for UTC</li>
<li>Time zone name from the <a href="https://www.iana.org/time-zones">tz database</a></li>
</ul>
<h4 id="offset-from-coordinated-universal-time-utc">Offset from Coordinated Universal Time (UTC)</h4>
<h5 id="offset-format">Offset Format</h5>
<pre class="codehilite"><code>(+|-)H[H][:M[M]]
Z</code></pre>
<h5 id="examples">Examples</h5>
<pre class="codehilite"><code>-08:00
-8:15
+3:00
+07:30
-7
Z</code></pre>
<p>When using this format, no space is allowed between the time zone and the rest
of the timestamp.</p>
<pre class="codehilite"><code>2014-09-27 12:30:00.45-8:00
2014-09-27T12:30:00.45Z</code></pre>
<h4 id="time-zone-name">Time zone name</h4>
<p>Time zone names are from the <a href="https://www.iana.org/time-zones">tz database</a>. For a
less comprehensive but simpler reference, see the
<a href="https://en.wikipedia.org/wiki/List_of_tz_database_time_zones">List of tz database time zones</a>
on Wikipedia.</p>
<h5 id="format">Format</h5>
<pre class="codehilite"><code>continent/[region/]city</code></pre>
<h5 id="examples_1">Examples</h5>
<pre class="codehilite"><code>America/Los_Angeles
America/Argentina/Buenos_Aires</code></pre>
<p>When using a time zone name, a space is required between the name and the rest
of the timestamp:</p>
<pre class="codehilite"><code>2014-09-27 12:30:00.45 America/Los_Angeles</code></pre>
<p>Note that not all time zone names are interchangeable even if they do happen to
report the same time during a given part of the year. For example,
<code>America/Los_Angeles</code> reports the same time as <code>UTC-7:00</code> during Daylight
Savings Time, but reports the same time as <code>UTC-8:00</code> outside of Daylight
Savings Time.</p>
<p>If a time zone is not specified, the default time zone value is used.</p>
<h4 id="leap-seconds">Leap seconds</h4>
<p>A timestamp is simply an offset from 1970-01-01 00:00:00 UTC, assuming there are
exactly 60 seconds per minute. Leap seconds are not represented as part of a
stored timestamp.</p>
<p>If your input contains values that use ":60" in the seconds field to represent a
leap second, that leap second is not preserved when converting to a timestamp
value. Instead that value is interpreted as a timestamp with ":00" in the
seconds field of the following minute.</p>
<p>Leap seconds do not affect timestamp computations. All timestamp computations
are done using Unix-style timestamps, which do not reflect leap seconds. Leap
seconds are only observable through functions that measure real-world time. In
these functions, it is possible for a timestamp second to be skipped or repeated
when there is a leap second.</p>

## Array type

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ARRAY</code></td>
<td>Ordered list of zero or more elements of any non-ARRAY type.</td>
</tr>
</tbody>
</table>
{{< /table >}}

<p>An ARRAY is an ordered list of zero or more elements of non-ARRAY values.
ARRAYs of ARRAYs are not allowed. Queries that would produce an ARRAY of
ARRAYs will return an error. Instead a STRUCT must be inserted between the
ARRAYs using the <code>SELECT AS STRUCT</code> construct.</p>
<p>An empty ARRAY and a <code>NULL</code> ARRAY are two distinct values. ARRAYs can contain
<code>NULL</code> elements.</p>

### Declaring an ARRAY type

<p>ARRAY types are declared using the angle brackets (<code>&lt;</code> and <code>&gt;</code>). The type
of the elements of an ARRAY can be arbitrarily complex with the exception that
an ARRAY cannot directly contain another ARRAY.</p>
<h4 id="format_1">Format</h4>
<pre class="codehilite"><code>ARRAY&lt;T&gt;</code></pre>
<h4 id="examples_2">Examples</h4>

{{< table >}}
<table>
<thead>
<tr>
<th>Type Declaration</th>
<th>Meaning</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>
ARRAY&lt;INT64&gt;
</code>
</td>
<td>Simple ARRAY of 64-bit integers.</td>
</tr>
<tr>
<td nowrap="">
<code>
ARRAY&lt;STRUCT&lt;INT64, INT64&gt;&gt;
</code>
</td>
<td>An ARRAY of STRUCTs, each of which contains two 64-bit integers.</td>
</tr>
<tr>
<td nowrap="">
<code>
ARRAY&lt;ARRAY&lt;INT64&gt;&gt;
</code><br/>
(not supported)
</td>
<td>This is an <strong>invalid</strong> type declaration which is included here
just in case you came looking for how to create a multi-level ARRAY. ARRAYs
cannot contain ARRAYs directly. Instead see the next example.</td>
</tr>
<tr>
<td nowrap="">
<code>
ARRAY&lt;STRUCT&lt;ARRAY&lt;INT64&gt;&gt;&gt;
</code>
</td>
<td>An ARRAY of ARRAYS of 64-bit integers. Notice that there is a STRUCT between
the two ARRAYs because ARRAYs cannot hold other ARRAYs directly.</td>
</tr>
</tbody></table>
{{< /table >}}

## Struct type

{{< table >}}
<table>
<thead>
<tr>
<th>Name</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>STRUCT</code></td>
<td>Container of ordered fields each with a type (required) and field name
(optional).</td>
</tr>
</tbody>
</table>
{{< /table >}}

### Declaring a STRUCT type

<p>STRUCT types are declared using the angle brackets (<code>&lt;</code> and <code>&gt;</code>). The type of
the elements of a STRUCT can be arbitrarily complex.</p>
<h4 id="format_2">Format</h4>
<pre class="codehilite"><code>STRUCT&lt;T&gt;</code></pre>
<h4 id="examples_3">Examples</h4>

{{< table >}}
<table>
<thead>
<tr>
<th>Type Declaration</th>
<th>Meaning</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>
STRUCT&lt;INT64&gt;
</code>
</td>
<td>Simple STRUCT with a single unnamed 64-bit integer field.</td>
</tr>
<tr>
<td nowrap="">
<code>
STRUCT&lt;x STRUCT&lt;y INT64, z INT64&gt;&gt;
</code>
</td>
<td>A STRUCT with a nested STRUCT named <code>x</code> inside it. The STRUCT
<code>x</code> has two fields, <code>y</code> and <code>z</code>, both of which
are 64-bit integers.</td>
</tr>
<tr>
<td nowrap="">
<code>
STRUCT&lt;inner_array ARRAY&lt;INT64&gt;&gt;
</code>
</td>
<td>A STRUCT containing an ARRAY named <code>inner_array</code> that holds
64-bit integer elements.</td>
</tr>
</tbody></table>
{{< /table >}}

### Limited comparisons for STRUCT
<p>STRUCTs can be directly compared using equality operators:</p>
<ul>
<li>Equal (<code>=</code>)</li>
<li>Not Equal (<code>!=</code> or <code>&lt;&gt;</code>)</li>
<li>[<code>NOT</code>] <code>IN</code></li>
</ul>
<p>Notice, though, that these direct equality comparisons compare the fields of
the STRUCT pairwise in ordinal order ignoring any field names. If instead you
want to compare identically named fields of a STRUCT, you can compare the
individual fields directly.</p>